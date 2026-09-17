"""Tests for the asynchronous job API, SQLite persistence and the worker."""

from __future__ import annotations

import sqlite3
import threading
import time

import pytest
from fastapi.testclient import TestClient

from app.jobs import CANCELLED, QUEUED, RUNNING, SUCCEEDED, JobStore, JobWorker
from app.main import app


# An unsatisfiable dependency chain forces heavy backtracking
# (~6.4k candidate evaluations, roughly a second on a normal machine),
# long enough to observe a running job and cancel it.
SLOW_N, SLOW_M = 7, 8


def slow_payload() -> dict:
    packages = {}
    for i in range(SLOW_N):
        packages[f"p{i}"] = {
            f"{v}.0": {"dependencies": {f"p{i+1}": f">={v}"}}
            for v in range(1, SLOW_M + 1)
        }
    packages[f"p{SLOW_N}"] = {}
    return {"packages": packages, "root": {"p0": ""}}


SIMPLE = {"packages": {"x": {"1.0": {}, "2.0": {}}}, "root": {"x": ""}}


@pytest.fixture
def client(tmp_path, monkeypatch):
    monkeypatch.setenv("RESOLVER_DB", str(tmp_path / "jobs.db"))
    with TestClient(app) as c:
        yield c


def wait_for(client, job_id, *, timeout=10.0):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        body = client.get(f"/jobs/{job_id}").json()
        if body["status"] in ("succeeded", "failed", "cancelled"):
            return body
        time.sleep(0.02)
    raise AssertionError(f"job {job_id} never finished: {body}")


def wait_until_status(client, job_id, status, *, timeout=5.0):
    deadline = time.monotonic() + timeout
    body = {}
    while time.monotonic() < deadline:
        body = client.get(f"/jobs/{job_id}").json()
        if body["status"] == status:
            return body
        time.sleep(0.01)
    raise AssertionError(f"job {job_id} never reached {status!r}: {body}")


def test_post_job_returns_202_with_location_and_eventually_succeeds(client):
    r = client.post("/jobs", json=SIMPLE)
    assert r.status_code == 202
    assert r.headers["location"] == f"/jobs/{r.json()['id']}"
    job = wait_for(client, r.json()["id"])
    assert job["status"] == "succeeded"
    assert job["response_status"] == 200
    assert job["response"]["resolved"] == {"x": "2.0"}
    assert job["attempts"] == 1
    assert job["finished_at"] is not None


def test_job_preserves_conflict_response_status_and_body(client):
    payload = {
        "packages": {
            "a": {"1.0": {"dependencies": {"x": ">=2.0"}}},
            "b": {"1.0": {"dependencies": {"x": "<2.0"}}},
            "x": {"1.0": {}, "2.0": {}},
        },
        "root": ["a", "b"],
    }
    jid = client.post("/jobs", json=payload).json()["id"]
    job = wait_for(client, jid)
    assert job["status"] == "succeeded"
    assert job["response_status"] == 409
    assert job["response"]["status"] == "conflict"


def test_invalid_input_rejected_before_queueing(client):
    r = client.post("/jobs", json={"packages": {"x": {"bad-version": {}}}, "root": {"x": ""}})
    assert r.status_code == 400
    assert r.json()["error"]["code"] == "invalid_request"

    # Extra fields are rejected by pydantic the same way as on /resolve.
    r = client.post("/jobs", json={**SIMPLE, "unexpected": 1})
    assert r.status_code == 422


def test_get_missing_job_and_delete_missing_job_are_404(client):
    assert client.get("/jobs/does-not-exist").status_code == 404
    assert client.delete("/jobs/does-not-exist").status_code == 404


def test_cancel_running_job_stops_at_candidate_boundary(client):
    jid = client.post("/jobs", json=slow_payload()).json()["id"]

    # Wait until the worker has claimed it.
    assert wait_until_status(client, jid, RUNNING)["status"] == RUNNING

    r = client.delete(f"/jobs/{jid}")
    assert r.status_code == 202
    assert r.json()["status"] == RUNNING
    assert r.json()["cancel_requested"] is True

    job = wait_for(client, jid, timeout=10)
    assert job["status"] == "cancelled"
    # Cancellation lands between candidate evaluations, never overwrites
    # with a result, and records how far the search got.
    assert "response" not in job
    assert job["attempts"] is not None
    assert job["attempts"] >= 0
    assert job["finished_at"] is not None


def test_cancelled_job_is_terminal_and_idempotent(client):
    jid = client.post("/jobs", json=slow_payload()).json()["id"]
    wait_until_status(client, jid, RUNNING)
    client.delete(f"/jobs/{jid}")
    job = wait_for(client, jid)
    assert job["status"] == "cancelled"

    # Repeated DELETE is a 200 no-op and cannot change the terminal state.
    assert client.delete(f"/jobs/{jid}").status_code == 200
    time.sleep(0.3)
    assert client.get(f"/jobs/{jid}").json()["status"] == "cancelled"


def test_cancel_queued_job_is_immediate(client):
    # Occupy the single worker with the slow job.
    running_id = client.post("/jobs", json=slow_payload()).json()["id"]
    wait_until_status(client, running_id, RUNNING)

    queued_id = client.post("/jobs", json=SIMPLE).json()["id"]
    time.sleep(0.1)
    assert client.get(f"/jobs/{queued_id}").json()["status"] == QUEUED

    r = client.delete(f"/jobs/{queued_id}")
    assert r.status_code == 200
    assert r.json()["status"] == "cancelled"
    time.sleep(0.2)
    assert client.get(f"/jobs/{queued_id}").json()["status"] == "cancelled"

    # The running job must be unaffected.
    assert client.get(f"/jobs/{running_id}").json()["status"] == RUNNING
    client.delete(f"/jobs/{running_id}")
    wait_for(client, running_id)


def test_cancel_finished_job_is_409_and_preserves_state(client):
    jid = client.post("/jobs", json=SIMPLE).json()["id"]
    job = wait_for(client, jid)
    assert job["status"] == "succeeded"

    r = client.delete(f"/jobs/{jid}")
    assert r.status_code == 409
    assert r.json()["error"]["code"] == "job_terminal"
    assert client.get(f"/jobs/{jid}").json()["status"] == "succeeded"


def test_jobs_claimed_in_fifo_order(tmp_path):
    store = JobStore(str(tmp_path / "fifo.db"))
    ids = [store.enqueue(SIMPLE)["id"] for _ in range(3)]
    claimed = []
    while True:
        job = store.claim_next()
        if job is None:
            break
        claimed.append(job["id"])
    assert claimed == ids
    store.close()


def test_claim_is_atomic_with_begin_immediate(tmp_path):
    # Many threads claiming concurrently: exactly one wins the job.  This is
    # the guarantee the BEGIN IMMEDIATE transaction provides.
    path = str(tmp_path / "a.db")
    s1 = JobStore(path)
    jid = s1.enqueue(SIMPLE)["id"]
    winners: list[str] = []
    barrier = threading.Barrier(8)

    def attempt(store):
        barrier.wait()
        job = store.claim_next()
        if job is not None:
            winners.append(job["id"])

    stores = [JobStore(path) for _ in range(8)]
    threads = [threading.Thread(target=attempt, args=(s,)) for s in stores]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    assert winners == [jid]
    # After the win, the queue is empty for everyone.
    assert all(s.claim_next() is None for s in stores)
    for s in stores:
        s.close()
    s1.close()


def test_terminal_state_cannot_be_overwritten_by_worker_finalize(tmp_path):
    store = JobStore(str(tmp_path / "t.db"))
    jid = store.enqueue(SIMPLE)["id"]
    assert store.request_cancel(jid) == "cancelled"
    # finalize only transitions running rows; a cancelled queued row is a
    # terminal state the worker cannot overwrite with a result.
    store.finalize(jid, cancelled=False, response={"ok": 1}, response_status=200)
    job = store.get(jid)
    assert job["status"] == "cancelled"
    assert job["response"] is None
    assert job["finished_at"] is not None
    store.close()


def test_request_cancel_running_race_after_claim_returns_running(tmp_path):
    store = JobStore(str(tmp_path / "r.db"))
    jid = store.enqueue(SIMPLE)["id"]
    claimed = store.claim_next()
    assert claimed["status"] == RUNNING
    assert store.request_cancel(jid) == RUNNING
    job = store.get(jid)
    assert job["status"] == RUNNING
    assert job["cancel_requested"] is True
    # Cooperative stop honours the flag at finalize.
    assert store.finalize(jid, cancelled=True, attempts=42)
    assert store.get(jid)["status"] == "cancelled"
    assert store.get(jid)["attempts"] == 42
    store.close()


def test_stale_running_jobs_requeued_on_startup(tmp_path, monkeypatch):
    path = str(tmp_path / "jobs.db")
    seed = JobStore(path)
    stale = seed.enqueue(SIMPLE)["id"]
    flagged = seed.enqueue(SIMPLE)["id"]
    queued = seed.enqueue(SIMPLE)["id"]
    done = seed.enqueue(SIMPLE)["id"]
    con = sqlite3.connect(path)
    con.execute("UPDATE jobs SET status='running' WHERE id=?", (stale,))
    con.execute(
        "UPDATE jobs SET status='running', cancel_requested=1 WHERE id=?", (flagged,)
    )
    con.execute(
        "UPDATE jobs SET status='succeeded', response=?, response_status=200 WHERE id=?",
        ('{"status":"ok"}', done),
    )
    con.commit()
    con.close()
    seed.close()

    monkeypatch.setenv("RESOLVER_DB", path)
    with TestClient(app) as client:
        assert wait_for(client, stale)["status"] == "succeeded"
        assert wait_for(client, queued)["status"] == "succeeded"
        assert client.get(f"/jobs/{flagged}").json()["status"] == "cancelled"
        assert client.get(f"/jobs/{done}").json()["status"] == "succeeded"


def test_jobs_persist_across_worker_store_restarts(tmp_path, monkeypatch):
    path = str(tmp_path / "persist.db")
    store = JobStore(path)
    jid = store.enqueue(SIMPLE)["id"]
    store.close()

    # A fresh store (new process) sees the queued row and processes it.
    monkeypatch.setenv("RESOLVER_DB", path)
    with TestClient(app) as client:
        job = wait_for(client, jid)
    assert job["response"]["resolved"] == {"x": "2.0"}

    # And the terminal result survives another restart.
    with TestClient(app) as client:
        again = client.get(f"/jobs/{jid}").json()
    assert again["status"] == "succeeded"
    assert again["response"]["resolved"] == {"x": "2.0"}


def test_db_file_is_created_on_disk(tmp_path, monkeypatch):
    path = tmp_path / "nested" / "dir" / "jobs.db"
    monkeypatch.setenv("RESOLVER_DB", str(path))
    with TestClient(app):
        assert path.exists()


def test_sync_resolve_still_available_alongside_jobs(client):
    r = client.post("/resolve", json=SIMPLE)
    assert r.status_code == 200
    assert r.json()["resolved"] == {"x": "2.0"}


def test_worker_runner_marks_cancelled_when_flag_set_mid_run(tmp_path):
    # Direct worker/runner integration with a controllable cancel flag.
    from app.main import run_resolution_job

    calls = {"n": 0}

    def should_cancel():
        calls["n"] += 1
        return calls["n"] == 3

    with pytest.raises(BaseException) as excinfo:
        run_resolution_job(slow_payload(), should_cancel)
    assert excinfo.value.attempts is not None


# ---------------------------------------------------------------------------
# Cancel vs. completion linearization
#
# These tests control the interleaving of DELETE and the worker's terminal
# write deterministically with events (no sleeps): a gated store parks the
# worker at the exact point where it commits the terminal state, so the
# test chooses which side commits first.
# ---------------------------------------------------------------------------


class GatedJobStore(JobStore):
    """JobStore test double that can hold ``finalize`` at the transaction
    boundary, letting a test land a DELETE inside the commit window."""

    def __init__(self, path: str) -> None:
        super().__init__(path)
        self._gate_enabled = False
        self.finalize_entered = threading.Event()
        self.finalize_release = threading.Event()

    def enable_finalize_gate(self) -> None:
        self._gate_enabled = True

    def finalize(self, job_id, **kwargs):
        if self._gate_enabled:
            self._gate_enabled = False  # one-shot
            self.finalize_entered.set()
            assert self.finalize_release.wait(timeout=5)
        return super().finalize(job_id, **kwargs)


def make_runner(outcome):
    """A runner that has already passed its last cooperative cancel check
    and completes with the given outcome."""

    def runner(payload, should_cancel):
        assert not should_cancel()
        if outcome == "success":
            return {"status": "ok", "resolved": {"x": "2.0"}}, 200, None, 1
        if outcome == "failure":
            return (
                {"error": {"code": "invalid_request", "message": "boom"}},
                400,
                {"code": "invalid_request", "message": "boom"},
                None,
            )
        raise RuntimeError("boom")

    return runner


@pytest.mark.parametrize("outcome", ["success", "failure", "exception"])
def test_cancel_committed_during_finalize_window_wins(tmp_path, outcome):
    """Interleaving 1: DELETE persists cancel_requested while the job is
    running, then the worker commits its (successful or failed) completion
    without ever having seen the flag.  The terminal state must be written
    atomically as cancelled, keeping no response or error."""
    store = GatedJobStore(str(tmp_path / "race.db"))
    jid = store.enqueue(SIMPLE)["id"]
    job = store.claim_next()
    assert job is not None

    worker = JobWorker(store, make_runner(outcome))
    store.enable_finalize_gate()
    thread = threading.Thread(target=worker._process, args=(job,))
    thread.start()
    # The worker is parked at the terminal write; land the DELETE first.
    assert store.finalize_entered.wait(timeout=5)
    assert store.request_cancel(jid) == RUNNING
    store.finalize_release.set()
    thread.join(timeout=5)
    assert not thread.is_alive()

    final = store.get(jid)
    assert final is not None
    assert final["status"] == CANCELLED
    assert final["cancel_requested"] is True
    assert final["response"] is None
    assert final["response_status"] is None
    assert final["error"] is None
    assert final["finished_at"] is not None
    store.close()


def test_completion_committed_before_delete_is_preserved(tmp_path):
    """Interleaving 2: the worker's terminal write commits first; a DELETE
    issued strictly after the commit keeps the result (the API maps this
    outcome to 409)."""
    store = JobStore(str(tmp_path / "race2.db"))
    jid = store.enqueue(SIMPLE)["id"]
    job = store.claim_next()
    assert job is not None
    committed = threading.Event()
    cancel_outcome = []

    def run_worker():
        JobWorker(store, make_runner("success"))._process(job)
        committed.set()

    def run_delete():
        # Deterministically ordered after the terminal commit.
        assert committed.wait(timeout=5)
        cancel_outcome.append(store.request_cancel(jid))

    worker_thread = threading.Thread(target=run_worker)
    delete_thread = threading.Thread(target=run_delete)
    worker_thread.start()
    delete_thread.start()
    worker_thread.join(timeout=5)
    delete_thread.join(timeout=5)
    assert not worker_thread.is_alive() and not delete_thread.is_alive()

    assert cancel_outcome == [SUCCEEDED]
    final = store.get(jid)
    assert final["status"] == SUCCEEDED
    assert final["response"] == {"status": "ok", "resolved": {"x": "2.0"}}
    assert final["response_status"] == 200
    store.close()


def test_delete_during_finalize_window_returns_202_then_job_cancelled(
    tmp_path, monkeypatch
):
    """End to end: DELETE returns 202 while the worker is already
    committing a successful result; the job still ends cancelled and the
    API view carries no response or error."""
    monkeypatch.setenv("RESOLVER_DB", str(tmp_path / "api-race.db"))
    monkeypatch.setattr("app.main.JobStore", GatedJobStore)
    with TestClient(app) as client:
        store = app.state.job_store
        assert isinstance(store, GatedJobStore)
        store.enable_finalize_gate()
        jid = client.post("/jobs", json=SIMPLE).json()["id"]
        # The worker finished resolving and is parked at the commit.
        assert store.finalize_entered.wait(timeout=5)
        r = client.delete(f"/jobs/{jid}")
        assert r.status_code == 202
        assert r.json()["status"] == RUNNING
        assert r.json()["cancel_requested"] is True
        store.finalize_release.set()
        job = wait_for(client, jid)
    assert job["status"] == "cancelled"
    assert "response" not in job
    assert "response_status" not in job
    assert "error" not in job
