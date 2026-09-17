"""FastAPI application exposing the local dependency resolver."""

from __future__ import annotations

import os
from contextlib import asynccontextmanager
from typing import Any, Callable, Optional

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse
from pydantic import BaseModel, ConfigDict, Field

from .jobs import (
    CANCELLED,
    FAILED,
    RUNNING,
    SUCCEEDED,
    JobStore,
    JobWorker,
)
from .resolver import (
    ResolutionInputError,
    parse_index,
    parse_locks,
    parse_requirements,
    resolve,
)
from .serialize import conflict_summary

DEFAULT_DB_PATH = os.path.join("data", "jobs.db")


class ResolveRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    packages: dict[str, dict[str, Any]] = Field(
        ...,
        description=(
            "Package index: package name -> version -> entry. "
            "Each entry may contain 'dependencies' as either "
            "{package: specifier} or a list of PEP 508 strings."
        ),
    )
    root: Any = Field(
        ...,
        description="Root requirements: {package: specifier} or a list of PEP 508 strings.",
    )
    locks: Optional[dict[str, str]] = Field(
        default=None,
        description="Optional pinned versions. Pins are kept while they satisfy all constraints.",
    )
    allow_prereleases: bool = Field(
        default=False,
        description="Include a/b/rc pre-releases for every package when true.",
    )
    marker_env: Optional[dict[str, str]] = Field(
        default=None,
        description="Override values for PEP 508 environment marker evaluation.",
    )
    max_attempts: int = Field(
        default=100_000, ge=1, le=10_000_000,
        description="Bound on candidate evaluations to keep resolution finite.",
    )


@asynccontextmanager
async def lifespan(_app: FastAPI):
    """Open the SQLite job store and start the background worker.

    Jobs left ``running`` by an earlier process are returned to ``queued``
    before the worker starts, so a crash/kill never strands work.
    """
    db_path = os.environ.get("RESOLVER_DB", DEFAULT_DB_PATH)
    store = JobStore(db_path)
    store.requeue_stale_running()
    worker = JobWorker(store, run_resolution_job)
    worker.start()
    _app.state.job_store = store
    _app.state.job_worker = worker
    try:
        yield
    finally:
        worker.stop()
        store.close()


app = FastAPI(
    title="Local PEP 440 Dependency Resolver",
    version="1.1.0",
    description=(
        "Resolves transitive dependencies from a self-contained package index. "
        "No external package index or account is used."
    ),
    lifespan=lifespan,
)


@app.exception_handler(ResolutionInputError)
async def _input_error_handler(_request: Request, exc: ResolutionInputError) -> JSONResponse:
    return JSONResponse(status_code=400, content={"error": {"code": "invalid_request", "message": str(exc)}})


def _parse_input(payload: ResolveRequest):
    """Parse and semantically validate a request.

    Cheap (no solving), so it can run synchronously on both ``/resolve`` and
    ``POST /jobs`` to reject malformed input with 400 before queueing.
    """
    index, index_errors = parse_index(payload.packages)
    if index_errors:
        raise ResolutionInputError("; ".join(index_errors))

    roots = parse_requirements(payload.root, "root requirements")

    locks, lock_errors = parse_locks(payload.locks or {})
    if lock_errors:
        raise ResolutionInputError("; ".join(lock_errors))

    return index, roots, locks


def _resolve_body(
    payload: ResolveRequest,
    should_cancel: Optional[Callable[[], bool]] = None,
) -> tuple[dict[str, Any], int, Optional[dict[str, Any]], Optional[int]]:
    """Validate and resolve. Returns ``(body, http_status, error, attempts)``.

    ``ResolutionCancelled`` is allowed to propagate so the job worker can
    finalise the job as cancelled at a candidate boundary.
    """
    index, roots, locks = _parse_input(payload)

    result = resolve(
        index,
        roots,
        locks,
        allow_prereleases=payload.allow_prereleases,
        marker_env=payload.marker_env,
        max_attempts=payload.max_attempts,
        should_cancel=should_cancel,
    )

    body: dict[str, Any] = {
        "status": result.status,
        "attempts": result.attempts,
        "allow_prereleases": payload.allow_prereleases,
    }
    if result.status == "ok":
        body["resolved"] = result.chosen
        body["dependency_graph"] = result.edges
        body["locks"] = result.lock_notes
        return body, 200, None, result.attempts
    if result.status == "conflict":
        body["locks"] = result.lock_notes
        body["conflict"] = conflict_summary(result)
        return body, 409, None, result.attempts
    # limit
    body["locks"] = result.lock_notes
    body["conflict"] = conflict_summary(result)
    body["error"] = {
        "code": "search_limit_exceeded",
        "message": f"resolution exceeded {payload.max_attempts} candidate evaluations",
    }
    return body, 409, None, result.attempts


def run_resolution_job(
    raw_payload: dict[str, Any],
    should_cancel: Callable[[], bool],
) -> tuple[Optional[dict[str, Any]], Optional[int], Optional[dict[str, Any]], Optional[int]]:
    """Runner used by the worker thread."""
    payload = ResolveRequest.model_validate(raw_payload)
    try:
        body, status_code, _error, attempts = _resolve_body(payload, should_cancel)
    except ResolutionInputError as exc:
        # Defensive: POST /jobs validates before queueing, so this only
        # happens for jobs persisted by older/newer code paths.
        return (
            {"error": {"code": "invalid_request", "message": str(exc)}},
            400,
            {"code": "invalid_request", "message": str(exc)},
            None,
        )
    return body, status_code, None, attempts


@app.post("/resolve")
async def resolve_endpoint(payload: ResolveRequest) -> JSONResponse:
    body, status_code, _error, _attempts = _resolve_body(payload)
    return JSONResponse(status_code=status_code, content=body)


# ---------------------------------------------------------------------------
# Asynchronous jobs
# ---------------------------------------------------------------------------


def _store(request: Request) -> JobStore:
    return request.app.state.job_store


def _job_view(job: dict[str, Any]) -> dict[str, Any]:
    view: dict[str, Any] = {
        "id": job["id"],
        "status": job["status"],
        "created_at": job["created_at"],
        "started_at": job["started_at"],
        "finished_at": job["finished_at"],
    }
    if job["status"] == RUNNING or job["cancel_requested"]:
        view["cancel_requested"] = job["cancel_requested"]
    if job["attempts"] is not None:
        view["attempts"] = job["attempts"]
    if job["status"] in (SUCCEEDED, FAILED) and job["response"] is not None:
        # The synchronous response body, including 400/409-style shapes.
        view["response"] = job["response"]
        view["response_status"] = job["response_status"]
    if job["error"] is not None:
        view["error"] = job["error"]
    return view


@app.post("/jobs", status_code=202)
async def create_job(payload: ResolveRequest, request: Request) -> JSONResponse:
    """Persist the same payload accepted by ``/resolve`` and queue it.

    Input is parsed and validated (but not solved) before queueing, so bad
    requests get the same 400/422 as ``/resolve`` instead of a dead job.
    """
    _parse_input(payload)
    store = _store(request)
    job = store.enqueue(payload.model_dump(mode="json"))
    request.app.state.job_worker.wake()
    return JSONResponse(
        status_code=202,
        content=_job_view(job),
        headers={"Location": f"/jobs/{job['id']}"},
    )


@app.get("/jobs/{job_id}")
async def get_job(job_id: str, request: Request) -> JSONResponse:
    job = _store(request).get(job_id)
    if job is None:
        return JSONResponse(
            status_code=404, content={"error": {"code": "not_found", "message": f"job {job_id!r} not found"}}
        )
    return JSONResponse(status_code=200, content=_job_view(job))


@app.delete("/jobs/{job_id}")
async def cancel_job(job_id: str, request: Request) -> JSONResponse:
    """Cancel a job.

    * queued    -> cancelled immediately (200)
    * running   -> cancel requested; stops at the next candidate-evaluation
                   boundary and becomes cancelled (202)
    * cancelled -> idempotent no-op (200)
    * succeeded/failed -> 409, terminal state is preserved
    """
    store = _store(request)
    status = store.request_cancel(job_id)
    if status is None:
        return JSONResponse(
            status_code=404, content={"error": {"code": "not_found", "message": f"job {job_id!r} not found"}}
        )
    job = store.get(job_id)
    if job is None:  # pragma: no cover - cannot happen within one request
        return JSONResponse(
            status_code=404, content={"error": {"code": "not_found", "message": f"job {job_id!r} not found"}}
        )
    if status == CANCELLED:
        return JSONResponse(status_code=200, content=_job_view(job))
    if status in (SUCCEEDED, FAILED):
        return JSONResponse(
            status_code=409,
            content={
                "error": {
                    "code": "job_terminal",
                    "message": f"job already finished with status {status!r}; terminal state cannot be cancelled",
                }
            },
        )
    # running: cancellation will be honoured cooperatively
    return JSONResponse(status_code=202, content=_job_view(job))


@app.get("/health")
async def health() -> dict[str, str]:
    return {"status": "ok"}
