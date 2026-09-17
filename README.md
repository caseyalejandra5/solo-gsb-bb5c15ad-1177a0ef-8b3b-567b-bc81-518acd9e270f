# Local PEP 440 Dependency Resolver

一个完全本地化、确定性的依赖版本解析服务。使用 **Python + FastAPI + packaging**
实现，依据请求中自带的包版本清单（无需 PyPI、无需网络、无需账号）解析出全部
传递依赖。

## 特性

- **PEP 440 版本语义**：由 [`packaging`](https://packaging.pypa.io/) 提供版本号
  解析、排序与 `>=`、`==`、`<`、`~=`、`!=` 等说明符匹配；兼容 epoch、post、dev
  等版本形式。
- **传递依赖解析**：递归处理每个选定版本声明的依赖，支持依赖图中的环。
- **锁定优先**：`locks` 中给出的版本只要仍满足所有约束，就会被优先保留；
  锁定版本被更深层依赖证伪时，才按规则回溯到其他版本，返回结果里会说明每个锁的
  处置（`applied` / `backtracked` / `unsatisfiable` / `missing-version` /
  `not-found` / `unused`）。
- **确定性回溯**：其余候选严格按版本**从高到低**尝试；候选顺序、依赖处理顺序、
  根需求顺序均为确定顺序，相同输入永远得到相同输出（含尝试次数）。
- **默认排除预发布版**：`a/b/rc/dev` 等预发布默认不参与；可通过
  `allow_prereleases=true` 全局放开，或由显式预发布钉版（如 `==2.0rc1`）、
  对该包的锁自动放开。
- **可操作的冲突报告**：无解时返回 409，并给出：
  - 冲突包名与原因（`unsatisfiable` / `not-found` / `no-versions`）；
  - 互相矛盾的约束（说明符、提出者及其版本）；
  - 每条约束完整的**依赖路径**（从根一路到提出约束的包）；
  - 候选版本清单与已尝试版本，以及整棵失败分支树，而不是一句笼统错误。
- **PEP 508**：依赖可写成 `name[extra] spec; marker` 字符串；环境标记在固定的
  默认环境下求值（可用 `marker_env` 覆盖），保证结果可复现。
- **同步 + 异步两种用法**：`POST /resolve` 立即返回；`POST /jobs` 把同样的请求
  持久化到 SQLite 后由后台 worker 顺序执行，可查询、可协作取消，重启自动恢复。

## 一条命令启动

需要 Docker（含 Compose v2）。在项目根目录执行：

```bash
docker compose up --build
```

服务监听 **http://localhost:8000**：

- `GET /health` — 健康检查
- `POST /resolve` — 同步解析依赖（保持原有行为不变）
- `POST /jobs` — 提交异步解析作业（立即返回 `202` 与作业 ID）
- `GET /jobs/{id}` — 查询作业状态与最终响应
- `DELETE /jobs/{id}` — 取消作业
- `GET /docs` — 交互式 OpenAPI 文档（Swagger UI）

镜像在构建时安装固定版本依赖；运行时不访问任何外部仓库。

## 异步作业 API

对于回溯很深、耗时较长的解析，可以用与 `/resolve` **完全相同的请求体**提交
一个持久化作业，由内置 worker 在后台顺序执行（FIFO）。状态保存在 SQLite 中，
服务重启、容器重建后作业与结果仍然保留（见[数据持久化](#数据持久化)）。

### 提交作业

```bash
curl -s -X POST localhost:8000/jobs -H 'Content-Type: application/json' -d '{
  "packages": { "x": {"1.0": {}, "2.0": {}} },
  "root": { "x": ">=1.0" }
}'
```

返回 `202 Accepted`，并通过 `Location: /jobs/{id}` 给出查询地址：

```json
{
  "id": "59150780924745849a94bbe17e0d5155",
  "status": "queued",
  "created_at": "2026-09-17T08:00:00.000000+00:00",
  "started_at": null,
  "finished_at": null
}
```

入队前会做与 `/resolve` 一致的语法/语义校验：非法请求直接返回 `400/422`，
不会产生注定失败的作业。

### 查询作业

```bash
curl -s localhost:8000/jobs/59150780924745849a94bbe17e0d5155
```

状态机：`queued → running → succeeded | failed | cancelled`。

- `queued` / `running`：返回时间戳；running 时若已请求取消，还会带
  `"cancel_requested": true` 与当前已求值的候选次数 `attempts`。
- `succeeded`：`response` 即同步 `/resolve` 的完整响应体，`response_status`
  为其原本的 HTTP 状态码（解析成功 `200`；无解/触顶为 `409`，与同步接口一致）。
- `failed`：作业执行出现异常，`error` 给出错误码与信息。
- `cancelled`：已取消（不包含 `response`，终态不可更改）。

### 取消作业

```bash
curl -s -X DELETE localhost:8000/jobs/{id}
```

- **queued**：立即转为 `cancelled`，返回 `200`；
- **running**：worker 在**下一个候选求值边界**协作式停止，作业随即变为
  `cancelled`（不会在回溯分支中途停在不一致状态），返回 `202`；
- **cancelled**：幂等，再次取消返回 `200`；
- **succeeded / failed**：终态受保护，返回 `409`，原结果不会被覆盖。

#### 取消与完成的线性化语义

`DELETE`（在作业仍为 `running` 时持久化 `cancel_requested=1`）与 worker
写回终态是**线性化**的：worker 的终结操作把"读取取消标志"和"写入终态"
放在同一个 `BEGIN IMMEDIATE` 事务里完成，因此两种交错顺序都有确定结果——

- **取消先提交**：即使解析器刚通过最后一次取消检查、worker 随后带着成功
  或失败的结果提交，终态也原子地写为 `cancelled`，且不保留 `response`、
  `response_status` 或 `error`；
- **完成先提交**：随后的 `DELETE` 返回 `409`，已持久化的结果原样保留。

换句话说，只要 `DELETE` 在作业仍为 `running` 时成功落库（返回 `202`），
该作业最终一定是 `cancelled`：不存在"返回了 `202` 却最终 `succeeded`"
或"已取消却仍带出响应体"的中间态。

### 崩溃恢复

worker 以单条 `BEGIN IMMEDIATE` 事务原子领取最早的 `queued` 作业（按入队顺序，
FIFO），因此即使将来扩容为多个 worker 也不会重复领取同一作业。每次启动时，
上一进程遗留的 `running` 作业会被退回 `queued` 重新排队；若遗留作业此前已收到
取消请求，则直接终结为 `cancelled`。

## 数据持久化

异步作业保存在 SQLite（WAL 模式），数据库路径由环境变量 `RESOLVER_DB` 控制，
默认 `./data/jobs.db`。`docker compose up --build` 通过命名卷 `resolver-data`
挂载到容器内的 `/app/data`，因此：

- 重建镜像、重启容器后，已排队和已完成的作业都还在；
- 未完成的作业在启动时自动重新入队执行。

如需清空历史，可删除卷：`docker compose down -v`。

## 请求格式

```json
{
  "packages": {
    "<包名>": {
      "<PEP440版本>": {
        "dependencies": { "<依赖包名>": "<版本说明符>" }
      }
    }
  },
  "root": { "<包名>": "<版本说明符>" },
  "locks": { "<包名>": "<锁定版本>" },
  "allow_prereleases": false,
  "marker_env": { "python_version": "3.11" }
}
```

- `packages`：本地包索引，包名 → 版本 → 条目。`dependencies` 可以是
  `{包名: 说明符}` 映射，也可以是 PEP 508 字符串数组，例如
  `["requests>=2.0", "tomli; python_version < '3.11'"]`。
- `root`：根依赖，映射或 PEP 508 字符串数组均可。
- `locks`：可选锁定版本。
- `allow_prereleases`：可选，默认 `false`。
- `marker_env`：可选，覆盖标记求值环境。
- `max_attempts`：可选，候选求值上限（默认 100000）。

### 成功响应（200）

```bash
curl -s -X POST localhost:8000/resolve -H 'Content-Type: application/json' -d '{
  "packages": {
    "app": {"1.0": {"dependencies": {"web": ">=2.0", "db": ""}}},
    "web": {"2.0": {"dependencies": {"common": "<2.0"}},
            "3.0": {"dependencies": {"common": ">=2.0"}}},
    "db":  {"1.0": {"dependencies": {"common": "==1.5"}}},
    "common": {"1.5": {}, "2.0": {}}
  },
  "root": {"app": ""}
}'
```

`web 3.0` 要求 `common>=2.0`，与 `db` 的 `common==1.5` 冲突，于是回溯到
`web 2.0`：

```json
{
  "status": "ok",
  "attempts": 5,
  "resolved": {"app": "1.0", "common": "1.5", "db": "1.0", "web": "2.0"},
  "dependency_graph": [ ... ],
  "locks": {}
}
```

### 锁定优先

```json
{ "packages": {"x": {"1.0": {}, "2.0": {}}},
  "root": {"x": ">=1.0"},
  "locks": {"x": "1.0"} }
```

只要锁仍满足约束，`x` 解析为 `1.0`（而非最高的 `2.0`），`locks.x = "applied"`。

### 冲突响应（409）

```json
{
  "packages": {
    "service-a": {"1.0": {"dependencies": {"libfoo": ">=2.0"}}},
    "service-b": {"1.0": {"dependencies": {"libfoo": "<2.0"}}},
    "libfoo": {"1.0": {}, "2.0": {}, "3.0": {}}
  },
  "root": ["service-a", "service-b"]
}
```

返回（节选）：

```json
{
  "status": "conflict",
  "conflict": {
    "terminal_conflicts": [
      {
        "package": "libfoo",
        "reason": "unsatisfiable",
        "available_versions": ["3.0", "2.0", "1.0"],
        "active_constraints": [
          {
            "requirement": "libfoo>=2.0",
            "required_by": "service-a",
            "required_by_version": "1.0",
            "path": [{"name": "<root>"}, {"name": "service-a", "version": "1.0"}]
          },
          {
            "requirement": "libfoo<2.0",
            "required_by": "service-b",
            "required_by_version": "1.0",
            "path": [{"name": "<root>"}, {"name": "service-b", "version": "1.0"}]
          }
        ]
      }
    ],
    "tree": { "...": "完整的失败分支树（含每步尝试的版本）..." }
  }
}
```

错误请求（非 PEP 440 版本、非法说明符、多余字段等）返回 `400/422` 并附带原因。

## 本地开发（不使用 Docker）

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
uvicorn app.main:app --reload --port 8000
```

运行测试：

```bash
pip install pytest httpx
pytest -q
```

本地运行时作业库默认创建在 `./data/jobs.db`（已加入 `.gitignore`），可用
`RESOLVER_DB=/path/to/jobs.db` 覆盖。

## 解析策略说明

- 每个包维护当前所有生效约束的交集；候选必须同时满足全部约束。
- 搜索采用带精确撤销日志的深度优先回溯：选定一个版本后施加其依赖并递归；
  更深层失败时，按逆序撤销该候选引入的全部约束、边与发现标记，再尝试下一候选。
- 求解器仅做“按时间顺序回溯”，保证完备：有解时一定能找到；无解时一定报告。
- 为防止病态输入无限膨胀，`max_attempts` 触顶会返回 `status: "limit"`（仍带
  结构化冲突信息）。

## 项目结构

```
app/
  resolver.py    # PEP 440/508 解析、候选排序、回溯求解器、冲突数据结构、取消点
  serialize.py   # 冲突树 / 终态冲突的 JSON 序列化
  jobs.py        # SQLite 作业存储、BEGIN IMMEDIATE 原子领取、单次事务终结（取消优先）、后台 worker
  main.py        # FastAPI 应用与 /resolve、/jobs、/health
tests/           # 端到端 API 测试、作业持久化/取消测试、求解器单元测试（含随机暴力对照）
data/            # 运行时生成：SQLite 数据库（RESOLVER_DB，已 gitignore）
Dockerfile
docker-compose.yml
requirements.txt
```
