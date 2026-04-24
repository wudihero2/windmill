# 第九章：自建工作流平台 — 實作計畫

## 目標

從零打造一個工作流 / 資料管線平台（暫名 **FlowForge**），支援：

- 即時寫 Python（未來多語言）並執行
- DAG 工作流（like Windmill Flow）
- 資料管線排程（like Airflow）
- **Day 1 沙箱隔離**（四模式：Rust 原生 / nsjail / WASM / K8s Pod）
- **Day 1 OpenTelemetry**
- **Day 1 Flow 版本控制**（不可變 revision，可 diff / rollback）
- **VS Code 風格多檔案編輯器**（多 Python 檔案互相引用，`main` 為入口）
- **資料引擎即時預覽**（DataPreviewTable 元件）

---

## 與 Windmill 的關鍵差異總結

先看全貌，後面各 Phase 會逐一實作：

| 面向 | Windmill | FlowForge |
|------|---------|-----------|
| Flow Editor | 自建 SVG（5000+ 行） | `@xyflow/svelte`（現成）+ 內嵌 Monaco tab |
| JS 求值 | QuickJS + Deno（C 依賴） | `boa_engine`（純 Rust） |
| 可觀測性 | 後加 OTel | Day 1 OpenTelemetry |
| 沙箱 | nsjail only（Linux only） | **四模式**：Rust 原生 / nsjail / WASM / K8s Pod |
| Script Hash | `i64`（不易讀） | SHA256 hex（可讀） |
| Crate 數量 | 50+（複雜） | 6 核心（簡潔） |
| Job 表設計 | 單表（queue + 結果混合） | 三表分離 + `LISTEN/NOTIFY` |
| 資料處理 | DuckDB 是附加 | DataEngine trait（DuckDB / DataFusion 可插拔） |
| 內建節點 | 只有 Script | Log / HTTP / Sleep / Assert / SetVariable / Query |
| 自訂節點 | 無 | 使用者可把 Script 封裝成可重用節點 |
| Flow 分享 | 無標準格式 | YAML / JSON 匯出匯入 |
| 程式碼編輯 | Script Editor 獨立頁面 | Flow Editor 內嵌 Monaco tab（like VS Code） |
| K8s 執行 | 只有 Worker 自動擴縮 | **Job-level K8s Pod 執行** |
| Multi-file 編輯 | Script 只有單檔案 | **VS Code 風格**：多檔案工作區 + 檔案樹 + `import` 互引 |
| Flow 版本控制 | 無（覆蓋更新） | **Day 1 Revision**：不可變版本 + diff + rollback |
| 資料預覽 | 無 | DataPreviewTable：即時預覽查詢結果 |
| 狀態追蹤 | 單一 status 欄位 | **State History**：完整狀態轉換歷史（學 Prefect） |
| 事件審計 | 自建 audit log | **Event Log**：通用事件日誌（學 Dagster） |
| 並發控制 | 無 tag 級限制 | **ConcurrencyLimit**：tag 級並發上限（學 Prefect） |

---

## 技術選型

| 層 | 選擇 | 理由 |
|---|------|------|
| Backend | Rust + Axum 0.8 + Tokio | 同 Windmill，效能最佳 |
| DB | PostgreSQL + SQLx 0.8 | 編譯時 SQL 檢查，`FOR UPDATE SKIP LOCKED` 做 queue |
| Frontend | Svelte 5 + SvelteKit + Vite | 同 Windmill，reactive runes |
| Code Editor | Monaco Editor | 業界標準，LSP 支援 |
| Flow Graph | `@xyflow/svelte` | **不同於 Windmill**（自建 SVG），省大量工作 |
| JS 求值 | `boa_engine`（純 Rust JS） | **不同於 Windmill**（QuickJS/Deno），無需 C 依賴 |
| OTel | `opentelemetry` + `tracing` | **不同於 Windmill**，Day 1 內建 |
| Object Storage | `aws-sdk-s3` | 大結果走 S3，與 Windmill 同 |
| 沙箱 | Landlock+seccomp / nsjail / WASM / K8s Pod | **不同於 Windmill**（只有 nsjail），Day 1 四模式 |

---

## 專案結構

```
flowforge/
├── backend/
│   ├── Cargo.toml                 # workspace
│   ├── src/main.rs                # Server + Worker 入口
│   ├── migrations/                # SQLx migrations
│   └── crates/
│       ├── api/                   # Axum 路由
│       │   ├── Cargo.toml         # deps: axum, tower, serde, sqlx, jsonwebtoken, argon2
│       │   └── src/
│       │       ├── lib.rs             # Router 組裝
│       │       ├── auth.rs            # JWT middleware
│       │       ├── scripts.rs         # Script CRUD
│       │       ├── flows.rs           # Flow CRUD + 版本控制
│       │       ├── flow_files.rs      # Flow 工作區檔案 CRUD
│       │       ├── jobs.rs            # Job 執行/查詢
│       │       └── sse.rs             # SSE 日誌串流
│       ├── types/                 # 領域型別
│       │   ├── Cargo.toml         # deps: serde, uuid, chrono
│       │   └── src/
│       │       ├── scripts.rs         # Script, ScriptLang
│       │       ├── flows.rs           # FlowValue, FlowModule, InputTransform
│       │       ├── flow_status.rs     # FlowStatus 狀態機
│       │       ├── flow_version.rs    # FlowRevision, FlowDiff
│       │       └── jobs.rs            # Job, JobKind
│       ├── queue/                 # Job Queue
│       │   ├── Cargo.toml         # deps: sqlx, chrono, uuid
│       │   └── src/
│       │       ├── push.rs            # 推入 job
│       │       ├── pull.rs            # FOR UPDATE SKIP LOCKED
│       │       └── complete.rs        # 完成/失敗
│       ├── worker/                # Worker
│       │   ├── Cargo.toml         # deps: tokio, sqlx, opentelemetry, reqwest
│       │   └── src/
│       │       ├── worker.rs          # 主迴圈
│       │       ├── sandbox.rs         # Sandbox trait + nsjail/k8s 實作
│       │       ├── python.rs          # Python executor
│       │       ├── typescript.rs      # TS executor（Phase 4）
│       │       ├── duckdb.rs          # DuckDB executor（Phase 4）
│       │       ├── handle_child.rs    # 子程序監控
│       │       └── flow_engine.rs     # Flow 狀態機
│       ├── jseval/                # boa_engine JS 表達式求值
│       │   ├── Cargo.toml         # deps: boa_engine, serde_json
│       │   └── src/lib.rs
│       └── object-store/          # S3 整合
│           ├── Cargo.toml         # deps: aws-sdk-s3
│           └── src/lib.rs
├── frontend/
│   ├── src/
│   │   ├── routes/
│   │   │   ├── +layout.svelte
│   │   │   ├── login/+page.svelte
│   │   │   ├── scripts/           # Script 頁面
│   │   │   ├── flows/             # Flow 頁面
│   │   │   ├── jobs/              # Job 頁面
│   │   │   └── schedules/         # 排程頁面
│   │   └── lib/
│   │       ├── components/
│   │       │   ├── ScriptEditor.svelte   # Monaco
│   │       │   ├── FlowEditor.svelte     # @xyflow/svelte DAG
│   │       │   ├── FlowFileTree.svelte   # VS Code 風格檔案樹
│   │       │   ├── FlowVersionPanel.svelte # 版本歷史 + diff
│   │       │   ├── DataPreviewTable.svelte # 資料查詢預覽表格
│   │       │   ├── LogViewer.svelte      # SSE 即時日誌
│   │       │   └── ArgInput.svelte       # JSON Schema → 表單
│   │       ├── gen/                      # OpenAPI 生成
│   │       └── stores/
│   └── package.json
├── nsjail/                        # nsjail config templates
│   ├── run.python3.config.proto
│   ├── run.bash.config.proto
│   └── run.typescript.config.proto
└── docker-compose.yml             # PostgreSQL + MinIO
```

---

## 架構決策

在進入各 Phase 之前，先釐清三個跨 Phase 的核心設計。

### Queue 機制：為什麼選 PostgreSQL

| 平台 | Queue 技術 | 外部依賴 | 延遲 |
|------|-----------|---------|------|
| **Airflow** | Celery (Redis/RabbitMQ) | 需要 Redis 或 RabbitMQ | 低（push） |
| **Prefect 3** | PostgreSQL + HTTP polling | 無 | ~15s（polling） |
| **Dagster** | PostgreSQL (run queue) | 無 | daemon polling |
| **Temporal** | 內建 Matching Service (Go) | 無（但 4 個內部服務） | ~0ms（sync match） |
| **Kestra** | PostgreSQL (JDBC) 或 Kafka | 可選 Kafka | 依 backend |
| **Windmill** | PostgreSQL `FOR UPDATE SKIP LOCKED` | 無 | ~50ms（polling） |

**我們的選擇：PostgreSQL `FOR UPDATE SKIP LOCKED` + `LISTEN/NOTIFY`**

理由：
1. 已被 Windmill（5000 RPS）和 Dagster 驗證可行
2. 零外部依賴（不需要 Redis/RabbitMQ/Kafka）
3. Job 入隊和元資料在同一個事務中，不會出現「job 已分發但元資料沒寫入」
4. `LISTEN/NOTIFY` 可以把延遲從 50ms 降到個位數毫秒
5. Rust + sqlx + Tokio 天然適配

### Job 三表分離設計

Windmill v1 把 queue 和結果放同一張表，後來 v2 才分離。我們直接跳到分離設計：

| 表 | 用途 | 生命週期 |
|---|------|---------|
| `job` | 不可變定義（script_hash, args, tag 等） | 永久 |
| `job_queue` | 可變狀態（running, worker, priority） | Job 完成即刪 |
| `job_completed` | 結果（result, duration, s3_key） | Job 完成時寫入 |

好處：`job_queue` 表永遠很小（只有未完成的 job），`FOR UPDATE SKIP LOCKED` 效能穩定。

### Sandbox 四模式策略

Windmill 的 nsjail 是**後加的**，導致每個 executor 都有 `if is_sandboxing_enabled()` 的分支邏輯。我們的做法：**Sandbox 是 trait，所有執行都經過它**。

| 面向 | Rust 原生 (Landlock+seccomp) | nsjail | WASM (wasmtime) | K8s Pod |
|------|---------------------------|--------|----------------|---------|
| **啟動延遲** | ~1-5ms | ~10-50ms | **~5μs - 1ms** | ~2-10s |
| **效能開銷** | ~0% | ~0% | ~10-15% slower | ~0%（容器內） |
| **跨平台** | Linux only | Linux only | **全平台** | 任何有 K8s 的環境 |
| **任意 pip 包** | **支援** | **支援** | 僅 Pyodide 內建 | **支援** |
| **Bash 執行** | **支援** | **支援** | 不支援 | **支援** |
| **GPU** | 不支援 | 不支援 | 不支援 | **K8s 原生** |
| **外部依賴** | 無（純 Rust crate） | nsjail binary (C++) | 無（純 Rust crate） | K8s cluster |
| **安全等級** | 高（多層防禦） | 高 | **最高**（記憶體安全） | 最高 |

**按 Job 類型選擇：**

```
Python (任意 pip 包)        → Rust 原生 / nsjail
Python (純運算 + pandas)    → WASM（最快啟動）
JavaScript/TypeScript       → WASM（QuickJS，微秒級）
Bash                        → Rust 原生 / nsjail
需要自訂 Docker image       → K8s Pod
需要 GPU                    → K8s Pod
macOS 開發環境              → WASM / None
```

四種模式的詳細實作程式碼見[附錄 A](#附錄-a-sandbox-四模式詳細實作)。

### Script SHA256 版本控制

Windmill 用 `i64` hash（不易讀）。我們用 SHA256 `CHAR(64)`（人類可讀），內容 = code + path + language 的 hash，類似 Git 的 content-addressable storage。

### Flow 版本控制策略（學習 Kestra）

Windmill 的 Flow 沒有版本控制（覆蓋更新）。Kestra 的做法經過驗證：**每次儲存建立不可變的 revision**，靠 `revision` 自增整數追蹤。

```sql
CREATE TABLE flow (
    workspace_id VARCHAR(50) NOT NULL,
    path VARCHAR(255) NOT NULL,
    revision INTEGER NOT NULL DEFAULT 1,  -- 自增，不可變
    summary TEXT,
    value JSONB NOT NULL,                 -- FlowValue
    schema JSONB,                         -- 輸入 JSON Schema
    edited_by VARCHAR(255) NOT NULL,
    edited_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (workspace_id, path, revision)
);
```

好處：
1. 每個版本不可變 → 可安全 rollback
2. `revision` 是整數 → diff 兩個版本只需 `WHERE revision IN ($1, $2)`
3. 執行時 job 記錄 `flow_revision` → 可追溯「這個 job 跑的是哪個版本」
4. 不需要 Git → 資料庫原生支援

### 多檔案 Flow 工作區（學習 Kestra NamespaceFiles）

**為什麼不會太複雜？**

看起來像 VS Code，但實際上只需要 3 個東西：
1. 一張 `flow_file` 表存檔案
2. Worker 執行時把所有檔案複製到 `job_dir`
3. 前端用 tree view 元件顯示

**不需要**實作的東西：
- 真正的檔案系統（全部存 DB）
- 檔案鎖定（Flow 一次只有一人編輯）
- LSP server（Monaco 本身支援基本 autocomplete）

```sql
CREATE TABLE flow_file (
    workspace_id VARCHAR(50) NOT NULL,
    flow_path VARCHAR(255) NOT NULL,
    file_path VARCHAR(500) NOT NULL,  -- "utils/helpers.py", "config.json", "main.py"
    content TEXT NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (workspace_id, flow_path, file_path)
);
```

**執行流程：**

```
1. Flow step 標記 entry_point = "main.py"
2. Worker pull job → 查 flow_file 取得該 flow 的所有檔案
3. 寫入 job_dir/main.py, job_dir/utils/helpers.py, job_dir/config.json
4. 執行 python3 wrapper.py（wrapper import main）
5. main.py 可以 `from utils.helpers import clean_data`
```

**前端呈現：**

```
┌────────────────────────────────────────────────────────────────────┐
│ Flow Editor                                               [Save v3]│
├──────────┬─────────────────────────────────────────────────────────┤
│ 📁 Files │ Tabs: [▼ DAG] [main.py] [utils/helpers.py] [config.json]│
│ ──────── │                                                         │
│ 📄 main.py       │  ┌─ Monaco Editor ─────────────────────────┐    │
│ 📁 utils/        │  │ from utils.helpers import clean_data    │    │
│   📄 helpers.py  │  │                                         │    │
│ 📄 config.json   │  │ def main(url: str):                     │    │
│                  │  │     data = requests.get(url).json()     │    │
│ [+ New File]     │  │     return clean_data(data)             │    │
│                  │  └─────────────────────────────────────────┘    │
├──────────┤       │  ┌─ Input Transforms ──────────────────────┐    │
│ DAG      │       │  │ url: results.step_a.endpoint   [JS ▾]   │    │
│ ┌──┐     │       │  └─────────────────────────────────────────┘    │
│ │ A│→┐   │                                                         │
│ └──┘ ▼   │                                                         │
│ ┌──────┐ │                                                         │
│ │B(py) │◀── 選中                                                    │
│ └──┬───┘ │                                                         │
│    ▼     │                                                         │
│ ┌──┐     │                                                         │
│ │ C│     │                                                         │
│ └──┘     │                                                         │
└──────────┴─────────────────────────────────────────────────────────┘
```

### 後端模型改進（從 Prefect / Dagster / Airflow / Kestra 學到的）

研究了 4 個開源工作流平台的後端模型，以下是我們**採納**和**不採納**的設計：

**採納的設計：**

| 來源 | 設計 | 我們的實作 |
|------|------|-----------|
| Kestra | 不可變 revision 版本控制 | `flow.revision` 自增（見上方） |
| Kestra | NamespaceFiles 多檔案 | `flow_file` 表（見上方） |
| Prefect | State 歷史記錄 | `job_state_history` 表：記錄每次狀態轉換 |
| Prefect | ConcurrencyLimit per tag | `concurrency_limit` 表：限制同 tag 並發數 |
| Dagster | Event Log 審計 | `event_log` 表：通用事件日誌 |
| Airflow | data_interval 時間窗口 | `schedule.data_interval_seconds`：排程對應的資料區間 |

**不採納的設計：**

| 來源 | 設計 | 不採納原因 |
|------|------|-----------|
| Dagster | Software-Defined Assets | 過於複雜，偏離「workflow」核心 |
| Airflow | XCom | 我們用 `results.step_id` 更直覺 |
| Prefect | Labels / Tags 系統 | 現有 tag 系統已足夠 |
| Dagster | Multi-dimensional Partitions | Phase 1 不需要分區概念 |
| Airflow | Trigger 非同步延遲模型 | 我們用 suspend / approval 代替 |

---

## Phase 1：基礎建設（Week 1-3）

### 目標

能在網頁上寫 Python code → 按 Run → 看到即時日誌和結果（含沙箱隔離）

### 1.1 資料庫 Schema

```sql
-- === 多租戶 ===

CREATE TABLE workspace (
    id VARCHAR(50) PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    owner VARCHAR(255) NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE account (
    email VARCHAR(255) PRIMARY KEY,
    password_hash VARCHAR(255) NOT NULL,  -- argon2
    is_admin BOOLEAN NOT NULL DEFAULT FALSE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE workspace_member (
    workspace_id VARCHAR(50) REFERENCES workspace(id),
    email VARCHAR(255) REFERENCES account(email),
    role VARCHAR(20) NOT NULL DEFAULT 'editor',  -- admin, editor, viewer
    PRIMARY KEY (workspace_id, email)
);

-- === Script ===

CREATE TABLE script (
    workspace_id VARCHAR(50) NOT NULL REFERENCES workspace(id),
    hash CHAR(64) NOT NULL,           -- SHA256（不同於 Windmill 的 i64）
    path VARCHAR(255) NOT NULL,
    content TEXT NOT NULL,
    language VARCHAR(20) NOT NULL,     -- python3, typescript, bash, duckdb
    schema JSONB,                      -- JSON Schema（函式簽名）
    parent_hashes TEXT[],              -- 版本鏈
    summary TEXT DEFAULT '',
    created_by VARCHAR(255) NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (workspace_id, hash)
);

-- 快速查詢最新版本
CREATE INDEX idx_script_path ON script(workspace_id, path, created_at DESC);

-- === Job（三表分離設計）===

-- job：不可變定義（建立後不修改）
CREATE TABLE job (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    workspace_id VARCHAR(50) NOT NULL REFERENCES workspace(id),
    kind VARCHAR(20) NOT NULL,         -- script, flow, preview, flow_preview
    script_hash CHAR(64),
    script_path VARCHAR(255),
    flow_value JSONB,                  -- preview 時的 inline flow
    raw_code TEXT,                     -- preview 時的 inline code
    language VARCHAR(20),
    args JSONB,
    tag VARCHAR(50) NOT NULL DEFAULT 'default',  -- 路由到哪種 worker/sandbox
    parent_job UUID,                   -- flow 的 child job
    root_job UUID,                     -- flow 的 root job
    flow_step_id VARCHAR(50),          -- 在 flow 中的步驟 ID
    flow_revision INTEGER,              -- 執行時的 flow 版本（可追溯）
    created_by VARCHAR(255) NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    -- OTel trace context
    trace_id CHAR(32),
    span_id CHAR(16)
);

-- job_queue：可變狀態（Worker 用 FOR UPDATE SKIP LOCKED 搶）
CREATE TABLE job_queue (
    id UUID PRIMARY KEY REFERENCES job(id),
    scheduled_for TIMESTAMPTZ NOT NULL DEFAULT now(),
    running BOOLEAN NOT NULL DEFAULT FALSE,
    started_at TIMESTAMPTZ,
    tag VARCHAR(50) NOT NULL DEFAULT 'default',
    priority SMALLINT NOT NULL DEFAULT 0,
    worker VARCHAR(100),
    last_ping TIMESTAMPTZ
);

CREATE INDEX idx_job_queue_pull ON job_queue(scheduled_for, priority DESC)
    WHERE running = FALSE;

-- job_completed：結果（完成後從 job_queue 刪除，插入這裡）
CREATE TABLE job_completed (
    id UUID PRIMARY KEY REFERENCES job(id),
    success BOOLEAN NOT NULL,
    result JSONB,
    result_s3_key VARCHAR(255),        -- 大結果存 S3
    duration_ms INTEGER NOT NULL,
    memory_peak_bytes BIGINT,
    completed_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- job_log：日誌（獨立表，避免頻繁 UPDATE job_queue）
CREATE TABLE job_log (
    job_id UUID NOT NULL REFERENCES job(id),
    log_offset INTEGER NOT NULL DEFAULT 0,
    logs TEXT NOT NULL DEFAULT '',
    PRIMARY KEY (job_id)
);

-- flow 狀態（獨立表）
CREATE TABLE job_flow_status (
    job_id UUID PRIMARY KEY REFERENCES job(id),
    flow_status JSONB NOT NULL
);

-- === Flow（Day 1 版本控制，學習 Kestra）===

CREATE TABLE flow (
    workspace_id VARCHAR(50) NOT NULL REFERENCES workspace(id),
    path VARCHAR(255) NOT NULL,
    revision INTEGER NOT NULL DEFAULT 1,  -- 每次儲存 +1，不可變
    summary TEXT DEFAULT '',
    description TEXT DEFAULT '',
    value JSONB NOT NULL,                 -- FlowValue
    schema JSONB,                         -- 輸入 JSON Schema
    edited_by VARCHAR(255) NOT NULL,
    edited_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (workspace_id, path, revision)
);

CREATE INDEX idx_flow_latest ON flow(workspace_id, path, revision DESC);

-- === Flow 工作區檔案（學習 Kestra NamespaceFiles）===

CREATE TABLE flow_file (
    workspace_id VARCHAR(50) NOT NULL,
    flow_path VARCHAR(255) NOT NULL,
    file_path VARCHAR(500) NOT NULL,      -- "main.py", "utils/helpers.py"
    content TEXT NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (workspace_id, flow_path, file_path)
);

-- === 狀態歷史（學習 Prefect）===

CREATE TABLE job_state_history (
    id BIGSERIAL PRIMARY KEY,
    job_id UUID NOT NULL REFERENCES job(id),
    state VARCHAR(20) NOT NULL,           -- queued, running, success, failure, cancelled, retrying
    timestamp TIMESTAMPTZ NOT NULL DEFAULT now(),
    message TEXT
);

CREATE INDEX idx_job_state_job ON job_state_history(job_id, timestamp);

-- === 事件日誌（學習 Dagster）===

CREATE TABLE event_log (
    id BIGSERIAL PRIMARY KEY,
    workspace_id VARCHAR(50) NOT NULL,
    event_type VARCHAR(50) NOT NULL,      -- job.created, job.completed, flow.saved, script.created
    entity_type VARCHAR(20) NOT NULL,     -- job, flow, script, schedule
    entity_id VARCHAR(255) NOT NULL,
    payload JSONB,
    timestamp TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX idx_event_log_entity ON event_log(workspace_id, entity_type, entity_id);
CREATE INDEX idx_event_log_time ON event_log(workspace_id, timestamp DESC);

-- === 並發控制（學習 Prefect）===

CREATE TABLE concurrency_limit (
    workspace_id VARCHAR(50) NOT NULL REFERENCES workspace(id),
    tag VARCHAR(50) NOT NULL,
    max_concurrent INTEGER NOT NULL DEFAULT 1,
    PRIMARY KEY (workspace_id, tag)
);

-- === Worker 健康 ===

CREATE TABLE worker_ping (
    worker VARCHAR(100) PRIMARY KEY,
    ping_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    tags TEXT[] NOT NULL DEFAULT '{}',
    ip VARCHAR(45),
    sandbox_mode VARCHAR(20)  -- "nsjail", "k8s", "none"
);
```

### 1.2 Sandbox Trait 設計

Executor 不需要知道用哪種沙箱，只需呼叫 `sandbox.execute(&ctx)`。完整的四模式實作見[附錄 A](#附錄-a-sandbox-四模式詳細實作)。

```rust
// crates/worker/src/sandbox.rs

use async_trait::async_trait;

/// 沙箱執行模式
#[derive(Debug, Clone, serde::Deserialize)]
#[serde(tag = "mode")]
pub enum SandboxMode {
    None,
    RustNative(RustNativeConfig),
    Nsjail(NsjailConfig),
    Wasm(WasmConfig),
    KubernetesPod(K8sPodConfig),
}

/// Sandbox trait — 所有執行器都透過這個介面執行程式碼
#[async_trait]
pub trait Sandbox: Send + Sync {
    async fn execute(&self, ctx: &SandboxContext) -> Result<SandboxResult, SandboxError>;
    async fn health_check(&self) -> Result<(), SandboxError>;
    fn name(&self) -> &str;
}

/// 執行上下文
pub struct SandboxContext {
    pub job_id: uuid::Uuid,
    pub job_dir: String,
    pub command: String,
    pub args: Vec<String>,
    pub env: std::collections::HashMap<String, String>,
    pub timeout_secs: u32,
    pub language: ScriptLang,
    pub custom_image: Option<String>,
    pub trace_context: Option<TraceContext>,
}

pub struct SandboxResult {
    pub exit_code: i32,
    pub stdout: String,
    pub stderr: String,
    pub duration_ms: u64,
    pub memory_peak_bytes: u64,
}

/// 最簡單的「沙箱」：直接 spawn 子程序（開發用）
struct NoneSandbox;

#[async_trait]
impl Sandbox for NoneSandbox {
    async fn execute(&self, ctx: &SandboxContext) -> Result<SandboxResult, SandboxError> {
        let mut cmd = tokio::process::Command::new(&ctx.command);
        cmd.current_dir(&ctx.job_dir)
            .args(&ctx.args)
            .envs(&ctx.env)
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::piped());

        let start = std::time::Instant::now();
        let child = cmd.spawn()?;
        let result = handle_child_process(child, ctx.timeout_secs).await?;

        Ok(SandboxResult {
            exit_code: result.exit_code,
            stdout: result.stdout,
            stderr: result.stderr,
            duration_ms: start.elapsed().as_millis() as u64,
            memory_peak_bytes: result.memory_peak,
        })
    }

    async fn health_check(&self) -> Result<(), SandboxError> { Ok(()) }
    fn name(&self) -> &str { "none" }
}

/// 根據 job 的 tag + 語言選擇最佳沙箱模式
pub struct SandboxRouter {
    rust_native: Option<RustNativeSandbox>,
    nsjail: Option<NsjailSandbox>,
    wasm: Option<WasmSandbox>,
    k8s: Option<K8sPodSandbox>,
    none: NoneSandbox,
}

impl SandboxRouter {
    /// 根據 tag + 語言選擇沙箱
    ///
    /// tag 規則：
    ///   "wasm"       → WASM（微秒級啟動，僅 JS/純 Python）
    ///   "fast"       → Rust 原生 / nsjail（毫秒級，全語言支援）
    ///   "heavy"/"gpu"→ K8s Pod（秒級，自訂 image/GPU）
    ///   "none"/"dev" → 不隔離（開發用）
    ///   預設          → 按優先級自動選擇
    pub fn select(&self, tag: &str, language: Option<ScriptLang>) -> &dyn Sandbox {
        match tag {
            "wasm" => {
                if matches!(language, Some(ScriptLang::TypeScript) | Some(ScriptLang::Python3)) {
                    if let Some(wasm) = &self.wasm { return wasm as &dyn Sandbox; }
                }
                self.select_default()
            }
            "fast" | "native" => {
                self.rust_native.as_ref().map(|s| s as &dyn Sandbox)
                    .or_else(|| self.nsjail.as_ref().map(|s| s as &dyn Sandbox))
                    .unwrap_or(&self.none)
            }
            "nsjail" => self.nsjail.as_ref().map(|s| s as &dyn Sandbox).unwrap_or(&self.none),
            "heavy" | "k8s" | "gpu" => self.k8s.as_ref().map(|s| s as &dyn Sandbox).unwrap_or(&self.none),
            "none" | "dev" => &self.none,
            _ => self.select_default(),
        }
    }

    /// 預設優先級：Rust 原生 → nsjail → WASM → K8s → none
    fn select_default(&self) -> &dyn Sandbox {
        if let Some(rn) = &self.rust_native { rn as &dyn Sandbox }
        else if let Some(nsjail) = &self.nsjail { nsjail as &dyn Sandbox }
        else if let Some(wasm) = &self.wasm { wasm as &dyn Sandbox }
        else if let Some(k8s) = &self.k8s { k8s as &dyn Sandbox }
        else { &self.none }
    }
}
```

**Executor 使用 Sandbox 的方式**（完全不知道用哪種沙箱）：

```rust
// crates/worker/src/python.rs

pub async fn handle_python_job(
    job: &QueuedJob,
    db: &PgPool,
    content: &str,
    job_dir: &str,
    sandbox: &dyn Sandbox,  // ← 注入的沙箱
) -> Result<serde_json::Value> {
    let requirements = parse_python_imports(content);
    if !requirements.is_empty() {
        install_python_deps(&requirements, job_dir, db).await?;
    }

    write_file(job_dir, "inner.py", content)?;
    write_file(job_dir, "wrapper.py", &generate_python_wrapper())?;
    create_args_and_out_file(job, job_dir).await?;

    let ctx = SandboxContext {
        job_id: job.id,
        job_dir: job_dir.to_string(),
        command: "python3".to_string(),
        args: vec!["wrapper.py".to_string()],
        env: get_reserved_variables(job),
        timeout_secs: job.timeout.unwrap_or(3600) as u32,
        language: ScriptLang::Python3,
        custom_image: job.custom_image.clone(),
        trace_context: get_current_trace_context(),
    };

    let result = sandbox.execute(&ctx).await?;
    if result.exit_code != 0 {
        return Err(Error::ExecutionErr(result.stderr));
    }
    read_result(job_dir).await
}
```

### 1.3 Queue 操作

```rust
// crates/queue/src/push.rs

pub async fn push_job(db: &PgPool, args: PushJobArgs<'_>) -> Result<Uuid> {
    let mut tx = db.begin().await?;
    let job_id = Uuid::new_v4();

    // 1. 插入 job（不可變定義）
    sqlx::query!(
        "INSERT INTO job (id, workspace_id, kind, script_hash, script_path,
         raw_code, language, args, tag, parent_job, root_job, flow_step_id,
         created_by, trace_id, span_id)
         VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15)",
        job_id, args.workspace_id, args.kind.as_str(),
        args.script_hash, args.script_path,
        args.raw_code, args.language.map(|l| l.as_str()),
        args.args, args.tag,
        args.parent_job, args.root_job, args.flow_step_id,
        args.created_by, args.trace_id, args.span_id,
    )
    .execute(&mut *tx)
    .await?;

    // 2. 插入 job_queue（可搶的）
    sqlx::query!(
        "INSERT INTO job_queue (id, scheduled_for, tag, priority)
         VALUES ($1, $2, $3, $4)",
        job_id, args.scheduled_for.unwrap_or_else(|| chrono::Utc::now()),
        args.tag, args.priority.unwrap_or(0),
    )
    .execute(&mut *tx)
    .await?;

    // 3. 建立空日誌
    sqlx::query!("INSERT INTO job_log (job_id) VALUES ($1)", job_id)
        .execute(&mut *tx).await?;

    // 4. LISTEN/NOTIFY（比純 polling 快 10x）
    sqlx::query("SELECT pg_notify('new_job', $1)")
        .bind(job_id.to_string())
        .execute(&mut *tx).await?;

    tx.commit().await?;
    Ok(job_id)
}

// crates/queue/src/pull.rs

/// FOR UPDATE SKIP LOCKED — 搶 job
pub async fn pull_job(db: &PgPool, worker_name: &str, tags: &[String]) -> Result<Option<PulledJob>> {
    let row = sqlx::query_as!(PulledJob,
        r#"
        WITH next_job AS (
            SELECT jq.id
            FROM job_queue jq
            WHERE jq.running = FALSE
              AND jq.scheduled_for <= now()
              AND jq.tag = ANY($1)
            ORDER BY jq.priority DESC, jq.scheduled_for ASC
            LIMIT 1
            FOR UPDATE SKIP LOCKED
        )
        UPDATE job_queue
        SET running = TRUE, started_at = now(), worker = $2, last_ping = now()
        FROM next_job
        WHERE job_queue.id = next_job.id
        RETURNING job_queue.id, job_queue.tag
        "#,
        tags, worker_name
    )
    .fetch_optional(db)
    .await?;

    if let Some(row) = row {
        let job = sqlx::query_as!(QueuedJob, "SELECT * FROM job WHERE id = $1", row.id)
            .fetch_one(db).await?;
        Ok(Some(PulledJob { job, tag: row.tag }))
    } else {
        Ok(None)
    }
}

/// LISTEN/NOTIFY 增強版 pull（近零延遲）
async fn pull_with_notify(db: &PgPool, worker: &str, tags: &[String]) -> Result<Option<Job>> {
    let mut listener = sqlx::postgres::PgListener::connect_with(&db).await?;
    listener.listen("new_job").await?;

    loop {
        if let Some(job) = pull_job(db, worker, tags).await? {
            return Ok(Some(job));
        }
        tokio::select! {
            _ = listener.recv() => { /* 收到通知，立即重試 */ }
            _ = tokio::time::sleep(Duration::from_secs(5)) => { /* 安全兜底 */ }
        }
    }
}

// crates/queue/src/complete.rs

pub async fn complete_job(
    db: &PgPool, job_id: Uuid, success: bool,
    result: serde_json::Value, duration_ms: i32,
    memory_peak: i64, s3_key: Option<&str>,
) -> Result<()> {
    let mut tx = db.begin().await?;
    sqlx::query!(
        "INSERT INTO job_completed (id, success, result, result_s3_key, duration_ms, memory_peak_bytes)
         VALUES ($1, $2, $3, $4, $5, $6)",
        job_id, success,
        if s3_key.is_some() { None } else { Some(result) },
        s3_key, duration_ms, memory_peak,
    )
    .execute(&mut *tx).await?;

    sqlx::query!("DELETE FROM job_queue WHERE id = $1", job_id)
        .execute(&mut *tx).await?;
    tx.commit().await?;
    Ok(())
}
```

### 1.4 Worker 主迴圈

```rust
// crates/worker/src/worker.rs

use opentelemetry::trace::{Tracer, SpanKind};

pub async fn run_worker(
    db: PgPool, worker_name: String, tags: Vec<String>,
    sandbox_router: Arc<SandboxRouter>,
    tracer: opentelemetry::global::BoxedTracer,
) {
    let worker_dir = format!("/tmp/flowforge/{}", worker_name);
    tokio::fs::create_dir_all(&worker_dir).await.unwrap();

    // 健康檢查：定期 ping
    let db2 = db.clone();
    let wn = worker_name.clone();
    tokio::spawn(async move {
        loop {
            sqlx::query!(
                "INSERT INTO worker_ping (worker, ping_at, tags)
                 VALUES ($1, now(), $2)
                 ON CONFLICT (worker) DO UPDATE SET ping_at = now()",
                wn, &tags_for_ping
            ).execute(&db2).await.ok();
            tokio::time::sleep(std::time::Duration::from_secs(15)).await;
        }
    });

    loop {
        match queue::pull_job(&db, &worker_name, &tags).await {
            Ok(Some(pulled)) => {
                let job = pulled.job;
                let job_dir = format!("{}/{}", worker_dir, job.id);
                tokio::fs::create_dir_all(&job_dir).await.unwrap();

                // OTel: 建立 span
                let span = tracer.span_builder(format!("job.{}", job.kind))
                    .with_kind(SpanKind::Consumer)
                    .with_attributes(vec![
                        KeyValue::new("job.id", job.id.to_string()),
                        KeyValue::new("job.workspace", job.workspace_id.clone()),
                        KeyValue::new("job.tag", pulled.tag.clone()),
                    ])
                    .start(&tracer);
                let cx = opentelemetry::Context::current_with_span(span);

                let sandbox = sandbox_router.select(&pulled.tag, None);
                let start = std::time::Instant::now();
                let result = handle_job(&job, &db, &job_dir, sandbox).await;
                let duration_ms = start.elapsed().as_millis() as i32;

                match result {
                    Ok((value, mem_peak)) => {
                        let s3_key = maybe_upload_to_s3(&value).await;
                        queue::complete_job(&db, job.id, true, value, duration_ms, mem_peak, s3_key.as_deref()).await.ok();
                        cx.span().set_status(opentelemetry::trace::StatusCode::Ok, "".into());
                    }
                    Err(e) => {
                        let error = serde_json::json!({"error": {"message": e.to_string()}});
                        queue::complete_job(&db, job.id, false, error, duration_ms, 0, None).await.ok();
                        cx.span().set_status(opentelemetry::trace::StatusCode::Error, e.to_string());
                    }
                }
                cx.span().end();
                tokio::fs::remove_dir_all(&job_dir).await.ok();

                // Flow child job 完成 → 通知 flow engine
                if let Some(parent_job) = job.parent_job {
                    update_flow_after_job_completion(&db, parent_job, job.id).await.ok();
                }
            }
            Ok(None) => tokio::time::sleep(std::time::Duration::from_millis(500)).await,
            Err(e) => {
                tracing::error!("Error pulling job: {:?}", e);
                tokio::time::sleep(std::time::Duration::from_secs(5)).await;
            }
        }
    }
}

async fn handle_job(
    job: &QueuedJob, db: &PgPool, job_dir: &str, sandbox: &dyn Sandbox,
) -> Result<(serde_json::Value, i64)> {
    match job.kind.as_str() {
        "flow" | "flow_preview" => handle_flow_job(job, db, job_dir, sandbox).await,
        _ => {
            let (content, language) = get_job_content(job, db).await?;
            match language {
                ScriptLang::Python3 => handle_python_job(job, db, &content, job_dir, sandbox).await,
                ScriptLang::Bash => handle_bash_job(job, db, &content, job_dir, sandbox).await,
                ScriptLang::TypeScript => handle_ts_job(job, db, &content, job_dir, sandbox).await,
                ScriptLang::DuckDB => handle_duckdb_job(job, db, &content, job_dir).await,
                _ => Err(Error::UnsupportedLanguage(language)),
            }
        }
    }
}
```

### 1.5 Python Wrapper

```python
# 自動生成的 wrapper.py
import json
import sys
import traceback

from inner import main

with open("args.json") as f:
    args = json.load(f)

try:
    result = main(**args)
except Exception as e:
    result = {
        "error": {
            "message": str(e),
            "name": type(e).__name__,
            "stack_trace": traceback.format_exc(),
        }
    }
    with open("result.json", "w") as f:
        json.dump(result, f)
    sys.exit(1)

with open("result.json", "w") as f:
    json.dump(result, f, default=str)
```

### 1.6 後端 API

#### Router 組裝

```rust
// crates/api/src/lib.rs

pub fn create_router(db: PgPool, sandbox: Arc<SandboxRouter>) -> Router {
    let auth_middleware = middleware::from_fn_with_state(db.clone(), auth::require_auth);

    Router::new()
        .route("/api/auth/login", post(auth::login))
        .route("/api/auth/signup", post(auth::signup))
        .nest("/api/w/:workspace_id", Router::new()
            // Scripts
            .route("/scripts/create", post(scripts::create_script))
            .route("/scripts/list", get(scripts::list_scripts))
            .route("/scripts/get/p/*path", get(scripts::get_script_by_path))
            .route("/scripts/get/h/:hash", get(scripts::get_script_by_hash))
            // Flows + 版本控制（Phase 2）
            .route("/flows/save/p/*path", post(flows::save_flow))
            .route("/flows/list", get(flows::list_flows))
            .route("/flows/get/p/*path", get(flows::get_flow))
            .route("/flows/revisions/p/*path", get(flows::list_flow_revisions))
            .route("/flows/diff/p/*path", get(flows::diff_flow_revisions))
            .route("/flows/rollback/p/*path/rev/:rev", post(flows::rollback_flow))
            // Flow 工作區檔案（Phase 2）
            .route("/flows/files/p/*path", get(flow_files::list_flow_files).put(flow_files::upsert_flow_file))
            .route("/flows/files/p/*path/f/*file_path", get(flow_files::get_flow_file).delete(flow_files::delete_flow_file))
            // Jobs
            .route("/jobs/run/p/*path", post(jobs::run_script_by_path))
            .route("/jobs/run/f/*path", post(jobs::run_flow))
            .route("/jobs/run/preview", post(jobs::run_preview))
            .route("/jobs/:id", get(jobs::get_job))
            .route("/jobs/:id/result", get(jobs::get_job_result))
            .route("/jobs/:id/logs", get(sse::stream_job_logs))
            .route("/jobs/:id/flow_status", get(jobs::get_flow_status))
            .route("/jobs/list", get(jobs::list_jobs))
            // Data Preview（Phase 4）
            .route("/data/preview", post(data_preview::preview_query))
            // Concurrency Limits（Phase 3）
            .route("/concurrency_limits", get(concurrency::list_limits))
            .route("/concurrency_limits/:tag", put(concurrency::set_limit).delete(concurrency::delete_limit))
            .layer(auth_middleware)
        )
        .with_state(AppState { db, sandbox })
}
```

#### Auth（JWT + Argon2）

```rust
// crates/api/src/auth.rs

use argon2::{Argon2, PasswordHash, PasswordVerifier, PasswordHasher};
use jsonwebtoken::{encode, decode, Header, Validation, EncodingKey, DecodingKey};

#[derive(serde::Serialize, serde::Deserialize)]
struct Claims { email: String, exp: u64 }

pub async fn login(
    State(db): State<PgPool>,
    Json(req): Json<LoginRequest>,
) -> Result<Json<LoginResponse>, ApiError> {
    let account = sqlx::query_as!(Account,
        "SELECT email, password_hash FROM account WHERE email = $1", req.email
    )
    .fetch_optional(&db).await?
    .ok_or(ApiError::Unauthorized)?;

    let parsed_hash = PasswordHash::new(&account.password_hash)
        .map_err(|_| ApiError::Internal("hash parse error".into()))?;
    Argon2::default()
        .verify_password(req.password.as_bytes(), &parsed_hash)
        .map_err(|_| ApiError::Unauthorized)?;

    let claims = Claims {
        email: account.email.clone(),
        exp: (chrono::Utc::now() + chrono::Duration::hours(24)).timestamp() as u64,
    };
    let token = encode(&Header::default(), &claims, &EncodingKey::from_secret(JWT_SECRET.as_bytes()))?;
    Ok(Json(LoginResponse { token }))
}
```

#### Script CRUD

```rust
// crates/api/src/scripts.rs

use sha2::{Sha256, Digest};

pub async fn create_script(
    State(state): State<AppState>,
    Path(workspace_id): Path<String>,
    Extension(user): Extension<AuthedUser>,
    Json(req): Json<CreateScriptRequest>,
) -> Result<Json<ScriptCreated>, ApiError> {
    let hash = {
        let mut hasher = Sha256::new();
        hasher.update(&req.content);
        hasher.update(&req.path);
        hasher.update(req.language.as_str());
        format!("{:x}", hasher.finalize())
    };

    let parent = sqlx::query_scalar!(
        "SELECT hash FROM script WHERE workspace_id = $1 AND path = $2
         ORDER BY created_at DESC LIMIT 1",
        workspace_id, req.path
    ).fetch_optional(&state.db).await?;

    let parent_hashes: Vec<String> = parent.into_iter().collect();
    let schema = match req.language {
        ScriptLang::Python3 => parse_python_signature(&req.content)?,
        ScriptLang::TypeScript => parse_typescript_signature(&req.content)?,
        _ => None,
    };

    sqlx::query!(
        "INSERT INTO script (workspace_id, hash, path, content, language, schema, parent_hashes, created_by)
         VALUES ($1, $2, $3, $4, $5, $6, $7, $8)",
        workspace_id, hash, req.path, req.content,
        req.language.as_str(), schema, &parent_hashes, user.email
    ).execute(&state.db).await?;

    Ok(Json(ScriptCreated { hash }))
}
```

#### Job 執行

```rust
// crates/api/src/jobs.rs

pub async fn run_script_by_path(
    State(state): State<AppState>,
    Path((workspace_id, path)): Path<(String, String)>,
    Extension(user): Extension<AuthedUser>,
    Json(args): Json<serde_json::Value>,
) -> Result<Json<JobCreated>, ApiError> {
    let script = sqlx::query_as!(Script,
        "SELECT * FROM script WHERE workspace_id = $1 AND path = $2
         ORDER BY created_at DESC LIMIT 1",
        workspace_id, path
    ).fetch_optional(&state.db).await?.ok_or(ApiError::NotFound)?;

    let job_id = queue::push_job(&state.db, PushJobArgs {
        workspace_id: &workspace_id,
        kind: JobKind::Script,
        script_hash: Some(&script.hash),
        script_path: Some(&script.path),
        language: Some(script.language),
        args: Some(args),
        tag: "default",
        created_by: &user.email,
        trace_id: current_trace_id(),
        span_id: current_span_id(),
        ..Default::default()
    }).await?;

    Ok(Json(JobCreated { id: job_id }))
}

pub async fn run_preview(
    State(state): State<AppState>,
    Path(workspace_id): Path<String>,
    Extension(user): Extension<AuthedUser>,
    Json(req): Json<PreviewRequest>,
) -> Result<Json<JobCreated>, ApiError> {
    let job_id = queue::push_job(&state.db, PushJobArgs {
        workspace_id: &workspace_id,
        kind: JobKind::Preview,
        raw_code: Some(&req.content),
        language: Some(req.language),
        args: req.args,
        tag: req.tag.as_deref().unwrap_or("default"),
        created_by: &user.email,
        trace_id: current_trace_id(),
        span_id: current_span_id(),
        ..Default::default()
    }).await?;

    Ok(Json(JobCreated { id: job_id }))
}
```

#### SSE 日誌串流

```rust
// crates/api/src/sse.rs

use axum::response::sse::{Event, Sse};
use futures::stream::Stream;

pub async fn stream_job_logs(
    State(state): State<AppState>,
    Path((workspace_id, job_id)): Path<(String, Uuid)>,
) -> Sse<impl Stream<Item = Result<Event, axum::Error>>> {
    let db = state.db.clone();

    let stream = async_stream::stream! {
        let mut last_offset = 0;

        loop {
            let row = sqlx::query!(
                "SELECT logs, log_offset FROM job_log WHERE job_id = $1", job_id
            ).fetch_optional(&db).await;

            if let Ok(Some(row)) = row {
                let current_len = row.logs.len();
                if current_len > last_offset {
                    let new_logs = &row.logs[last_offset..];
                    yield Ok(Event::default().event("log").data(new_logs));
                    last_offset = current_len;
                }
            }

            let completed = sqlx::query_scalar!(
                "SELECT EXISTS(SELECT 1 FROM job_completed WHERE id = $1)", job_id
            ).fetch_one(&db).await;

            if let Ok(Some(true)) = completed {
                let result = sqlx::query!(
                    "SELECT success, result FROM job_completed WHERE id = $1", job_id
                ).fetch_optional(&db).await;

                if let Ok(Some(r)) = result {
                    yield Ok(Event::default().event("result").data(
                        serde_json::to_string(&serde_json::json!({
                            "success": r.success, "result": r.result,
                        })).unwrap()
                    ));
                }
                break;
            }

            tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
        }
    };

    Sse::new(stream)
}
```

### 1.7 前端元件

#### ScriptEditor.svelte

```svelte
<!-- src/lib/components/ScriptEditor.svelte -->
<script lang="ts">
  import { onMount } from 'svelte'
  import type { editor } from 'monaco-editor'

  let {
    content = $bindable(),
    language = 'python',
    onRun,
  }: {
    content: string
    language?: string
    onRun?: (content: string) => void
  } = $props()

  let editorContainer: HTMLDivElement
  let monacoEditor: editor.IStandaloneCodeEditor

  onMount(async () => {
    const monaco = await import('monaco-editor')
    monacoEditor = monaco.editor.create(editorContainer, {
      value: content,
      language: language === 'python3' ? 'python' : language,
      theme: 'vs-dark',
      minimap: { enabled: false },
      fontSize: 14,
      automaticLayout: true,
    })

    monacoEditor.onDidChangeModelContent(() => { content = monacoEditor.getValue() })

    monacoEditor.addAction({
      id: 'run-script',
      label: 'Run Script',
      keybindings: [monaco.KeyMod.CtrlCmd | monaco.KeyCode.Enter],
      run: () => onRun?.(content),
    })
  })
</script>

<div class="editor-wrapper">
  <div class="toolbar">
    <select bind:value={language}>
      <option value="python3">Python</option>
      <option value="bash">Bash</option>
      <option value="typescript">TypeScript</option>
    </select>
    <button onclick={() => onRun?.(content)}>Run</button>
  </div>
  <div bind:this={editorContainer} class="editor-container"></div>
</div>

<style>
  .editor-wrapper { display: flex; flex-direction: column; height: 100%; }
  .toolbar { display: flex; gap: 8px; padding: 8px; background: #1e1e1e; border-bottom: 1px solid #333; }
  .editor-container { flex: 1; }
</style>
```

#### LogViewer.svelte

```svelte
<!-- src/lib/components/LogViewer.svelte -->
<script lang="ts">
  let {
    jobId, workspaceId, onResult,
  }: {
    jobId: string
    workspaceId: string
    onResult?: (result: { success: boolean; result: any }) => void
  } = $props()

  let logs = $state('')
  let status = $state<'running' | 'success' | 'failure'>('running')
  let logContainer: HTMLPreElement

  $effect(() => {
    if (!jobId) return
    const eventSource = new EventSource(`/api/w/${workspaceId}/jobs/${jobId}/logs`)

    eventSource.addEventListener('log', (e) => {
      logs += e.data
      if (logContainer) logContainer.scrollTop = logContainer.scrollHeight
    })

    eventSource.addEventListener('result', (e) => {
      const data = JSON.parse(e.data)
      status = data.success ? 'success' : 'failure'
      onResult?.(data)
      eventSource.close()
    })

    eventSource.onerror = () => { status = 'failure'; eventSource.close() }
    return () => eventSource.close()
  })
</script>

<div class="log-viewer">
  <div class="status-bar" class:success={status === 'success'} class:failure={status === 'failure'}>
    {#if status === 'running'}Running...{:else if status === 'success'}Completed{:else}Failed{/if}
  </div>
  <pre bind:this={logContainer} class="logs">{logs}</pre>
</div>

<style>
  .log-viewer { display: flex; flex-direction: column; height: 100%; background: #1e1e1e; color: #d4d4d4; font-family: 'Fira Code', monospace; }
  .status-bar { padding: 4px 12px; background: #333; font-size: 12px; }
  .status-bar.success { color: #4ec9b0; }
  .status-bar.failure { color: #f44747; }
  .logs { flex: 1; overflow-y: auto; padding: 12px; margin: 0; white-space: pre-wrap; font-size: 13px; }
</style>
```

### 1.8 程式進入點 + 基礎建設

#### main.rs

```rust
// backend/src/main.rs

use opentelemetry::global;
use opentelemetry_otlp::WithExportConfig;
use tracing_subscriber::layer::SubscriberExt;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // 1. OTel 初始化（Day 1）
    let tracer = opentelemetry_otlp::new_pipeline()
        .tracing()
        .with_exporter(opentelemetry_otlp::new_exporter().tonic().with_endpoint("http://localhost:4317"))
        .install_batch(opentelemetry_sdk::runtime::Tokio)?;

    let telemetry = tracing_opentelemetry::layer().with_tracer(tracer.clone());
    let subscriber = tracing_subscriber::registry()
        .with(tracing_subscriber::fmt::layer())
        .with(telemetry);
    tracing::subscriber::set_global_default(subscriber)?;

    // 2. DB 連線 + Migration
    let db = sqlx::PgPool::connect(
        &std::env::var("DATABASE_URL")
            .unwrap_or_else(|_| "postgres://postgres:changeme@localhost:5432/flowforge".into())
    ).await?;
    sqlx::migrate!("./migrations").run(&db).await?;

    // 3. Sandbox Router
    let config: WorkerConfig = load_config()?;
    let sandbox = Arc::new(SandboxRouter::from_config(&config));

    // 4. API Server
    let app = api::create_router(db.clone(), sandbox.clone());
    let server = tokio::spawn(async move {
        let addr = "0.0.0.0:8000";
        tracing::info!("API server listening on {}", addr);
        let listener = tokio::net::TcpListener::bind(addr).await.unwrap();
        axum::serve(listener, app).await.unwrap();
    });

    // 5. Workers
    let num_workers = config.num_workers.unwrap_or(4);
    let workers: Vec<_> = (0..num_workers).map(|i| {
        let db = db.clone();
        let sandbox = sandbox.clone();
        let tags = config.tags.clone();
        let tracer = global::tracer("flowforge-worker");
        tokio::spawn(async move {
            worker::run_worker(db, format!("worker-{}", i), tags, sandbox, tracer).await;
        })
    }).collect();

    // 6. 等待結束
    tokio::select! {
        _ = server => {},
        _ = futures::future::join_all(workers) => {},
        _ = tokio::signal::ctrl_c() => { tracing::info!("Shutting down..."); }
    }

    global::shutdown_tracer_provider();
    Ok(())
}
```

#### docker-compose.yml

```yaml
version: "3.8"

services:
  postgres:
    image: postgres:16-alpine
    environment:
      POSTGRES_DB: flowforge
      POSTGRES_USER: postgres
      POSTGRES_PASSWORD: changeme
    ports: ["5432:5432"]
    volumes: [pgdata:/var/lib/postgresql/data]

  minio:
    image: minio/minio
    command: server /data --console-address ":9001"
    environment:
      MINIO_ROOT_USER: minioadmin
      MINIO_ROOT_PASSWORD: minioadmin
    ports: ["9000:9000", "9001:9001"]
    volumes: [miniodata:/data]

  jaeger:
    image: jaegertracing/all-in-one:latest
    environment:
      COLLECTOR_OTLP_ENABLED: true
    ports: ["16686:16686", "4317:4317", "4318:4318"]

volumes:
  pgdata:
  miniodata:
```

### 1.9 驗證方式

```
# 後端
cargo test
cargo run    # 啟動 server + worker

# 前端
npm run dev  # http://localhost:5173

# 手動測試
1. 登入 → 建立 Python script → Run → 看 SSE 日誌串流 → 看結果
2. 建立有 bug 的 script → Run → 確認錯誤訊息正確
3. 同時 Run 多個 job → 確認 queue 用 FOR UPDATE SKIP LOCKED 正常分配
4. 設定 tag="k8s" → 確認 job 走 K8s Pod 執行
5. Jaeger UI → 確認 trace 有 job span
```

---

## Phase 2：Flow Engine（Week 4-6）

### 目標

能在網頁上用拖拉方式設計工作流（DAG），步驟之間傳遞資料

### 2.1 Flow 資料模型

複製 Windmill 經過驗證的設計，關鍵型別：

```rust
// crates/types/src/flows.rs

/// 整個 Flow 的定義，存在 flow.value JSONB
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FlowValue {
    pub modules: Vec<FlowModule>,
    pub failure_module: Option<Box<FlowModule>>,
    pub same_worker: bool,
    pub concurrent_limit: Option<u32>,
}

/// 一個步驟
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FlowModule {
    pub id: String,                  // "a", "b", "c"...
    pub value: FlowModuleValue,
    pub retry: Option<Retry>,
    pub sleep: Option<InputTransform>,
    pub summary: Option<String>,
}

/// 步驟類型
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type")]
pub enum FlowModuleValue {
    /// 引用已存在的 script
    Script {
        path: String,
        hash: Option<String>,
        input_transforms: HashMap<String, InputTransform>,
        tag_override: Option<String>,
    },
    /// 內嵌程式碼（不存 script）
    RawScript {
        content: String,
        language: ScriptLang,
        input_transforms: HashMap<String, InputTransform>,
        tag_override: Option<String>,
    },
    /// For 迴圈
    ForloopFlow {
        iterator: InputTransform,
        modules: Vec<FlowModule>,
        parallel: bool,
        skip_failures: bool,
    },
    /// 條件分支（走第一個 true 的）
    BranchOne {
        branches: Vec<Branch>,
        default: Vec<FlowModule>,
    },
    /// 並行分支（所有都跑）
    BranchAll {
        branches: Vec<BranchAllItem>,
        parallel: bool,
    },
    /// 身份轉換（直接傳遞前一步的結果）
    Identity,
}

/// 步驟間資料傳遞
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type")]
pub enum InputTransform {
    Static { value: serde_json::Value },
    /// JavaScript 表達式（用 boa_engine 求值），例如 "results.step_a.count + 1"
    Javascript { expr: String },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Branch {
    pub summary: Option<String>,
    pub expr: String,
    pub modules: Vec<FlowModule>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BranchAllItem {
    pub summary: Option<String>,
    pub modules: Vec<FlowModule>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Retry {
    pub constant: Option<RetryConstant>,
    pub exponential: Option<RetryExponential>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RetryConstant { pub attempts: u32, pub seconds: u32 }

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RetryExponential {
    pub attempts: u32,
    pub multiplier: u32,
    pub seconds: u32,
    pub random_factor: Option<f64>,
}
```

### 2.2 Flow 狀態機

```rust
// crates/types/src/flow_status.rs

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FlowStatus {
    pub step: usize,
    pub modules: Vec<FlowStatusModule>,
    pub failure_module: FlowStatusModule,
    pub retry: RetryStatus,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type")]
pub enum FlowStatusModule {
    WaitingForPriorSteps { id: String },
    InProgress {
        id: String,
        job: Uuid,
        iterator: Option<IteratorStatus>,
        branch_chosen: Option<BranchChosen>,
        parallel: bool,
        flow_jobs: Option<Vec<Uuid>>,
    },
    Success { id: String, job: Uuid, result: serde_json::Value },
    Failure { id: String, job: Uuid, error: serde_json::Value },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IteratorStatus {
    pub index: usize,
    pub itered: Vec<serde_json::Value>,
    pub args: serde_json::Value,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BranchChosen { pub branch: usize }

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RetryStatus {
    pub fail_count: u32,
    pub previous_result: Option<serde_json::Value>,
}
```

### 2.3 Flow Engine 核心

```rust
// crates/worker/src/flow_engine.rs

/// 處理 Flow job — 讀取 FlowStatus，決定下一步
pub async fn handle_flow_job(
    flow_job: &QueuedJob, db: &PgPool, job_dir: &str, sandbox: &dyn Sandbox,
) -> Result<(serde_json::Value, i64)> {
    let flow_value: FlowValue = serde_json::from_value(
        flow_job.flow_value.clone().ok_or(Error::MissingFlowValue)?
    )?;

    let mut status = get_or_init_flow_status(db, flow_job.id, &flow_value).await?;

    loop {
        let step = status.step;
        if step >= flow_value.modules.len() {
            let last_result = status.modules.last()
                .and_then(|m| match m {
                    FlowStatusModule::Success { result, .. } => Some(result.clone()),
                    _ => None,
                })
                .unwrap_or(serde_json::Value::Null);
            return Ok((last_result, 0));
        }

        let module = &flow_value.modules[step];

        match &module.value {
            FlowModuleValue::Script { path, input_transforms, tag_override, .. } => {
                let args = transform_inputs(input_transforms, &status, db).await?;
                let child_id = queue::push_job(db, PushJobArgs {
                    workspace_id: &flow_job.workspace_id,
                    kind: JobKind::Script,
                    script_path: Some(path),
                    args: Some(args),
                    tag: tag_override.as_deref().unwrap_or(&flow_job.tag),
                    parent_job: Some(flow_job.id),
                    root_job: Some(flow_job.root_job.unwrap_or(flow_job.id)),
                    flow_step_id: Some(&module.id),
                    created_by: &flow_job.created_by,
                    ..Default::default()
                }).await?;

                status.modules[step] = FlowStatusModule::InProgress {
                    id: module.id.clone(), job: child_id,
                    iterator: None, branch_chosen: None, parallel: false, flow_jobs: None,
                };
                save_flow_status(db, flow_job.id, &status).await?;
                return Ok((serde_json::Value::Null, 0));
            }
            FlowModuleValue::RawScript { content, language, input_transforms, tag_override } => {
                let args = transform_inputs(input_transforms, &status, db).await?;
                let child_id = queue::push_job(db, PushJobArgs {
                    workspace_id: &flow_job.workspace_id,
                    kind: JobKind::Preview,
                    raw_code: Some(content),
                    language: Some(*language),
                    args: Some(args),
                    tag: tag_override.as_deref().unwrap_or(&flow_job.tag),
                    parent_job: Some(flow_job.id),
                    root_job: Some(flow_job.root_job.unwrap_or(flow_job.id)),
                    flow_step_id: Some(&module.id),
                    created_by: &flow_job.created_by,
                    ..Default::default()
                }).await?;

                status.modules[step] = FlowStatusModule::InProgress {
                    id: module.id.clone(), job: child_id,
                    iterator: None, branch_chosen: None, parallel: false, flow_jobs: None,
                };
                save_flow_status(db, flow_job.id, &status).await?;
                return Ok((serde_json::Value::Null, 0));
            }
            FlowModuleValue::ForloopFlow { iterator, modules, parallel, skip_failures } => {
                let items = eval_input_transform(iterator, &status)?;
                let items = items.as_array()
                    .ok_or(Error::BadRequest("ForLoop iterator must be an array"))?;

                if *parallel {
                    let mut child_ids = Vec::new();
                    for (i, item) in items.iter().enumerate() {
                        let child_id = push_forloop_iteration(db, flow_job, module, modules, item, i).await?;
                        child_ids.push(child_id);
                    }
                    status.modules[step] = FlowStatusModule::InProgress {
                        id: module.id.clone(), job: Uuid::nil(),
                        iterator: Some(IteratorStatus { index: items.len(), itered: items.clone(), args: serde_json::Value::Null }),
                        branch_chosen: None, parallel: true, flow_jobs: Some(child_ids),
                    };
                } else {
                    if let Some(first) = items.first() {
                        let child_id = push_forloop_iteration(db, flow_job, module, modules, first, 0).await?;
                        status.modules[step] = FlowStatusModule::InProgress {
                            id: module.id.clone(), job: child_id,
                            iterator: Some(IteratorStatus { index: 0, itered: items.clone(), args: serde_json::Value::Null }),
                            branch_chosen: None, parallel: false, flow_jobs: None,
                        };
                    }
                }
                save_flow_status(db, flow_job.id, &status).await?;
                return Ok((serde_json::Value::Null, 0));
            }
            FlowModuleValue::BranchOne { branches, default } => {
                let mut chosen = None;
                for (i, branch) in branches.iter().enumerate() {
                    let result = eval_js_expr(&branch.expr, &status)?;
                    if result.as_bool().unwrap_or(false) {
                        chosen = Some((i, &branch.modules));
                        break;
                    }
                }
                let (branch_idx, modules) = chosen.unwrap_or((branches.len(), default));
                let child_id = push_branch_as_flow(db, flow_job, module, modules).await?;

                status.modules[step] = FlowStatusModule::InProgress {
                    id: module.id.clone(), job: child_id,
                    iterator: None, branch_chosen: Some(BranchChosen { branch: branch_idx }),
                    parallel: false, flow_jobs: None,
                };
                save_flow_status(db, flow_job.id, &status).await?;
                return Ok((serde_json::Value::Null, 0));
            }
            FlowModuleValue::BranchAll { branches, parallel } => {
                let mut child_ids = Vec::new();
                for branch in branches {
                    let child_id = push_branch_as_flow(db, flow_job, module, &branch.modules).await?;
                    child_ids.push(child_id);
                }
                status.modules[step] = FlowStatusModule::InProgress {
                    id: module.id.clone(), job: Uuid::nil(),
                    iterator: None, branch_chosen: None,
                    parallel: *parallel, flow_jobs: Some(child_ids),
                };
                save_flow_status(db, flow_job.id, &status).await?;
                return Ok((serde_json::Value::Null, 0));
            }
            FlowModuleValue::Identity => {
                let prev_result = if step > 0 {
                    match &status.modules[step - 1] {
                        FlowStatusModule::Success { result, .. } => result.clone(),
                        _ => serde_json::Value::Null,
                    }
                } else { serde_json::Value::Null };
                status.modules[step] = FlowStatusModule::Success {
                    id: module.id.clone(), job: Uuid::nil(), result: prev_result,
                };
                status.step += 1;
                save_flow_status(db, flow_job.id, &status).await?;
            }
        }
    }
}

/// Child job 完成後更新 Flow 狀態
pub async fn update_flow_after_job_completion(
    db: &PgPool, flow_job_id: Uuid, child_job_id: Uuid,
) -> Result<()> {
    let completed = sqlx::query!(
        "SELECT success, result FROM job_completed WHERE id = $1", child_job_id
    ).fetch_one(db).await?;

    let mut status = get_flow_status(db, flow_job_id).await?;
    let step = status.step;

    if completed.success {
        status.modules[step] = FlowStatusModule::Success {
            id: status.modules[step].id().to_string(),
            job: child_job_id,
            result: completed.result.unwrap_or(serde_json::Value::Null),
        };
        status.step += 1;
        status.retry.fail_count = 0;
    } else {
        let flow_value = get_flow_value(db, flow_job_id).await?;
        let module = &flow_value.modules[step];
        if let Some(retry) = &module.retry {
            if should_retry(&status.retry, retry) {
                status.retry.fail_count += 1;
                save_flow_status(db, flow_job_id, &status).await?;
                return Ok(());
            }
        }

        status.modules[step] = FlowStatusModule::Failure {
            id: status.modules[step].id().to_string(),
            job: child_job_id,
            error: completed.result.unwrap_or(serde_json::Value::Null),
        };

        if let Some(failure_module) = &flow_value.failure_module {
            // ... 推入 failure_module 作為 child job ...
        }

        save_flow_status(db, flow_job_id, &status).await?;
        queue::complete_job(db, flow_job_id, false, /* ... */).await?;
        return Ok(());
    }

    save_flow_status(db, flow_job_id, &status).await?;
    queue::re_enqueue_flow(db, flow_job_id).await?;
    Ok(())
}
```

### 2.4 JS 表達式求值（boa_engine）

```rust
// crates/jseval/src/lib.rs

use boa_engine::{Context, JsValue, Source, property::Attribute};

/// 求值 InputTransform 中的 JavaScript 表達式
///
/// 可用變數：
/// - `results.{step_id}` → 該步驟的結果
/// - `flow_input` → Flow 的輸入參數
/// - `previous_result` → 前一步的結果
pub fn eval_js_expr(
    expr: &str,
    results: &HashMap<String, serde_json::Value>,
    flow_input: &serde_json::Value,
    previous_result: &serde_json::Value,
) -> Result<serde_json::Value, JsEvalError> {
    let mut context = Context::default();

    let results_obj = serde_json_to_js_value(&serde_json::to_value(results)?, &mut context)?;
    context.register_global_property("results", results_obj, Attribute::READONLY)?;

    let flow_input_val = serde_json_to_js_value(flow_input, &mut context)?;
    context.register_global_property("flow_input", flow_input_val, Attribute::READONLY)?;

    let prev_val = serde_json_to_js_value(previous_result, &mut context)?;
    context.register_global_property("previous_result", prev_val, Attribute::READONLY)?;

    let result = context.eval(Source::from_bytes(expr.as_bytes()))
        .map_err(|e| JsEvalError::EvalError(e.to_string()))?;

    js_value_to_serde_json(&result, &mut context)
}

fn serde_json_to_js_value(val: &serde_json::Value, ctx: &mut Context) -> Result<JsValue, JsEvalError> {
    match val {
        serde_json::Value::Null => Ok(JsValue::null()),
        serde_json::Value::Bool(b) => Ok(JsValue::from(*b)),
        serde_json::Value::Number(n) => Ok(JsValue::from(n.as_f64().unwrap_or(0.0))),
        serde_json::Value::String(s) => Ok(JsValue::from(boa_engine::JsString::from(s.as_str()))),
        serde_json::Value::Array(arr) => {
            let js_arr = boa_engine::object::builtins::JsArray::new(ctx);
            for item in arr {
                let js_item = serde_json_to_js_value(item, ctx)?;
                js_arr.push(js_item, ctx)?;
            }
            Ok(js_arr.into())
        }
        serde_json::Value::Object(obj) => {
            let js_obj = boa_engine::JsObject::with_object_proto(ctx.intrinsics());
            for (key, value) in obj {
                let js_val = serde_json_to_js_value(value, ctx)?;
                js_obj.set(
                    boa_engine::property::PropertyKey::from(boa_engine::JsString::from(key.as_str())),
                    js_val, false, ctx,
                )?;
            }
            Ok(js_obj.into())
        }
    }
}
```

### 2.5 前端 Flow Editor（VS Code 風格 + @xyflow/svelte）

整個 Flow Editor 頁面整合了：DAG 畫布 + 檔案樹 + Monaco 程式碼編輯 + 版本控制面板。

```svelte
<!-- src/lib/components/FlowEditor.svelte — 主容器 -->
<script lang="ts">
  import { SvelteFlow, Controls, Background, MiniMap, type Node, type Edge } from '@xyflow/svelte'
  import '@xyflow/svelte/dist/style.css'
  import StepNode from './flow/StepNode.svelte'
  import FlowFileTree from './FlowFileTree.svelte'
  import FlowVersionPanel from './FlowVersionPanel.svelte'
  import ScriptEditor from './ScriptEditor.svelte'
  import StepConfigPanel from './flow/StepConfigPanel.svelte'

  let {
    workspaceId,
    flowPath,
    flowValue = $bindable(),
    revision = $bindable(),
    onSave,
  }: {
    workspaceId: string
    flowPath: string
    flowValue: FlowValue
    revision: number
    onSave?: (flow: FlowValue) => Promise<void>
  } = $props()

  const nodeTypes = { step: StepNode }

  // === DAG 狀態 ===
  let nodes = $state<Node[]>([])
  let edges = $state<Edge[]>([])
  let selectedNodeId = $state<string | null>(null)

  // === 編輯器 tab 狀態 ===
  type TabItem = { id: string; name: string; type: 'dag' | 'code' | 'version' }
  let openTabs = $state<TabItem[]>([{ id: '__dag__', name: 'DAG', type: 'dag' }])
  let activeTabId = $state('__dag__')

  // === 檔案樹狀態 ===
  let flowFiles = $state<{ file_path: string; content: string }[]>([])

  $effect(() => {
    loadFlowFiles()
  })

  async function loadFlowFiles() {
    const resp = await fetch(`/api/w/${workspaceId}/flows/files/p/${flowPath}`)
    flowFiles = await resp.json()
  }

  $effect(() => {
    const { n, e } = flowValueToGraph(flowValue)
    nodes = n
    edges = e
  })

  function flowValueToGraph(flow: FlowValue): { n: Node[]; e: Edge[] } {
    const n: Node[] = []
    const e: Edge[] = []
    flow.modules.forEach((mod, i) => {
      n.push({
        id: mod.id, type: 'step',
        position: { x: 300, y: i * 150 },
        data: { module: mod, index: i },
      })
      if (i > 0) {
        e.push({
          id: `${flow.modules[i - 1].id}-${mod.id}`,
          source: flow.modules[i - 1].id, target: mod.id, animated: true,
        })
      }
    })
    return { n, e }
  }

  // 點擊 DAG 節點 → 如果是 RawScript 就開 tab
  function onNodeClick(event: CustomEvent) {
    const nodeId = event.detail.node.id
    selectedNodeId = nodeId
    const mod = flowValue.modules.find(m => m.id === nodeId)
    if (mod?.value.type === 'RawScript') {
      openCodeTab(nodeId, mod)
    }
  }

  // 點擊檔案樹 → 開 tab
  function onFileClick(filePath: string) {
    const tabId = `file:${filePath}`
    if (!openTabs.find(t => t.id === tabId)) {
      openTabs = [...openTabs, { id: tabId, name: filePath.split('/').pop()!, type: 'code' }]
    }
    activeTabId = tabId
  }

  function openCodeTab(moduleId: string, mod: FlowModule) {
    const tabId = `step:${moduleId}`
    if (!openTabs.find(t => t.id === tabId)) {
      const ext = mod.value.language === 'python3' ? '.py' : '.ts'
      openTabs = [...openTabs, { id: tabId, name: `${moduleId}${ext}`, type: 'code' }]
    }
    activeTabId = tabId
  }

  function closeTab(tabId: string) {
    openTabs = openTabs.filter(t => t.id !== tabId)
    if (activeTabId === tabId) activeTabId = '__dag__'
  }

  function addStep(type: string) {
    const id = String.fromCharCode(97 + flowValue.modules.length)
    const newModule: FlowModule = {
      id,
      value: type === 'script'
        ? { type: 'Script', path: '', input_transforms: {} }
        : { type: 'RawScript', content: 'def main():\n    return "hello"', language: 'python3', input_transforms: {} },
      retry: null, sleep: null, summary: `Step ${id}`,
    }
    flowValue.modules = [...flowValue.modules, newModule]
  }

  // 取得目前 tab 對應的內容
  let activeContent = $derived.by(() => {
    if (activeTabId.startsWith('step:')) {
      const moduleId = activeTabId.replace('step:', '')
      const mod = flowValue.modules.find(m => m.id === moduleId)
      return mod?.value.type === 'RawScript' ? mod.value.content : ''
    }
    if (activeTabId.startsWith('file:')) {
      const filePath = activeTabId.replace('file:', '')
      return flowFiles.find(f => f.file_path === filePath)?.content ?? ''
    }
    return ''
  })

  let selectedModule = $derived(
    selectedNodeId ? flowValue.modules.find(m => m.id === selectedNodeId) : null
  )
</script>

<div class="flow-editor-ide">
  <!-- 左側：步驟庫 + 檔案樹 + 小 DAG -->
  <div class="sidebar">
    <div class="step-palette">
      <h3>Steps</h3>
      <button onclick={() => addStep('script')}>+ Script</button>
      <button onclick={() => addStep('raw')}>+ Inline Code</button>
      <button onclick={() => addStep('forloop')}>+ For Loop</button>
      <button onclick={() => addStep('branch')}>+ Branch</button>
    </div>

    <FlowFileTree
      files={flowFiles}
      onFileClick={onFileClick}
      onCreateFile={async (path) => {
        await fetch(`/api/w/${workspaceId}/flows/files/p/${flowPath}`, {
          method: 'PUT', body: JSON.stringify({ file_path: path, content: '' })
        })
        await loadFlowFiles()
      }}
    />

    <div class="mini-dag">
      <SvelteFlow {nodes} {edges} {nodeTypes} fitView on:nodeclick={onNodeClick}>
        <MiniMap />
      </SvelteFlow>
    </div>
  </div>

  <!-- 右側：Tab bar + 編輯區 -->
  <div class="main-panel">
    <div class="tab-bar">
      {#each openTabs as tab}
        <button class:active={activeTabId === tab.id} onclick={() => activeTabId = tab.id}>
          {tab.name}
          {#if tab.id !== '__dag__'}
            <span class="close" onclick|stopPropagation={() => closeTab(tab.id)}>×</span>
          {/if}
        </button>
      {/each}
      <button class="version-btn" onclick={() => {
        if (!openTabs.find(t => t.id === '__version__')) {
          openTabs = [...openTabs, { id: '__version__', name: `v${revision}`, type: 'version' }]
        }
        activeTabId = '__version__'
      }}>v{revision}</button>
    </div>

    <div class="editor-area">
      {#if activeTabId === '__dag__'}
        <SvelteFlow {nodes} {edges} {nodeTypes} fitView on:nodeclick={onNodeClick}>
          <Controls />
          <Background />
        </SvelteFlow>
      {:else if activeTabId === '__version__'}
        <FlowVersionPanel {workspaceId} {flowPath} currentRevision={revision} />
      {:else}
        <ScriptEditor content={activeContent} language="python3"
          onchange={(c) => {
            if (activeTabId.startsWith('step:')) {
              const moduleId = activeTabId.replace('step:', '')
              flowValue.modules = flowValue.modules.map(m =>
                m.id === moduleId && m.value.type === 'RawScript'
                  ? { ...m, value: { ...m.value, content: c } } : m
              )
            }
            // file: tabs → save to API
          }}
        />
      {/if}
    </div>

    {#if selectedModule && activeTabId === '__dag__'}
      <StepConfigPanel
        module={selectedModule}
        onUpdate={(updated) => {
          flowValue.modules = flowValue.modules.map(m => m.id === updated.id ? updated : m)
        }}
      />
    {/if}
  </div>
</div>

<style>
  .flow-editor-ide { display: grid; grid-template-columns: 220px 1fr; height: 100vh; }
  .sidebar { display: flex; flex-direction: column; border-right: 1px solid #333; background: #252526; }
  .step-palette { padding: 12px; border-bottom: 1px solid #333; }
  .mini-dag { height: 200px; border-top: 1px solid #333; }
  .main-panel { display: flex; flex-direction: column; }
  .tab-bar { display: flex; background: #2d2d2d; border-bottom: 1px solid #333; overflow-x: auto; }
  .tab-bar button { padding: 6px 16px; border: none; background: #2d2d2d; color: #ccc; cursor: pointer; }
  .tab-bar button.active { background: #1e1e1e; color: #fff; border-bottom: 2px solid #007acc; }
  .version-btn { margin-left: auto; font-size: 12px; color: #888; }
  .editor-area { flex: 1; }
</style>
```

**FlowFileTree.svelte — VS Code 風格檔案樹：**

```svelte
<!-- src/lib/components/FlowFileTree.svelte -->
<script lang="ts">
  let {
    files,
    onFileClick,
    onCreateFile,
  }: {
    files: { file_path: string; content: string }[]
    onFileClick: (path: string) => void
    onCreateFile: (path: string) => void
  } = $props()

  // 將扁平檔案列表轉成樹狀結構
  type TreeNode = { name: string; path: string; children?: TreeNode[]; isDir: boolean }

  let tree = $derived.by(() => {
    const root: TreeNode = { name: 'files', path: '', children: [], isDir: true }
    for (const file of files) {
      const parts = file.file_path.split('/')
      let current = root
      for (let i = 0; i < parts.length; i++) {
        const isLast = i === parts.length - 1
        const name = parts[i]
        if (isLast) {
          current.children!.push({ name, path: file.file_path, isDir: false })
        } else {
          let dir = current.children!.find(c => c.name === name && c.isDir)
          if (!dir) {
            dir = { name, path: parts.slice(0, i + 1).join('/'), children: [], isDir: true }
            current.children!.push(dir)
          }
          current = dir
        }
      }
    }
    return root.children!
  })

  let newFileName = $state('')
  let showNewFile = $state(false)
</script>

<div class="file-tree">
  <div class="tree-header">
    <span>FILES</span>
    <button onclick={() => showNewFile = !showNewFile}>+</button>
  </div>
  {#if showNewFile}
    <input bind:value={newFileName} placeholder="e.g. utils/helpers.py"
      onkeydown={(e) => { if (e.key === 'Enter' && newFileName) { onCreateFile(newFileName); newFileName = ''; showNewFile = false; } }}
    />
  {/if}
  {#each tree as node}
    {#if node.isDir}
      <details open>
        <summary>{node.name}/</summary>
        {#each node.children ?? [] as child}
          <button class="file-item" onclick={() => onFileClick(child.path)}>
            {child.name}
          </button>
        {/each}
      </details>
    {:else}
      <button class="file-item" onclick={() => onFileClick(node.path)}>
        {node.name}
      </button>
    {/if}
  {/each}
</div>
```

**FlowVersionPanel.svelte — 版本歷史 + Diff：**

```svelte
<!-- src/lib/components/FlowVersionPanel.svelte -->
<script lang="ts">
  let {
    workspaceId, flowPath, currentRevision,
  }: {
    workspaceId: string
    flowPath: string
    currentRevision: number
  } = $props()

  type RevisionSummary = { revision: number; summary: string; edited_by: string; edited_at: string }

  let revisions = $state<RevisionSummary[]>([])
  let diffResult = $state<any>(null)
  let compareFrom = $state<number | null>(null)

  $effect(() => {
    fetch(`/api/w/${workspaceId}/flows/revisions/p/${flowPath}`)
      .then(r => r.json())
      .then(r => revisions = r)
  })

  async function showDiff(from: number, to: number) {
    const resp = await fetch(`/api/w/${workspaceId}/flows/diff/p/${flowPath}?from=${from}&to=${to}`)
    diffResult = await resp.json()
  }

  async function rollback(targetRevision: number) {
    if (!confirm(`Rollback to revision ${targetRevision}?`)) return
    await fetch(`/api/w/${workspaceId}/flows/rollback/p/${flowPath}/rev/${targetRevision}`, { method: 'POST' })
    location.reload()
  }
</script>

<div class="version-panel">
  <h3>Version History</h3>
  <div class="revision-list">
    {#each revisions as rev}
      <div class="revision-item" class:current={rev.revision === currentRevision}>
        <span class="rev-num">v{rev.revision}</span>
        <span class="rev-info">{rev.edited_by} · {new Date(rev.edited_at).toLocaleString()}</span>
        <div class="rev-actions">
          {#if rev.revision !== currentRevision}
            <button onclick={() => showDiff(rev.revision, currentRevision)}>Diff</button>
            <button onclick={() => rollback(rev.revision)}>Rollback</button>
          {:else}
            <span class="current-badge">current</span>
          {/if}
        </div>
      </div>
    {/each}
  </div>
  {#if diffResult}
    <div class="diff-view">
      <h4>Diff: v{diffResult.from} → v{diffResult.to}</h4>
      <pre>{JSON.stringify(diffResult.changes, null, 2)}</pre>
    </div>
  {/if}
</div>
```

### 2.6 Flow 版本控制 API

```rust
// crates/api/src/flows.rs

/// 建立或更新 Flow（自動建立新 revision）
pub async fn save_flow(
    State(state): State<AppState>,
    Path((workspace_id, path)): Path<(String, String)>,
    Extension(user): Extension<AuthedUser>,
    Json(req): Json<SaveFlowRequest>,
) -> Result<Json<FlowSaved>, ApiError> {
    // 取得目前最新 revision
    let current_rev = sqlx::query_scalar!(
        "SELECT MAX(revision) FROM flow WHERE workspace_id = $1 AND path = $2",
        workspace_id, path
    ).fetch_one(&state.db).await?.unwrap_or(0);

    let new_rev = current_rev + 1;

    sqlx::query!(
        "INSERT INTO flow (workspace_id, path, revision, summary, description, value, schema, edited_by)
         VALUES ($1, $2, $3, $4, $5, $6, $7, $8)",
        workspace_id, path, new_rev,
        req.summary, req.description, req.value, req.schema, user.email,
    ).execute(&state.db).await?;

    // 記錄事件
    emit_event(&state.db, &workspace_id, "flow.saved", "flow", &path,
        serde_json::json!({ "revision": new_rev, "edited_by": user.email })
    ).await?;

    Ok(Json(FlowSaved { path: path.clone(), revision: new_rev }))
}

/// 列出 Flow 版本歷史
pub async fn list_flow_revisions(
    Path((workspace_id, path)): Path<(String, String)>,
) -> Result<Json<Vec<FlowRevisionSummary>>, ApiError> {
    let revisions = sqlx::query_as!(FlowRevisionSummary,
        "SELECT revision, summary, edited_by, edited_at
         FROM flow WHERE workspace_id = $1 AND path = $2
         ORDER BY revision DESC",
        workspace_id, path
    ).fetch_all(&state.db).await?;
    Ok(Json(revisions))
}

/// 取得特定版本
pub async fn get_flow_at_revision(
    Path((workspace_id, path, revision)): Path<(String, String, i32)>,
) -> Result<Json<Flow>, ApiError> {
    let flow = sqlx::query_as!(Flow,
        "SELECT * FROM flow WHERE workspace_id = $1 AND path = $2 AND revision = $3",
        workspace_id, path, revision
    ).fetch_optional(&state.db).await?.ok_or(ApiError::NotFound)?;
    Ok(Json(flow))
}

/// Diff 兩個版本
pub async fn diff_flow_revisions(
    Path((workspace_id, path)): Path<(String, String)>,
    Query(params): Query<DiffParams>,  // ?from=2&to=3
) -> Result<Json<FlowDiff>, ApiError> {
    let from = sqlx::query_scalar!(
        "SELECT value FROM flow WHERE workspace_id = $1 AND path = $2 AND revision = $3",
        workspace_id, path, params.from
    ).fetch_one(&state.db).await?;
    let to = sqlx::query_scalar!(
        "SELECT value FROM flow WHERE workspace_id = $1 AND path = $2 AND revision = $3",
        workspace_id, path, params.to
    ).fetch_one(&state.db).await?;

    let diff = json_diff(&from, &to);  // 用 serde_json 遞迴 diff
    Ok(Json(FlowDiff { from: params.from, to: params.to, changes: diff }))
}

/// Rollback 到指定版本（實際上是複製該版本建立新 revision）
pub async fn rollback_flow(
    Path((workspace_id, path, target_revision)): Path<(String, String, i32)>,
) -> Result<Json<FlowSaved>, ApiError> {
    let old = sqlx::query!(
        "SELECT value, schema, summary FROM flow
         WHERE workspace_id = $1 AND path = $2 AND revision = $3",
        workspace_id, path, target_revision
    ).fetch_one(&state.db).await?;

    // 建立新 revision，內容等於 target
    save_flow(/* ... with old.value, old.schema ... */).await
}
```

### 2.7 Flow 工作區檔案 API

```rust
// crates/api/src/flow_files.rs

/// 列出 flow 的所有工作區檔案
pub async fn list_flow_files(
    Path((workspace_id, flow_path)): Path<(String, String)>,
) -> Result<Json<Vec<FlowFileEntry>>, ApiError> {
    let files = sqlx::query_as!(FlowFileEntry,
        "SELECT file_path, length(content) as size_bytes, updated_at
         FROM flow_file WHERE workspace_id = $1 AND flow_path = $2
         ORDER BY file_path",
        workspace_id, flow_path
    ).fetch_all(&state.db).await?;
    Ok(Json(files))
}

/// 讀取單一檔案
pub async fn get_flow_file(
    Path((workspace_id, flow_path, file_path)): Path<(String, String, String)>,
) -> Result<String, ApiError> {
    let content = sqlx::query_scalar!(
        "SELECT content FROM flow_file
         WHERE workspace_id = $1 AND flow_path = $2 AND file_path = $3",
        workspace_id, flow_path, file_path
    ).fetch_optional(&state.db).await?.ok_or(ApiError::NotFound)?;
    Ok(content)
}

/// 建立或更新檔案
pub async fn upsert_flow_file(
    Path((workspace_id, flow_path)): Path<(String, String)>,
    Json(req): Json<UpsertFlowFileRequest>,
) -> Result<(), ApiError> {
    sqlx::query!(
        "INSERT INTO flow_file (workspace_id, flow_path, file_path, content, updated_at)
         VALUES ($1, $2, $3, $4, now())
         ON CONFLICT (workspace_id, flow_path, file_path) DO UPDATE
         SET content = $4, updated_at = now()",
        workspace_id, flow_path, req.file_path, req.content
    ).execute(&state.db).await?;
    Ok(())
}

/// 刪除檔案
pub async fn delete_flow_file(
    Path((workspace_id, flow_path, file_path)): Path<(String, String, String)>,
) -> Result<(), ApiError> {
    sqlx::query!(
        "DELETE FROM flow_file WHERE workspace_id = $1 AND flow_path = $2 AND file_path = $3",
        workspace_id, flow_path, file_path
    ).execute(&state.db).await?;
    Ok(())
}
```

**Worker 執行時取用 flow 工作區檔案：**

```rust
// crates/worker/src/flow_engine.rs 中，推入 RawScript child job 前

async fn prepare_flow_workspace(
    db: &PgPool, workspace_id: &str, flow_path: &str, job_dir: &str,
) -> Result<()> {
    let files = sqlx::query!(
        "SELECT file_path, content FROM flow_file
         WHERE workspace_id = $1 AND flow_path = $2",
        workspace_id, flow_path
    ).fetch_all(db).await?;

    for file in files {
        let full_path = format!("{}/{}", job_dir, file.file_path);
        if let Some(parent) = std::path::Path::new(&full_path).parent() {
            tokio::fs::create_dir_all(parent).await?;
        }
        tokio::fs::write(&full_path, &file.content).await?;
    }
    Ok(())
}
```

### 2.8 新增 API 彙整

```rust
// === Flow CRUD + 版本控制 ===
POST   /api/w/{ws}/flows/save/p/{path}               // 儲存（自動建新 revision）
GET    /api/w/{ws}/flows/list                          // 列出 flows（含最新 revision）
GET    /api/w/{ws}/flows/get/p/{path}                  // 取得最新版
GET    /api/w/{ws}/flows/get/p/{path}/rev/{rev}        // 取得指定版本
GET    /api/w/{ws}/flows/revisions/p/{path}            // 版本歷史
GET    /api/w/{ws}/flows/diff/p/{path}?from=2&to=3     // Diff 兩版本
POST   /api/w/{ws}/flows/rollback/p/{path}/rev/{rev}   // Rollback

// === Flow 工作區檔案 ===
GET    /api/w/{ws}/flows/files/p/{path}                // 列出檔案
GET    /api/w/{ws}/flows/files/p/{path}/f/{file_path}  // 讀取檔案
PUT    /api/w/{ws}/flows/files/p/{path}                // 建立/更新檔案
DELETE /api/w/{ws}/flows/files/p/{path}/f/{file_path}  // 刪除檔案

// === Job 執行 ===
POST   /api/w/{ws}/jobs/run/f/{path}                   // 執行 flow（最新版）
POST   /api/w/{ws}/jobs/run/f/{path}/rev/{rev}         // 執行指定版本
GET    /api/w/{ws}/jobs/{id}/flow_status                // 查詢 flow 執行狀態
```

### 2.9 驗證方式

```
1. 建立 3 步驟 Flow：
   A(return 42) → B(return results.a * 2) → C(return results.b + 10)
   預期結果：94

2. 殺掉 Worker 重啟 → 確認 flow 從中斷點恢復
   （靠 FlowStatus JSONB + re-enqueue）

3. 讓 Step B 失敗 → 確認 failure_module 執行

4. 在 Jaeger 看到完整的 flow trace：
   flow_job → child_a → child_b → child_c（parent-child span 關係）

5. Flow 版本控制：
   - 儲存 → revision 1 → 修改 → 儲存 → revision 2
   - GET /revisions → 確認列出 2 個版本
   - GET /diff?from=1&to=2 → 確認 diff 正確
   - POST /rollback/rev/1 → 確認建立 revision 3（內容等於 revision 1）
   - 執行 flow → 確認 job 記錄了 flow_revision

6. 多檔案工作區：
   - 建立 flow + 3 個檔案（main.py, utils/helpers.py, config.json）
   - main.py 中 `from utils.helpers import clean_data` → 執行成功
   - 前端檔案樹顯示正確的目錄結構
```

---

## Phase 3：排程與進階 Flow（Week 7-9）

### 目標

排程、大資料傳遞、內建節點——讓 Flow 成為完整的工作流系統

### 3.1 Cron 排程系統

```sql
CREATE TABLE schedule (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    workspace_id VARCHAR(50) NOT NULL REFERENCES workspace(id),
    path VARCHAR(255) NOT NULL,
    cron_expr VARCHAR(100) NOT NULL,     -- "*/5 * * * *"
    timezone VARCHAR(50) NOT NULL DEFAULT 'UTC',
    enabled BOOLEAN NOT NULL DEFAULT TRUE,
    script_path VARCHAR(255),
    flow_path VARCHAR(255),
    args JSONB NOT NULL DEFAULT '{}',
    on_failure_path VARCHAR(255),
    on_recovery_path VARCHAR(255),
    on_success_path VARCHAR(255),
    last_triggered_at TIMESTAMPTZ,
    next_trigger_at TIMESTAMPTZ,
    created_by VARCHAR(255) NOT NULL,
    UNIQUE (workspace_id, path)
);
```

```rust
// 背景 scheduler loop
async fn schedule_loop(db: PgPool) {
    loop {
        let schedules = sqlx::query_as!(Schedule,
            "SELECT * FROM schedule
             WHERE enabled = TRUE AND next_trigger_at <= now()
             ORDER BY next_trigger_at ASC LIMIT 100
             FOR UPDATE SKIP LOCKED"
        ).fetch_all(&db).await.unwrap_or_default();

        for schedule in schedules {
            let result = if let Some(script_path) = &schedule.script_path {
                queue::push_job(&db, PushJobArgs {
                    kind: JobKind::Script,
                    script_path: Some(script_path),
                    args: Some(schedule.args.clone()),
                    ..Default::default()
                }).await
            } else if let Some(flow_path) = &schedule.flow_path {
                queue::push_job(&db, PushJobArgs {
                    kind: JobKind::Flow,
                    ..Default::default()
                }).await
            } else { continue; };

            let next = cron::Schedule::from_str(&schedule.cron_expr)
                .ok().and_then(|s| s.upcoming(chrono::Utc).next());

            sqlx::query!(
                "UPDATE schedule SET last_triggered_at = now(), next_trigger_at = $1 WHERE id = $2",
                next, schedule.id
            ).execute(&db).await.ok();
        }

        tokio::time::sleep(std::time::Duration::from_secs(1)).await;
    }
}
```

### 3.2 S3 大結果

```rust
// 結果 > 2MB → 上傳 S3
async fn maybe_upload_to_s3(result: &serde_json::Value) -> Option<String> {
    let serialized = serde_json::to_vec(result).ok()?;

    if serialized.len() > 2 * 1024 * 1024 {
        let key = format!("results/{}/{}.json",
            chrono::Utc::now().format("%Y/%m/%d"), Uuid::new_v4()
        );
        let client = aws_sdk_s3::Client::new(&aws_config::load_defaults(BehaviorVersion::latest()).await);
        client.put_object()
            .bucket(&S3_BUCKET).key(&key)
            .body(serialized.into())
            .content_type("application/json")
            .send().await.ok()?;
        Some(key)
    } else { None }
}
```

### 3.3 內建節點類型（學習 Kestra）

Windmill 的步驟只有 Script 和 RawScript。我們加入型別化的內建節點，直接在 Rust 中執行（不需 spawn 子程序）：

```rust
// 擴展 FlowModuleValue enum
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type")]
pub enum FlowModuleValue {
    // === Phase 2 已有的 ===
    Script { /* ... */ },
    RawScript { /* ... */ },
    ForloopFlow { /* ... */ },
    BranchOne { /* ... */ },
    BranchAll { /* ... */ },
    Identity,

    // === Phase 3 新增：內建節點 ===

    /// 日誌節點 — 打印訊息到 job log
    Log {
        message: InputTransform,
        level: LogLevel,             // Debug, Info, Warn, Error
    },

    /// HTTP 請求節點
    HttpRequest {
        url: InputTransform,
        method: HttpMethod,          // GET, POST, PUT, DELETE, PATCH
        headers: HashMap<String, InputTransform>,
        body: Option<InputTransform>,
        timeout_secs: Option<u32>,
    },

    /// 延遲節點
    Sleep { duration: InputTransform },

    /// 條件閘道 — 條件為 false 時 flow 提前失敗
    Assert {
        expr: String,
        error_message: Option<String>,
    },

    /// 變數設定 — 在 results 中設定一個值
    SetVariable {
        key: String,
        value: InputTransform,
    },

    /// 自訂節點（Phase 4 實作，這裡先定義 enum variant）
    Custom {
        node_type_id: String,
        input_transforms: HashMap<String, InputTransform>,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum LogLevel { Debug, Info, Warn, Error }

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum HttpMethod { GET, POST, PUT, DELETE, PATCH, HEAD }
```

**Worker 執行內建節點（不需 spawn 子程序）：**

```rust
// crates/worker/src/builtin_nodes.rs

pub async fn handle_builtin_node(
    module: &FlowModuleValue, status: &FlowStatus, db: &PgPool, job_id: Uuid,
) -> Result<serde_json::Value> {
    match module {
        FlowModuleValue::Log { message, level } => {
            let msg = eval_input_transform(message, status)?;
            let msg_str = msg.as_str().unwrap_or(&msg.to_string());
            append_logs(job_id, &format!("[{:?}] {}", level, msg_str), db).await?;
            Ok(serde_json::Value::Null)
        }
        FlowModuleValue::HttpRequest { url, method, headers, body, timeout_secs } => {
            let url = eval_input_transform(url, status)?.as_str().unwrap().to_string();
            let client = reqwest::Client::new();
            let mut req = match method {
                HttpMethod::GET => client.get(&url),
                HttpMethod::POST => client.post(&url),
                HttpMethod::PUT => client.put(&url),
                HttpMethod::DELETE => client.delete(&url),
                _ => client.get(&url),
            };
            for (key, val) in headers {
                let v = eval_input_transform(val, status)?;
                req = req.header(key, v.as_str().unwrap_or_default());
            }
            if let Some(body_transform) = body {
                let b = eval_input_transform(body_transform, status)?;
                req = req.json(&b);
            }
            if let Some(timeout) = timeout_secs {
                req = req.timeout(std::time::Duration::from_secs(*timeout as u64));
            }
            let resp = req.send().await?;
            Ok(serde_json::json!({
                "status": resp.status().as_u16(),
                "body": resp.json::<serde_json::Value>().await.unwrap_or(serde_json::Value::Null),
            }))
        }
        FlowModuleValue::Sleep { duration } => {
            let secs = eval_input_transform(duration, status)?.as_f64().unwrap_or(1.0);
            tokio::time::sleep(std::time::Duration::from_secs_f64(secs)).await;
            Ok(serde_json::Value::Null)
        }
        FlowModuleValue::SetVariable { key, value } => {
            let val = eval_input_transform(value, status)?;
            Ok(val)
        }
        FlowModuleValue::Assert { expr, error_message } => {
            let result = eval_js_expr(expr, status)?;
            if result.as_bool().unwrap_or(false) {
                Ok(serde_json::json!(true))
            } else {
                Err(Error::AssertionFailed(
                    error_message.clone().unwrap_or_else(|| format!("Assertion failed: {}", expr))
                ))
            }
        }
        _ => Err(Error::NotBuiltinNode),
    }
}
```

### 3.4 Flow 匯出 / 匯入（YAML + JSON）

讓 Flow 可以被分享、版本控制、複製：

```rust
// crates/api/src/flows.rs

pub async fn export_flow_yaml(
    Path((workspace_id, path)): Path<(String, String)>,
) -> Result<String, ApiError> {
    let flow = get_flow(db, &workspace_id, &path).await?;
    let export = FlowExport {
        version: "1.0".to_string(),
        path: flow.path, summary: flow.summary,
        description: flow.description,
        value: flow.value, schema: flow.schema,
    };
    Ok(serde_yaml::to_string(&export)?)
}

pub async fn import_flow(
    Json(req): Json<ImportFlowRequest>,
) -> Result<Json<FlowCreated>, ApiError> {
    let export: FlowExport = match req.format.as_str() {
        "yaml" => serde_yaml::from_str(&req.content)?,
        "json" => serde_json::from_str(&req.content)?,
        _ => return Err(ApiError::BadRequest("format must be yaml or json")),
    };
    Ok(Json(FlowCreated { path: export.path }))
}
```

**匯出的 YAML 範例（學習 Kestra 的清晰格式）：**

```yaml
version: "1.0"
path: "f/data-team/daily_etl"
summary: "Daily ETL Pipeline"

value:
  modules:
    - id: a
      summary: "Extract from API"
      value:
        type: HttpRequest
        url: { type: Static, value: "https://api.example.com/data" }
        method: GET
        headers:
          Authorization: { type: Static, value: "Bearer ${secrets.API_TOKEN}" }

    - id: b
      summary: "Transform with Python"
      value:
        type: RawScript
        language: python3
        content: |
          import json
          def main(data):
              return [row for row in data if row["status"] == "active"]
        input_transforms:
          data: { type: Javascript, expr: "results.a.body" }

    - id: c
      summary: "Load to DuckDB"
      value:
        type: Query
        engine: duckdb
        query: |
          INSERT INTO analytics.daily_users SELECT * FROM source_0
        sources:
          - type: PreviousStepResult
            step_id: b

    - id: d
      summary: "Notify Slack"
      value:
        type: Custom
        node_type_id: "my_team/slack_notify"
        input_transforms:
          channel: { type: Static, value: "#data-alerts" }
          message: { type: Javascript, expr: "'ETL completed: ' + results.c.length + ' rows'" }

  failure_module:
    id: error_handler
    value:
      type: Log
      message: { type: Javascript, expr: "'ETL failed: ' + JSON.stringify(error)" }
      level: Error
```

**新增 API：**

```
GET  /api/w/{ws}/flows/export/p/{path}?format=yaml   → YAML 字串
GET  /api/w/{ws}/flows/export/p/{path}?format=json   → JSON
POST /api/w/{ws}/flows/import                         → 匯入
```

### 3.5 Tag 級並發控制（學習 Prefect ConcurrencyLimit）

限制同一個 tag 下最多同時跑 N 個 job，避免打爆外部 API 或資料庫：

```rust
// crates/queue/src/pull.rs 中檢查並發限制

async fn check_concurrency_limit(
    db: &PgPool, workspace_id: &str, tag: &str,
) -> Result<bool> {
    let limit = sqlx::query_scalar!(
        "SELECT max_concurrent FROM concurrency_limit
         WHERE workspace_id = $1 AND tag = $2",
        workspace_id, tag
    ).fetch_optional(db).await?;

    if let Some(max) = limit {
        let running = sqlx::query_scalar!(
            "SELECT COUNT(*) FROM job_queue jq
             JOIN job j ON jq.id = j.id
             WHERE j.workspace_id = $1 AND jq.tag = $2 AND jq.running = TRUE",
            workspace_id, tag
        ).fetch_one(db).await?.unwrap_or(0);

        Ok(running < max as i64)
    } else {
        Ok(true) // 沒設限制 → 放行
    }
}
```

**API：**

```
PUT    /api/w/{ws}/concurrency_limits/{tag}   // 設定限制（max_concurrent）
GET    /api/w/{ws}/concurrency_limits          // 列出所有限制
DELETE /api/w/{ws}/concurrency_limits/{tag}    // 刪除限制
```

### 3.6 Schedule 增加 data_interval（學習 Airflow）

排程觸發時，自動帶入「這次排程對應的時間窗口」：

```sql
ALTER TABLE schedule ADD COLUMN data_interval_seconds INTEGER;
-- e.g. 每小時排程 → data_interval_seconds = 3600
-- job args 自動注入 data_interval_start / data_interval_end
```

Worker 執行排程 job 時，自動注入時間窗口到 args：

```rust
if let Some(interval) = schedule.data_interval_seconds {
    let end = schedule.last_triggered_at.unwrap_or_else(|| chrono::Utc::now());
    let start = end - chrono::Duration::seconds(interval as i64);
    args["data_interval_start"] = serde_json::json!(start.to_rfc3339());
    args["data_interval_end"] = serde_json::json!(end.to_rfc3339());
}
```

**用途**：ETL 管線中，Python 程式碼可以直接用 `data_interval_start` 查詢「這批」資料，而不需要自己算時間。

### 3.7 驗證方式

```
1. 建立 `*/1 * * * *` 排程 → 確認每分鐘觸發
2. 建立 retry 3 次的 flow step → 讓它失敗 → 確認重試 3 次 + 指數間隔
3. ForLoop parallel → 迭代 [1,2,3] → 確認 3 個 child job 同時啟動
4. 產生 5MB 結果 → 確認上傳 S3 + DB 只存 s3_key
5. 內建節點: Log / HTTP Request / Sleep / Assert 正常工作
6. 匯出匯入: Flow → YAML → 匯入另一個 workspace → 執行成功
7. 所有操作在 Jaeger 可追蹤
8. 並發控制: 設 tag="api" max=2 → 同時推 5 個 job → 確認最多 2 個同時跑
9. data_interval: 每小時排程 → 確認 args 自動帶入正確的時間窗口
```

---

## Phase 4：擴展與差異化（Week 10-12）

### 4.1 DataEngine trait — 可插拔的資料處理引擎

**設計原則**：不要把 DuckDB 寫死。用 trait 抽象，Day 1 實作 DuckDB，未來可接 DataFusion、ClickHouse、Polars 等。

```rust
// crates/worker/src/data_engine.rs

#[async_trait]
pub trait DataEngine: Send + Sync {
    fn name(&self) -> &str;
    async fn execute_query(
        &self, query: &str, sources: &[DataSource], s3_config: &S3Config,
    ) -> Result<serde_json::Value, DataEngineError>;
    async fn health_check(&self) -> Result<(), DataEngineError>;
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type")]
pub enum DataSource {
    Parquet { url: String },
    Csv { url: String },
    Json { url: String },
    Postgres { url: String, table: String },
    PreviousStepResult { step_id: String },
}
```

**DuckDB 實作：**

```rust
pub struct DuckDbEngine;

#[async_trait]
impl DataEngine for DuckDbEngine {
    fn name(&self) -> &str { "duckdb" }

    async fn execute_query(
        &self, query: &str, sources: &[DataSource], s3_config: &S3Config,
    ) -> Result<serde_json::Value, DataEngineError> {
        let conn = duckdb::Connection::open_in_memory()?;
        conn.execute_batch("INSTALL httpfs; LOAD httpfs;")?;
        conn.execute_batch(&format!(
            "SET s3_region='{}'; SET s3_access_key_id='{}'; SET s3_secret_access_key='{}';",
            s3_config.region, s3_config.access_key, s3_config.secret_key
        ))?;

        for (i, source) in sources.iter().enumerate() {
            let table_name = format!("source_{}", i);
            match source {
                DataSource::Parquet { url } => {
                    conn.execute(&format!("CREATE VIEW {} AS SELECT * FROM read_parquet('{}')", table_name, url), [])?;
                }
                DataSource::Csv { url } => {
                    conn.execute(&format!("CREATE VIEW {} AS SELECT * FROM read_csv('{}')", table_name, url), [])?;
                }
                DataSource::Postgres { url, table } => {
                    conn.execute_batch(&format!(
                        "INSTALL postgres; LOAD postgres; ATTACH '{}' AS pg (TYPE POSTGRES); CREATE VIEW {} AS SELECT * FROM pg.{};",
                        url, table_name, table
                    ))?;
                }
                _ => {}
            }
        }

        let mut stmt = conn.prepare(query)?;
        let column_names: Vec<String> = (0..stmt.column_count())
            .map(|i| stmt.column_name(i).unwrap().to_string()).collect();

        let rows: Vec<serde_json::Value> = stmt.query_map([], |row| {
            let mut obj = serde_json::Map::new();
            for (i, name) in column_names.iter().enumerate() {
                let val: duckdb::types::Value = row.get(i)?;
                obj.insert(name.clone(), duckdb_value_to_json(val));
            }
            Ok(serde_json::Value::Object(obj))
        })?.filter_map(|r| r.ok()).collect();

        Ok(serde_json::Value::Array(rows))
    }

    async fn health_check(&self) -> Result<(), DataEngineError> {
        duckdb::Connection::open_in_memory().map(|_| ()).map_err(|e| DataEngineError::Unavailable(e.to_string()))
    }
}
```

**DataFusion 實作（預留）：**

```rust
pub struct DataFusionEngine;

#[async_trait]
impl DataEngine for DataFusionEngine {
    fn name(&self) -> &str { "datafusion" }

    async fn execute_query(
        &self, query: &str, sources: &[DataSource], s3_config: &S3Config,
    ) -> Result<serde_json::Value, DataEngineError> {
        let ctx = datafusion::prelude::SessionContext::new();
        for (i, source) in sources.iter().enumerate() {
            let table_name = format!("source_{}", i);
            match source {
                DataSource::Parquet { url } => { ctx.register_parquet(&table_name, url, Default::default()).await?; }
                DataSource::Csv { url } => { ctx.register_csv(&table_name, url, Default::default()).await?; }
                _ => {}
            }
        }
        let df = ctx.sql(query).await?;
        let batches = df.collect().await?;
        Ok(arrow_batches_to_json(&batches)?)
    }

    async fn health_check(&self) -> Result<(), DataEngineError> { Ok(()) }
}
```

**DataEngine Router + Flow 中使用：**

```rust
pub struct DataEngineRouter {
    engines: HashMap<String, Box<dyn DataEngine>>,
}

impl DataEngineRouter {
    pub fn new() -> Self {
        let mut engines: HashMap<String, Box<dyn DataEngine>> = HashMap::new();
        engines.insert("duckdb".into(), Box::new(DuckDbEngine));
        // 未來：engines.insert("datafusion".into(), Box::new(DataFusionEngine));
        Self { engines }
    }

    pub fn get(&self, name: &str) -> Option<&dyn DataEngine> {
        self.engines.get(name).map(|e| e.as_ref())
    }
}

// FlowModuleValue 新增 Query variant（Phase 3 的 enum 中已預留位置）：
FlowModuleValue::Query {
    engine: String,         // "duckdb", "datafusion"
    query: String,          // SQL
    sources: Vec<DataSource>,
    input_transforms: HashMap<String, InputTransform>,
}
```

### 4.2 自訂節點系統（User-defined Node Types）

使用者可以把常用的 Script 包裝成可重用的節點類型：

```sql
CREATE TABLE custom_node_type (
    id VARCHAR(100) NOT NULL,        -- "my_team/slack_notify"
    workspace_id VARCHAR(50) NOT NULL REFERENCES workspace(id),
    name VARCHAR(255) NOT NULL,      -- "Slack 通知"
    description TEXT,
    icon VARCHAR(50),                -- emoji 或 icon name
    color VARCHAR(7),                -- "#FF6B6B"
    script_path VARCHAR(255) NOT NULL,
    input_schema JSONB NOT NULL,     -- JSON Schema，定義前端表單
    output_schema JSONB,
    created_by VARCHAR(255) NOT NULL,
    PRIMARY KEY (workspace_id, id)
);
```

```rust
// 執行自訂節點 = 執行底層 Script + 參數映射
FlowModuleValue::Custom { node_type_id, input_transforms } => {
    let node_type = get_custom_node_type(db, workspace_id, node_type_id).await?;
    let args = transform_inputs(input_transforms, status, db).await?;
    let child_id = queue::push_job(db, PushJobArgs {
        kind: JobKind::Script,
        script_path: Some(&node_type.script_path),
        args: Some(args),
        ..Default::default()
    }).await?;
}
```

**前端 Flow Editor 中的步驟庫：**

```
├── 內建
│   ├── Script (Python/TS/Bash)
│   ├── Log / HTTP Request / Sleep / Assert / Set Variable
│   ├── For Loop / Branch
│   └── Query (DuckDB / DataFusion)
└── 自訂（使用者建立的）
    ├── 🔔 Slack 通知
    ├── 📧 Send Email
    └── 🗄️ S3 Upload
```

### 4.3 TypeScript（Bun runtime）

```rust
// crates/worker/src/typescript.rs

pub async fn handle_ts_job(
    job: &QueuedJob, db: &PgPool, content: &str, job_dir: &str, sandbox: &dyn Sandbox,
) -> Result<(serde_json::Value, i64)> {
    write_file(job_dir, "main.ts", content)?;
    let wrapper = r#"
import { main } from "./main.ts";
const args = JSON.parse(await Bun.file("args.json").text());
try {
    const result = await main(...Object.values(args));
    await Bun.write("result.json", JSON.stringify(result));
} catch (e) {
    await Bun.write("result.json", JSON.stringify({ error: { message: e.message, name: e.name } }));
    process.exit(1);
}
"#;
    write_file(job_dir, "wrapper.ts", wrapper)?;
    create_args_and_out_file(job, job_dir).await?;

    let ctx = SandboxContext {
        job_id: job.id, job_dir: job_dir.to_string(),
        command: "bun".to_string(),
        args: vec!["run".to_string(), "wrapper.ts".to_string()],
        env: get_reserved_variables(job),
        timeout_secs: job.timeout.unwrap_or(3600) as u32,
        language: ScriptLang::TypeScript,
        custom_image: job.custom_image.clone(),
        trace_context: get_current_trace_context(),
    };

    let result = sandbox.execute(&ctx).await?;
    if result.exit_code != 0 { return Err(Error::ExecutionErr(result.stderr)); }
    read_result(job_dir).await.map(|v| (v, result.memory_peak_bytes as i64))
}
```

### 4.4 Dedicated Worker（消除冷啟動）

每個 job 都 spawn 新子程序，冷啟動佔比高。Dedicated Worker 保持語言 runtime 常駐：

```
正常模式：  spawn python3 → import → main() → exit    ~65ms (冷啟動 60ms + 執行 5ms)
Dedicated： python3 常駐 → loop { 收 job → main() }   ~5ms (13x 加速)
```

```rust
// crates/worker/src/dedicated.rs

use tokio::sync::mpsc;

pub struct DedicatedRunner {
    tx: mpsc::Sender<DedicatedJob>,
    handle: tokio::task::JoinHandle<()>,
    language: ScriptLang,
    script_path: String,
}

struct DedicatedJob {
    args: serde_json::Value,
    result_tx: tokio::sync::oneshot::Sender<Result<serde_json::Value>>,
}

impl DedicatedRunner {
    pub async fn spawn(
        language: ScriptLang, script_path: &str, script_content: &str,
        job_dir: &str, sandbox: &dyn Sandbox,
    ) -> Result<Self> {
        let (tx, mut rx) = mpsc::channel::<DedicatedJob>(64);

        let runner_code = match language {
            ScriptLang::Python3 => format!(r#"
import json, sys
from inner import main

while True:
    line = sys.stdin.readline()
    if not line:
        break
    args = json.loads(line)
    try:
        result = main(**args)
        print(json.dumps({{"ok": result}}), flush=True)
    except Exception as e:
        print(json.dumps({{"err": str(e)}}), flush=True)
"#),
            ScriptLang::TypeScript => format!(r#"
import {{ main }} from "./inner.ts";
const reader = Bun.stdin.stream().getReader();
const decoder = new TextDecoder();
while (true) {{
    const {{ done, value }} = await reader.read();
    if (done) break;
    const args = JSON.parse(decoder.decode(value));
    try {{
        const result = await main(...Object.values(args));
        console.log(JSON.stringify({{ ok: result }}));
    }} catch (e) {{
        console.log(JSON.stringify({{ err: e.message }}));
    }}
}}
"#),
            _ => return Err(Error::UnsupportedLanguage(language)),
        };

        // spawn 常駐子程序 + 收發 job 的 background task
        let handle = tokio::spawn(async move {
            while let Some(job) = rx.recv().await {
                // 寫 args 到 stdin → 讀 result 從 stdout → 回傳 oneshot
            }
        });

        Ok(Self { tx, handle, language, script_path: script_path.to_string() })
    }

    pub async fn execute(&self, args: serde_json::Value) -> Result<serde_json::Value> {
        let (result_tx, result_rx) = tokio::sync::oneshot::channel();
        self.tx.send(DedicatedJob { args, result_tx }).await?;
        result_rx.await?
    }
}
```

### 4.5 DataPreviewTable — 資料引擎即時預覽

讓使用者在前端即時預覽 DataEngine 查詢結果，用於 Flow 中的 Query 步驟調試：

**後端 API：**

```rust
// crates/api/src/data_preview.rs

/// 執行查詢並返回預覽資料（限制 100 行）
pub async fn preview_query(
    State(state): State<AppState>,
    Path(workspace_id): Path<String>,
    Json(req): Json<PreviewQueryRequest>,
) -> Result<Json<PreviewQueryResult>, ApiError> {
    let engine = state.data_engines.get(&req.engine)
        .ok_or(ApiError::BadRequest("unknown engine"))?;

    // 自動加上 LIMIT 100 保護
    let safe_query = if req.query.to_uppercase().contains("LIMIT") {
        req.query.clone()
    } else {
        format!("{} LIMIT 100", req.query)
    };

    let result = engine.execute_query(&safe_query, &req.sources, &state.s3_config).await
        .map_err(|e| ApiError::Internal(e.to_string()))?;

    let rows = result.as_array().unwrap_or(&vec![]);
    let columns: Vec<String> = rows.first()
        .and_then(|r| r.as_object())
        .map(|obj| obj.keys().cloned().collect())
        .unwrap_or_default();

    Ok(Json(PreviewQueryResult {
        columns,
        rows: rows.to_vec(),
        total_rows: rows.len(),
        truncated: rows.len() >= 100,
    }))
}

// POST /api/w/{ws}/data/preview
```

**前端元件：**

```svelte
<!-- src/lib/components/DataPreviewTable.svelte -->
<script lang="ts">
  let {
    workspaceId,
    engine = 'duckdb',
  }: {
    workspaceId: string
    engine?: string
  } = $props()

  let query = $state('SELECT 1 as test')
  let result = $state<{ columns: string[]; rows: any[]; truncated: boolean } | null>(null)
  let error = $state<string | null>(null)
  let loading = $state(false)
  let sortColumn = $state<string | null>(null)
  let sortAsc = $state(true)

  async function runQuery() {
    loading = true
    error = null
    try {
      const resp = await fetch(`/api/w/${workspaceId}/data/preview`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ engine, query, sources: [] }),
      })
      if (!resp.ok) throw new Error(await resp.text())
      result = await resp.json()
    } catch (e: any) {
      error = e.message
    } finally {
      loading = false
    }
  }

  function toggleSort(col: string) {
    if (sortColumn === col) { sortAsc = !sortAsc }
    else { sortColumn = col; sortAsc = true }
  }

  let sortedRows = $derived.by(() => {
    if (!result?.rows || !sortColumn) return result?.rows ?? []
    return [...result.rows].sort((a, b) => {
      const va = a[sortColumn!], vb = b[sortColumn!]
      const cmp = va < vb ? -1 : va > vb ? 1 : 0
      return sortAsc ? cmp : -cmp
    })
  })
</script>

<div class="data-preview">
  <div class="query-bar">
    <textarea bind:value={query} rows="3" placeholder="SELECT * FROM ..." />
    <button onclick={runQuery} disabled={loading}>
      {loading ? 'Running...' : 'Run Query'}
    </button>
  </div>

  {#if error}
    <div class="error">{error}</div>
  {/if}

  {#if result}
    <div class="table-info">
      {result.rows.length} rows {#if result.truncated}(truncated to 100){/if}
    </div>
    <div class="table-wrapper">
      <table>
        <thead>
          <tr>
            {#each result.columns as col}
              <th onclick={() => toggleSort(col)} class="sortable">
                {col}
                {#if sortColumn === col}{sortAsc ? ' ▲' : ' ▼'}{/if}
              </th>
            {/each}
          </tr>
        </thead>
        <tbody>
          {#each sortedRows as row}
            <tr>
              {#each result.columns as col}
                <td>{JSON.stringify(row[col])}</td>
              {/each}
            </tr>
          {/each}
        </tbody>
      </table>
    </div>
  {/if}
</div>

<style>
  .data-preview { display: flex; flex-direction: column; height: 100%; }
  .query-bar { display: flex; gap: 8px; padding: 8px; }
  .query-bar textarea { flex: 1; font-family: monospace; }
  .table-wrapper { flex: 1; overflow: auto; }
  table { width: 100%; border-collapse: collapse; font-size: 13px; }
  th { text-align: left; padding: 6px 12px; background: #2d2d2d; border-bottom: 1px solid #444; }
  th.sortable { cursor: pointer; }
  td { padding: 4px 12px; border-bottom: 1px solid #333; white-space: nowrap; }
  tr:hover td { background: #2a2a2a; }
  .error { color: #f44747; padding: 8px; }
  .table-info { padding: 4px 8px; font-size: 12px; color: #888; }
</style>
```

**在 Flow Editor 中的使用方式：**

使用者點擊 Query 類型的步驟時，右側面板顯示 DataPreviewTable，可以即時測試 SQL 查詢並預覽結果：

```
┌─ Step Config Panel ──────────────┐
│ Engine: [DuckDB ▾]               │
│                                   │
│ ┌─ SQL Editor ────────────────┐  │
│ │ SELECT count(*) as total     │  │
│ │ FROM read_parquet('s3://...')│  │
│ └──────────────────────────────┘  │
│ [▶ Preview]                       │
│                                   │
│ ┌─ Preview Results ───────────┐  │
│ │  total  │                    │  │
│ │ ────── │                    │  │
│ │  42,891 │                    │  │
│ └──────────────────────────────┘  │
│ 1 rows                            │
└───────────────────────────────────┘
```

### 4.6 OpenTelemetry — 完整鏈路

Phase 1 已建好基礎，Phase 4 加強 Flow 的 parent-child span 關係：

```
Jaeger 上看到的 trace 結構：

flow_job (root span)
├── step_a (child span) ── python execution
├── step_b (child span) ── python execution
│   └── retry_1 (child span)
└── step_c (child span) ── duckdb query
    └── s3_upload (child span)
```

### 4.7 驗證方式

```
1. DataEngine: DuckDB Query step 查詢 S3 Parquet → 確認結果正確
2. DataPreview: 在 Flow Editor 中選 Query 步驟 → Preview → 表格顯示結果 → 可排序
3. OTel: 啟動 Jaeger → 跑 flow → 確認 trace 完整（parent-child span）
4. TypeScript: Bun 執行成功
5. 自訂節點: 建立 "Slack Notify" 自訂節點 → 在 flow 中使用
6. Dedicated Worker: 連續跑 100 個輕量 Python job → 對比正常模式延遲
7. Sandbox: tag="k8s" → K8s Pod / tag="fast" → nsjail
```

---

## 未來方向（Phase 4 之後）

- Iggy 取代 PostgreSQL queue（100K+ msg/sec）
- Event-driven CEP（時間窗口、事件關聯）
- Plugin-based trigger 框架（Webhook、Cron、Kafka、MQTT）
- App Builder（低代碼 UI）
- Dedicated Worker 進階：Runner Groups（多個 script 共用依賴時共享同一 runtime）
- Firecracker microVM 作為第五種沙箱模式（~125ms 啟動，AWS Lambda 底層技術）
- Marketplace（分享自訂節點和 Flow 範本）
- Flow 版本 diff UI 強化（visual diff，像 GitHub PR 的 side-by-side 比較）
- Software-Defined Assets（學 Dagster，將 data lineage 做為 first-class 概念）

---

## 附錄 A：Sandbox 四模式詳細實作

### A.1 方案 1：Rust 原生沙箱 — Landlock + seccomp（推薦預設）

**不需要外部 binary**，純 Rust 實作，用 Linux 核心原生的安全機制：

| 技術 | Crate | 作用 | Linux 版本需求 |
|------|-------|------|--------------|
| **Landlock** | `landlock` (v0.4) | 檔案系統 + 網路（TCP）隔離，path-based ACL | 5.13+（ABI v4 需 6.7+） |
| **seccomp** | `seccompiler` (v0.5, rust-vmm/AWS) | 系統呼叫白名單，BPF 過濾 | 3.5+ |
| **User Namespace** | `nix` crate | PID/Mount/Network namespace 隔離 | 3.8+ |

**Landlock 範例（檔案系統隔離）：**

```rust
use landlock::{Access, AccessFs, PathBeneath, PathFd, Ruleset, RulesetAttr, RulesetCreatedAttr, ABI};

fn sandbox_filesystem(job_dir: &str) -> Result<()> {
    let abi = ABI::V4;
    Ruleset::default()
        .handle_access(AccessFs::from_all(abi))?
        .create()?
        .add_rule(PathBeneath::new(PathFd::new("/usr")?, AccessFs::from_read(abi)))?
        .add_rule(PathBeneath::new(PathFd::new("/lib")?, AccessFs::from_read(abi)))?
        .add_rule(PathBeneath::new(PathFd::new("/bin")?, AccessFs::from_read(abi)))?
        .add_rule(PathBeneath::new(PathFd::new(job_dir)?, AccessFs::from_all(abi)))?
        .restrict_self()?;
    Ok(())
}
```

**seccomp 範例（系統呼叫白名單）：**

```rust
use seccompiler::{SeccompAction, SeccompFilter, SeccompRule, BpfProgram};
use std::collections::BTreeMap;

fn sandbox_syscalls() -> Result<()> {
    let filter = SeccompFilter::new(
        BTreeMap::from([
            (libc::SYS_read, vec![SeccompRule::new(vec![])?]),
            (libc::SYS_write, vec![SeccompRule::new(vec![])?]),
            (libc::SYS_openat, vec![SeccompRule::new(vec![])?]),
            (libc::SYS_close, vec![SeccompRule::new(vec![])?]),
            (libc::SYS_mmap, vec![SeccompRule::new(vec![])?]),
            (libc::SYS_munmap, vec![SeccompRule::new(vec![])?]),
            (libc::SYS_brk, vec![SeccompRule::new(vec![])?]),
            (libc::SYS_futex, vec![SeccompRule::new(vec![])?]),
            (libc::SYS_clone3, vec![SeccompRule::new(vec![])?]),
            (libc::SYS_socket, vec![SeccompRule::new(vec![])?]),
            (libc::SYS_connect, vec![SeccompRule::new(vec![])?]),
            (libc::SYS_exit_group, vec![SeccompRule::new(vec![])?]),
        ]),
        SeccompAction::KillProcess,
        SeccompAction::Allow,
        std::env::consts::ARCH.try_into()?,
    )?;
    let bpf: BpfProgram = filter.try_into()?;
    seccompiler::apply_filter(&bpf)?;
    Ok(())
}
```

**整合成 RustNativeSandbox：**

```rust
pub struct RustNativeSandbox { config: RustNativeConfig }

impl RustNativeSandbox {
    async fn execute(&self, ctx: &SandboxContext) -> Result<SandboxResult, SandboxError> {
        let child = unsafe { nix::unistd::fork() };

        match child {
            Ok(nix::unistd::ForkResult::Child) => {
                nix::sched::unshare(
                    nix::sched::CloneFlags::CLONE_NEWPID |
                    nix::sched::CloneFlags::CLONE_NEWNS |
                    nix::sched::CloneFlags::CLONE_NEWUSER
                ).ok();
                sandbox_filesystem(&ctx.job_dir).ok();
                sandbox_syscalls().ok();
                set_rlimits(self.config.memory_limit, self.config.cpu_time_limit);

                let err = nix::unistd::execvpe(
                    &std::ffi::CString::new(ctx.command.as_str()).unwrap(),
                    &ctx.args.iter().map(|a| std::ffi::CString::new(a.as_str()).unwrap()).collect::<Vec<_>>(),
                    &ctx.env.iter().map(|(k, v)| std::ffi::CString::new(format!("{}={}", k, v)).unwrap()).collect::<Vec<_>>(),
                );
                std::process::exit(1);
            }
            Ok(nix::unistd::ForkResult::Parent { child: pid }) => {
                handle_child_pid(pid, ctx.timeout_secs).await
            }
            Err(e) => Err(SandboxError::ForkFailed(e.to_string())),
        }
    }
}
```

**優勢**：無外部依賴、啟動 ~1-5ms、效能 ~0%、用 Firecracker 同款 seccompiler crate

**限制**：Linux only、需為每種語言調整 syscall 白名單

### A.2 方案 2：nsjail（Windmill 使用中）

見 [04-worker-executor.md](./04-worker-executor.md) 的詳細分析。

```rust
pub struct NsjailSandbox { config: NsjailConfig }

#[async_trait]
impl Sandbox for NsjailSandbox {
    async fn execute(&self, ctx: &SandboxContext) -> Result<SandboxResult, SandboxError> {
        let template = match ctx.language {
            ScriptLang::Python3 => include_str!("../../nsjail/run.python3.config.proto"),
            ScriptLang::Bash => include_str!("../../nsjail/run.bash.config.proto"),
            ScriptLang::TypeScript => include_str!("../../nsjail/run.typescript.config.proto"),
            _ => return Err(SandboxError::UnsupportedLanguage(ctx.language)),
        };

        let nsjail_timeout = ctx.timeout_secs + 15;
        let config_content = template
            .replace("{JOB_DIR}", &ctx.job_dir)
            .replace("{TIMEOUT}", &nsjail_timeout.to_string())
            .replace("{CLONE_NEWUSER}", "true")
            .replace("{TMPFS_SIZE}", &self.config.tmpfs_size.to_string());

        let config_path = format!("{}/run.config.proto", ctx.job_dir);
        tokio::fs::write(&config_path, &config_content).await?;

        let mut cmd = tokio::process::Command::new(&self.config.nsjail_path);
        cmd.current_dir(&ctx.job_dir)
            .env_clear().envs(&ctx.env)
            .args(&["--config", "run.config.proto", "--"])
            .arg(&ctx.command).args(&ctx.args)
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::piped());

        if let Some(trace) = &ctx.trace_context {
            cmd.env("TRACEPARENT", &trace.traceparent);
            cmd.env("TRACESTATE", &trace.tracestate);
        }

        let start = std::time::Instant::now();
        let child = cmd.spawn()?;
        let result = handle_child_process(child, ctx.timeout_secs).await?;

        Ok(SandboxResult {
            exit_code: result.exit_code, stdout: result.stdout, stderr: result.stderr,
            duration_ms: start.elapsed().as_millis() as u64,
            memory_peak_bytes: result.memory_peak,
        })
    }

    async fn health_check(&self) -> Result<(), SandboxError> {
        let output = tokio::process::Command::new(&self.config.nsjail_path).arg("--help").output().await?;
        if output.status.success() { Ok(()) }
        else { Err(SandboxError::Unavailable("nsjail binary not found".into())) }
    }

    fn name(&self) -> &str { "nsjail" }
}
```

### A.3 方案 3：WASM 沙箱（wasmtime）

**兩大 runtime：**

| 特性 | wasmtime (Bytecode Alliance) | wasmer |
|------|---------------------------|--------|
| 冷啟動 | **5μs - 1ms**（AOT） | 1-10ms |
| WASI | WASIp2 + WASIp3 | WASIX |
| Fuel metering | 每個 operator 可配權重 | 有 |
| 生產使用者 | Fastly, Microsoft | Shopify |

**WASM 適合什麼？**

| 語言 | 可行性 | 限制 |
|------|--------|------|
| JavaScript/TS | 可行（QuickJS/Javy → WASM） | 無 Node.js API |
| Python（純運算） | 可行（Pyodide） | `requests` 不行 |
| Python（任意 pip） | 不可行 | C extensions |
| Bash | 不可行 | 需要完整 OS |

```rust
use wasmtime::*;

pub struct WasmSandbox { engine: Engine, config: WasmConfig }

impl WasmSandbox {
    pub fn new(config: WasmConfig) -> Self {
        let mut engine_config = Config::new();
        engine_config.consume_fuel(true);
        engine_config.wasm_component_model(true);
        engine_config.strategy(Strategy::Cranelift);
        Self { engine: Engine::new(&engine_config).unwrap(), config }
    }

    async fn execute_js(&self, code: &str, args: &serde_json::Value) -> Result<SandboxResult> {
        let module = Module::from_file(&self.engine, "quickjs.wasm")?;
        let mut store = Store::new(&self.engine, WasmState::new());

        store.set_fuel(self.config.max_fuel)?;
        store.limiter(|state| &mut state.limiter);

        let wasi = WasiCtxBuilder::new()
            .inherit_stdout().inherit_stderr()
            .preopened_dir(&ctx.job_dir, "/tmp/job", DirPerms::all(), FilePerms::all())?
            .build();

        let instance = Linker::new(&self.engine).instantiate(&mut store, &module)?;
        let main = instance.get_typed_func::<(i32, i32), i32>(&mut store, "eval")?;
        let result = main.call(&mut store, (code_ptr, args_ptr))?;
        let fuel_consumed = self.config.max_fuel - store.get_fuel()?;

        Ok(SandboxResult { /* ... */ })
    }
}
```

**殺手級優勢**：跨平台、啟動 5μs、Fuel metering 精確、記憶體天然隔離

**致命限制（2026 現狀）**：無法跑任意 pip 包、無多執行緒、不支援 Bash

### A.4 方案 4：K8s Pod

```rust
use k8s_openapi::api::core::v1::Pod;
use kube::{Api, Client, api::PostParams};

pub struct K8sPodSandbox { client: Client, config: K8sPodConfig }

#[async_trait]
impl Sandbox for K8sPodSandbox {
    async fn execute(&self, ctx: &SandboxContext) -> Result<SandboxResult, SandboxError> {
        let pods: Api<Pod> = Api::namespaced(self.client.clone(), &self.config.namespace);
        let image = ctx.custom_image.as_deref().unwrap_or(&self.config.default_image);
        let pod_name = format!("ff-job-{}", ctx.job_id);

        let pod: Pod = serde_json::from_value(serde_json::json!({
            "apiVersion": "v1", "kind": "Pod",
            "metadata": {
                "name": pod_name, "namespace": self.config.namespace,
                "labels": { "app": "flowforge", "job-id": ctx.job_id.to_string() }
            },
            "spec": {
                "restartPolicy": "Never",
                "serviceAccountName": self.config.service_account,
                "containers": [{
                    "name": "job", "image": image,
                    "command": [ctx.command.clone()], "args": ctx.args.clone(),
                    "env": ctx.env.iter().map(|(k, v)| serde_json::json!({"name": k, "value": v})).collect::<Vec<_>>(),
                    "resources": {
                        "requests": { "cpu": self.config.cpu_request, "memory": self.config.memory_request },
                        "limits": { "cpu": self.config.cpu_limit, "memory": self.config.memory_limit },
                    },
                    "volumeMounts": [{ "name": "job-data", "mountPath": "/tmp/job" }]
                }],
                "volumes": [{ "name": "job-data", "configMap": { "name": format!("ff-job-{}", ctx.job_id) } }],
                "activeDeadlineSeconds": ctx.timeout_secs as i64,
            }
        }))?;

        let start = std::time::Instant::now();
        pods.create(&PostParams::default(), &pod).await?;
        let result = self.wait_for_pod(&pods, &pod_name, ctx).await?;

        if self.config.auto_cleanup { pods.delete(&pod_name, &Default::default()).await.ok(); }

        Ok(SandboxResult {
            exit_code: result.exit_code, stdout: result.stdout, stderr: result.stderr,
            duration_ms: start.elapsed().as_millis() as u64,
            memory_peak_bytes: 0,
        })
    }

    async fn health_check(&self) -> Result<(), SandboxError> {
        let ns: Api<k8s_openapi::api::core::v1::Namespace> = Api::all(self.client.clone());
        ns.get(&self.config.namespace).await.map(|_| ())
            .map_err(|e| SandboxError::Unavailable(format!("K8s check failed: {e}")))
    }

    fn name(&self) -> &str { "kubernetes-pod" }
}
```

### A.5 Config 型別定義

```rust
#[derive(Debug, Clone, serde::Deserialize)]
pub struct NsjailConfig {
    pub nsjail_path: String,      // 預設 "nsjail"
    pub memory_limit: u64,        // 預設 1GB
    pub cpu_time_limit: u32,      // 預設 1000s
    pub clone_newnet: bool,       // 預設 false
    pub tmpfs_size: u64,          // 預設 500MB
}

#[derive(Debug, Clone, serde::Deserialize)]
pub struct K8sPodConfig {
    pub namespace: String,        // "flowforge-jobs"
    pub default_image: String,
    pub cpu_request: String,      // "100m"
    pub cpu_limit: String,        // "1000m"
    pub memory_request: String,   // "128Mi"
    pub memory_limit: String,     // "1Gi"
    pub service_account: Option<String>,
    pub node_selector: Option<std::collections::HashMap<String, String>>,
    pub image_pull_secrets: Vec<String>,
    pub auto_cleanup: bool,
}

#[derive(Debug, Clone, serde::Deserialize)]
pub struct RustNativeConfig {
    pub memory_limit: u64,        // 1GB
    pub cpu_time_limit: u32,      // 1000s
    pub isolate_network: bool,    // false
    pub enable_landlock: bool,    // true
    pub enable_seccomp: bool,     // true
    pub readonly_paths: Vec<String>,  // ["/usr", "/lib", "/bin"]
}

#[derive(Debug, Clone, serde::Deserialize)]
pub struct WasmConfig {
    pub max_fuel: u64,            // 10_000_000
    pub max_memory: usize,        // 1GB
    pub quickjs_module: Option<String>,
    pub pyodide_module: Option<String>,
    pub allow_network: bool,      // false
}
```

### A.6 設定檔範例

```toml
# flowforge.toml

[worker]
name = "worker-01"
tags = ["default", "fast"]

# === 模式 1：Rust 原生（推薦預設）===
[worker.sandbox.rust_native]
memory_limit = 1073741824
cpu_time_limit = 1000
enable_landlock = true
enable_seccomp = true
readonly_paths = ["/usr", "/lib", "/lib64", "/bin", "/etc"]

# === 模式 2：nsjail ===
[worker.sandbox.nsjail]
nsjail_path = "nsjail"
memory_limit = 1073741824
cpu_time_limit = 1000
tmpfs_size = 524288000

# === 模式 3：WASM ===
[worker.sandbox.wasm]
max_fuel = 10000000
max_memory = 1073741824
quickjs_module = "/opt/flowforge/quickjs.wasm"
pyodide_module = "/opt/flowforge/pyodide.wasm"

# === 模式 4：K8s Pod ===
[worker.sandbox.k8s_pod]
namespace = "flowforge-jobs"
default_image = "flowforge/python-runner:3.12"
cpu_request = "100m"
cpu_limit = "2000m"
memory_request = "256Mi"
memory_limit = "4Gi"
service_account = "flowforge-job-runner"
auto_cleanup = true

[worker.sandbox.k8s_pod.node_selector]
"node-type" = "compute"
```

### A.7 各模式適用場景

```
生產環境（Linux server，高吞吐）：
  → rust_native（預設）+ k8s_pod（重型 job）

生產環境（Linux server，已有 nsjail）：
  → nsjail（預設）+ k8s_pod（重型 job）

Edge / Serverless（需要微秒級冷啟動）：
  → wasm（JS/TS job）+ rust_native（Python/Bash）

macOS / Windows 開發環境：
  → wasm（JS/TS）+ none（Python/Bash，開發時不隔離）

多租戶 SaaS（最強隔離需求）：
  → k8s_pod（所有 job 都走 Pod）
  → 或 rust_native + seccomp（成本更低）
```
