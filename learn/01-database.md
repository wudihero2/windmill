# 第一章：資料庫設計

## 為什麼 PostgreSQL 就夠了？

Windmill 用 PostgreSQL 實現了大多數系統需要 Redis + RabbitMQ + 專用 Queue 的功能：

| 功能 | 傳統方案 | Windmill 方案 |
|------|----------|---------------|
| Job Queue | Redis/RabbitMQ | `queue` 表 + `FOR UPDATE SKIP LOCKED` |
| 分散式鎖 | Redis SETNX | Advisory Locks / `concurrency_locks` 表 |
| 快取 | Redis | `cache_ttl` 欄位 + 結果重用 |
| Pub/Sub | Redis Pub/Sub | PostgreSQL `LISTEN/NOTIFY` |
| 權限 | 應用層 RBAC | Row-Level Security (RLS) |
| 全文搜尋 | Elasticsearch | PostgreSQL 全文搜尋索引 |

## 核心表設計

### 你需要的最小 Schema

如果你要從零開始，以下是最核心的表：

```sql
-- 1. 多租戶基礎
CREATE TABLE workspace (
    id VARCHAR(50) PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    owner VARCHAR(255) NOT NULL,
    deleted BOOLEAN DEFAULT FALSE
);

-- 2. 使用者（每個 workspace 獨立）
CREATE TABLE usr (
    workspace_id VARCHAR(50) REFERENCES workspace(id),
    username VARCHAR(55),
    email VARCHAR(255) NOT NULL,
    is_admin BOOLEAN DEFAULT FALSE,
    operator BOOLEAN DEFAULT FALSE,  -- 只能執行、不能編輯
    disabled BOOLEAN DEFAULT FALSE,
    PRIMARY KEY (workspace_id, username)
);

-- 3. 密碼 / 認證
CREATE TABLE password (
    email VARCHAR(255) PRIMARY KEY,
    password_hash VARCHAR(255) NOT NULL,
    super_admin BOOLEAN DEFAULT FALSE,
    verified BOOLEAN DEFAULT FALSE,
    login_type VARCHAR(20) DEFAULT 'password'  -- password, github, oauth
);

-- 4. API Token
CREATE TABLE token (
    token VARCHAR(255) PRIMARY KEY,  -- 存 hash，不存原文
    label VARCHAR(255),
    expiration TIMESTAMPTZ,
    workspace_id VARCHAR(50),
    owner VARCHAR(55),
    email VARCHAR(255),
    super_admin BOOLEAN DEFAULT FALSE,
    scopes TEXT[]  -- 權限範圍
);
```

### Script 系統

```sql
-- 腳本語言枚舉
CREATE TYPE script_lang AS ENUM (
    'python3', 'deno', 'go', 'bash', 'postgresql',
    'nativets', 'bun', 'rust', 'php', 'java',
    'ruby', 'csharp', 'powershell', 'nu'
    -- Windmill 實際有 25+ 種
);

-- 腳本種類
CREATE TYPE script_kind AS ENUM (
    'script',       -- 一般腳本
    'trigger',      -- 觸發器
    'failure',      -- 錯誤處理
    'command',      -- CLI 命令
    'approval',     -- 審批步驟
    'preprocessor'  -- 前處理
);

-- 腳本表（核心設計：Hash-based Versioning）
CREATE TABLE script (
    workspace_id VARCHAR(50) NOT NULL,
    hash BIGINT NOT NULL,               -- 內容 hash = 版本 ID
    path VARCHAR(255) NOT NULL,          -- 路徑 = 邏輯名稱
    parent_hashes BIGINT[],              -- 版本鏈（誰是我的上一版）
    summary TEXT DEFAULT '',
    description TEXT DEFAULT '',
    content TEXT NOT NULL,               -- 實際程式碼
    schema JSONB,                        -- 函式簽名（自動解析）
    language script_lang NOT NULL,
    kind script_kind DEFAULT 'script',
    created_by VARCHAR(55) NOT NULL,
    created_at TIMESTAMPTZ DEFAULT now(),
    archived BOOLEAN DEFAULT FALSE,
    lock TEXT,                           -- 依賴鎖檔（pip freeze / package-lock）
    extra_perms JSONB DEFAULT '{}',      -- 額外權限
    PRIMARY KEY (workspace_id, hash)
);

-- 重要索引
CREATE INDEX idx_script_path_created ON script(workspace_id, path, created_at DESC);
CREATE INDEX idx_script_extra_perms ON script USING GIN(extra_perms);
```

**關鍵設計點：**
- `hash` 是內容的 hash，不是 auto-increment ID
- 同一個 `path` 可以有多個 `hash`（= 多個版本）
- `parent_hashes` 記錄版本鏈，讓你可以追蹤歷史
- `schema` 是自動從程式碼解析出的 JSON Schema（用 parser crate）

### Flow 系統

```sql
-- Flow 定義
CREATE TABLE flow (
    workspace_id VARCHAR(50) NOT NULL,
    path VARCHAR(255) NOT NULL,
    summary TEXT DEFAULT '',
    description TEXT DEFAULT '',
    value JSONB NOT NULL,            -- 整個 DAG 定義（見下方 JSON 結構）
    schema JSONB,                    -- 輸入參數 schema
    edited_by VARCHAR(55) NOT NULL,
    edited_at TIMESTAMPTZ DEFAULT now(),
    archived BOOLEAN DEFAULT FALSE,
    extra_perms JSONB DEFAULT '{}',
    PRIMARY KEY (workspace_id, path)
);

-- Flow 版本（不可變）
CREATE TABLE flow_version (
    id BIGSERIAL PRIMARY KEY,
    workspace_id VARCHAR(50) NOT NULL,
    path VARCHAR(255) NOT NULL,
    value JSONB NOT NULL,
    schema JSONB,
    created_by VARCHAR(55) NOT NULL,
    created_at TIMESTAMPTZ DEFAULT now()
);
```

**Flow `value` 的 JSON 結構：**

```jsonc
{
  "modules": [
    {
      "id": "a",                          // 步驟 ID
      "value": {
        "type": "rawscript",              // 類型：script, rawscript, flow, forloopflow, branchone, branchall, identity
        "content": "def main(x): ...",
        "language": "python3",
        "input_transforms": {             // 輸入轉換（從上一步取值）
          "x": {
            "type": "javascript",
            "expr": "results.a.value"     // JS 表達式
          }
        }
      },
      "summary": "處理資料",
      "retry": { "constant": { "attempts": 3, "seconds": 10 } },
      "suspend": null,                    // null = 不暫停，有值 = 等待人工審批
      "sleep": null                       // 延遲執行
    },
    {
      "id": "b",
      "value": {
        "type": "forloopflow",            // For 迴圈
        "iterator": { "type": "javascript", "expr": "results.a" },
        "skip_failures": true,
        "parallel": true,                 // 並行執行
        "parallelism": 5,                 // 最大並行數
        "modules": [                      // 迴圈內的步驟
          { "id": "b-1", "value": { "type": "script", "path": "u/admin/process_item" } }
        ]
      }
    },
    {
      "id": "c",
      "value": {
        "type": "branchone",              // 條件分支（擇一）
        "branches": [
          {
            "expr": "results.a.status == 'ok'",   // 條件判斷
            "modules": [{ "id": "c-1", "value": { "type": "script", "path": "..." } }]
          }
        ],
        "default": [{ "id": "c-default", "value": { "type": "script", "path": "..." } }]
      }
    }
  ],
  "failure_module": {                     // 全域錯誤處理
    "id": "failure",
    "value": { "type": "script", "path": "u/admin/notify_failure" }
  },
  "same_worker": false                    // 所有步驟在同一個 worker 執行
}
```

### Job Queue 系統

```sql
-- Job 狀態枚舉
CREATE TYPE job_kind AS ENUM (
    'script',              -- 執行腳本
    'flow',                -- 執行 flow（父 job）
    'flowscript',          -- flow 中的一個步驟
    'preview',             -- 預覽執行
    'dependencies',        -- 依賴解析
    'identity',            -- 直接傳遞結果
    'noop',                -- 空操作
    'singlestepflow'       -- 單步 flow（最佳化）
);

CREATE TYPE job_status AS ENUM ('success', 'failure', 'canceled', 'skipped');

-- 執行中/等待中的 Job
CREATE TABLE queue (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    workspace_id VARCHAR(50) NOT NULL,
    parent_job UUID,                       -- Flow 的父 job ID
    created_by VARCHAR(55) NOT NULL,
    created_at TIMESTAMPTZ DEFAULT now(),
    started_at TIMESTAMPTZ,
    scheduled_for TIMESTAMPTZ NOT NULL,    -- 排程時間
    running BOOLEAN DEFAULT FALSE,
    script_hash BIGINT,
    script_path VARCHAR(255),
    args JSONB,                            -- 輸入參數
    logs TEXT,                             -- 累積日誌
    job_kind job_kind NOT NULL,
    language script_lang,
    tag VARCHAR(50) DEFAULT 'default',     -- Worker 標籤（路由到特定 worker）
    canceled BOOLEAN DEFAULT FALSE,
    canceled_by VARCHAR(55),
    flow_status JSONB,                     -- Flow 狀態機（只在 flow job 上）
    is_flow_step BOOLEAN DEFAULT FALSE,
    same_worker BOOLEAN DEFAULT FALSE,
    suspend INT,                           -- 暫停計數（人工審批）
    root_job UUID,                         -- 最頂層的 flow job
    priority SMALLINT DEFAULT 0,           -- 優先級
    timeout INT,                           -- 超時（秒）
    concurrent_limit INT,                  -- 併發限制
    cache_ttl INT,                         -- 快取 TTL（秒）
    permissioned_as VARCHAR(55) NOT NULL,  -- 以誰的身份執行
    visible_to_owner BOOLEAN DEFAULT TRUE
);

-- Worker 搶 job 用的索引（關鍵效能）
CREATE INDEX idx_queue_scheduled ON queue(scheduled_for) WHERE running = FALSE AND canceled = FALSE;
CREATE INDEX idx_queue_tag ON queue(tag) WHERE running = FALSE;
CREATE INDEX idx_queue_priority ON queue(priority DESC, scheduled_for ASC) WHERE running = FALSE;

-- 已完成的 Job（從 queue 表移過來）
CREATE TABLE completed_job (
    id UUID PRIMARY KEY,
    workspace_id VARCHAR(50) NOT NULL,
    parent_job UUID,
    created_by VARCHAR(55) NOT NULL,
    created_at TIMESTAMPTZ NOT NULL,
    started_at TIMESTAMPTZ,
    duration_ms INT,                       -- 執行時長
    success BOOLEAN NOT NULL,
    result JSONB,                          -- 輸出結果
    job_kind job_kind NOT NULL,
    script_hash BIGINT,
    script_path VARCHAR(255),
    args JSONB,
    logs TEXT,
    language script_lang,
    canceled BOOLEAN DEFAULT FALSE,
    canceled_by VARCHAR(55),
    deleted BOOLEAN DEFAULT FALSE          -- 軟刪除
);

CREATE INDEX idx_completed_created ON completed_job(workspace_id, created_at DESC);
CREATE INDEX idx_completed_script ON completed_job(workspace_id, script_path, created_at DESC);
```

**Job 生命週期：**

```
                    ┌──────────────────────┐
                    │   API 接收請求        │
                    │   (run_script_by_path)│
                    └──────────┬───────────┘
                               │
                    ┌──────────▼───────────┐
                    │ INSERT INTO queue     │
                    │ (running = false)     │
                    │ (scheduled_for = now) │
                    └──────────┬───────────┘
                               │
                    ┌──────────▼───────────┐
                    │ Worker POLL           │
                    │ SELECT ... WHERE      │
                    │   running = false     │
                    │   AND scheduled_for   │
                    │       <= now()        │
                    │ FOR UPDATE SKIP LOCKED│
                    └──────────┬───────────┘
                               │
                    ┌──────────▼───────────┐
                    │ UPDATE queue SET      │
                    │   running = true,     │
                    │   started_at = now()  │
                    └──────────┬───────────┘
                               │
                    ┌──────────▼───────────┐
                    │ 執行腳本              │
                    │ (子程序 + 沙箱)       │
                    │ 持續寫入 logs         │
                    └──────────┬───────────┘
                               │
              ┌────────────────┼────────────────┐
              ▼                                 ▼
    ┌─────────────────┐              ┌─────────────────┐
    │ INSERT INTO      │              │ INSERT INTO      │
    │ completed_job    │              │ completed_job    │
    │ (success = true) │              │ (success = false)│
    └─────────────────┘              └─────────────────┘
              │                                 │
              └────────────────┬────────────────┘
                               ▼
                    ┌─────────────────────┐
                    │ DELETE FROM queue    │
                    │ WHERE id = $1       │
                    └─────────────────────┘
```

## RBAC 權限設計

### 路徑導向的權限模型

Windmill 的權限系統基於**路徑前綴**：

```
u/alice/my_script     → 使用者 alice 擁有
g/devops/deploy       → devops 群組擁有
f/production/scripts  → production 資料夾下
```

### Row-Level Security (RLS)

```sql
-- 啟用 RLS
ALTER TABLE script ENABLE ROW LEVEL SECURITY;

-- 管理員看所有
CREATE POLICY admin_policy ON script
    FOR ALL
    USING (
        current_setting('session.is_admin')::boolean = true
    );

-- 使用者看自己的
CREATE POLICY see_own ON script
    FOR SELECT
    USING (
        SPLIT_PART(path, '/', 1) = 'u'
        AND SPLIT_PART(path, '/', 2) = current_setting('session.user')
    );

-- 群組成員看群組的
CREATE POLICY see_member ON script
    FOR SELECT
    USING (
        SPLIT_PART(path, '/', 1) = 'g'
        AND SPLIT_PART(path, '/', 2) = ANY(
            string_to_array(current_setting('session.groups'), ',')
        )
    );

-- 額外權限（細粒度分享）
CREATE POLICY see_extra_perms ON script
    FOR SELECT
    USING (
        extra_perms ? ('u/' || current_setting('session.user'))
        OR extra_perms ?| (
            SELECT array_agg('g/' || g)
            FROM unnest(string_to_array(current_setting('session.groups'), ',')) g
        )
    );
```

**每次請求時設定 session 變數：**

```rust
// Rust 端在每次 DB 請求前設定
sqlx::query("SELECT set_config('session.user', $1, true)")
    .bind(&username)
    .execute(&pool)
    .await?;
sqlx::query("SELECT set_config('session.groups', $1, true)")
    .bind(&groups.join(","))
    .execute(&pool)
    .await?;
sqlx::query("SELECT set_config('session.is_admin', $1, true)")
    .bind(&is_admin.to_string())
    .execute(&pool)
    .await?;
```

## Resources 和 Variables

```sql
-- 外部資源（資料庫連線、API Key 等）
CREATE TABLE resource (
    workspace_id VARCHAR(50) NOT NULL,
    path VARCHAR(255) NOT NULL,
    value JSONB,                       -- 加密儲存
    resource_type VARCHAR(255),        -- postgres, mysql, slack, etc.
    description TEXT,
    extra_perms JSONB DEFAULT '{}',
    PRIMARY KEY (workspace_id, path)
);

-- 資源類型定義（JSON Schema 驗證）
CREATE TABLE resource_type (
    workspace_id VARCHAR(50) NOT NULL,
    name VARCHAR(255) NOT NULL,
    schema JSONB,                      -- JSON Schema for validation
    description TEXT,
    PRIMARY KEY (workspace_id, name)
);

-- 變數（包含 secrets）
CREATE TABLE variable (
    workspace_id VARCHAR(50) NOT NULL,
    path VARCHAR(255) NOT NULL,
    value TEXT NOT NULL,               -- 加密儲存
    is_secret BOOLEAN DEFAULT FALSE,
    description TEXT,
    extra_perms JSONB DEFAULT '{}',
    PRIMARY KEY (workspace_id, path)
);
```

## 排程系統

```sql
CREATE TABLE schedule (
    workspace_id VARCHAR(50) NOT NULL,
    path VARCHAR(255) NOT NULL,
    schedule VARCHAR(255) NOT NULL,    -- Cron 表達式 "0 */5 * * *"
    timezone VARCHAR(100) DEFAULT 'UTC',
    script_path VARCHAR(255) NOT NULL,
    is_flow BOOLEAN DEFAULT FALSE,
    args JSONB DEFAULT '{}',
    enabled BOOLEAN DEFAULT TRUE,
    on_failure VARCHAR(255),           -- 失敗時執行的腳本
    on_failure_times INT DEFAULT 1,
    on_recovery VARCHAR(255),          -- 恢復時執行的腳本
    PRIMARY KEY (workspace_id, path)
);
```

## 你自己實作時的建議

### Phase 1 最小 Schema

只需要這些表就能跑起來：

1. `workspace` — 多租戶
2. `usr` + `password` + `token` — 認證
3. `script` — 腳本儲存
4. `queue` + `completed_job` — Job 執行
5. `variable` — 變數/秘密

### 使用 SQLx

Windmill 使用 `sqlx` 作為 Rust 的 SQL toolkit：

```rust
// Cargo.toml
[dependencies]
sqlx = { version = "0.8", features = ["runtime-tokio", "postgres", "json", "uuid", "chrono"] }

// 連線池
let pool = PgPoolOptions::new()
    .max_connections(50)
    .connect(&database_url)
    .await?;

// 型別安全查詢
let job = sqlx::query_as!(
    QueuedJob,
    "SELECT * FROM queue WHERE id = $1 AND workspace_id = $2",
    job_id,
    workspace_id
)
.fetch_optional(&pool)
.await?;
```

### Migration 管理

```bash
# 建立新的 migration
cargo sqlx migrate add -r create_scripts_table

# 產生的檔案：
# migrations/20240101000000_create_scripts_table.up.sql
# migrations/20240101000000_create_scripts_table.down.sql

# 執行 migration
cargo sqlx migrate run
```

**重要：** Windmill 已經有 544 個 migration 檔案（272 對 up/down），這代表 schema 持續演進。你的系統也要從一開始就用 migration 管理。
