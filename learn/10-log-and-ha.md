# 規格：內建日誌查看與高可用性

> 草案規格，待審核。核准後合併至 PRD。

---

## 第一部分：內建系統日誌查看

### 1.1 問題陳述

企業客戶可能沒有可觀測性平台（Jaeger、Grafana Loki、Datadog）。即使有，運維人員也需要一個「開箱即用」的 UI 日誌查看器：

1. **Run 日誌** — Script 執行的 stdout/stderr（Phase 1 已規劃）
2. **服務日誌** — API Server / Worker / Scheduler 的程序級日誌（尚未規劃）

若無內建查看功能，排除生產問題需要 SSH 存取或外部工具 — 這對多租戶 SaaS 或託管部署而言是不可接受的。

### 1.2 業界比較

| 引擎 | Run 日誌儲存 | 服務日誌儲存 | 即時串流 | 多後端支援 |
|------|-------------|------------|---------|-----------|
| **Airflow** | FileTaskHandler（本地磁碟）+ 可插拔遠端後端（S3、GCS、ES、CloudWatch、HDFS、Azure Blob） | 不存於 DB；依賴外部聚合 | `TaskLogReader.read_log_stream()` — 1 秒輪詢 | 每部署一個 handler，但 Python logging 允許多 handler |
| **Windmill** | `job_logs` DB 表（`INSERT ... ON CONFLICT DO UPDATE SET logs = concat(...)`） | `service_logs` API + `log_file` DB 表 | 基於追加的 DB 寫入，API 輪詢 | DB 為主；物件儲存用於清理/歸檔 |
| **Kestra** | DB 中的 `LogEntry` 模型，透過 `LogRepositoryInterface`（JDBC） | 同一 `LogEntry` 模型（tenantId、namespace、flowId、triggerId） | `Flux<LogEntry>` 響應式串流（`findAsync`、`findAllAsync`） | 單一 DB 後端；豐富查詢 API + 分頁 |
| **Prefect** | DB 中的 `logs` 表（PostgreSQL/MySQL/SQLite） | 未與 flow/task 日誌分離 | WebSocket 串流（`ws:///api/logs/out`）+ REST 備援 | DB 為主；Python logging handler 可扇出（CloudWatch、file、syslog） |
| **Dagster** | 兩種：Event logs（DB）+ Compute logs（可插拔 `ComputeLogManager`：local、S3、Azure、GCS） | Event logs 在 DB 中（GraphQL 可查詢） | GraphQL subscriptions + `upload_interval` 用於 compute logs | 每實例一個 ComputeLogManager |

### 1.3 關鍵結論

1. **所有引擎都將 run 日誌存於 DB 或可插拔後端** — 從不只用 "stdout"
2. **Windmill 和 Kestra 也將服務日誌存於 DB** — 對內建 UI 而言最簡單
3. **Airflow 擁有最豐富的可插拔後端系統**（9+ 後端）— 但以簡潔性換取彈性
4. **即時串流**：WebSocket（Prefect）、Reactive Flux（Kestra）、輪詢（Airflow/Windmill）
5. **沒有引擎在基礎設施層做真正的雙寫**（可觀測性平台 + DB 同時）；它們依賴應用層 handler 扇出

### 1.4 CoveFlow 的設計方案

#### 1.4.1 架構：透過 `tracing` Subscriber Layer 實現雙寫

CoveFlow 已使用 `tracing` crate 進行結構化日誌（見 `observability.md`）。我們新增一個**第二 subscriber layer**，在現有 stdout/OTel layer 旁邊寫入 DB。

```
tracing::info!("job completed", job_id = %id, duration_ms = elapsed)
           |
           v
    ┌─────────────────────────────────────────┐
    │       tracing-subscriber (layered)       │
    │                                         │
    │  Layer 1: fmt (stdout) ──────────> stdout│
    │  Layer 2: OTel bridge ──> Jaeger/Loki   │  （既有，observability.md）
    │  Layer 3: DB writer ──> PostgreSQL       │  （新增）
    └─────────────────────────────────────────┘
```

**相較 Windmill 方式的優勢：**
- Windmill 使用 `job_logs` 表，在程式碼中散布手動 `INSERT` 呼叫
- CoveFlow 在 `tracing` subscriber 層攔截 — 業務邏輯零修改
- 同一行日誌同時送到 stdout、OTel 和 DB

#### 1.4.2 儲存策略選型

業界有三種做法，各有明確 trade-off：

| 方案 | 代表引擎 | 做法 | 每 run 1 萬行日誌的 row 數 | 優點 | 缺點 |
|------|---------|------|---------------------------|------|------|
| **A. 單行追加** | Windmill | 一個 run 一行，`UPDATE concat(logs, $1)` | **1 row** | 最簡單 | MVCC dead tuple（每次 UPDATE 複製整行）；TOAST 反覆壓縮解壓；無法按 level 過濾/分頁；Windmill 在 9KB 就得壓縮到 S3 |
| **B. 每條一行** | Prefect / Kestra | 每條 log line 一行 | **10,000 rows** | 天然 level 過濾 + cursor 分頁 | 1000 runs/天 = 1000 萬 rows/天；index 維護成本高 |
| **C. 分塊行** | **CoveFlow（採用）** | 每次 flush（500ms / 100 條）一行，JSONB 陣列 | **~100 rows** | row 數少 100 倍；`min_level` 整塊跳過；batch INSERT 天然對齊 | JSONB 內無法用 DB index 過濾單條；需應用層解析 |

**選擇方案 C 的理由：**

1. **寫入放大最小**：DbLogLayer 已按 500ms/100 條批次化，每次 flush = 一次 `INSERT` = 一行。不像方案 A 的 `UPDATE` 會複製整行產生 dead tuple。
2. **row 數可控**：一個跑 5 分鐘、產生 1 萬行日誌的 run ≈ 100-600 rows（vs 方案 B 的 10,000 rows）。1000 runs/天 ≈ 10-60 萬 rows/天，30 天保留 ≈ 1000-1800 萬 rows，加 index 約 2-5 GB — 對 PostgreSQL 而言完全可控。
3. **過濾仍然實用**：`min_level` 欄位讓 DB 層面可跳過整塊 DEBUG chunk（不需解析 JSONB）。細粒度過濾由應用層處理，但每塊最多 100 條，解析成本極低。
4. **SSE 增量推送天然對齊**：前端持有 `last_chunk_id`，每次只拉新 chunk。

#### 1.4.3 DB Schema

```sql
-- Run 執行日誌（分塊儲存）
CREATE TABLE run_log (
    id           BIGSERIAL PRIMARY KEY,
    run_id       UUID NOT NULL REFERENCES run(id),
    workspace_id UUID NOT NULL,
    seq          INT NOT NULL,             -- chunk 序號（同一 run 內遞增）
    created_at   TIMESTAMPTZ NOT NULL DEFAULT now(),  -- chunk 第一條日誌的時間
    min_level    SMALLINT NOT NULL,        -- chunk 內最低 level（用於快速跳過）
    max_level    SMALLINT NOT NULL,        -- chunk 內最高 level（用於快速過濾）
    line_count   SMALLINT NOT NULL,        -- chunk 內日誌條數
    entries      JSONB NOT NULL,           -- [{ts, level, msg, node_id?, ...}, ...]
    -- 反正規化以加速查詢
    flow_id      UUID,
    node_id      TEXT,                     -- 若 chunk 全屬同一 node 則填入，否則 NULL
    worker_name  TEXT,
    archived     BOOLEAN NOT NULL DEFAULT false  -- 已歸檔至物件儲存
);

-- 主要查詢：特定 run 的日誌（按序取 chunks）
CREATE INDEX idx_run_log_run_seq ON run_log (run_id, seq);
-- Dashboard 查詢：workspace 級別日誌瀏覽
CREATE INDEX idx_run_log_workspace ON run_log (workspace_id, created_at);
-- 只看 ERROR/WARN 的快速路徑
CREATE INDEX idx_run_log_level ON run_log (run_id, max_level) WHERE max_level >= 4;
-- 歸檔清理
CREATE INDEX idx_run_log_archive ON run_log (created_at) WHERE archived = false;

-- JSONB entries 格式範例：
-- [
--   {"ts": "2024-01-01T00:00:00.123Z", "level": 3, "msg": "Starting task..."},
--   {"ts": "2024-01-01T00:00:00.456Z", "level": 3, "msg": "Processing row 1"},
--   {"ts": "2024-01-01T00:00:00.789Z", "level": 5, "msg": "Connection failed", "node_id": "step_2"}
-- ]

-- 服務級日誌（API/Worker/Scheduler 程序日誌）
-- 同樣採用分塊策略
CREATE TABLE service_log (
    id           BIGSERIAL PRIMARY KEY,
    instance_id  TEXT NOT NULL,             -- 例如 "worker-01"、"api-02"
    service      TEXT NOT NULL,             -- "api" | "worker" | "scheduler"
    seq          INT NOT NULL,
    created_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    min_level    SMALLINT NOT NULL,
    max_level    SMALLINT NOT NULL,
    line_count   SMALLINT NOT NULL,
    entries      JSONB NOT NULL
);

CREATE INDEX idx_service_log_instance ON service_log (instance_id, created_at);
CREATE INDEX idx_service_log_service  ON service_log (service, created_at);
```

**容量估算：**

| 場景 | runs/天 | 平均日誌行數/run | chunks/run | rows/天 | 30 天保留（含 index） |
|------|--------|----------------|-----------|--------|---------------------|
| 小型 | 100 | 1,000 | ~10 | 1,000 | ~50 MB |
| 中型 | 1,000 | 10,000 | ~100 | 100,000 | ~3 GB |
| 大型 | 10,000 | 10,000 | ~100 | 1,000,000 | ~30 GB |
| 超大型 | 10,000 | 100,000 | ~1,000 | 10,000,000 | ~300 GB（需分區） |

超大型場景建議啟用按月分區 + 冷歸檔至物件儲存。

#### 1.4.4 DB Writer Subscriber

```rust
/// 批次非同步 DB 日誌寫入器。
/// 緩衝日誌事件，每 500ms 或 100 筆事件刷新為一個 chunk 寫入 DB。
pub struct DbLogLayer {
    tx: tokio::sync::mpsc::Sender<LogEvent>,  // bounded(10_000)
}

struct LogEvent {
    timestamp: chrono::DateTime<chrono::Utc>,
    level: tracing::Level,
    target: String,
    message: String,
    fields: serde_json::Value,
    // context：從 span 填入
    run_id: Option<uuid::Uuid>,
    node_id: Option<String>,
    instance_id: String,
    service: String,
}

/// 背景 flush task 將多條 LogEvent 組裝成一個 chunk
struct LogChunk {
    run_id: uuid::Uuid,
    workspace_id: uuid::Uuid,
    seq: i32,
    created_at: chrono::DateTime<chrono::Utc>,  // chunk 第一條的 timestamp
    min_level: i16,
    max_level: i16,
    line_count: i16,
    entries: serde_json::Value,  // [{ts, level, msg, node_id?}, ...]
    flow_id: Option<uuid::Uuid>,
    node_id: Option<String>,    // 若 chunk 全屬同一 node 則填入
    worker_name: String,
}
```

**刷新策略**：
- 透過 bounded channel（容量 10,000）在記憶體中緩衝事件
- 背景任務每 **500ms** 或緩衝達 **100 筆事件** 時觸發 flush
- flush 時按 `run_id` 分組，每組組裝成一個 `LogChunk`，計算 `min_level` / `max_level`
- 每個 chunk 一次 `INSERT`；若同時有多個 run 的日誌，用 batch `INSERT INTO ... VALUES (...), (...)`
- DB 錯誤時：寫入 stderr（永不阻塞應用程式），以指數退避重試
- 關閉時：退出前刷新剩餘緩衝

**資料丟失風險與緩解：**

批次寫入 DB 存在以下掉資料情境：

| 情境 | 丟失量 | 緩解措施 |
|------|--------|---------|
| 程序崩潰（SIGKILL / OOM） | 最多 500ms 內的緩衝日誌 | 註冊 shutdown hook（SIGTERM），graceful drain buffer |
| DB 持續不可用 | 重試佇列滿後丟棄 | 重試佇列上限 50,000 筆；超過後寫入本地 WAL 檔案（`/var/log/coveflow/log-wal/`），恢復後重播 |
| Channel 滿（背壓） | 新事件被丟棄 | 使用 `try_send`，失敗時遞增 `log_events_dropped` counter（Prometheus metric），運維可告警 |

**設計決策**：日誌是最終一致的輔助資料，不應阻塞主流程。接受極端情境下少量丟失，但透過 WAL + metrics 將丟失量降至最低並使其可觀測。

#### 1.4.6 API 端點

```
GET /api/v1/runs/{run_id}/logs
    ?level=INFO            -- 最低等級過濾（DB 層用 max_level >= level 跳過不符 chunk）
    &after_chunk=<id>      -- 基於 chunk id 的游標分頁（用於 SSE 增量拉取）
    &limit=50              -- 每頁最大 chunk 數（非 log line 數）
    Response: {
      chunks: [
        { id: 123, seq: 0, created_at: "...", min_level: 3, max_level: 5,
          entries: [{ts, level, msg}, ...] },
        ...
      ],
      next_cursor: 125
    }
    -- 前端收到 chunks 後在客戶端展平 entries 並按 level 過濾顯示

GET /api/v1/services/logs
    ?service=worker        -- 依服務類型過濾
    &instance=worker-01    -- 依實例過濾
    &level=WARN
    &after_chunk=<id>
    &limit=50
    Response: { chunks: [...], next_cursor: "..." }

GET /api/v1/runs/{run_id}/logs/stream
    SSE 端點：即時尾端追蹤
    ?level=INFO&after_chunk=<last_id>
    Server-Sent Events:
      data: {"chunk_id":124,"seq":5,"entries":[{...},{...}]}

GET /api/v1/services/logs/stream
    SSE 端點：服務日誌即時追蹤
    ?service=worker&level=INFO
```

**為何選擇 SSE 而非 WebSocket：**
- 實作更簡單（無升級握手、無 ping/pong）
- 無需特殊配置即可穿越代理和 CDN
- 單向（日誌是唯讀的）
- 瀏覽器原生 `EventSource` API
- Kestra 和 Airflow 都使用單向串流；只有 Prefect 使用雙向 WebSocket

#### 1.4.5 日誌保留、清理與冷歸檔

**三階段生命週期：Hot → Warm → Cold**

```
Hot（DB，即時查詢）     Warm（DB，降頻查詢）      Cold（物件儲存，按需取回）
  0 ~ 7 天               7 ~ 30 天                 30 天+
  ───────────────────>  ───────────────────────>  ─────────────────────>
  run_log 原表           run_log 原表（即將歸檔）    S3/GCS/MinIO (NDJSON.gz)
```

**Cold 歸檔設計：**

```rust
/// LogArchiver trait — 可插拔儲存後端
#[async_trait]
pub trait LogArchiver: Send + Sync {
    /// Archive logs older than cutoff, return archived count
    async fn archive(&self, cutoff: DateTime<Utc>) -> Result<u64>;
    /// Retrieve archived logs for a specific run
    async fn retrieve(&self, run_id: Uuid) -> Result<Vec<LogEntry>>;
}

// Implementations: S3Archiver, GcsArchiver, LocalFileArchiver (dev)
```

歸檔流程（在 scheduler 迴圈中執行）：
1. 查詢 `run_log WHERE created_at < now() - hot_retention AND NOT archived`
2. 依 `run_id` 分組，序列化為 NDJSON 格式
3. gzip 壓縮後上傳至物件儲存：`s3://{bucket}/logs/{workspace_id}/{run_id}.ndjson.gz`
4. 上傳成功後標記 `archived = true`，後續清理刪除已歸檔記錄

**DB 清理（熱區保留後刪除已歸檔資料）：**

```sql
-- 歸檔完成後的清理
DELETE FROM run_log
WHERE created_at < now() - INTERVAL '30 days'
AND archived = true;

DELETE FROM service_log
WHERE created_at < now() - INTERVAL '7 days';
```

**API 整合：** 前端查詢日誌時，若 DB 中無資料，API 自動從物件儲存取回：

```
GET /api/v1/runs/{run_id}/logs
    若 run 已完成超過 30 天 → 從物件儲存串流取回
    否則 → 從 DB 查詢
```

透過 `WorkspaceSettings` 可配置：
- `run_log_hot_retention_days: 7`（預設，DB 中保留天數）
- `run_log_archive_retention_days: 30`（預設，歸檔前在 DB 的總天數）
- `run_log_cold_retention_days: 365`（預設，物件儲存保留天數）
- `service_log_retention_days: 7`（預設）
- `log_archive_backend: "s3" | "gcs" | "local"`（預設：local）
- 大型部署：考慮依月份對 `run_log` 分區（`PARTITION BY RANGE (created_at)`）

#### 1.4.6 功能開關

```rust
// 在 config 中
pub struct LogConfig {
    pub db_log_enabled: bool,           // 預設：true
    pub db_log_flush_interval_ms: u64,  // 預設：500
    pub db_log_batch_size: usize,       // 預設：100
    pub db_log_min_level: Level,        // 預設：INFO（DB 跳過 DEBUG/TRACE）
    pub service_log_enabled: bool,      // 預設：true
}
```

當 `db_log_enabled = false` 時，`DbLogLayer` 變為 no-op — 零開銷。
當外部可觀測性（Jaeger/Loki）已配置時，運維人員可停用 DB 日誌以減少 DB 負載。

#### 1.4.7 實作優先序

| 階段 | 範圍 |
|------|------|
| Phase 1.x | `run_log` 表 + 僅用於 run 執行日誌的 `DbLogLayer` + 本地 WAL 防丟失 |
| Phase 2 | `service_log` 表 + 服務日誌 API + SSE 串流 |
| Phase 2 | 冷日誌歸檔至物件儲存（`LogArchiver` trait + S3/GCS 實作） |
| Phase 2+ | Scheduler 迴圈中的日誌保留清理 + 分區策略 |

---

## 第二部分：高可用性

### 2.1 背景

CoveFlow 遵循 Windmill 的「PostgreSQL 就是一切」架構：
- Job 佇列：`FOR UPDATE SKIP LOCKED`
- 分散式鎖：Advisory locks / `concurrency_locks` 表
- Pub/Sub：`LISTEN/NOTIFY`
- 權限：Row-Level Security（RLS）

PostgreSQL 是**最關鍵的單一元件**。所有 HA 決策都以此事實為基礎。

### 2.2 為何 Active-Active PostgreSQL 對 CoveFlow 不可行

這是最需要記錄的架構限制。

CoveFlow 的 job 佇列依賴三個 PostgreSQL 功能，這些功能與多主複製（BDR/pglogical）**根本不相容**：

| 功能 | 為何在 Active-Active 中失效 |
|------|---------------------------|
| `FOR UPDATE SKIP LOCKED` | 行鎖是實例本地的。DC-B 的 Worker 看到 DC-A 已鎖定的行為未鎖定。**結果：重複執行 job。** |
| `pg_try_advisory_lock` | Advisory locks 只存在於發出實例的共享記憶體中。從不複製。**並發限制完全被繞過。** |
| `LISTEN/NOTIFY` | 實例本地。PG-A 上的 `NOTIFY` 永遠不會到達 PG-B 的監聽器。**即時排程失效。** |

**每個主要工作流引擎都證實了這一點：**
- Windmill：「PostgreSQL 包含整個狀態」— 單一主節點，始終如此
- Airflow：資料庫 HA 委託給託管服務（RDS Multi-AZ），單一主節點
- Dagster：依賴託管 PostgreSQL HA，無多主文件
- Temporal：使用 Cassandra（支援多 DC）或 PostgreSQL（每叢集單一主節點）

### 2.3 Temporal 多叢集複製分析

我們檢視了 Temporal 的原始碼（`/Users/stanhsu/projects/temporal`）以了解其如何處理多資料中心：

#### 2.3.1 架構

Temporal 使用 **namespace 級別的 active/standby 複製**：

```
Cluster A（namespace "prod" 的 Active）     Cluster B（"prod" 的 Standby）
  ┌─────────────────┐                        ┌─────────────────┐
  │ Frontend Service │                        │ Frontend Service │
  │ History Service  │ ──非同步複製──>        │ History Service  │
  │ Matching Service │                        │ Matching Service │
  │ Worker Service   │                        │ Worker Service   │
  └────────┬────────┘                        └────────┬────────┘
           │                                          │
     ┌─────┴─────┐                              ┌─────┴─────┐
     │ Database A │                              │ Database B │
     │ (PG/Cass)  │                              │ (PG/Cass)  │
     └───────────┘                              └───────────┘
```

關鍵：**每個叢集有自己獨立的資料庫**。無共享儲存。

#### 2.3.2 原始碼發現

來自 Temporal 程式碼庫：

- **`ReplicationStream` 介面**（`service/history/interfaces/replication_stream.go`）：
  - `SubscribeReplicationNotification(pollingCluster)` — standby 叢集訂閱
  - `ConvertReplicationTask(task, clusterID)` — 將歷史事件轉換為複製任務
  - `GetReplicationTasksIter(pollingCluster, minID, maxID)` — 基於拉取的任務迭代

- **`HistoryReplicator`**（`service/history/ndc/history_replicator.go`）：
  - `ApplyEvents()` — 將複製的事件套用至本地歷史
  - `ReplicateHistoryEvents()` — 帶版本歷史驗證的批次版本
  - 使用 `BranchMgr` + `ConflictResolver` 處理版本歷史樹分歧
  - NDC = "Non-Deterministic Conflict resolution"

- **`Replicator`** worker（`service/worker/replicator/replicator.go`）：
  - 每個遠端叢集一個 `replicationMessageProcessor`
  - 動態監聽 `clusterMetadata` 變更
  - `NamespaceReplicationQueue` 帶 DLQ（Dead Letter Queue）處理失敗任務
  - 清理間隔：5 分鐘

- **`ReplicationResolver`**（`common/namespace/replication_resolver.go`）：
  - `ActiveClusterName()` — 決定哪個叢集處理變更
  - `FailoverVersion` — 每次故障轉移單調遞增的版本
  - `ActiveInCluster(clusterName)` — 本地 namespace 始終為 "active"

#### 2.3.3 衝突解決

Temporal 將工作流執行歷史建模為**版本樹**：
- 每個歷史事件攜帶來源叢集的版本
- 發生分歧時（兩個叢集在網路分區期間都進行變更），形成分支
- **解決方式：最高版本分支獲勝** — 從獲勝分支重建可變狀態
- 版本規則：`(namespace 中的版本) % (共享版本增量) == (叢集的初始版本)`

#### 2.3.4 限制（實驗性）

| 限制 | 影響 |
|------|------|
| **實驗性狀態** | 不受正常版本/支援政策約束 |
| **最終一致性** | Standby 叢集讀取可能過時 |
| **Activity 完成不複製** | Activity 在故障轉移後逾時並重試 |
| **進度可能回退** | 工作流狀態在故障轉移期間可能恢復 |
| **每個叢集需要自己的 DB** | 2 倍基礎設施成本 |

#### 2.3.5 對 CoveFlow 的適用性

Temporal 的方式**不適用於** CoveFlow，原因如下：

1. **架構不同**：Temporal 將佇列（matching service）與狀態（history service）分離。CoveFlow 兩者都使用 PostgreSQL。
2. **複雜度**：Temporal 的複製涉及 NDC 衝突解決、版本歷史樹、DLQ 處理和 per-namespace 路由。這是 Go monolith 中數千行程式碼。
3. **取捨**：Temporal 接受 activity 完成丟失、進度回退和最終一致性。Job 佇列引擎不能接受重複執行。
4. **每叢集獨立 DB**：Temporal 的模型要求每叢集獨立資料庫。CoveFlow 的價值主張是「一個 PostgreSQL，一切搞定」。

**結論**：對 CoveFlow 而言，正確的多資料中心策略是 **active-passive PostgreSQL 複製**，而非 Temporal 式的多叢集。

### 2.4 建議的 HA 層級

#### Tier 0：開發環境（無 HA）

```
單一 PostgreSQL 實例
單一 API server + 內嵌 worker
```

- **RTO**：數小時（從備份還原）
- **RPO**：最後備份
- **成本**：約 $50/月

#### Tier 1：最小生產 HA

```
PostgreSQL：1 Primary + 1 Sync Standby（同區域，不同 AZ）
  託管服務（RDS、Cloud SQL、Aurora）或 Patroni + etcd（3 節點）
PgBouncer：1 實例（transaction mode + session mode pool）
API Server：2 實例在負載平衡器後方
Workers：2-5 實例
```

- **RTO**：30-60 秒（自動故障轉移）
- **RPO**：零（區域內同步複製）
- **成本**：約 $300-600/月

**PgBouncer 備註**：CoveFlow 同時使用 `FOR UPDATE SKIP LOCKED`（交易鎖，可在 transaction mode 中運作）和 `LISTEN/NOTIFY`（session 級別，需要 session mode）。解決方案：**兩個 PgBouncer 池** — transaction mode 用於一般查詢，session mode 用於 LISTEN/NOTIFY 連線。

#### Tier 2：標準生產 HA

```
PostgreSQL：1 Primary + 1 Sync Standby + 1-2 Async Read Replicas
PgBouncer：HA 配對（2 實例）
API Server：3+ 實例（K8s HPA）
Workers：10-50 實例，基於 tag 的路由
Scheduler：內嵌於每個 worker（FOR UPDATE SKIP LOCKED 分配工作）

Read replica 目標：
  - Dashboard/監控查詢
  - Job 歷史瀏覽（completed_job 表）
  - Script/flow CRUD 讀取
  - 稽核日誌讀取

不放在 read replicas 上：
  - Job 佇列輪詢（需要 primary）
  - Advisory locks
  - LISTEN/NOTIFY
```

- **RTO**：15-30 秒
- **RPO**：零
- **成本**：約 $1000-3000/月

#### Tier 3：企業多區域 HA

```
主要區域（Active）：
  PostgreSQL：Aurora Multi-AZ 或 Patroni 叢集
  PgBouncer：HA 配對
  API Server：5+ 實例
  Workers：50-200+ 實例
  Scheduler：內嵌於 workers

DR 區域（Warm Standby）：
  PostgreSQL：跨區域非同步副本（Aurora Global DB 或串流複製）
  API Server：預部署，閒置
  Workers：預部署，閒置
  DNS：Route 53 / CloudFlare 健康檢查故障轉移

故障轉移程序：
  1. 偵測主要區域故障（健康檢查逾時）
  2. 將 DR PostgreSQL 副本提升為主節點
  3. DNS 故障轉移將流量路由至 DR 區域
  4. DR 區域的 Workers 開始處理佇列
  5. Zombie 偵測處理故障區域中正在執行的 jobs
```

- **RTO**：5-15 分鐘（DNS 傳播 + 提升）
- **RPO**：秒級（非同步跨區域複製延遲）
- **成本**：約 $5000-15000/月

**資料丟失處理**：
- 故障瞬間在佇列中的 Jobs 可能丟失（非同步間隙）
- 緩解措施：冪等鍵用於重新提交、cron jobs 自動重新觸發、zombie 偵測捕獲孤立的 running jobs

### 2.5 PostgreSQL HA 工具比較

| 工具 | 架構 | 外部依賴 | 故障轉移時間 | 最適合 |
|------|------|---------|------------|--------|
| **託管 PG**（Aurora、Cloud SQL） | 供應商管理 | 無 | 30-60 秒 | 多數部署 — 建議預設 |
| **Patroni** | 透過 DCS 的 leader 選舉 | etcd/ZooKeeper/Consul（3+ 節點） | 5-30 秒 | 自建、K8s、大規模 |
| **pg_auto_failover** | 基於 Monitor 的狀態機 | Monitor 節點（單一 PG 實例） | 10-30 秒 | 自建、小規模（2-3 節點） |
| **Stolon** | 基於 etcd，K8s 原生 | etcd | 10-30 秒 | Kubernetes 原生部署 |

**建議**：除非有特定自建需求，否則使用託管 PostgreSQL（Aurora/Cloud SQL）。若自建，使用 Patroni。

### 2.6 連線池策略

CoveFlow workers 同時使用交易式查詢和 session 級功能：

```
                   ┌─────────────────────────────────────┐
                   │           PgBouncer                  │
                   │                                     │
  Workers ─────>   │  Pool A（transaction mode）          │──> PostgreSQL Primary
  （job 佇列、     │    max_client_conn = 1000           │
   CRUD）          │    default_pool_size = 50           │
                   │                                     │
  Workers ─────>   │  Pool B（session mode）              │──> PostgreSQL Primary
  （LISTEN/NOTIFY、│    max_client_conn = 100            │
   advisory locks）│    default_pool_size = 20           │
                   └─────────────────────────────────────┘
```

**擴展公式**：N 個 workers，每個需要約 5 個連線：
- N < 50：不需要 PgBouncer（PostgreSQL 預設 100 連線即足夠）
- 50 < N < 200：建議使用 PgBouncer
- N > 200：必須使用 PgBouncer，考慮增加 PostgreSQL `max_connections`

### 2.7 同步複製效能

根據業界基準測試：

| 網路距離 | 往返延遲 | 同步複製開銷 | 建議 |
|---------|---------|------------|------|
| 同一 AZ | 0.1-0.5ms | 可忽略（<1%） | 始終同步 |
| 同區域，不同 AZ | 1-3ms | 低（2-5%） | 建議同步 |
| 跨區域，同大洲 | 20-50ms | 高（30-50% 吞吐量損失） | 僅非同步 |
| 跨洲 | 100-200ms | 嚴重（50%+ 吞吐量損失） | 僅非同步 |

對於 CoveFlow 的 `INSERT INTO queue` + `FOR UPDATE SKIP LOCKED` 模式，每次寫入都需要同步提交。在跨區域延遲下，最大單執行緒寫入吞吐量降至約 20 TPS — 對高吞吐量工作負載而言不可接受。

**決策**：僅在區域內使用同步複製。跨區域始終為非同步（接受數秒潛在資料丟失）。

### 2.8 備份與災難復原：pgBackRest

HA 解決故障轉移（failover），但無法解決**資料損毀、人為誤操作、或需要回溯到特定時間點**的需求。pgBackRest 是 PostgreSQL 生態系中最成熟的備份工具，提供 WAL 歸檔、全量/增量備份、以及 PITR（Point-in-Time Recovery）。

> **註**：若使用託管 PostgreSQL（Aurora、Cloud SQL、RDS），供應商已內建自動備份與 PITR。本節適用於自建（self-hosted）PostgreSQL 部署。

#### 2.8.1 核心機制

**WAL 歸檔（Write-Ahead Log Archiving）**

PostgreSQL 所有寫入操作先寫入 WAL，再套用到資料檔案。pgBackRest 將這些 WAL 段檔案（每個 16MB）持續推送至備份儲存庫：

```
PostgreSQL 寫入流程：
  Client → WAL Buffer → WAL 段檔案（pg_wal/） → 資料檔案

pgBackRest 歸檔：
  WAL 段檔案 → archive-push → 備份儲存庫（本地磁碟 / S3 / GCS / Azure）
```

兩種模式：

| 模式 | 機制 | 延遲 | 適用場景 |
|------|------|------|---------|
| **同步（archive_command）** | PostgreSQL 寫滿 16MB WAL 段後呼叫 `pgbackrest archive-push` | 段寫滿才推送（可能數秒到數分鐘） | Tier 1 基本保護 |
| **非同步（archive-push-async）** | pgBackRest 啟動 spool 目錄 + 多個平行 worker，持續監控 `pg_wal/` | 接近即時（秒級） | Tier 2/3 需要低 RPO |

**全量備份（Full Backup）**

pgBackRest 對整個 PostgreSQL data directory 做一致性快照：

1. 呼叫 `pg_backup_start()`（非阻塞 checkpoint）
2. 複製所有資料檔案至備份儲存庫（支援平行傳輸 + 壓縮）
3. 呼叫 `pg_backup_stop()`
4. 記錄 manifest（每個檔案的路徑、大小、checksum）

**差異備份（Differential Backup）**

只備份自上次**全量備份**以來有變更的檔案。pgBackRest 比對 manifest 中的 checksum 來判斷哪些檔案改變了：

```
Full (Day 1) ─── Diff (Day 2) ─── Diff (Day 3) ─── Diff (Day 4)
還原 Day 4 = Full + Diff(Day 4)  ← 只需兩份
```

**增量備份（Incremental Backup）**

只備份自上次**任何類型備份**以來有變更的檔案：

```
Full (Day 1) ─── Incr (Day 2) ─── Incr (Day 3) ─── Incr (Day 4)
還原 Day 4 = Full + Incr(Day 2) + Incr(Day 3) + Incr(Day 4)  ← 需要全部
```

**比較：**

| 類型 | 備份速度 | 備份大小 | 還原速度 | 還原依賴 |
|------|---------|---------|---------|---------|
| Full | 最慢 | 最大 | 最快 | 只需自身 |
| Differential | 中等 | 中等 | 快 | Full + 該 Diff |
| Incremental | 最快 | 最小 | 最慢 | Full + 所有中間 Incr |

**PITR（Point-in-Time Recovery）**

結合 base backup + WAL replay，可將資料庫恢復到**任意時間點**：

```
還原流程：
  1. 找到目標時間之前最近的 base backup
  2. 還原 base backup 的資料檔案
  3. 從備份儲存庫取得該 backup 之後的 WAL 段
  4. 重播 WAL 直到目標時間（recovery_target_time）
  5. 開啟資料庫

時間軸：
  ──[Full Backup]──WAL──WAL──WAL──[誤操作!]──WAL──
                                    ↑
                            recovery_target_time
                            恢復到這一刻之前
```

#### 2.8.2 實際配置

**PostgreSQL 設定（`postgresql.conf`）**

```ini
# 啟用 WAL 歸檔
wal_level = replica                    # 至少 replica（logical 也可以）
archive_mode = on                      # 啟用歸檔
archive_command = 'pgbackrest --stanza=coveflow archive-push %p'

# 效能調校
max_wal_senders = 5                    # 預留給串流複製 + pgBackRest
wal_keep_size = 1GB                    # 緩衝，避免 WAL 被回收太快
```

**pgBackRest 設定（`/etc/pgbackrest/pgbackrest.conf`）**

```ini
[global]
# 備份儲存庫位置（三選一）
repo1-type=posix                       # 本地磁碟
repo1-path=/var/lib/pgbackrest

# 或 S3：
# repo1-type=s3
# repo1-s3-bucket=coveflow-backup
# repo1-s3-region=ap-northeast-1
# repo1-s3-endpoint=s3.amazonaws.com
# repo1-s3-key=<access-key>
# repo1-s3-key-secret=<secret-key>

# 或 GCS：
# repo1-type=gcs
# repo1-gcs-bucket=coveflow-backup
# repo1-gcs-key=/etc/pgbackrest/gcs-key.json

# 壓縮
repo1-cipher-type=aes-256-cbc         # 加密備份（建議）
repo1-cipher-pass=<encryption-passphrase>
compress-type=zst                      # zstandard 壓縮（速度/比率最佳）
compress-level=3

# 平行處理
process-max=4                          # 備份/還原時使用 4 個平行 worker

# 保留策略
repo1-retention-full=2                 # 保留最近 2 份全量備份
repo1-retention-diff=7                 # 保留最近 7 份差異備份

# 非同步 WAL 歸檔（建議 Tier 2+）
archive-async=y                        # 啟用非同步推送
spool-path=/var/spool/pgbackrest       # spool 目錄
archive-push-queue-max=4GB             # WAL 佇列上限

# 日誌
log-level-console=info
log-level-file=detail
log-path=/var/log/pgbackrest

[coveflow]
# stanza 定義（一個 stanza = 一個 PG 叢集）
pg1-path=/var/lib/postgresql/16/main   # PostgreSQL data directory
pg1-port=5432
pg1-user=postgres
```

#### 2.8.3 操作指令

**初始化（首次設定）**

```bash
# 建立 stanza（在 pgBackRest 儲存庫中註冊此 PG 叢集）
pgbackrest --stanza=coveflow stanza-create

# 驗證設定正確
pgbackrest --stanza=coveflow check
```

**執行備份**

```bash
# 全量備份
pgbackrest --stanza=coveflow --type=full backup

# 差異備份（自上次 full 以來的變更）
pgbackrest --stanza=coveflow --type=diff backup

# 增量備份（自上次任何備份以來的變更）
pgbackrest --stanza=coveflow --type=incr backup
```

**建議排程（crontab）**

```cron
# 每週日 02:00 全量備份
0 2 * * 0  pgbackrest --stanza=coveflow --type=full backup

# 每天 02:00 差異備份（週日除外）
0 2 * * 1-6  pgbackrest --stanza=coveflow --type=diff backup
```

**查看備份資訊**

```bash
# 列出所有備份
pgbackrest --stanza=coveflow info

# 輸出範例：
# stanza: coveflow
#     status: ok
#     cipher: aes-256-cbc
#
#     db (current)
#         wal archive min/max (16): 000000010000000000000001/000000010000000000000047
#
#         full backup: 20250101-020000F
#             timestamp start/stop: 2025-01-01 02:00:00+00 / 2025-01-01 02:15:30+00
#             database size: 2.5GB, database backup size: 2.5GB
#             repo1: backup size: 800MB
#
#         diff backup: 20250101-020000F_20250102-020000D
#             timestamp start/stop: 2025-01-02 02:00:00+00 / 2025-01-02 02:02:10+00
#             database size: 2.5GB, database backup size: 120MB
#             repo1: backup size: 40MB
```

**還原操作**

```bash
# 停止 PostgreSQL
sudo systemctl stop postgresql

# 還原到最新狀態（使用最近的備份 + 所有可用 WAL）
pgbackrest --stanza=coveflow restore

# 還原到特定時間點（PITR）
pgbackrest --stanza=coveflow restore \
    --type=time \
    --target="2025-01-15 14:30:00+08"

# 還原到特定備份
pgbackrest --stanza=coveflow restore \
    --set=20250101-020000F_20250102-020000D

# Delta 還原（只還原有差異的檔案，速度更快）
pgbackrest --stanza=coveflow restore \
    --delta \
    --type=time \
    --target="2025-01-15 14:30:00+08"

# 啟動 PostgreSQL（會自動重播 WAL 到目標時間）
sudo systemctl start postgresql
```

**WAL 歸檔驗證**

```bash
# 檢查 WAL 歸檔是否正常運作
pgbackrest --stanza=coveflow check

# 手動觸發 WAL 段切換（測試用）
psql -c "SELECT pg_switch_wal();"
```

#### 2.8.4 與 CoveFlow HA 層級的對應

| 層級 | 備份策略 | RPO（備份） | 建議配置 |
|------|---------|------------|---------|
| **Tier 0**（開發） | 每日 full backup 至本地磁碟 | 24 小時 | `repo1-type=posix`，手動排程 |
| **Tier 1**（最小生產） | 每週 full + 每日 diff + WAL 同步歸檔至 S3 | 數分鐘（WAL 段寫滿才推送） | `archive_command` 同步模式 |
| **Tier 2**（標準生產） | 每週 full + 每日 diff + WAL 非同步歸檔至 S3 | 秒級 | `archive-async=y` + `process-max=4` |
| **Tier 3**（企業多區域） | 多 repo（`repo1` 本地 + `repo2` 異地 S3） + WAL 非同步歸檔 | 秒級 + 異地副本 | 雙 repo 配置，`repo2` 指向 DR 區域的 S3 |

**Tier 3 雙 repo 配置範例：**

```ini
[global]
# 本地 repo（快速還原）
repo1-type=posix
repo1-path=/var/lib/pgbackrest
repo1-retention-full=2

# 異地 S3 repo（災難復原）
repo2-type=s3
repo2-s3-bucket=coveflow-backup-dr
repo2-s3-region=us-west-2
repo2-retention-full=4
repo2-retention-diff=14
```

**重要提醒：**
- pgBackRest 備份是 HA 的**補充**，不是替代。Patroni/託管 PG 處理即時故障轉移（秒級 RTO），pgBackRest 處理**資料保護**（防損毀、防誤刪、合規留存）。
- 定期測試還原流程。備份從未被驗證過等於不存在。建議每月在測試環境執行一次完整還原演練。
- 監控 WAL 歸檔延遲：若 `archive-push` 落後，代表 RPO 增大。可透過 `pg_stat_archiver` 視圖監控。

#### 2.8.5 部署模式：同機 vs 獨立備份機 vs K8s Sidecar

**模式 A：同機部署（Local）**

```
┌─────────────────────────────┐
│  同一台主機                   │
│  PostgreSQL + pgBackRest     │
│       │                      │
│       └──> /var/lib/pgbackrest (本地 repo)
│            或 → S3/GCS (遠端 repo)
└─────────────────────────────┘
```

- 最簡單，Tier 0/1 適用
- pgBackRest 直接讀 `pg_wal/` 和 data directory，不需任何網路協定
- 缺點：主機掛了，本地 repo 也沒了（因此通常搭配 S3 repo）

**模式 B：獨立備份機（Remote Repo Host）**

```
┌──────────────┐     SSH 或 TLS (8432)     ┌──────────────────┐
│  PG 主機      │  ◄────────────────────►  │  備份主機（Repo Host）│
│  PostgreSQL   │                          │  pgBackRest       │
│  pgbackrest   │                          │  /var/lib/pgbackrest
│  (client)     │                          │  (儲存所有備份+WAL) │
└──────────────┘                           └──────────────────┘
```

- PG 機器上裝 pgBackRest 但只負責 `archive-push`（推 WAL）
- 備份主機負責排程備份、儲存、保留策略
- 好處：備份 I/O 不搶 DB 的磁碟頻寬；PG 主機完全掛掉，備份仍在
- 通訊協定選擇見下方 §2.8.6

**模式 C：K8s Sidecar + S3（建議 K8s 環境使用）**

K8s 環境中 Pod 短暫、無固定 IP，SSH 管理困難。主流做法是**不用獨立備份機**，直接從 PG Pod 推到 S3：

```
┌─ PG Pod ─────────────────────┐
│  Container 1: PostgreSQL      │
│  Container 2: pgBackRest      │  ──直接推──> S3 / GCS
│    (sidecar, 共享 PVC)        │
└──────────────────────────────┘
```

- pgBackRest 以 sidecar container 跑在同一個 Pod，透過共享 PVC 存取 `pg_wal/` 和 data directory
- repo 直接指 S3/GCS，完全不需要 SSH 或 TLS
- 主流 K8s PG Operator（CloudNativePG、CrunchyData PGO）都內建此模式

```yaml
# K8s Pod spec 示意
spec:
  containers:
    - name: postgresql
      image: postgres:16
      volumeMounts:
        - name: pgdata
          mountPath: /var/lib/postgresql/data

    - name: pgbackrest
      image: pgbackrest/pgbackrest:latest
      volumeMounts:
        - name: pgdata
          mountPath: /var/lib/postgresql/data   # 共享 PG data
          readOnly: true
        - name: spool
          mountPath: /var/spool/pgbackrest
      env:
        - name: PGBACKREST_REPO1_TYPE
          value: s3
        - name: PGBACKREST_REPO1_S3_BUCKET
          value: coveflow-backup
        - name: PGBACKREST_REPO1_S3_REGION
          value: ap-northeast-1

  volumes:
    - name: pgdata
      persistentVolumeClaim:
        claimName: pg-data-pvc
    - name: spool
      emptyDir: {}
```

K8s 環境下各需求的對應做法：

| 需求 | 做法 |
|------|------|
| WAL 歸檔 | sidecar `archive-push` → S3 |
| 定時備份 | CronJob 或 Operator 自動排程 → S3 |
| 還原 | 新 Pod 的 init container 從 S3 拉 backup + replay WAL |
| 獨立備份機 | **不需要**，S3 就是 repo |

**各模式適用場景：**

| 層級 | VM / Bare Metal | K8s |
|------|----------------|-----|
| Tier 0 | 模式 A（同機 + 本地 repo） | 模式 C（sidecar + S3） |
| Tier 1 | 模式 A（同機 + S3 repo） | 模式 C（sidecar + S3） |
| Tier 2/3 | 模式 B（獨立備份機 + S3） | 模式 C（sidecar + S3 + 雙 repo） |

#### 2.8.6 遠端通訊協定：SSH vs TLS

模式 B（獨立備份機）需要 PG 主機與備份主機之間通訊。pgBackRest 支援兩種協定：

**SSH（傳統方式）**

```ini
# PG 主機
[global]
repo1-host=backup-server.internal
repo1-host-user=pgbackrest
# repo1-host-type 預設就是 ssh
```

每次操作 fork 一個 ssh process，透過 SSH tunnel 傳輸資料。

**TLS（內建協定，pgBackRest 2.x+）**

pgBackRest 內建了一個 TCP server（預設 port 8432），使用 mutual TLS (mTLS) 雙向憑證驗證，不依賴 SSH：

```
PG 主機                                    Repo 主機
┌──────────────────┐                      ┌──────────────────┐
│ pgbackrest       │   mTLS (port 8432)   │ pgbackrest server│
│ (client)         │ ◄──────────────────► │ (daemon)         │
└──────────────────┘                      └──────────────────┘
```

Repo 主機配置 — 啟動 TLS server：

```ini
[global]
tls-server-address=0.0.0.0
tls-server-port=8432
tls-server-ca-file=/etc/pgbackrest/ca.crt
tls-server-cert-file=/etc/pgbackrest/server.crt
tls-server-key-file=/etc/pgbackrest/server.key
tls-server-auth=pg-primary.internal=coveflow   # 允許哪些 client 存取哪些 stanza
```

```bash
pgbackrest server --fork   # 背景 daemon
```

PG 主機配置 — 指向 repo host 用 TLS：

```ini
[global]
repo1-host=repo-host.internal
repo1-host-port=8432
repo1-host-type=tls                            # 關鍵：改成 tls
repo1-host-ca-file=/etc/pgbackrest/ca.crt
repo1-host-cert-file=/etc/pgbackrest/client.crt
repo1-host-key-file=/etc/pgbackrest/client.key
```

**SSH vs TLS 比較：**

| | SSH | TLS（內建） |
|--|-----|-----------|
| 連線方式 | 每次操作 fork ssh process | 長連線 TCP daemon |
| 效能 | 每次 fork 開銷大 | 連線復用，更快 |
| K8s 適用 | 需要 sshd + key 管理，麻煩 | 掛憑證 Secret 即可 |
| 認證 | SSH key pair | x509 憑證（mTLS 雙向驗證） |
| 設定 | authorized_keys, known_hosts | CA cert + client/server cert |
| Port | 22 | 8432（可自訂） |

**建議**：VM/Bare Metal 環境下，若已有 SSH 基礎設施則用 SSH（零額外設定）；若新建或安全要求較高則用 TLS。K8s 環境建議直接用模式 C（sidecar + S3），不需要任何遠端協定。

#### 2.8.7 archive-async 詳解

`archive-async=y` 是 pgBackRest 的非同步 WAL 歸檔模式，與同步模式的差異在於**推送時機和是否阻塞 PG**：

```
同步模式（archive_command）：
  WAL 寫入 → 段寫滿 16MB → PG 呼叫 archive_command → 等待推送完成 → PG 繼續
                                                       ↑
                                                  阻塞 PG，一次一個段

非同步模式（archive-async）：
  WAL 寫入 → 段寫滿 16MB → 檔案出現在 pg_wal/
                              ↓
            pgBackRest 背景 process 持續監控 pg_wal/
            檔案一出現 → 立即多 worker 平行推送 → PG 不等待，繼續寫入
```

| | 同步 | 非同步 |
|--|------|--------|
| **觸發者** | PostgreSQL 呼叫 `archive_command` | pgBackRest 背景 process 主動掃描 |
| **平行度** | 一次一個段（PG 限制） | 多個 worker 平行推送（`process-max`） |
| **阻塞 PG** | 是，推送完才繼續 | 否，PG 寫完就繼續 |
| **堆積處理** | 逐個排隊 | 平行消化積壓 |
| **RPO** | 段級（16MB） | 同樣段級（16MB），但推送更快完成 |

**async 的真正優勢不是 RPO 更小**（兩者都是段級 16MB），而是：
1. **不阻塞 PG** — 同步模式下如果 S3 慢或暫時不可用，PG 寫入會卡住
2. **平行推送** — WAL 堆積時能快速消化（同步模式只能一個一個推）
3. **高寫入負載下更穩定** — PG 不用等 archive_command 完成

**是否需要同機？** archive-async 的背景 process 必須能讀到 `pg_wal/` 目錄，因此必須與 PostgreSQL 在同一台機器或同一個 Pod（sidecar）。但 repo（備份目的地）可以是遠端 S3/GCS。

```
必須同機/同Pod:              可以遠端:
  PostgreSQL                  S3 / GCS repo
  pgBackRest async process    獨立備份機（透過 SSH/TLS）
  spool-path 目錄
  pg_wal/ 目錄（讀取）
```

### 2.9 實作優先序

| 階段 | 範圍 |
|------|------|
| Phase 1 | 單一 PostgreSQL，無 HA（Tier 0）— 專注於功能 |
| Phase 1.x | pgBackRest 基本設定：每日全量備份至本地磁碟 + WAL 同步歸檔（Tier 0 資料保護） |
| Phase 2 | 撰寫 Tier 1 部署指南，以託管 PG 測試；pgBackRest 備份改為 S3 + 每週 full / 每日 diff 排程 |
| Phase 3 | PgBouncer 整合，dashboard 的 read replica 路由；pgBackRest 非同步 WAL 歸檔 + 雙 repo |
| 未來 | Tier 3 多區域 DR 操作手冊；定期還原演練自動化 |

### 2.10 我們明確不建構的功能

| 功能 | 原因 |
|------|------|
| Active-Active PostgreSQL | 與 SKIP LOCKED + advisory locks 不相容（第 2.2 節） |
| Temporal 式多叢集複製 | 巨大複雜度，架構不同（第 2.3.5 節） |
| Cassandra/CockroachDB 佇列後端 | 放棄「PostgreSQL 就是一切」的價值主張 |
| 全域佇列（Kafka/NATS） | 增加基礎設施依賴，Phase 1 範圍蔓延 |

---

## 附錄 A：Temporal 複製原始碼參考

| 檔案 | 用途 |
|------|------|
| `service/history/interfaces/replication_stream.go` | ReplicationStream 介面（訂閱、轉換、迭代任務） |
| `service/history/ndc/history_replicator.go` | NDC 歷史複製，帶分支管理 + 衝突解決 |
| `service/history/ndc/workflow_state_replicator.go` | 完整工作流狀態複製 |
| `service/history/ndc/activity_state_replicator.go` | Activity 狀態複製 |
| `service/worker/replicator/replicator.go` | Worker 端複製處理器（per-cluster 訊息處理器） |
| `common/namespace/replication_resolver.go` | Namespace 級別 active 叢集解析 |
| `common/persistence/namespace_replication_queue.go` | 複製任務佇列，帶 DLQ 支援 |
| `common/namespace/nsreplication/replication_task_executor.go` | Namespace 元資料複製（跨叢集建立/更新 namespace） |

## 附錄 B：日誌查看原始碼參考

| 引擎 | 關鍵檔案 |
|------|---------|
| Airflow | `.venv/.../airflow/utils/log/file_task_handler.py`（基礎 handler）、`log_reader.py`（TaskLogReader API） |
| Windmill | `backend/windmill-api/src/service_logs.rs`（服務日誌 API）、`job_logs` DB 表 |
| Kestra | `core/.../models/executions/LogEntry.java`（模型）、`repositories/LogRepositoryInterface.java`（查詢 API） |
| Prefect | `server/models/logs.py`（ORM）、`server/api/logs.py`（REST API）、WebSocket 於 `/api/logs/out` |
| Dagster | `_core/storage/compute_log_manager.py`（抽象）、`local_compute_log_manager.py`（預設實作） |
