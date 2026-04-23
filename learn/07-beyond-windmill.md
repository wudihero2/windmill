# 第七章：超越 Windmill — 打造更強的產品

## Windmill 的局限

經過深入研究 Windmill 的原始碼，我識別出以下可以超越的方向：

### 1. PostgreSQL 瓶頸

Windmill 把所有東西都放在 PostgreSQL：

- **Job Queue 效能上限**：`SELECT FOR UPDATE SKIP LOCKED` 在高併發下有上限（~10K jobs/sec）
- **大量日誌寫入**：每個 job 的 `logs` 直接 UPDATE 到 queue 表，高頻 I/O
- **結果儲存**：大型結果（幾 MB 的 JSON）直接存 DB

### 2. Worker 模型限制

- **每個 job 一個子程序**：冷啟動延遲（Python 要 import、Go 要編譯）
- **無 warm pool**：不能保持語言 runtime 常駐
- **沙箱粒度粗**：nsjail 只能做 process 級隔離

### 3. Flow Engine 局限

- **純 JSON 定義**：5000+ 行的 `worker_flow.rs` 暴露了狀態機的複雜度
- **無 DAG 最佳化**：不會自動並行化獨立步驟
- **表達式只支援 JavaScript**：需要依賴 Deno runtime

---

## 差異化功能建議

### 1. 高效能訊息佇列：Iggy

[Iggy](https://github.com/iggy-rs/iggy) 是一個用 Rust 寫的高效能 message streaming 平台。

**為什麼用 Iggy 取代 PostgreSQL Queue：**

| 指標 | PostgreSQL Queue | Iggy |
|------|-----------------|------|
| 吞吐量 | ~10K msg/sec | ~100K+ msg/sec |
| 延遲 | ~1-10ms | ~0.1ms (P99) |
| 持久性 | WAL | Zero-copy + mmap |
| Consumer 模型 | Polling | Push + Pull |
| 水平擴展 | 需要 PgBouncer | 原生分區 |

**整合架構：**

```rust
use iggy::client::{Client, StreamClient, TopicClient};
use iggy::messages::send_messages::Message;

// Job Queue 用 Iggy
struct IggyJobQueue {
    client: Client,
    stream_id: u32,     // "windmill-jobs"
}

impl IggyJobQueue {
    // 推入 job
    async fn push_job(&self, job: &QueuedJob) -> Result<()> {
        let topic_id = self.get_topic_for_tag(&job.tag);
        let payload = serde_json::to_vec(job)?;

        self.client.send_messages(
            &self.stream_id,
            &topic_id,
            &Partitioning::balanced(),  // 自動分區
            &[Message::new(None, payload.into())],
        ).await?;

        Ok(())
    }

    // Worker 消費 job
    async fn poll_job(&self, worker_tags: &[String]) -> Result<Option<QueuedJob>> {
        for tag in worker_tags {
            let topic_id = self.get_topic_for_tag(tag);
            let messages = self.client.poll_messages(
                &self.stream_id,
                &topic_id,
                None,           // partition
                &consumer_id,
                &PollingStrategy::next(),
                1,              // count
                false,          // auto_commit
            ).await?;

            if let Some(msg) = messages.messages.first() {
                let job: QueuedJob = serde_json::from_slice(&msg.payload)?;
                // Acknowledge
                self.client.store_consumer_offset(/* ... */).await?;
                return Ok(Some(job));
            }
        }

        Ok(None)
    }
}
```

**日誌串流也用 Iggy：**

```rust
// 日誌不再寫入 PostgreSQL，改用 Iggy stream
// 前端透過 WebSocket 訂閱特定 job 的 topic
async fn stream_logs(job_id: &Uuid, log_line: &str) {
    iggy_client.send_messages(
        &LOGS_STREAM,
        &format!("job-{}", job_id),  // 每個 job 一個 topic
        &Partitioning::balanced(),
        &[Message::new(None, log_line.as_bytes().into())],
    ).await.ok();
}
```

### 2. 高效能資料源整合

#### Apache Arrow + DataFusion

用 [DataFusion](https://github.com/apache/datafusion) 做 in-process SQL 引擎：

```rust
use datafusion::prelude::*;
use arrow::datatypes::*;

// Flow step 可以直接查詢多種資料源
async fn handle_data_query_step(
    sources: Vec<DataSource>,
    query: &str,
) -> Result<Vec<RecordBatch>> {
    let ctx = SessionContext::new();

    // 註冊多種資料源
    for source in sources {
        match source {
            DataSource::Parquet { path } => {
                ctx.register_parquet("data", &path, Default::default()).await?;
            }
            DataSource::Csv { path } => {
                ctx.register_csv("data", &path, Default::default()).await?;
            }
            DataSource::PostgreSQL { url, table } => {
                // 用 DataFusion 的 PostgreSQL connector
                let pg = PostgresTableProvider::new(&url, &table).await?;
                ctx.register_table("data", Arc::new(pg))?;
            }
            DataSource::S3 { bucket, key } => {
                // 直接讀 S3 上的 Parquet
                let s3_path = format!("s3://{}/{}", bucket, key);
                ctx.register_parquet("data", &s3_path, Default::default()).await?;
            }
        }
    }

    // 用 SQL 查詢
    let df = ctx.sql(query).await?;
    let batches = df.collect().await?;

    Ok(batches)
}
```

#### DuckDB 整合

```rust
use duckdb::Connection;

// 比 PostgreSQL 快 100x 的分析查詢
async fn handle_analytics_step(
    query: &str,
    sources: Vec<DataSource>,
) -> Result<serde_json::Value> {
    let conn = Connection::open_in_memory()?;

    // DuckDB 可以直接讀 Parquet、CSV、JSON
    conn.execute_batch("INSTALL httpfs; LOAD httpfs;")?;

    for source in sources {
        match source {
            DataSource::Parquet { url } => {
                conn.execute(&format!(
                    "CREATE TABLE data AS SELECT * FROM read_parquet('{}')", url
                ), [])?;
            }
            DataSource::PostgreSQL { url, table } => {
                conn.execute_batch(&format!(
                    "INSTALL postgres; LOAD postgres;
                     ATTACH '{}' AS pg (TYPE POSTGRES);", url
                ))?;
            }
        }
    }

    let mut stmt = conn.prepare(query)?;
    let rows = stmt.query_map([], |row| {
        // 轉換為 JSON
    })?;

    Ok(serde_json::to_value(rows)?)
}
```

### 3. WASM Worker（取代 nsjail）

用 WebAssembly 做更安全、更快的沙箱：

```rust
use wasmtime::*;

struct WasmWorker {
    engine: Engine,
    linker: Linker<WasmState>,
}

impl WasmWorker {
    // Python via RustPython (compiled to WASM)
    // JavaScript via QuickJS (compiled to WASM)
    async fn execute_wasm(
        &self,
        wasm_module: &[u8],
        args: serde_json::Value,
    ) -> Result<serde_json::Value> {
        let module = Module::new(&self.engine, wasm_module)?;
        let mut store = Store::new(&self.engine, WasmState::new());

        // 資源限制
        store.limiter(|state| &mut state.limiter);
        store.set_fuel(1_000_000)?;  // CPU 限制

        let instance = self.linker.instantiate(&mut store, &module)?;
        let main = instance.get_typed_func::<(i32, i32), i32>(&mut store, "main")?;

        // 傳入 args，取回 result
        let result = main.call(&mut store, (args_ptr, args_len))?;

        Ok(read_result_from_wasm(&store, result))
    }
}
```

**優勢：**
- 啟動時間 < 1ms（vs nsjail ~50ms）
- 記憶體隔離是 WASM 原生的
- 跨平台（macOS、Windows 也能用沙箱）
- 可以限制 CPU 用量（fuel metering）

### 4. 智慧 DAG 最佳化

Windmill 的 Flow 是使用者手動定義的步驟順序。你可以加入自動最佳化：

```rust
// 自動分析步驟的依賴關係，並行化獨立步驟
fn optimize_flow(flow: &FlowValue) -> FlowValue {
    // 1. 建立依賴圖
    let deps = analyze_dependencies(&flow.modules);

    // 2. 找出可並行的步驟
    let parallel_groups = topological_sort_with_parallelism(&deps);

    // 3. 自動插入 BranchAll
    let optimized_modules = parallel_groups.iter().map(|group| {
        if group.len() == 1 {
            group[0].clone()
        } else {
            FlowModule {
                id: generate_id(),
                value: FlowModuleValue::BranchAll {
                    branches: group.iter().map(|m| BranchAllItem {
                        modules: vec![m.clone()],
                        ..Default::default()
                    }).collect(),
                    parallel: true,
                },
                ..Default::default()
            }
        }
    }).collect();

    FlowValue { modules: optimized_modules, ..flow.clone() }
}

fn analyze_dependencies(modules: &[FlowModule]) -> HashMap<String, Vec<String>> {
    let mut deps = HashMap::new();
    for module in modules {
        // 解析 input_transforms 中引用的 results.xxx
        let referenced_steps = extract_referenced_steps(&module.value);
        deps.insert(module.id.clone(), referenced_steps);
    }
    deps
}
```

### 5. Event-Driven Architecture（事件驅動）

```
傳統 Windmill:
  Trigger → Job → Worker → Complete

你的系統:
  Event → Event Bus (Iggy) → Matcher → Job → Worker → Event → ...
```

```rust
// 事件驅動的 Flow 執行
struct EventDrivenFlow {
    // 不再是固定的步驟順序，而是事件觸發規則
    rules: Vec<EventRule>,
}

struct EventRule {
    // 當收到什麼事件
    trigger: EventPattern,
    // 執行什麼
    action: FlowModule,
    // 產出什麼事件
    emit: Vec<EventTemplate>,
}

// 這讓 Flow 可以是反應式的，而不是命令式的
```

### 6. 更多資料源 Connector

| 類別 | 建議整合的資料源 |
|------|-----------------|
| Streaming | **Iggy**, Kafka, NATS, Redpanda, Pulsar |
| OLAP | **DuckDB**, ClickHouse, Apache Druid |
| 時序 | QuestDB, TimescaleDB, InfluxDB |
| 向量 | Qdrant, Milvus, pgvector |
| 物件儲存 | S3, MinIO, R2 (直接讀 Parquet) |
| 圖 | Neo4j, SurrealDB |
| 即時 | Redis Streams, NATS JetStream |

### 7. 可觀測性

Windmill 的可觀測性較弱。你可以加入：

```rust
// 整合 OpenTelemetry
use opentelemetry::trace::Tracer;

async fn execute_job(job: &QueuedJob) -> Result<Value> {
    let span = tracer.start(&format!("job.{}", job.job_kind.as_str()));

    // 每個 flow step 自動產生 trace span
    // 可以在 Jaeger/Grafana Tempo 看到完整的 flow 執行鏈路

    span.set_attribute("job.id", job.id.to_string());
    span.set_attribute("job.workspace", &job.workspace_id);
    span.set_attribute("job.language", job.language.map(|l| l.as_str()).unwrap_or("unknown"));

    let result = do_execute(job).await;

    span.set_status(if result.is_ok() { StatusCode::Ok } else { StatusCode::Error });
    span.end();

    result
}
```

## 具體建議：你的產品定位

### 方案 A：「高效能工作流引擎」

**定位**：專注於處理大量資料的工作流

**差異點**：
1. Iggy 做 job queue（比 Windmill 快 10x）
2. DuckDB/DataFusion 做 in-flow 資料處理
3. Arrow 做步驟間的零拷貝資料傳遞
4. 自動 DAG 並行化

**適用場景**：ETL、資料管線、批次處理

### 方案 B：「安全的多租戶程式碼執行平台」

**定位**：SaaS 級的安全性

**差異點**：
1. WASM 沙箱（取代 nsjail）
2. 更細粒度的資源限制（CPU fuel、記憶體、網路）
3. 租戶級的資源隔離
4. 審計日誌 + 合規報告

**適用場景**：SaaS 產品、多租戶平台

### 方案 C：「事件驅動的自動化平台」

**定位**：反應式、即時的工作流

**差異點**：
1. 事件匯流排（Iggy/NATS）作為核心
2. 反應式 Flow（事件觸發，非步驟順序）
3. 即時串流處理（不只是批次）
4. Complex Event Processing (CEP)

**適用場景**：IoT、即時告警、事件處理

## 最小可行產品 (MVP) 建議

如果你要在 2-3 個月內做出一個可用的 MVP：

### 第 1 月
- [ ] PostgreSQL schema + Axum API
- [ ] JWT 認證
- [ ] Script CRUD + 版本管理
- [ ] 單語言 Worker（Python）
- [ ] 基本 Web UI（Script 列表 + 編輯器 + 執行）

### 第 2 月
- [ ] Flow 資料模型 + 狀態機
- [ ] Flow Editor UI（SVG 流程圖）
- [ ] Sequential Flow 執行
- [ ] Input Transforms
- [ ] TypeScript Worker

### 第 3 月
- [ ] 條件分支 + For 迴圈
- [ ] 排程系統（Cron）
- [ ] Webhook 觸發
- [ ] Go Worker
- [ ] 依賴快取

### 第 4 月+（差異化）
- [ ] Iggy 整合（高效能 queue）
- [ ] DuckDB 資料處理 step
- [ ] WASM 沙箱
- [ ] OpenTelemetry
- [ ] 自動 DAG 並行化

## 深入：Windmill 的事件處理能力

### Windmill 已支援的 Trigger 種類

Windmill 其實已經有相當完整的事件觸發能力，共 **14 種 trigger**：

```rust
// backend/windmill-types/src/triggers.rs
pub enum TriggerKind {
    Webhook,           // HTTP webhooks
    Http,              // HTTP polling
    Websocket,         // WebSocket 長連線（串流）
    Kafka,             // Kafka 訊息佇列
    DefaultEmail,      // 預設 Email
    Email,             // 自訂 Email
    Nats,              // NATS 訊息匯流排
    Mqtt,              // MQTT pub/sub（IoT 常用）
    Sqs,               // AWS SQS 佇列
    Postgres,          // PostgreSQL LISTEN/NOTIFY
    Gcp,               // Google Cloud Pub/Sub
    Nextcloud,         // Nextcloud 事件
    Google,            // Google Workspace 整合
    Github,            // GitHub webhooks/events
}
```

### WebSocket Trigger — 最接近 Event Streaming 的實作

WebSocket trigger 是 Windmill 中最接近真正串流處理的功能：

```rust
// backend/windmill-trigger-websocket/src/listener.rs (line ~181)
// consume() 用 tokio::select! 同時管理三個任務
tokio::select! {
    _ = killpill_rx.recv() => {},          // 關閉信號
    _ = self.loop_ping(...) => {},         // 健康檢查
    _ = async { /* heartbeat */ } => {},   // 定期心跳
    _ = async {
        loop {
            if let Some(msg) = reader.next().await {
                // 過濾訊息並觸發 flow
            }
        }
    } => {}
}
```

WebSocket trigger 支援：
- **持久連線**：維持與 WebSocket server 的長連線
- **事件過濾**：`filter_logic`（"and"/"or"）過濾訊息
- **雙向通訊**：flow 執行結果可以回傳 WebSocket
- **心跳保活**：定期發送 heartbeat 維持連線
- **初始訊息**：連線時發送 setup 訊息

```rust
// backend/windmill-trigger-websocket/src/handler.rs (line ~335)
let use_or = listening_trigger.trigger_config.filter_logic == "or";
let should_handle = check_filters(&text, &filters, use_or);
if should_handle {
    let _ = self.handle_event(db, listening_trigger, text,
        trigger_info, return_message_channels.clone()).await;
}
```

### Suspend/Resume — 等待外部事件

Flow 可以暫停等待外部事件：

```rust
// backend/windmill-types/src/flows.rs (line ~386)
pub struct Suspend {
    pub required_events: Option<u32>,              // 需要幾個事件才繼續
    pub timeout: Option<u32>,                      // 超時（秒）
    pub resume_form: Option<serde_json::Value>,    // 審批表單 UI
    pub user_auth_required: Option<bool>,          // 是否需要認證
    pub user_groups_required: Option<InputTransform>,
    pub self_approval_disabled: Option<bool>,
    pub hide_cancel: Option<bool>,
    pub continue_on_disapprove_timeout: Option<bool>,
}
```

### Trigger 的錯誤處理與重試

```rust
// backend/windmill-trigger/src/types.rs (line ~57)
pub struct TriggerErrorHandling {
    pub error_handler_path: Option<String>,      // 錯誤處理 script/flow
    pub error_handler_args: Option<...>,         // 錯誤處理參數
    pub retry: Option<Retry>,                    // 重試設定
}
```

### Windmill Event Processing 的限制

| 面向 | Windmill 現狀 | 限制 |
|------|-------------|------|
| 事件模型 | 每個事件 → 獨立 job | 無 event bus，不能做跨事件關聯 |
| CEP | 不支援 | 無法做「事件 A 和 B 在 5 分鐘內同時發生才觸發」 |
| Suspend/Resume | 主要用於人工審批 | 不是通用的 event-wait 機制 |
| 串流處理 | WebSocket 最接近 | 但每個訊息仍是獨立 job，無批次/窗口處理 |
| Trigger 擴展 | 14 種硬寫 | 無通用 Event Adapter 框架 |

### 你可以超越的方向

| Windmill 的做法 | 超越方向 |
|-----------------|---------|
| 每個事件 → 獨立 job | 事件匯流排（Iggy/NATS）+ 批次處理 |
| 14 種硬寫的 trigger | 通用 Event Adapter 框架（plugin 化） |
| 無 CEP | 時間窗口、事件關聯、模式匹配引擎 |
| Suspend 只等審核 | 通用的 event-wait step（等待任意外部事件） |
| PostgreSQL queue 為核心 | 專用 message broker 處理高吞吐 |
| 觸發式（Event → Job） | 串流式（持續處理事件流、滑動窗口聚合） |

### 結論

Windmill 的 event processing 是**「觸發式」**的：

```
事件到達 → push 一個 job 進 PostgreSQL queue → Worker 處理 → 完成
```

而非**「串流式」**的：

```
事件流 → Event Bus → 窗口/聚合/關聯 → 產生新事件 → 觸發下游
```

如果要做真正的 event-driven platform，需要在 Windmill 的 trigger 架構之上加入：

1. **Event Bus**（Iggy/NATS JetStream）作為事件路由核心
2. **CEP 引擎**（時間窗口、事件關聯、模式匹配）
3. **通用 Event Adapter**（plugin 化的 trigger 框架）
4. **串流聚合**（滑動窗口、tumbling window、session window）

---

## 深入：Windmill 如何鎖定企業功能（Licensing 機制）

這是一個很有趣的工程問題：開源專案如何在程式碼公開的情況下鎖住付費功能？

### 三層防線

Windmill 用了**三層機制**確保企業功能需要授權：

#### 第 1 層：編譯時隔離 — Feature Flags + 私有 repo

```toml
# backend/Cargo.toml
[features]
enterprise = ["windmill-worker/enterprise", "windmill-queue/enterprise", ...]
private = [...]    # 閉源程式碼的 feature flag
license = [...]    # 需要 license key 驗證的功能
```

企業程式碼放在**另一個私有 repo**（`windmill-ee-private`），透過 symlink 引入：

```
backend/
├── src/ee_oss.rs          # 開源版 — 所有函式都是空殼
├── src/ee.rs → symlink    # 指向 windmill-ee-private 的真正實作
└── windmill-common/
    └── src/ee_oss.rs      # 開源版的 stub
```

開源版的 `ee_oss.rs` 長這樣：

```rust
// backend/src/ee_oss.rs — 這是你在 GitHub 上看到的
#[cfg(not(feature = "private"))]
pub async fn set_license_key(_license_key: String, _db: Option<&DB>) -> () {
    // Implementation is not open source
}

#[cfg(all(feature = "enterprise", not(feature = "private")))]
pub async fn verify_license_key() -> () {
    // Implementation is not open source
}
```

```rust
// backend/windmill-common/src/ee_oss.rs — 開源版的 license 狀態
#[cfg(not(feature = "private"))]
lazy_static! {
    pub static ref LICENSE_KEY_VALID: AtomicBool = AtomicBool::new(true);  // 開源版永遠 true
    pub static ref LICENSE_KEY_ID: ArcSwap<String> = ArcSwap::from_pointee("".to_string());
}

#[cfg(not(feature = "private"))]
pub async fn get_license_plan() -> LicensePlan {
    return Community;  // 開源版永遠回傳 Community
}
```

**關鍵**：開源版 `LICENSE_KEY_VALID` 永遠是 `true`，所以開源版不需要 license key。但企業版（用 `private` feature 編譯）會用 `ee.rs` 替換掉這些 stub，裡面有真正的驗證邏輯。

#### 第 2 層：運行時 License Key 驗證

企業版二進制檔在啟動時和執行時都會檢查 license key：

```rust
// backend/src/main.rs (line ~960)
// 啟動時
if let Err(err) = reload_license_key(&conn).await { ... }
let valid_key = LICENSE_KEY_VALID.load(Ordering::Relaxed);
if !valid_key && !server_mode {
    tracing::error!("Invalid license key, workers require a valid license key");
}

// 嘗試線上續約
let renewed_now = maybe_renew_license_key_on_start(
    &HTTP_CLIENT, &db,
    !valid_key && !LICENSE_KEY_ID.load().is_empty(), // 有 key 但過期 → 強制續約
).await;
```

**Worker 主迴圈**中也檢查——沒有有效 key 就**拒絕處理 job**：

```rust
// backend/windmill-worker/src/worker.rs (line ~2142)
#[cfg(feature = "enterprise")]
{
    let valid_key = LICENSE_KEY_VALID.load(Ordering::Relaxed);
    if !valid_key {
        tracing::error!("Invalid license key, sleeping for 10s waiting for valid key");
        tokio::time::sleep(Duration::from_secs(10)).await;
        continue;  // 不處理任何 job
    }
}
```

**排程也擋**：

```rust
// backend/windmill-queue/src/schedule.rs (line ~133)
if !LICENSE_KEY_VALID.load(Ordering::Relaxed) {
    return Err(Error::BadRequest(
        "License key is not valid. Go to superadmin settings to update."
    ));
}
```

**API 也擋**：

```rust
// backend/windmill-api-jobs/src/execution.rs (line ~64)
#[cfg(feature = "enterprise")]
pub async fn check_license_key_valid() -> Result<()> {
    let valid = LICENSE_KEY_VALID.load(Ordering::Relaxed);
    if !valid {
        return Err(Error::BadRequest("License key is not valid."));
    }
    Ok(())
}
```

#### 第 3 層：License Key 續約（需要聯網）

```rust
// backend/windmill-common/src/ee_oss.rs
pub async fn maybe_renew_license_key_on_start(
    _http_client: &reqwest::Client,   // 用 HTTP 呼叫 Windmill 的授權伺服器
    _db: &DB,
    force_renew_now: bool,
) -> bool {
    // Implementation is not open source
}

pub async fn renew_license_key(
    _http_client: &reqwest::Client,   // 線上續約
    _db: &DB,
    _key: Option<String>,
    _reason: RenewReason,             // Manual / Schedule / OnStart
) -> String {
    // Implementation is not open source
}
```

定期驗證（在 `monitor.rs` 的背景循環中）：

```rust
// backend/src/monitor.rs (line ~2351)
let verify_license_key_f = async {
    loop {
        verify_license_key().await;  // 定期呼叫（真正實作在私有 repo）
        tokio::time::sleep(...).await;
    }
};
```

### 回答你的問題：私有網路也能鎖嗎？

**可以，但方式不同：**

```
場景 1：有網路
  啟動 → reload_license_key() → 本地驗證 key 格式/簽章
  定期 → verify_license_key() → 呼叫 Windmill 授權伺服器驗證
  到期 → renew_license_key() → 線上續約

場景 2：私有網路（無外網）
  啟動 → reload_license_key() → 本地驗證 key 格式/簽章
  定期 → verify_license_key() → 無法呼叫外部 → ？
```

雖然真正的驗證邏輯在私有 repo 看不到，但根據程式碼結構可以推斷：

1. **License key 本身包含加密資訊**（過期時間、plan 類型等），用**數位簽章**驗證——這不需要聯網
2. **續約需要聯網**（`renew_license_key` 用 `reqwest::Client`）——離線環境需要手動更新 key
3. **離線寬限期**——key 中嵌入了過期時間，在過期前不需要聯網驗證

### License Key 的真正格式（從原始碼反推）

雖然驗證邏輯不公開，但 key 的**格式**寫在公開的原始碼裡：

```rust
// backend/windmill-common/src/instance_config.rs (line ~994)

/// License keys have the format `<client_id>.<expiry>.<signature>`.
fn license_key_expiry(value: &serde_json::Value) -> Option<u64> {
    let s = value.as_str()?;
    let parts: Vec<&str> = s.split('.').collect();
    if parts.len() != 3 {
        return None;
    }
    parts[1].parse::<u64>().ok()  // 第二段是過期時間戳
}

/// Returns true if two license key values share the same client ID
fn license_keys_same_client(a: &serde_json::Value, b: &serde_json::Value) -> bool {
    let a_parts: Vec<&str> = a_str.split('.').collect();
    let b_parts: Vec<&str> = b_str.split('.').collect();
    // 比較第一段（client_id）
    a_parts.first() == b_parts.first()
}
```

所以 License Key 的格式是：

```
<client_id>.<expiry_timestamp>.<signature>

例如：
cust_abc123.1735689600.a1b2c3d4e5f6...

     │           │           │
     │           │           └── 數位簽章（防偽造）
     │           └── Unix 時間戳（過期時間，純數字）
     └── 客戶 ID
```

### 私有網路如何手動更新 License Key

有**三種方式**可以在離線環境更新 key：

#### 方式 1：透過 Web UI（Superadmin Settings）

```
瀏覽器 → http://your-windmill:8000/#superadmin-settings
→ Core 區塊 → License key 欄位 → 貼上新 key → Save
```

前端的實作：

```typescript
// frontend/src/lib/components/instanceSettings.ts
{
    label: 'License key',
    description: 'License key required to use the EE (switch image for windmill-ee).',
    key: 'license_key',
    fieldType: 'license_key',
    placeholder: 'only for EE',
    storage: 'setting'     // 存到 global_settings 表
}
```

儲存後，前端呼叫 API → 寫入 PostgreSQL `global_settings` 表 → 觸發 `reload_license_key()`。

#### 方式 2：環境變數

```bash
# Docker Compose 或 K8s 環境變數
LICENSE_KEY=cust_abc123.1735689600.a1b2c3d4e5f6...
```

```rust
// backend/src/monitor.rs (line ~1882)
let mut value = std::env::var("LICENSE_KEY")   // 先讀環境變數
    .ok()
    .unwrap_or(String::new());

if let Some(q) = q {        // DB 的值會覆蓋環境變數
    value = v;
}
set_license_key(value, conn.as_sql()).await;
```

優先級：**DB global_settings > 環境變數**。

#### 方式 3：直接寫資料庫

```sql
-- 最底層的方式，任何環境都能用
INSERT INTO global_settings (name, value)
VALUES ('license_key', '"cust_abc123.1735689600.new_signature"')
ON CONFLICT (name) DO UPDATE SET value = EXCLUDED.value;
```

系統會在背景循環中自動偵測設定變更並 reload：

```rust
// backend/src/main.rs (line ~1687)
LICENSE_KEY_SETTING => {
    if let Err(e) = reload_license_key(&db.into()).await {
        tracing::error!("Failed to reload license key: {e:#}");
    }
    verify_license_key().await;   // 立即驗證新 key
}
```

### 離線更新的完整流程

```
1. 客戶聯絡 Windmill 銷售/支援
2. Windmill 用私鑰簽發新 key：
   new_key = "cust_abc123.1767225600.new_signature"
                          ^^^^^^^^^
                          新的過期時間（例如延後一年）
3. 客戶透過以下任一方式更新（不需要網路）：
   a) Web UI：/#superadmin-settings → 貼上新 key
   b) 環境變數：修改 docker-compose.yml 的 LICENSE_KEY
   c) SQL：直接 UPDATE global_settings

4. 系統自動 reload → verify → LICENSE_KEY_VALID = true
```

### Key 更新時的防護邏輯

系統還會檢查：新 key 的過期時間必須比舊 key **更晚**，防止降級攻擊：

```rust
// backend/windmill-common/src/instance_config.rs (line ~1120)
Some(existing) if key == LICENSE_KEY_SETTING => {
    if license_keys_same_client(existing, &value) {
        let current_expiry = license_key_expiry(existing).unwrap_or(0);
        let desired_expiry = license_key_expiry(&value).unwrap_or(0);
        if desired_expiry > current_expiry {
            // 新 key 過期時間更晚 → 允許更新
            upserts.insert(key.clone(), value);
        } else {
            // 新 key 過期更早 → 拒絕（防止用舊 key 覆蓋新 key）
            tracing::info!("Skipping license_key update: desired expiry ({}) \
                is not posterior to current expiry ({})", desired_expiry, current_expiry);
        }
    } else {
        // 不同客戶的 key → 直接替換（正常的首次設定或更換客戶）
        upserts.insert(key.clone(), value);
    }
}
```

### 這個模式的架構總結

```
┌─────────────────────────────────────────────────────────┐
│ 第 1 層：編譯時隔離（最強）                                │
│                                                         │
│  GitHub 公開 repo          私有 repo (windmill-ee-private)│
│  ┌──────────┐              ┌──────────┐                  │
│  │ ee_oss.rs│   symlink    │ ee.rs    │                  │
│  │ 空殼函式  │ ←──────────  │ 真正實作  │                  │
│  │ 永遠 true│              │ 驗證邏輯  │                  │
│  └──────────┘              └──────────┘                  │
│                                                         │
│  cfg(not(feature="private"))  cfg(feature="private")     │
│  → 用空殼                     → 用真正實作                │
├─────────────────────────────────────────────────────────┤
│ 第 2 層：運行時攔截                                       │
│                                                         │
│  LICENSE_KEY_VALID = false 時：                           │
│  ✗ Worker 拒絕處理 job（sleep 10s loop）                  │
│  ✗ 排程拒絕推入 job                                      │
│  ✗ API 拒絕執行請求                                      │
├─────────────────────────────────────────────────────────┤
│ 第 3 層：License Key 簽章 + 線上續約                       │
│                                                         │
│  key 內含數位簽章 → 離線也能驗證真偽                        │
│  定期線上續約 → 確認帳戶狀態                               │
│  私有網路 → 靠 key 內建的過期時間 + 離線寬限期               │
└─────────────────────────────────────────────────────────┘
```

### 你的產品可以學到什麼

1. **私有 repo 存放核心商業邏輯**——不要把驗證邏輯放在公開 repo
2. **用 Rust feature flag 做編譯時隔離**——比 runtime check 更安全
3. **空殼模式（Stub Pattern）**——開源版用空函式，企業版用 symlink 替換
4. **多點攔截**——API、Worker、Scheduler 都檢查，即使繞過一個也過不了其他的
5. **數位簽章的 License Key**——離線也能驗證，不依賴聯網

---

## 結語

Windmill 是一個成熟的產品（544 個 migration、1436 個前端組件、25+ 種語言支援），從零重建它不現實。但你可以：

1. **學習它的架構模式**（PostgreSQL as queue、hash-based versioning、flow state machine）
2. **從 MVP 開始**，每次只做一個語言、一種 flow 類型
3. **在特定方向超越**（高效能資料處理、更好的沙箱、事件驅動）
4. **用現代工具**（Iggy、DuckDB、WASM）替代它的技術選擇

關鍵是找到你的獨特定位，而不是做一個「另一個 Windmill」。
