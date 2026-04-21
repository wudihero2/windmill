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

## 結語

Windmill 是一個成熟的產品（544 個 migration、1436 個前端組件、25+ 種語言支援），從零重建它不現實。但你可以：

1. **學習它的架構模式**（PostgreSQL as queue、hash-based versioning、flow state machine）
2. **從 MVP 開始**，每次只做一個語言、一種 flow 類型
3. **在特定方向超越**（高效能資料處理、更好的沙箱、事件驅動）
4. **用現代工具**（Iggy、DuckDB、WASM）替代它的技術選擇

關鍵是找到你的獨特定位，而不是做一個「另一個 Windmill」。
