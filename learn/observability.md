# 第十章：可觀測性方案

## 一、現狀盤點

`09-implementation-plan.md` 已有的可觀測性設計：

| 項目 | 現有設計 | 缺口 |
|------|---------|------|
| **Tracing** | Job-level span + Flow parent-child 傳播 + trace_id/span_id 存 DB | 內部函數沒有 span；沒有 feature-gate 控制開銷 |
| **Metrics** | worker_ping 回報系統指標（CPU/RAM/Disk）→ Dashboard | 沒有應用層 metrics（job 計數、佇列深度、沙箱錯誤率）；沒有 Prometheus endpoint |
| **Logging** | `tracing::{info, warn, error}` | 沒有結構化 log 規範；沒有 log 聚合策略 |

**核心問題**：worker_ping 是 DB 輪詢式指標，適合 Dashboard 展示，但不適合即時告警和 Grafana 聚合查詢。缺少 Prometheus 時序指標 → 無法做 rate/histogram/alerting。

---

## 二、設計原則

參考 foyer / DataFusion / sail 三個 Rust 專案的觀測性模式（見 `test_some_idea/trace/observability.md`），選擇**侵入性最低**且符合 CoveFlow 場景的方案：

| 支柱 | 選型 | 理由 |
|------|------|------|
| **Tracing** | `tracing::instrument` attribute | 一行 attribute 不改函數體；`tracing-opentelemetry` 直接橋接 Jaeger；不加新依賴（09 已用 `tracing` crate） |
| **Metrics** | DataFusion 式 BaselineMetrics + RAII Timer | RAII 不會忘記停止計時；baseline 提供通用指標不用每層重寫；比 sail YAML 生成簡單、比 foyer inline 更乾淨 |
| **Logging** | `tracing` crate 結構化 log | 只在生命週期事件/錯誤/狀態轉換記錄；不在 hot path 記每一筆 |

```
                侵入性
                  ↑
                  │  ✗ 每一行都 log
                  │  △ foyer 式 inline metrics    ← 可接受
                  │  ○ DataFusion 式 RAII Timer    ← 我們用這個（Metrics）
                  │  ◎ #[instrument] attribute     ← 我們用這個（Tracing）
                  │  ● sail 式 wrapper             ← 需要架構支持，CoveFlow 不適用
                  └───────────────────────────→ 實作複雜度
```

---

## 三、Tracing — 分散式追蹤

### 3.1 架構：兩層 Span

```
Layer 1: Job-level span（09 已有）
  → 每個 job = 1 span，flow 的 children 繼承 parent trace_id
  → 屬性：job.id, job.workspace, job.tag, job.cpus, job.memory_mb

Layer 2: 內部函數 span（新增，feature-gated）
  → 關鍵函數用 #[cfg_attr] 加 span
  → feature = "tracing" 關閉時零開銷
```

### 3.2 `#[instrument]` Attribute

09 的 `main.rs` 已經用了 `tracing` + `tracing-opentelemetry`，直接用 `#[instrument]` 不需要加新依賴：

```rust
// crates/worker/src/sandbox/nsjail.rs
// 一行 attribute，不改函數體。span 自動建立和結束。

#[tracing::instrument(
    name = "sandbox::nsjail::execute",
    skip(self, ctx),
    fields(job_id = %ctx.job_id, language = ?ctx.language)
)]
async fn execute(&self, ctx: &SandboxContext) -> Result<SandboxResult, SandboxError> {
    // 純業務邏輯 — 沒有任何 tracing 程式碼
}

#[tracing::instrument(name = "sandbox::nsjail::build_config", skip(self, ctx))]
fn build_nsjail_config(&self, ctx: &SandboxContext) -> String {
    // 純業務邏輯
}
```

**`#[instrument]` 使用要點**：
- `skip(self, ctx)` — 避免大 struct 被 Debug 印出（效能 + 安全）
- `fields(...)` — 只挑需要的欄位加到 span attribute
- `tracing-opentelemetry` layer 自動把這些 span 轉成 OTel span → Jaeger
- 即使沒有 subscriber，`#[instrument]` 仍有微小開銷（建立 span metadata），但對秒級 job 來說可忽略

### 3.3 Job-level Span（09 已有，補強）

09 已有的設計是正確的，補上幾個缺漏：

```rust
// 原有（保留）
let span = tracer.span_builder(format!("job.{}", job.kind))
    .with_kind(SpanKind::Consumer)
    .with_attributes(vec![
        KeyValue::new("job.id", job.id.to_string()),
        KeyValue::new("job.workspace", job.workspace_id.clone()),
        KeyValue::new("job.tag", pulled.tag.clone()),
        KeyValue::new("job.cpus", pulled.cpus as f64),
        KeyValue::new("job.memory_mb", pulled.memory_mb as i64),
        KeyValue::new("job.disk_mb", pulled.disk_mb as i64),
    ])
    .start(&tracer);

// 新增：結果階段補充 span 屬性（之前只設 status）
cx.span().set_attribute(KeyValue::new("job.duration_ms", duration_ms as i64));
cx.span().set_attribute(KeyValue::new("job.memory_peak_bytes", mem_peak as i64));
cx.span().set_attribute(KeyValue::new("job.sandbox_mode", sandbox.name().to_string()));
```

### 3.4 Flow Trace 傳播

09 已有 trace_id / span_id 存 DB + Jaeger 視覺化，這部分完整不需改動：

```
flow_job (root span)
├── step_a (child span) ── python execution
├── step_b (child span) ── python execution
│   └── retry_1 (child span)
└── step_c (child span) ── duckdb query
    └── s3_upload (child span)
```

### 3.5 哪些函數值得加 span？

不是每個函數都該加。只在**跨邊界**或**耗時操作**加：

| 函數 | 加 `#[instrument]`？ | 理由 |
|------|---------------------|------|
| `Sandbox::execute()` | 是 | 跨 process 邊界，是 job 的核心操作 |
| `pull_job()` SQL query | 是 | 跨 DB 邊界 |
| `complete_job()` | 是 | 寫 DB + 可能上傳 S3 |
| `resolve_dependencies()` | 是 | 可能觸發 pip install，耗時不確定 |
| `ResourceManager::try_acquire()` | 否 | 幾個 ns 的加減法，加 span 反而是 overhead |
| `build_nsjail_config()` | 可選 | 只在 debug 需要 |

---

## 四、Metrics — 應用指標

### 4.1 技術棧

```
CoveFlow App → prometheus-client crate → /metrics endpoint → Prometheus scrape → Grafana
```

選 `prometheus-client`（非 `prometheus` crate）：
- OpenMetrics 標準（Prometheus 2.x 和 OTel Collector 都支援）
- 更現代的 API、型別安全
- 比 `opentelemetry` metrics 更輕量（不需要 OTel Collector 做中轉）

### 4.2 Metrics 結構設計（DataFusion BaselineMetrics 模式）

```rust
// crates/worker/src/metrics.rs

use prometheus_client::metrics::{counter::Counter, gauge::Gauge, histogram::Histogram};
use prometheus_client::registry::Registry;
use std::sync::Arc;

/// 通用 baseline — 所有元件共用（像 DataFusion 的 BaselineMetrics）
pub struct BaselineMetrics {
    pub registry: Registry,
}

/// Worker 層 metrics
pub struct WorkerMetrics {
    // 佇列
    pub jobs_pulled_total: Counter,           // 拉取的 job 總數
    pub jobs_completed_total: Counter,        // 完成的 job 數（含成功+失敗）
    pub jobs_failed_total: Counter,           // 失敗的 job 數
    pub jobs_timeout_total: Counter,          // 逾時的 job 數

    // 執行
    pub job_duration_seconds: Histogram,      // job 執行耗時分佈
    pub job_queue_wait_seconds: Histogram,    // job 在佇列中等待的時間

    // 資源池（ResourceManager 狀態）
    pub resource_cpus_total: Gauge,           // Worker 總 CPU
    pub resource_cpus_used: Gauge,            // 已使用 CPU
    pub resource_memory_total: Gauge,         // Worker 總 RAM (MB)
    pub resource_memory_used: Gauge,          // 已使用 RAM (MB)
    pub resource_disk_total: Gauge,           // Worker 總 Disk (MB)
    pub resource_disk_used: Gauge,            // 已使用 Disk (MB)
    pub resource_acquire_wait_seconds: Histogram, // 等待資源的時間
}

/// Sandbox 層 metrics
pub struct SandboxMetrics {
    pub sandbox_execute_total: Counter,       // 沙箱執行總數（by mode label）
    pub sandbox_execute_errors: Counter,      // 沙箱執行失敗數
    pub sandbox_execute_duration: Histogram,  // 沙箱執行耗時
    pub sandbox_memory_peak_bytes: Histogram, // 記憶體峰值分佈
}

/// API Server 層 metrics
pub struct ApiMetrics {
    pub http_requests_total: Counter,         // HTTP 請求總數（by method, path, status）
    pub http_request_duration: Histogram,     // HTTP 請求耗時
    pub queue_depth: Gauge,                   // 當前佇列深度（pending jobs）
    pub active_workers: Gauge,                // 活躍 worker 數
}
```

### 4.3 RAII Timer 模式

```rust
// crates/worker/src/metrics.rs

use std::time::Instant;

/// RAII Timer — 建立即開始計時，drop 時自動記錄
/// （像 DataFusion 的 metrics::Time + timer()）
pub struct Timer<'a> {
    histogram: &'a Histogram,
    start: Instant,
}

impl<'a> Timer<'a> {
    pub fn new(histogram: &'a Histogram) -> Self {
        Self { histogram, start: Instant::now() }
    }

    /// 手動停止並記錄（不等 drop）
    pub fn done(self) {
        self.histogram.observe(self.start.elapsed().as_secs_f64());
        std::mem::forget(self); // 避免 drop 重複記錄
    }
}

impl<'a> Drop for Timer<'a> {
    fn drop(&mut self) {
        self.histogram.observe(self.start.elapsed().as_secs_f64());
    }
}
```

### 4.4 業務程式碼中的使用（侵入性分析）

```rust
// Worker 主迴圈 — 只有前後各 1~2 行 metrics 呼叫

async fn execute_job(
    job: &Job, sandbox: &dyn Sandbox, resources: &SandboxResources,
    metrics: &WorkerMetrics, sandbox_metrics: &SandboxMetrics,
) -> Result<(serde_json::Value, i64)> {
    // 1 行：開始計時（RAII，結束自動記錄）
    let _timer = Timer::new(&metrics.job_duration_seconds);

    metrics.jobs_pulled_total.inc();                                  // 1 行

    let result = sandbox.execute(&ctx).await;

    match &result {
        Ok(r) => {
            metrics.jobs_completed_total.inc();                       // 1 行
            sandbox_metrics.sandbox_memory_peak_bytes
                .observe(r.memory_peak_bytes as f64);                 // 1 行
        }
        Err(_) => {
            metrics.jobs_failed_total.inc();                          // 1 行
            sandbox_metrics.sandbox_execute_errors.inc();              // 1 行
        }
    }

    result
}
// 侵入性：function body 中 ~6 行 metrics 呼叫（含 timer），業務邏輯不受影響
```

### 4.5 Prometheus Endpoint

```rust
// crates/api/src/routes/metrics.rs

use axum::{routing::get, Router, response::IntoResponse};
use prometheus_client::encoding::text::encode;

pub fn metrics_router(registry: Arc<Registry>) -> Router {
    Router::new().route("/metrics", get(move || {
        let registry = registry.clone();
        async move {
            let mut buf = String::new();
            encode(&mut buf, &registry).unwrap();
            (
                [(axum::http::header::CONTENT_TYPE, "application/openmetrics-text; version=1.0.0; charset=utf-8")],
                buf,
            )
        }
    }))
}
```

### 4.6 完整 Metrics 清單

#### Worker 層

| Metric | 類型 | Labels | 說明 |
|--------|------|--------|------|
| `coveflow_jobs_pulled_total` | Counter | worker, tag | 拉取的 job 總數 |
| `coveflow_jobs_completed_total` | Counter | worker, tag, success | 完成的 job 數 |
| `coveflow_jobs_failed_total` | Counter | worker, tag, error_type | 失敗的 job 數 |
| `coveflow_jobs_timeout_total` | Counter | worker, tag | 逾時的 job 數 |
| `coveflow_job_duration_seconds` | Histogram | worker, tag, kind | job 執行耗時 |
| `coveflow_job_queue_wait_seconds` | Histogram | tag | 佇列等待耗時 |

#### ResourceManager 層

| Metric | 類型 | Labels | 說明 |
|--------|------|--------|------|
| `coveflow_resource_cpus_total` | Gauge | worker | 總 CPU |
| `coveflow_resource_cpus_used` | Gauge | worker | 已用 CPU |
| `coveflow_resource_memory_bytes_total` | Gauge | worker | 總 RAM |
| `coveflow_resource_memory_bytes_used` | Gauge | worker | 已用 RAM |
| `coveflow_resource_disk_bytes_total` | Gauge | worker | 總 Disk |
| `coveflow_resource_disk_bytes_used` | Gauge | worker | 已用 Disk |
| `coveflow_resource_acquire_wait_seconds` | Histogram | worker | 等待資源耗時 |

#### Sandbox 層

| Metric | 類型 | Labels | 說明 |
|--------|------|--------|------|
| `coveflow_sandbox_execute_total` | Counter | mode, language | 沙箱執行次數 |
| `coveflow_sandbox_execute_errors_total` | Counter | mode, error_type | 沙箱錯誤次數 |
| `coveflow_sandbox_execute_duration_seconds` | Histogram | mode | 沙箱執行耗時 |
| `coveflow_sandbox_memory_peak_bytes` | Histogram | mode | 記憶體峰值 |

#### API Server 層

| Metric | 類型 | Labels | 說明 |
|--------|------|--------|------|
| `coveflow_http_requests_total` | Counter | method, path, status | HTTP 請求總數 |
| `coveflow_http_request_duration_seconds` | Histogram | method, path | 請求耗時 |
| `coveflow_queue_depth` | Gauge | tag | 當前佇列深度 |
| `coveflow_active_workers` | Gauge | — | 活躍 worker 數 |

#### Flow 層

| Metric | 類型 | Labels | 說明 |
|--------|------|--------|------|
| `coveflow_flow_executions_total` | Counter | workspace, path | Flow 執行次數 |
| `coveflow_flow_duration_seconds` | Histogram | workspace | Flow 端到端耗時 |
| `coveflow_flow_steps_total` | Counter | step_type | 步驟執行次數 |
| `coveflow_flow_retries_total` | Counter | workspace, path | 重試次數 |

### 4.7 Label 基數控制

**陷阱**：高基數 label 會讓 Prometheus 記憶體爆炸。

| Label | 基數 | 安全？ |
|-------|------|--------|
| `worker` | ~10 | 安全 |
| `tag` | ~20 | 安全 |
| `mode` (sandbox) | 4 | 安全 |
| `method` (HTTP) | 5 | 安全 |
| `path` (HTTP) | **∞** | **危險** — 必須正規化（`/api/w/{workspace}/jobs/{id}` → `/api/w/:workspace/jobs/:id`） |
| `job_id` | **∞** | **禁止** — 永遠不要用 job_id 當 label |
| `workspace` | ~100 | 多租戶場景邊界，注意 |

---

## 五、Logging — 結構化日誌

### 5.1 分層策略

```
Layer 1: tracing crate（結構化 log）→ stdout / stderr
Layer 2: 部署時用 log collector（Loki / Fluentd / CloudWatch）聚合
Layer 3: tracing-opentelemetry bridge — log 自動關聯到 trace_id
```

### 5.2 Log 規範：什麼該記、什麼不該記

```rust
// ✅ 該記：生命週期事件
tracing::info!(worker = %worker_name, "worker started, tags={:?}", tags);
tracing::info!(worker = %worker_name, "worker shutting down");

// ✅ 該記：錯誤和異常
tracing::error!(job_id = %job.id, error = %e, "job execution failed");
tracing::warn!(elapsed = ?duration, "sandbox health check slow");

// ✅ 該記：狀態轉換
tracing::info!(job_id = %job.id, from = "pending", to = "running", "job state transition");
tracing::info!(checkpoint_id = %id, "checkpoint completed");

// ❌ 不該記：hot path 逐筆資料
tracing::trace!("processing row {}: {:?}", i, row);  // 用 metrics counter 代替

// ❌ 不該記：正常 happy path 細節
tracing::debug!("successfully parsed JSON");  // 沒有資訊量
```

### 5.3 Log 與 Trace 橋接

`tracing-opentelemetry` 自動把 `tracing::info_span!` 中的 structured fields 轉成 OTel span attributes，log event 轉成 span event。這在 09 的 `main.rs` 初始化中已設定：

```rust
let telemetry = tracing_opentelemetry::layer().with_tracer(tracer.clone());
let subscriber = tracing_subscriber::registry()
    .with(tracing_subscriber::fmt::layer())  // → stdout（人讀）
    .with(telemetry);                         // → OTel Collector（機讀）
```

好處：一個 `tracing::error!()` 同時出現在 stdout 和 Jaeger span events 中。

---

## 六、部署架構

### 6.1 完整 Observability Stack

```
                    ┌─────────────┐
                    │  Grafana     │ ← Dashboard + Alerting
                    │  :3001       │
                    └──────┬──────┘
                           │ query
              ┌────────────┼────────────┐
              │            │            │
        ┌─────▼────┐ ┌────▼─────┐ ┌───▼────┐
        │Prometheus│ │  Jaeger  │ │  Loki  │
        │  :9090   │ │  :16686  │ │  :3100 │
        │ (metrics)│ │ (traces) │ │ (logs) │
        └─────▲────┘ └────▲─────┘ └───▲────┘
              │            │            │
              │ scrape     │ OTLP       │ push
              │ /metrics   │ gRPC:4317  │
              │            │            │
        ┌─────┴────────────┴────────────┴─────┐
        │           CoveFlow App               │
        │  ┌─────────────────────────────────┐ │
        │  │ prometheus-client → /metrics     │ │
        │  │ opentelemetry   → OTLP exporter │ │
        │  │ tracing         → stdout + OTel │ │
        │  └─────────────────────────────────┘ │
        └──────────────────────────────────────┘
```

### 6.2 docker-compose 擴充

09 已有 postgres + minio + jaeger，新增 prometheus + grafana + loki：

```yaml
# docker-compose.yml（追加到 09 的基礎上）

  prometheus:
    image: prom/prometheus:latest
    volumes:
      - ./deploy/prometheus.yml:/etc/prometheus/prometheus.yml
    ports: ["9090:9090"]
    command: --config.file=/etc/prometheus/prometheus.yml

  loki:
    image: grafana/loki:latest
    ports: ["3100:3100"]

  grafana:
    image: grafana/grafana:latest
    environment:
      GF_SECURITY_ADMIN_PASSWORD: changeme
    ports: ["3001:3000"]
    volumes:
      - ./deploy/grafana/provisioning:/etc/grafana/provisioning
      - grafana_data:/var/lib/grafana
    depends_on: [prometheus, jaeger, loki]
```

```yaml
# deploy/prometheus.yml
global:
  scrape_interval: 15s

scrape_configs:
  - job_name: coveflow
    static_configs:
      - targets: ["coveflow:8000"]  # API server 的 /metrics
    metrics_path: /metrics
```

### 6.3 Grafana Dashboard 建議面板

#### Dashboard 1: Job Overview

| 面板 | PromQL | 類型 |
|------|--------|------|
| Job 吞吐量 | `rate(coveflow_jobs_completed_total[5m])` | Graph |
| Job 失敗率 | `rate(coveflow_jobs_failed_total[5m]) / rate(coveflow_jobs_pulled_total[5m])` | Gauge |
| Job 耗時 P50/P95/P99 | `histogram_quantile(0.95, rate(coveflow_job_duration_seconds_bucket[5m]))` | Graph |
| 佇列深度 | `coveflow_queue_depth` | Graph |
| 佇列等待 P95 | `histogram_quantile(0.95, rate(coveflow_job_queue_wait_seconds_bucket[5m]))` | Stat |

#### Dashboard 2: Worker Resources

| 面板 | PromQL | 類型 |
|------|--------|------|
| CPU 使用率 | `coveflow_resource_cpus_used / coveflow_resource_cpus_total` | Gauge per worker |
| RAM 使用率 | `coveflow_resource_memory_bytes_used / coveflow_resource_memory_bytes_total` | Gauge per worker |
| Disk 使用率 | `coveflow_resource_disk_bytes_used / coveflow_resource_disk_bytes_total` | Gauge per worker |
| 資源等待時間 | `histogram_quantile(0.95, rate(coveflow_resource_acquire_wait_seconds_bucket[5m]))` | Graph |

#### Dashboard 3: Sandbox

| 面板 | PromQL | 類型 |
|------|--------|------|
| 執行次數 by mode | `rate(coveflow_sandbox_execute_total[5m])` | Stacked Graph |
| 錯誤率 by mode | `rate(coveflow_sandbox_execute_errors_total[5m])` | Graph |
| 記憶體峰值 P95 | `histogram_quantile(0.95, rate(coveflow_sandbox_memory_peak_bytes_bucket[5m]))` | Stat |

---

## 七、告警策略

### 7.1 Critical（立即通知）

```yaml
# deploy/prometheus/alerts.yml

groups:
  - name: coveflow_critical
    rules:
      - alert: HighJobFailureRate
        expr: rate(coveflow_jobs_failed_total[5m]) / rate(coveflow_jobs_pulled_total[5m]) > 0.1
        for: 5m
        labels: { severity: critical }
        annotations:
          summary: "Job 失敗率 > 10%"

      - alert: QueueBacklog
        expr: coveflow_queue_depth > 100
        for: 10m
        labels: { severity: critical }
        annotations:
          summary: "佇列積壓 > 100 jobs 超過 10 分鐘"

      - alert: NoActiveWorkers
        expr: coveflow_active_workers == 0
        for: 2m
        labels: { severity: critical }
        annotations:
          summary: "沒有活躍的 worker"
```

### 7.2 Warning（工作時間通知）

```yaml
      - alert: HighJobLatency
        expr: histogram_quantile(0.95, rate(coveflow_job_duration_seconds_bucket[5m])) > 60
        for: 10m
        labels: { severity: warning }
        annotations:
          summary: "Job P95 耗時 > 60 秒"

      - alert: ResourcePressure
        expr: coveflow_resource_cpus_used / coveflow_resource_cpus_total > 0.9
        for: 15m
        labels: { severity: warning }
        annotations:
          summary: "Worker CPU 使用率 > 90% 超過 15 分鐘"

      - alert: SandboxErrors
        expr: rate(coveflow_sandbox_execute_errors_total[5m]) > 0.5
        for: 5m
        labels: { severity: warning }
        annotations:
          summary: "沙箱錯誤率偏高"
```

---

## 八、09 需要修改的地方

現有 09-implementation-plan.md 與本章的差異，按優先順序：

### 8.1 新增 Prometheus endpoint（Phase 1）

09 的 `main.rs` 需要在 API Router 加入 `/metrics` 路由。

### 8.2 新增 WorkerMetrics / SandboxMetrics struct（Phase 1）

Worker 主迴圈和 Sandbox::execute() 加入 RAII Timer + Counter（~6 行 / 函數）。

### 8.3 docker-compose 擴充（Phase 1）

加入 prometheus + grafana + loki 三個 service。

### 8.4 內部函數加 `#[instrument]`（Phase 2）

關鍵函數（sandbox execute, pull_job, complete_job, resolve_dependencies）加上 `#[tracing::instrument]`。Phase 1 不需要，因為 job-level span 已足夠。

### 8.5 worker_ping vs Prometheus 的關係

兩者共存：
- **worker_ping**（DB）：前端 Dashboard 用（已有 UI），15 秒粒度
- **Prometheus metrics**：Grafana + 告警用，scrape 間隔可調

不需要把 worker_ping 移除，但高頻指標（job 計數、耗時分佈）不應該存 DB。

---

## 九、與 observability.md 分析的對照

| observability.md 建議 | CoveFlow 採用？ | 說明 |
|----------------------|---------------|------|
| foyer 式 `#[cfg_attr]` + fastrace | 否 | 09 已用 `tracing` crate，不想加新依賴；job 秒級執行，span metadata 開銷可忽略 |
| `#[instrument]`（tracing crate） | **是** | 不加新依賴，`tracing-opentelemetry` 直接橋接 Jaeger |
| sail 式 wrapper tracing | 否 | CoveFlow 沒有 tree transform 架構 |
| DataFusion 式 BaselineMetrics | **是** | WorkerMetrics / SandboxMetrics struct |
| DataFusion 式 RAII Timer | **是** | Timer struct，drop 自動記錄 |
| sail 式 YAML 生成 metrics | 否 | 小團隊 overkill |
| foyer 式 inline metrics | 可接受 | 簡單 counter.inc() 場景用這個 |
| 標準 log，只在關鍵點 | **是** | 生命週期 / 錯誤 / 狀態轉換 |

### 為什麼不用 sail / foyer 的 tracing 模式？

1. **sail Wrapper pattern**：依賴 DataFusion 的 `ExecutionPlan` tree transform。CoveFlow 的 job 執行是線性的（pull → sandbox → complete），沒有 operator tree 可以 wrap。
2. **foyer `#[cfg_attr]` + fastrace**：能做到真正零開銷（編譯消除），但需要加 fastrace 依賴 + 額外的 OTel bridge。CoveFlow 已有 `tracing` + `tracing-opentelemetry` 全套生態，`#[instrument]` 的微小開銷（span metadata ~幾 ns）對秒級 job 完全可忽略。不值得為此加新依賴。
3. **YAML 生成**：sail 的 build.rs 生成適合大團隊防止 metrics 名稱不一致。CoveFlow 初期規模小，struct 定義就夠了。

---

## 十、總結：三個支柱的侵入性

```
┌──────────────────────────────────────────────────────────────┐
│                    CoveFlow 觀測性架構                         │
├──────────────┬───────────────────────────────────────────────┤
│ Tracing      │ Layer 1: job span（09 已有，自動）              │
│              │ Layer 2: #[instrument] 內部 span（opt-in）     │
│              │ 侵入性：零（attribute 不改函數體）                │
├──────────────┼───────────────────────────────────────────────┤
│ Metrics      │ prometheus-client → /metrics → Prometheus      │
│              │ WorkerMetrics + SandboxMetrics struct          │
│              │ RAII Timer（drop 自動記錄）                     │
│              │ 侵入性：低（每個函數 ~2-6 行）                   │
├──────────────┼───────────────────────────────────────────────┤
│ Logging      │ tracing crate → stdout + OTel bridge           │
│              │ 只記：生命週期 / 錯誤 / 狀態轉換                 │
│              │ 侵入性：必要的 inline（無法避免，但控制範圍）     │
├──────────────┼───────────────────────────────────────────────┤
│ 部署         │ Prometheus + Grafana + Jaeger + Loki           │
│              │ docker-compose 一鍵啟動                         │
│              │ 告警：Critical（失敗率/佇列積壓/無 worker）      │
│              │       Warning（延遲高/資源壓力/沙箱錯誤）        │
└──────────────┴───────────────────────────────────────────────┘
```
