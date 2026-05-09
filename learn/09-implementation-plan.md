# 第九章：自建工作流平台 — 實作計畫

## 目標

從零打造一個工作流 / 資料管線平台（暫名 **CoveFlow**），支援：

- 即時寫 Python（未來多語言）並執行
- DAG 工作流（like Windmill Flow）
- 資料管線排程（like Airflow）
- **Day 1 沙箱隔離**（三模式可插拔：nsjail / K8s Pod / Firecracker）
- **Day 1 OpenTelemetry**
- **Day 1 Flow 版本控制**（不可變 revision，可 diff / rollback）
- **VS Code 風格多檔案編輯器**（多 Python 檔案互相引用，`main` 為入口）
- **資料引擎即時預覽**（DataPreviewTable 元件）
- **Resource 系統**（集中管理外部連線/密鑰，使用者程式碼零 SDK 存取）

---

## 與 Windmill 的關鍵差異總結

先看全貌，後面各 Phase 會逐一實作：

| 面向 | Windmill | CoveFlow |
|------|---------|-----------|
| Flow Editor | 自建 SVG（5000+ 行） | `@xyflow/svelte`（現成）+ 內嵌 Monaco tab |
| JS 求值 | QuickJS + Deno（C 依賴） | `boa_engine`（純 Rust） |
| 可觀測性 | 後加 OTel | Day 1 OpenTelemetry |
| 沙箱 | nsjail only（Linux only） | **三模式可插拔**：nsjail / K8s Pod / Firecracker（Sandbox trait） |
| Worker 資源模型 | 1 worker = 1 job | **Resource-based**：job 宣告 cpus/memory_mb/disk_mb，Worker 追蹤三維可用資源 |
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
| 同步執行 | `run_wait_result`（DB 輪詢） | **同步模式**：NOTIFY 事件驅動 + Early Return + 斷線自動取消 |
| Resource | `$res:path` 引用 + JSON Schema UI | **Resource 系統**：AES 加密 + `$res:` 引用替換 + 零 SDK 存取 |
| 部署審核 | 無（直接覆蓋） | **Deploy Approval Gate**：路徑級審核政策 + 多人審批 + diff 預覽 |
| 集群資源可視化 | vCPU + 記憶體（無磁碟、無 CPU 使用率） | **完整 Dashboard**：CPU 使用率 + 記憶體 + 磁碟 + 佔用率 |
| 檔案上傳 | S3 上傳 + SDK 存取（EE 功能） | **File Storage**：S3 / 本地雙模式 + 拖拉上傳 + 檔案瀏覽器 |
| 多團隊 RBAC | Group + Folder + extra_perms JSONB | **Team + Folder + folder_acl + Quota**：正規化 ACL 表 + 團隊配額（併發+CPU/RAM+儲存）|

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
| 沙箱 | nsjail / K8s Pod / Firecracker（可插拔 trait） | **不同於 Windmill**（只有 nsjail），三模式 + Sandbox trait 可擴展 |

---

## 專案結構

```
coveflow/
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
│       │       ├── resources.rs      # Resource CRUD + 加解密
│       │       ├── deploy.rs        # Deploy Approval Gate（審核政策 + 部署請求）
│       │       ├── files.rs         # File Storage（上傳/下載/列表/預覽）
│       │       ├── cluster.rs       # Cluster Dashboard（worker 資源監控）
│       │       ├── teams.rs          # Team CRUD + member management
│       │       ├── folders.rs       # Folder CRUD + ACL management (folder_acl table)
│       │       ├── acl.rs           # Permission checks (require_reader/writer/owner on AuthedUser)
│       │       └── sse.rs             # SSE 日誌串流（chunk-based run_log）
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
│       │       ├── resolve_args.rs    # $res: / $var: 引用替換
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
│   │   │   ├── resources/         # Resource 管理頁面
│   │   │   ├── schedules/         # 排程頁面
│   │   │   ├── files/             # 檔案瀏覽器頁面
│   │   │   ├── workers/           # 集群資源 Dashboard
│   │   │   ├── groups/            # 團隊管理 + 配額
│   │   │   └── settings/          # 設定頁面（Approval Policies, Folders ACL）
│   │   └── lib/
│   │       ├── components/
│   │       │   ├── ScriptEditor.svelte   # Monaco
│   │       │   ├── FlowEditor.svelte     # @xyflow/svelte DAG
│   │       │   ├── FlowFileTree.svelte   # VS Code 風格檔案樹
│   │       │   ├── FlowVersionPanel.svelte # 版本歷史 + diff
│   │       │   ├── DataPreviewTable.svelte # 資料查詢預覽表格
│   │       │   ├── LogViewer.svelte      # SSE 即時日誌
│   │       │   ├── ArgInput.svelte       # JSON Schema → 表單
│   │       │   ├── ResourceEditor.svelte # Resource 管理 UI
│   │       │   ├── DeployGate.svelte    # 部署審核面板（diff + approve/reject）
│   │       │   ├── FileUpload.svelte    # 拖拉上傳 + 進度條
│   │       │   ├── FileBrowser.svelte   # S3/本地檔案瀏覽器
│   │       │   ├── ClusterDashboard.svelte # Worker 資源監控儀表板
│   │       │   ├── GroupManager.svelte  # 團隊 CRUD + 成員管理
│   │       │   ├── FolderAcl.svelte    # Folder ACL 編輯器（權限矩陣）
│   │       │   └── QuotaPanel.svelte   # 團隊配額設定 + 用量儀表板（併發/CPU/RAM/每日/儲存）
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

**我們的選擇：PostgreSQL `FOR UPDATE SKIP LOCKED` + `LISTEN/NOTIFY` 雙軌制**

理由：
1. 已被 Windmill（5000 RPS）和 Dagster 驗證可行
2. 零外部依賴（不需要 Redis/RabbitMQ/Kafka）
3. Job 入隊和元資料在同一個事務中，不會出現「job 已分發但元資料沒寫入」
4. `LISTEN/NOTIFY` 可以把延遲從 50ms 降到個位數毫秒
5. Rust + sqlx + Tokio 天然適配

**雙軌制 Worker Pull 策略**：

```
┌─────────────────────────────────────────────────┐
│ 主要：LISTEN/NOTIFY 事件驅動                      │
│   push_job() → NOTIFY new_job                    │
│   Worker LISTEN new_job → 收到通知 → 立即 pull    │
│   延遲：~1-5ms                                    │
│                                                   │
│ 兜底：長間隔 Polling（每 5 秒）                    │
│   防止：PG 連線斷開重連、NOTIFY 丟失、             │
│         scheduled_for 延遲排程的 job               │
│                                                   │
│ Job 完成也 NOTIFY（給 run_wait_result 用）：       │
│   complete_job() → NOTIFY job_completed, '{id}'  │
│   run_wait_result LISTEN job_completed → 比對 id │
└─────────────────────────────────────────────────┘
```

**為什麼 Windmill 沒用 NOTIFY？** Windmill worker 以 50ms 間隔輪詢，高吞吐時幾乎永遠有 job 可拉，NOTIFY 收益不大。
但 CoveFlow 定位支援低延遲同步 API（`run_wait_result`），NOTIFY 把 queue→start 從 ~50ms 降到 ~1-5ms，非常值得。

### Job 三表分離設計

Windmill v1 把 queue 和結果放同一張表，後來 v2 才分離。我們直接跳到分離設計：

| 表 | 用途 | 生命週期 |
|---|------|---------|
| `job` | 不可變定義（script_hash, args, tag 等） | 永久 |
| `job_queue` | 可變狀態（running, worker, priority） | Job 完成即刪 |
| `job_completed` | 結果（result, duration, s3_key） | Job 完成時寫入 |

好處：`job_queue` 表永遠很小（只有未完成的 job），`FOR UPDATE SKIP LOCKED` 效能穩定。

### 全域 Worker 並發控制

**問題**：Worker 數量 = 同時跑的 job 數。如果部署太多 Worker 或大量同步請求湧入，可能壓垮 DB / runtime / 外部 API。

**六層防禦**（含 Resource-based 模型）：

```
┌────────────────────────────────────────────────────────────────┐
│ L1: 物理 Worker 數量（部署層）                                    │
│     由部署決定（docker replicas, K8s replicas）                   │
├────────────────────────────────────────────────────────────────┤
│ L2: 全域 max_concurrent_jobs（worker_config 表）                 │
│     整個叢集的硬上限                                              │
│     → pull_job() 檢查 running count，超過就不搶                  │
├────────────────────────────────────────────────────────────────┤
│ L3: Tag 級 concurrency_limit                                    │
│     例：tag="gpu" max_concurrent=4                               │
│     → pull_job() SQL 中 NOT EXISTS 子查詢過濾                     │
├────────────────────────────────────────────────────────────────┤
│ L4: Worker 反壓（LISTEN/NOTIFY + poll 間隔）                     │
│     Worker 有容量才拉取                                          │
├────────────────────────────────────────────────────────────────┤
│ L5: 團隊配額（team_quota）                                       │
│     L5a: max_concurrent_jobs：每個團隊的併發 job 數上限            │
│     L5b: max_cpus：每個團隊可佔用的 CPU 總量上限                   │
│     L5c: max_memory_mb：每個團隊可佔用的 RAM 總量上限              │
│     → push_job() 檢查併發數+CPU/RAM，pull_job() 檢查 CPU/RAM 用量 │
├────────────────────────────────────────────────────────────────┤
│ L6: Worker 資源可用性                                            │
│     L6a: j.cpus <= available_cpus                                │
│     L6b: j.memory_mb <= available_memory_mb                      │
│     L6c: j.disk_mb <= available_disk_mb                          │
│     確保 job 物理上能塞進該 Worker                                │
│                                                                  │
│     Normal：       每 job try_acquire(cpu,mem,disk)，完成即歸還    │
│     Dedicated：    啟動時 reserve(cpu,mem,disk)，持有到關閉         │
│     Runner Group： 啟動時 reserve(cpu,mem,disk)，持有到閒置回收     │
└────────────────────────────────────────────────────────────────┘
```

**Windmill 的做法**：只有 Layer 1（每 worker 跑一個 job，部署幾個 worker = 幾個併發）+ 部分 Layer 4（`QUEUE_LIMIT_WAIT_RESULT`）。
我們增加 L2（全域上限）、L3（tag 級限制）、L5（團隊配額：併發數 + CPU/RAM 雙重限制）、L6（Worker 三維資源約束），更精細的控制。

### Sandbox 三模式策略（可插拔 Trait）

Windmill 的 nsjail 是**後加的**，導致每個 executor 都有 `if is_sandboxing_enabled()` 的分支邏輯。我們的做法：**Sandbox 是 trait，所有執行都經過它**。新模式只需實作 trait 即可。

| 面向 | nsjail | K8s Pod | Firecracker（Phase 4+） |
|------|--------|---------|----------------------|
| **啟動延遲** | ~10-50ms | ~2-10s | ~125ms |
| **效能開銷** | ~0% | ~0%（容器內） | ~0%（microVM） |
| **跨平台** | Linux only | 任何有 K8s 的環境 | Linux only |
| **任意 pip 包** | **支援** | **支援** | **支援** |
| **Bash 執行** | **支援** | **支援** | **支援** |
| **GPU** | 不支援 | **K8s 原生** | 可設定直通 |
| **外部依賴** | nsjail binary (C++) | K8s cluster | Firecracker binary |
| **磁碟限制** | tmpfs_size ✅ | ephemeral-storage ✅ | virtio-blk ✅ |
| **隔離等級** | 高（cgroup + namespace） | 最高（container） | 最高（硬體虛擬化） |
| **適用場景** | VM/Bare Metal 生產環境 | K8s 叢集、重型/GPU job | 多租戶 SaaS、最強隔離 |

**Phase 1-3 專注 nsjail + None（開發）。Phase 3+ 加入 K8s Pod。Phase 4+ 加入 Firecracker。**

**按 Job 類型選擇：**

```
Python / Bash（VM 環境）    → nsjail（預設）
需要自訂 Docker image       → K8s Pod
需要 GPU                    → K8s Pod
多租戶 SaaS（最強隔離）      → Firecracker（Phase 4+）
macOS 開發環境              → None（不隔離）
```

三種模式的詳細實作程式碼見[附錄 A](#附錄-a-sandbox-三模式詳細實作)。

### Resource-based 資源模型

**問題**：傳統的「1 worker = 1 job」模型（Windmill 的做法）在小任務上浪費大量資源。一台 8 vCPU 的機器跑一個只需 2 vCPU 的 job，75% 的算力閒置。

**解法**：**Resource-based 模型** — Job 直接宣告需要的 `cpus` + `memory_mb` + `disk_mb`，Worker 追蹤三維可用資源。

```
Worker（8 vCPU, 32GB RAM, 100GB Disk）
  total_cpus: 8.0, total_memory_mb: 32768, total_disk_mb: 102400

  ┌──────────────────────────────────────────────────────────┐
  │ CPU  [████░░░░░░] 4.0 / 8.0 cores                       │
  │ RAM  [██████░░░░] 12.0 / 32.0 GB                        │
  │ Disk [██░░░░░░░░]  8.0 / 100.0 GB                       │
  │                                                          │
  │ ├─ [Dedicated] predict_api   2.0 cpu  4.0 GB  2.0 GB    │
  │ ├─ [Normal]    job-abc       1.0 cpu  4.0 GB  2.0 GB    │
  │ ├─ [Normal]    job-def       1.0 cpu  4.0 GB  4.0 GB    │
  │ └─ (free: 4.0 cpu, 20.0 GB RAM, 92.0 GB disk)           │
  └──────────────────────────────────────────────────────────┘
```

**Resource × 執行模式 — 正交設計**：ResourceManager 管「資源分配」，執行模式管「程序生命週期」。

```
模式             資源佔用               每 job 有 sandbox？  冷啟動
──────────────── ───────────────────── ─────────────────── ──────────
Normal           try_acquire(cpu,mem,disk)  有（獨立隔離）   ~65ms
Dedicated 1:1    reserve(cpu,mem,disk)      無（同一程序）   ~5ms
Runner Group     reserve(cpu,mem,disk)      無（同一程序）   ~5ms
```

**漸進式導入**：Phase 1-3 設 `default_job_cpus=1, default_job_memory_mb=512`（一次一個小 job），但 ResourceManager 已存在於程式碼中。Phase 3+ 啟用多 job 並行。Phase 4 加入 Dedicated/Runner Group。

詳細的 ResourceManager 實作、pull_job 整合、6 層併發控制見下方各 Phase 的實作節。

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

### Resource 系統（學習 Windmill Resource + Airflow Connection）

**問題**：使用者寫的 Python 需要連外部服務（DB、API），但不應該把密碼寫死在程式碼裡。需要一個平台級的資源管理系統。

**各平台做法比較**：

| 平台 | 定義 | 存取方式 | 加密 | 使用者體驗 |
|------|------|---------|------|-----------|
| Airflow | UI / CLI / env | `Hook.get_connection("id")` SDK 呼叫 | Fernet | 需要 import Airflow |
| Prefect | Python class + UI | `Block.load("name")` SDK 呼叫 | 全值加密 | 需要 import Prefect |
| Dagster | Python class in code | **依賴注入**（函數參數） | 無（靠 EnvVar） | 最優雅但無 UI |
| Kestra | YAML + UI | `{{ secret('KEY') }}` 模板 | AES（Enterprise） | 無型別 |
| **Windmill** | **UI（JSON Schema 表單）** | **`$res:path` → Worker 替換 → args.json** | 加密存儲 | **零 SDK** |

**我們的選擇：Windmill 模式 + AES 加密**

理由：**使用者的程式碼完全不需要 import 任何 CoveFlow SDK**。Worker 在執行前把 `$res:path` 替換成實際值，寫入 `args.json`，使用者的函數收到的就是普通 dict。

```
資料流：
  UI 建立 Resource（JSON Schema 表單）
    → 存 DB（value 用 AES-256-GCM 加密）
    → Script 參數標 resource type
    → Flow InputTransform 引用 "$res:users/admin/prod_db"
    → Worker 執行前 resolve_args()：$res: → 解密 → 替換
    → 寫 args.json
    → 使用者的 def main(db: dict) 直接拿到明文 dict
```

```python
# 使用者寫的 script —— 完全不知道 CoveFlow 的存在
def main(db: dict, api_key: str):
    # db = {"host": "prod-pg.example.com", "port": 5432, "password": "s3cret"}
    import psycopg2
    conn = psycopg2.connect(**db)
    cursor = conn.cursor()
    cursor.execute("SELECT count(*) FROM users")
    return {"count": cursor.fetchone()[0]}
```

### 同步執行模式（學習 Windmill `run_wait_result`）

**使用場景**：把 CoveFlow 當作 API 使用——外部系統呼叫 workflow，阻塞等待結果後回傳。

```
典型情境：貸款系統 → POST /api/workspaces/prod/jobs/run_wait_result/f/credit-scoring
         → CoveFlow 執行信用評分 flow
         → HTTP 阻塞等待
         → 200 OK { "score": 720, "approved": true }
         → 總延遲 < 200ms（Dedicated Worker + 輕量 Python）
```

**Windmill 做法分析**：

| 機制 | Windmill 實作 | 我們的改進 |
|------|-------------|-----------|
| 等待策略 | DB 輪詢（50ms fast → 200ms slow） | **NOTIFY 事件驅動** + 200ms 兜底輪詢 |
| 斷線處理 | RAII Guard（Drop 時取消 job） | 相同 |
| 超時 | `TIMEOUT_WAIT_RESULT`（預設 600s） | 相同 |
| 佇列反壓 | `QUEUE_LIMIT_WAIT_RESULT` | 相同 |
| 回應格式 | `WindmillCompositeResult`（自訂 status/headers） | 相同 |
| Flow 部分回傳 | 指定某個 node 的結果作為回傳 | **Early Return**：指定回傳節點，剩餘步驟背景執行 |
| Webhook 同步 | `RequestType::Sync` | 相同（Phase 3 整合） |
| 最低延遲 | Dedicated Worker ~12ms 開銷 | Dedicated + Runner Group ~5ms |

**三種執行模式**：

```
1. Async（預設）：POST /jobs/run/p/{path} → 立即回傳 { id: uuid }
2. Sync：POST /jobs/run_wait_result/p/{path} → 阻塞等結果 → 回傳 result
3. Sync SSE：POST /jobs/run_wait_result/p/{path}?sse=true → SSE 串流日誌 + 最終結果
```

**延遲預估（同步模式）**：

```
                    Queue   Start   Execute   Total
Normal Worker:      ~5ms  + ~60ms + exec     ≈ 65ms + exec
Dedicated Worker:   ~5ms  + ~0ms  + exec     ≈ 5ms + exec
Runner Group:       ~5ms  + ~0ms  + exec     ≈ 5ms + exec

範例：信用評分 Python（~50ms 推論）
  Normal:     65 + 50 = ~115ms ✓ 子秒
  Dedicated:   5 + 50 = ~55ms  ✓ 遠低於 1 秒
```

### Deploy Approval Gate（部署審核門禁）

**問題**：生產環境的 Flow/Script 不能隨便改。需要審核機制防止未經授權的變更上線。

**各平台比較**：

| 平台 | 部署審核機制 |
|------|-------------|
| Windmill | 無內建。依賴 Git Sync + 外部 PR review |
| Airflow | 無內建。DAG 部署靠 CI/CD |
| Prefect | 無內建。Deployment 靠 CLI + CI |
| Kestra | EE 有 Namespace-level 權限，但無逐次審核 |
| Temporal | 無內建。靠 CI/CD |
| **CoveFlow** | **內建 Deploy Approval Gate：路徑級審核政策 + 多人審批 + diff 預覽** |

**設計原則**：

1. **零阻力預設**：沒有設 policy 的路徑 → 跟以前一樣直接 deploy
2. **路徑級粒度**：`folders/production/*` 要 2 人審核，`folders/sandbox/*` 不需要
3. **類似 PR Review**：看 diff → approve/reject + 留言
4. **自動生效**：達到 `min_approvals` 後，一鍵部署或自動部署

```
使用者修改 Flow
      │
      ▼
  儲存 Draft（不影響線上版本）
      │
      ▼
  ┌─ 檢查 approval_policy ─┐
  │                         │
  │ 無 policy               │ 有 policy（match path_pattern）
  │ → 直接 deploy           │ → 建立 deploy_request
  │                         │     │
  └─────────────────────────┘     ▼
                              通知 approvers（in-app / webhook）
                                  │
                              ┌───┴───┐
                              │ 審核  │
                              │diff+留言│
                              └───┬───┘
                            ┌────┴────┐
                         Approve    Reject
                            │         │
                            ▼         ▼
                  達到 min_approvals  回到 Draft
                            │       （附 reject 原因）
                            ▼
                       正式 Deploy
                     （新版本上線）
```

### File Storage（檔案儲存系統）

**問題**：使用者需要上傳 CSV、JSON、ML 模型等檔案給 Script/Flow 使用。

**各平台比較**：

| 平台 | 檔案上傳 | 儲存後端 | Script 存取方式 | 預覽 |
|------|---------|---------|----------------|------|
| Windmill | ✅ REST 串流（EE） | S3/Azure/GCS/本地 | SDK: `loadS3File()` | ✅ CSV/Parquet |
| Kestra | ✅ multipart | S3/GCS/Azure/本地 | `kestra:///` URI | ❌ |
| Airflow | ❌ 無內建 | 靠 XCom + providers | 手動用 boto3 | ❌ |
| Prefect | ❌ 無內建 | 靠 Storage Blocks | 手動用 boto3 | ❌ |
| **CoveFlow** | **✅ 拖拉上傳 + 進度** | **S3 / 本地雙模式** | **自動下載到 job_dir** | **✅ CSV/JSON/text** |

**CoveFlow 與 Windmill 的關鍵差異**：

| 面向 | Windmill | CoveFlow |
|------|---------|---------|
| Script 讀取方式 | 需要用 SDK `loadS3File()` | **Worker 自動下載到 job_dir**，`open("input/data.csv")` 即可 |
| 儲存後端 | S3/Azure/GCS/本地（多種） | **S3 + 本地**（兩種，簡化配置） |
| OSS 支援 | 串流上傳是 EE 功能 | **完全開源** |

**資料流**：

```
使用者拖拉上傳
    │
    ▼
POST /api/workspaces/{ws}/files/upload
    │
    ├── storage_mode = "s3"  → 上傳到 S3/MinIO
    │                          回傳 FileRef { s3: "uploads/2025/data.csv" }
    │
    └── storage_mode = "local" → 寫入 {data_dir}/files/{workspace}/{path}
                                 回傳 FileRef { local: "data.csv" }

Script 參數中引用：
    args = { "input_file": { "s3": "uploads/2025/data.csv" } }
    │
    ▼
Worker 執行前自動處理：
    resolve_file_refs(args, job_dir)
    │
    ├── S3 模式 → 下載到 {job_dir}/input/data.csv
    └── 本地模式 → symlink 或 copy 到 {job_dir}/input/data.csv

使用者程式碼：
    import pandas as pd
    df = pd.read_csv("input/data.csv")  # 就這麼簡單
```

### 多團隊 RBAC（Group + Folder + ACL + Quota）

**問題**：大公司有多個團隊（ML、Data Engineering、Finance），需要：
1. 團隊間**資源隔離**（Script / Flow / Resource 互不可見或唯讀）
2. 團隊級**資源配額**（防止某團隊吃光所有 Worker / Storage）
3. **路徑級存取控制**（誰能讀、誰能寫、誰是 owner）
4. **審計可追溯**（哪個團隊消耗多少資源）

**各平台比較**：

| 平台 | 團隊/群組 | 路徑 ACL | 團隊配額 |
|------|----------|---------|---------|
| Windmill | ✅ Group + Folder + extra_perms JSONB | ✅ 路徑級讀/寫/owner | ❌ 無 |
| Airflow | ❌ 靠外部 LDAP | ❌ DAG-level role | ❌ 無 |
| Prefect | ❌ workspace 級 | ❌ 無路徑 ACL | ❌ 無 |
| Kestra | ✅ Namespace 級（EE） | ❌ 粗粒度 | ❌ 無 |
| **CoveFlow** | **✅ Team + Folder + folder_acl** | **✅ 正規化 ACL（reader/writer/owner）** | **✅ 團隊級配額（併發+CPU/RAM+儲存）** |

**核心設計（學 Windmill，加入團隊配額）**：

```
Workspace（公司/組織）
    │
    ├── Group（團隊）
    │   ├── ml-team      members: [alice, bob]
    │   ├── data-eng     members: [charlie, dave]
    │   └── all          members: [*]  （內建，所有人）
    │
    ├── Folder（路徑級 ACL 容器）
    │   ├── folders/ml-team/          owners: [teams/ml-team]
    │   │   ├── folders/ml-team/training_flow
    │   │   └── folders/ml-team/predict_script
    │   ├── folders/data-eng/         owners: [teams/data-eng]
    │   ├── folders/shared/           owners: [teams/all]     ← 所有人可存取
    │   └── folders/production/       owners: [users/admin]   ← 需 Deploy Approval
    │
    └── Team Quota（資源配額）
        ├── ml-team:    max_concurrent=10, max_cpus=16, max_memory_mb=32768, max_storage=50GB
        └── data-eng:   max_concurrent=20, max_cpus=32, max_memory_mb=65536, max_storage=100GB
```

**路徑規範**：

```
users/alice/my_script       → 個人路徑，只有 alice 可讀寫
folders/ml-team/training    → 團隊路徑，由 folder ACL 控制
folders/shared/utils        → 共用路徑，所有人可讀
```

**ACL 模型（正規化 `folder_acl` 表）**：

Windmill 用 `owners TEXT[]` + `extra_perms JSONB` 雙軌存權限，查詢需要 `LATERAL jsonb_each()` + `unnest()` + `UNION`。
CoveFlow 採用正規化關聯表，用標準 SQL 即可查詢：

```sql
-- folder_acl 表中的記錄範例
(workspace_id='acme', folder_name='ml-team', subject='teams/ml-team',  role='owner')
(workspace_id='acme', folder_name='ml-team', subject='users/bob',      role='writer')
(workspace_id='acme', folder_name='ml-team', subject='teams/data-eng', role='reader')
```

三級角色：`owner`（完全控制）> `writer`（讀寫）> `reader`（唯讀），key 不存在 = 無權限。

**權限檢查優先序**：

```
1. is_admin? → 全部放行
2. path 以 "users/{username}/" 開頭？ → 只有本人可讀寫
3. path 以 "folders/{folder}/" 開頭？ → 查 folder_acl 表
   → 找出所有 subject ∈ perm_subjects 的記錄
   → 取最高角色（owner > writer > reader）
4. 都沒 match → 無權限（403）
```

**團隊配額防禦層（擴充現有並發控制）**：

```
┌────────────────────────────────────────────────────────┐
│ Layer 1-4（現有）：Worker 數量 / 全域上限 / Tag 級 / 反壓  │
├────────────────────────────────────────────────────────┤
│ Layer 5：團隊配額（team_quota 表）NEW                     │
│                                                        │
│   a) 併發限制（push_job 檢查）                            │
│      ml-team: max_concurrent_jobs = 10                 │
│      → 正在跑的 job 數 >= 10 → reject                    │
│                                                        │
│   b) CPU 限制（push_job + pull_job 檢查）NEW              │
│      ml-team: max_cpus = 16                            │
│      → 正在跑的 SUM(j.cpus) + 新 job.cpus > 16          │
│      → 延後（等其他 job 完成釋放 CPU）                     │
│                                                        │
│   c) RAM 限制（push_job + pull_job 檢查）NEW              │
│      ml-team: max_memory_mb = 32768                    │
│      → 正在跑的 SUM(j.memory_mb) + 新 job > 32768       │
│      → 延後（等其他 job 完成釋放 RAM）                     │
│                                                        │
│   d) 儲存限制（upload_file 檢查）                          │
│      ml-team: max_storage_bytes = 50GB                 │
│      → 超過 → 403 "quota exceeded"                      │
│      （注意：disk 不做團隊配額——磁碟是 Worker 本地暫存，    │
│       job 完成即清理。團隊儲存配額走 max_storage_bytes。） │
├────────────────────────────────────────────────────────┤
│ 如何判斷 job 屬於哪個團隊？                                │
│   → 看 job.script_path / flow_path 的路徑前綴              │
│   → "folders/ml-team/training" → folder "ml-team" → owners │
│   → 取第一個 teams/ owner 作為 team                       │
│   → 或直接在 run 表記錄 team_owner                        │
└────────────────────────────────────────────────────────┘
```

**影響範圍**：

| 現有段落 | 需要修改 |
|---------|---------|
| Phase 1 Schema | 新增 `team`、`team_member`、`folder`、`folder_acl`、`team_quota`（含 `max_cpus, max_memory_mb`）表 |
| Phase 1 Auth | `AuthedUser` 增加 teams + folders（`FolderRole` enum）欄位 |
| Phase 1 Router | 新增 Team / Folder / Quota API 路由 |
| Phase 1 Script CRUD | 加 `require_writer(path)` 檢查 |
| Phase 1 Job push | 加團隊歸屬 + 配額檢查（併發 + CPU/RAM 雙重） |
| Phase 1 Job pull | L5 子查詢加 `max_cpus` + `max_memory_mb` 用量檢查 |
| Phase 3 Resource | Resource/Variable 納入 Folder ACL |
| Phase 3 File Storage | 加團隊儲存配額 |
| Phase 3 Cluster Dashboard | 加團隊資源用量視角 |
| Phase 3 Deploy Approval | 修復 `team_member` 引用（現在有定義了）|

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
    role VARCHAR(20) NOT NULL DEFAULT 'editor',  -- admin, editor, viewer, operator
    PRIMARY KEY (workspace_id, email)
);

-- === 團隊 + 路徑 ACL ===

-- 團隊
CREATE TABLE team (
    workspace_id VARCHAR(50) NOT NULL REFERENCES workspace(id),
    name VARCHAR(100) NOT NULL,              -- "ml-team", "sre", "data-eng"
    summary TEXT DEFAULT '',
    PRIMARY KEY (workspace_id, name)
);

-- 團隊管理權限（誰可以新增/移除成員）
-- admin 永遠可以管理所有 team；team_acl 用於委派管理權給非 admin
CREATE TABLE team_acl (
    workspace_id VARCHAR(50)  NOT NULL,
    team_name    VARCHAR(100) NOT NULL,
    subject      VARCHAR(255) NOT NULL,  -- 'users/alice' or 'teams/sre'
    role         VARCHAR(20)  NOT NULL CHECK (role IN ('manager')),
    PRIMARY KEY (workspace_id, team_name, subject),
    FOREIGN KEY (workspace_id, team_name)
        REFERENCES team(workspace_id, name) ON DELETE CASCADE
);

CREATE INDEX idx_team_acl_subject ON team_acl(workspace_id, subject);

-- 使用者 ↔ 團隊 對應
CREATE TABLE team_member (
    workspace_id VARCHAR(50) NOT NULL,
    email VARCHAR(255) NOT NULL,
    team_name VARCHAR(100) NOT NULL,
    PRIMARY KEY (workspace_id, email, team_name),
    FOREIGN KEY (workspace_id, team_name) REFERENCES team(workspace_id, name) ON DELETE CASCADE
);

-- 資料夾（路徑級 ACL 的核心）
-- 所有 folders/ 開頭的路徑都由 folder + folder_acl 管理權限
CREATE TABLE folder (
    workspace_id VARCHAR(50) NOT NULL REFERENCES workspace(id),
    name VARCHAR(100) NOT NULL,              -- "ml-team", "production", "shared"
    display_name VARCHAR(255) DEFAULT '',
    PRIMARY KEY (workspace_id, name)
);

-- Folder 權限控制（正規化設計，取代 Windmill 的 owners TEXT[] + extra_perms JSONB）
-- 三級角色：owner（完全控制）> writer（讀寫）> reader（唯讀）
CREATE TABLE folder_acl (
    workspace_id VARCHAR(50)  NOT NULL,
    folder_name  VARCHAR(100) NOT NULL,
    subject      VARCHAR(255) NOT NULL,  -- 'users/alice' or 'teams/backend'
    role         VARCHAR(20)  NOT NULL CHECK (role IN ('owner', 'writer', 'reader')),
    PRIMARY KEY (workspace_id, folder_name, subject),
    FOREIGN KEY (workspace_id, folder_name)
        REFERENCES folder(workspace_id, name) ON DELETE CASCADE
);

-- Auth middleware 用：快速查出某使用者可存取的所有 folder
CREATE INDEX idx_folder_acl_subject ON folder_acl(workspace_id, subject);

-- 團隊資源配額（Windmill 沒有，CoveFlow 獨有）
CREATE TABLE team_quota (
    workspace_id VARCHAR(50) NOT NULL,
    team_name VARCHAR(100) NOT NULL,
    -- 並發 run 數上限（NULL = 不限）
    max_concurrent_runs INTEGER,
    -- 團隊可佔用的 CPU 總量上限（NULL = 不限）
    -- 例：max_cpus = 16 → 團隊所有 running runs 的 SUM(cpus) <= 16
    max_cpus REAL,
    -- 團隊可佔用的 RAM 總量上限 MB（NULL = 不限）
    -- 例：max_memory_mb = 32768 → 團隊所有 running runs 的 SUM(memory_mb) <= 32768
    -- （注意：disk 不做團隊配額——磁碟是 Worker 本地暫存，run 完成即清理，
    --  不像 CPU/RAM 是跨 Worker 共享池的稀缺資源。團隊儲存配額走 max_storage_bytes。）
    max_memory_mb BIGINT,
    -- 每日 run 數上限（NULL = 不限）
    max_daily_runs INTEGER,
    -- 檔案儲存配額 bytes（NULL = 不限）
    max_storage_bytes BIGINT,
    -- 單一 run 最大執行時間秒（NULL = 用全域預設）
    max_run_timeout_secs INTEGER,
    PRIMARY KEY (workspace_id, team_name),
    FOREIGN KEY (workspace_id, team_name) REFERENCES team(workspace_id, name) ON DELETE CASCADE
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
    requirements TEXT[] NOT NULL DEFAULT '{}',  -- pip packages: ["pandas>=2.0", "requests"]
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
    -- Resource-based 資源需求
    cpus REAL NOT NULL DEFAULT 1,                -- 此 job 需要多少 vCPU
    memory_mb INTEGER NOT NULL DEFAULT 512,      -- 此 job 需要多少 MB RAM
    disk_mb INTEGER NOT NULL DEFAULT 1024,       -- 此 job 需要多少 MB 磁碟空間
    requirements TEXT[] NOT NULL DEFAULT '{}',  -- copied from script, or inline for preview
    -- 團隊歸屬（從 script/flow path 自動推導）
    team_owner VARCHAR(100),             -- NULL（個人 users/...）或 team 名（folders/ml-team/...）
    created_by VARCHAR(255) NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    -- OTel trace context
    trace_id CHAR(32),
    span_id CHAR(16),
    -- Run Operations（詳見 11-run-operations.md）
    rerun_of UUID REFERENCES job(id)    -- 若為重跑，指向原始 run
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
    last_ping TIMESTAMPTZ,
    -- Run Operations（詳見 11-run-operations.md）
    canceled_by VARCHAR(255),               -- 誰取消的（NULL = 未取消）
    canceled_reason TEXT,                    -- 取消原因
    cancel_requested_at TIMESTAMPTZ         -- 取消請求時間
);

CREATE INDEX idx_job_queue_pull ON job_queue(scheduled_for, priority DESC)
    WHERE running = FALSE;

-- 已取消但仍在執行的 run（worker 偵測用）
CREATE INDEX idx_job_queue_cancel ON job_queue(id)
    WHERE canceled_by IS NOT NULL AND running = TRUE;

-- job_completed：結果（完成後從 job_queue 刪除，插入這裡）
CREATE TABLE job_completed (
    id UUID PRIMARY KEY REFERENCES job(id),
    success BOOLEAN NOT NULL,
    result JSONB,
    result_s3_key VARCHAR(255),        -- 大結果存 S3
    duration_ms INTEGER NOT NULL,
    memory_peak_bytes BIGINT,
    completed_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    -- Run Operations（詳見 11-run-operations.md）
    canceled_by VARCHAR(255),          -- 取消者（cancel_run / force_cancel_run 寫入）
    canceled_reason TEXT,              -- 取消原因
    marked_by VARCHAR(255),            -- 標記者（mark_success / mark_fail 寫入）
    mark_reason TEXT                   -- 標記原因
);

-- run_log：Run 執行日誌（分塊儲存，設計詳見 10-log-and-ha.md §1.4）
-- 每次 flush（500ms / 100 條）產生一行，entries 為 JSONB 陣列
CREATE TABLE run_log (
    id           BIGSERIAL PRIMARY KEY,
    run_id       UUID NOT NULL REFERENCES job(id),
    workspace_id UUID NOT NULL,
    seq          INT NOT NULL,             -- chunk 序號（同一 run 內遞增）
    created_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    min_level    SMALLINT NOT NULL,        -- chunk 內最低 level（快速跳過 DEBUG）
    max_level    SMALLINT NOT NULL,        -- chunk 內最高 level（快速過濾）
    line_count   SMALLINT NOT NULL,        -- chunk 內日誌條數
    entries      JSONB NOT NULL,           -- [{ts, level, msg, node_id?, ...}, ...]
    flow_id      UUID,
    node_id      TEXT,                     -- 若 chunk 全屬同一 node 則填入
    worker_name  TEXT,
    archived     BOOLEAN NOT NULL DEFAULT false
);

CREATE INDEX idx_run_log_run_seq ON run_log (run_id, seq);
CREATE INDEX idx_run_log_workspace ON run_log (workspace_id, created_at);
CREATE INDEX idx_run_log_level ON run_log (run_id, max_level) WHERE max_level >= 4;
CREATE INDEX idx_run_log_archive ON run_log (created_at) WHERE archived = false;

-- service_log：服務級日誌（API/Worker/Scheduler 程序日誌，同樣分塊）
CREATE TABLE service_log (
    id           BIGSERIAL PRIMARY KEY,
    instance_id  TEXT NOT NULL,
    service      TEXT NOT NULL,            -- "api" | "worker" | "scheduler"
    seq          INT NOT NULL,
    created_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    min_level    SMALLINT NOT NULL,
    max_level    SMALLINT NOT NULL,
    line_count   SMALLINT NOT NULL,
    entries      JSONB NOT NULL
);

CREATE INDEX idx_service_log_instance ON service_log (instance_id, created_at);
CREATE INDEX idx_service_log_service  ON service_log (service, created_at);

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

-- === Resource（集中管理外部連線 / 密鑰）===

-- Resource 類型定義（JSON Schema 驅動 UI 表單）
CREATE TABLE resource_type (
    workspace_id VARCHAR(50) NOT NULL REFERENCES workspace(id),
    name VARCHAR(100) NOT NULL,           -- "postgres", "openai", "s3", "slack"
    schema JSONB NOT NULL,                -- JSON Schema（定義此類型有哪些欄位）
    description TEXT DEFAULT '',
    PRIMARY KEY (workspace_id, name)
);

-- Resource 實例（加密存儲）
CREATE TABLE resource (
    workspace_id VARCHAR(50) NOT NULL REFERENCES workspace(id),
    path VARCHAR(255) NOT NULL,           -- "users/admin/prod_db", "folders/shared/openai_key"
    resource_type VARCHAR(100) NOT NULL,  -- FK → resource_type.name
    value_encrypted BYTEA NOT NULL,       -- AES-256-GCM 加密的 JSONB
    description TEXT DEFAULT '',
    created_by VARCHAR(255) NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (workspace_id, path)
);

-- Variable（簡單 key-value，可被 Resource 引用）
CREATE TABLE variable (
    workspace_id VARCHAR(50) NOT NULL REFERENCES workspace(id),
    path VARCHAR(255) NOT NULL,           -- "users/admin/api_key"
    value_encrypted BYTEA NOT NULL,       -- AES-256-GCM 加密
    is_secret BOOLEAN NOT NULL DEFAULT TRUE,
    description TEXT DEFAULT '',
    created_by VARCHAR(255) NOT NULL,
    PRIMARY KEY (workspace_id, path)
);

-- === Deploy Approval Gate（部署審核）===

-- 審核政策：哪些路徑的 Flow/Script 需要審核才能上線
CREATE TABLE approval_policy (
    workspace_id    VARCHAR(50) NOT NULL REFERENCES workspace(id),
    path_pattern    VARCHAR(255) NOT NULL,  -- glob: 'folders/production/*', 'folders/finance/*'
    min_approvals   INTEGER NOT NULL DEFAULT 1,
    approvers       TEXT[] NOT NULL,         -- ['users/alice', 'teams/sre-team']
    auto_deploy     BOOLEAN DEFAULT FALSE,  -- 達到 min_approvals 後自動部署？
    PRIMARY KEY (workspace_id, path_pattern)
);

-- 部署請求（類似 Pull Request）
CREATE TABLE deploy_request (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    workspace_id    VARCHAR(50) NOT NULL REFERENCES workspace(id),
    target_path     VARCHAR(255) NOT NULL,       -- 要部署的 flow/script 路徑
    target_kind     VARCHAR(10) NOT NULL,        -- 'flow' | 'script'
    draft_value     JSONB NOT NULL,              -- 新版本內容
    previous_hash   VARCHAR(64),                 -- 舊版本 hash（用於 diff）
    requested_by    VARCHAR(255) NOT NULL,
    status          VARCHAR(20) NOT NULL DEFAULT 'pending', -- pending/approved/rejected/deployed
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    deployed_at     TIMESTAMPTZ
);

CREATE INDEX idx_deploy_request_ws_status ON deploy_request(workspace_id, status);

-- 個別審核紀錄
CREATE TABLE deploy_approval (
    deploy_request_id  UUID NOT NULL REFERENCES deploy_request(id),
    approver           VARCHAR(255) NOT NULL,
    decision           VARCHAR(10) NOT NULL,     -- 'approved' | 'rejected'
    comment            TEXT DEFAULT '',
    decided_at         TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (deploy_request_id, approver)
);

-- === Worker 管理 ===

-- 全域 Worker 並發控制
CREATE TABLE worker_config (
    workspace_id VARCHAR(50) NOT NULL REFERENCES workspace(id),
    -- 全域上限：所有 worker 同時跑的 job 總數
    -- NULL = 不限（由 worker 數量自然限制）
    max_concurrent_jobs INTEGER,
    -- 各 tag 上限（與 concurrency_limit 互補，這是全域層級）
    max_workers_per_tag JSONB DEFAULT '{}',  -- {"gpu": 2, "heavy": 4}
    PRIMARY KEY (workspace_id)
);

-- Worker 健康 + 狀態 + 資源監控
CREATE TABLE worker_ping (
    worker VARCHAR(100) PRIMARY KEY,
    ping_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    tags TEXT[] NOT NULL DEFAULT '{}',
    ip VARCHAR(45),
    sandbox_mode VARCHAR(20),       -- "nsjail", "k8s", "none"
    current_job_id UUID,            -- 正在跑的 job（NULL = 閒置）
    jobs_completed INTEGER DEFAULT 0, -- 累計完成數（監控用）
    -- Resource-based 資源模型
    total_cpus REAL,                -- 此 Worker 的 CPU 總量
    used_cpus REAL,                 -- 目前已佔用的 CPU 量
    total_memory_mb BIGINT,         -- 此 Worker 的 RAM 總量 (MB)
    used_memory_mb BIGINT,          -- 目前已佔用的 RAM (MB)
    total_disk_mb BIGINT,           -- 此 Worker 的 Disk 總量 (MB)
    used_disk_mb BIGINT,            -- 目前已佔用的 Disk (MB)
    -- 資源配額（靜態，啟動時偵測）
    vcpus INTEGER,                  -- vCPU 數（cgroup quota / sysinfo）
    memory_total BIGINT,            -- 總記憶體 bytes（cgroup limit / meminfo）
    disk_total BIGINT,              -- job_dir 所在磁碟總容量 bytes
    -- 資源用量（每次 ping 更新）
    cpu_usage_percent REAL,         -- CPU 使用率 %（/proc/stat delta）
    memory_usage BIGINT,            -- 目前記憶體用量 bytes（cgroup current）
    disk_usage BIGINT,              -- 目前磁碟用量 bytes（statvfs）
    -- 佔用率（多時間窗口）
    occupancy_15s REAL,             -- 15 秒佔用率
    occupancy_5m REAL,              -- 5 分鐘佔用率
    occupancy_30m REAL              -- 30 分鐘佔用率
);

-- === File Storage 設定 ===

CREATE TABLE workspace_settings (
    workspace_id VARCHAR(50) PRIMARY KEY REFERENCES workspace(id),
    -- 檔案儲存模式：'s3' | 'local'
    file_storage_mode VARCHAR(10) NOT NULL DEFAULT 'local',
    -- S3 模式設定
    s3_bucket VARCHAR(255),
    s3_region VARCHAR(50),
    s3_endpoint VARCHAR(255),         -- 自建 MinIO: "http://minio:9000"
    s3_access_key_encrypted BYTEA,    -- AES-256-GCM 加密
    s3_secret_key_encrypted BYTEA,    -- AES-256-GCM 加密
    -- 本地模式設定
    local_data_dir VARCHAR(500) DEFAULT '/data/coveflow/files',
    -- 限制
    max_file_size BIGINT DEFAULT 104857600  -- 100MB 預設
);
```

### 1.2 Sandbox Trait 設計（可插拔）

Executor 不需要知道用哪種沙箱，只需呼叫 `sandbox.execute(&ctx)`。新增沙箱模式只需：(1) 實作 `Sandbox` trait，(2) 在 `SandboxRouter` 註冊。完整的三模式實作見[附錄 A](#附錄-a-sandbox-三模式詳細實作)。

```rust
// crates/worker/src/sandbox.rs

use async_trait::async_trait;

/// 沙箱執行模式（可插拔：新增模式只需加 variant + 實作 trait）
#[derive(Debug, Clone, serde::Deserialize)]
#[serde(tag = "mode")]
pub enum SandboxMode {
    None,                              // 開發環境：不隔離
    Nsjail(NsjailConfig),              // Phase 1：cgroup + namespace（VM/Bare Metal）
    KubernetesPod(K8sPodConfig),       // Phase 3+：K8s Pod 級隔離
    Firecracker(FirecrackerConfig),    // Phase 4+：microVM（多租戶 SaaS）
}

/// Sandbox trait — 所有執行器都透過這個介面執行程式碼
/// 新增沙箱模式只需實作此 trait（Strategy Pattern）
#[async_trait]
pub trait Sandbox: Send + Sync {
    /// 在沙箱中執行程式碼（核心方法）
    async fn execute(&self, ctx: &SandboxContext) -> Result<SandboxResult, SandboxError>;
    /// 健康檢查（啟動時驗證沙箱可用）
    async fn health_check(&self) -> Result<(), SandboxError>;
    /// 沙箱名稱（用於日誌和監控）
    fn name(&self) -> &str;
    /// 資源限制能力（供 ResourceManager 查詢）
    fn supports_resource_limits(&self) -> bool { true }
}

/// 執行上下文（傳入沙箱的所有資訊）
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
    // Resource-based：沙箱的資源限制直接來自 job 宣告的 cpus/memory_mb/disk_mb
    pub resource_limits: SandboxResources,
}

/// Resource-based 模型提供的資源限制
pub struct SandboxResources {
    pub cpus: f32,         // 直接來自 job.cpus
    pub memory: u64,       // job.memory_mb × 1024 × 1024 (bytes)
    pub disk: u64,         // job.disk_mb × 1024 × 1024 (bytes)
    pub timeout: u32,      // 最大執行秒數
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
    fn supports_resource_limits(&self) -> bool { false }
}

/// 根據 job 的 tag 選擇沙箱（Strategy Pattern + Chain of Responsibility）
pub struct SandboxRouter {
    nsjail: Option<NsjailSandbox>,
    k8s: Option<K8sPodSandbox>,
    firecracker: Option<FirecrackerSandbox>,  // Phase 4+
    none: NoneSandbox,
}

impl SandboxRouter {
    /// 根據 tag 選擇沙箱
    ///
    /// tag 規則：
    ///   "nsjail"          → nsjail（VM/Bare Metal 預設）
    ///   "heavy"/"k8s"/"gpu" → K8s Pod（秒級，自訂 image/GPU）
    ///   "firecracker"/"fc"  → Firecracker microVM（Phase 4+）
    ///   "none"/"dev"      → 不隔離（開發用）
    ///   預設               → 按優先級自動選擇
    pub fn select(&self, tag: &str, _language: Option<ScriptLang>) -> &dyn Sandbox {
        match tag {
            "nsjail" => self.nsjail.as_ref().map(|s| s as &dyn Sandbox).unwrap_or(&self.none),
            "heavy" | "k8s" | "gpu" => self.k8s.as_ref().map(|s| s as &dyn Sandbox).unwrap_or(&self.none),
            "firecracker" | "fc" => self.firecracker.as_ref().map(|s| s as &dyn Sandbox).unwrap_or(&self.none),
            "none" | "dev" => &self.none,
            _ => self.select_default(),
        }
    }

    /// 預設優先級：nsjail → K8s → Firecracker → none
    fn select_default(&self) -> &dyn Sandbox {
        if let Some(nsjail) = &self.nsjail { nsjail as &dyn Sandbox }
        else if let Some(k8s) = &self.k8s { k8s as &dyn Sandbox }
        else if let Some(fc) = &self.firecracker { fc as &dyn Sandbox }
        else { &self.none }
    }
}

// 未來擴展只需：
// 1. 新增 SandboxMode variant（例 SandboxMode::Gvisor）
// 2. 實作 Sandbox trait for GvisorSandbox
// 3. 在 SandboxRouter 加一個 field + match arm
// 不需修改任何 executor 程式碼
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
    // Requirements are user-specified in the frontend rather than auto-parsed from imports.
    // This avoids the fragility of import parsing (e.g., `import cv2` requires `opencv-python`,
    // not `cv2`), gives users explicit control over version pinning, and eliminates the need
    // for a Python stdlib allowlist.
    if !job.requirements.is_empty() {
        install_python_deps(&job.requirements, job_dir, sandbox).await?;
    }

    write_file(job_dir, "inner.py", content)?;
    write_file(job_dir, "wrapper.py", &generate_python_wrapper())?;
    create_args_and_out_file(job, job_dir, db, &state.encryption_key).await?;

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

    // 0. Layer 5：團隊配額檢查（如果 run 屬於某團隊）
    if let Some(team_owner) = args.team_owner {
        // 查對應團隊的 team_quota
        let quota = sqlx::query!(
            "SELECT max_concurrent_runs, max_cpus, max_memory_mb, max_daily_runs FROM team_quota
             WHERE workspace_id = $1 AND team_name = $2",
            args.workspace_id, team_owner
        ).fetch_optional(&mut *tx).await?;

        if let Some(q) = quota {
            // 檢查並發上限（run 數量）
            if let Some(max_conc) = q.max_concurrent_runs {
                let running: i64 = sqlx::query_scalar!(
                    "SELECT COUNT(*) FROM run_queue rq
                     JOIN run r ON r.id = rq.id
                     WHERE r.workspace_id = $1
                       AND r.team_owner = $2
                       AND rq.running = TRUE",
                    args.workspace_id, team_owner
                ).fetch_one(&mut *tx).await?.unwrap_or(0);
                if running >= max_conc as i64 {
                    return Err(anyhow::anyhow!(
                        "team '{}' concurrent run limit reached ({}/{})",
                        team_owner, running, max_conc
                    ));
                }
            }
            // 檢查 CPU 上限（佔用 CPU 總量）
            // 注意：這裡只做「即時拒絕」，防止團隊無限推入大 run
            // claim_run 的 L5 子查詢才是真正的排程卡控
            if let Some(max_c) = q.max_cpus {
                let used_cpus: f64 = sqlx::query_scalar!(
                    "SELECT COALESCE(SUM(r.cpus::DOUBLE PRECISION), 0) FROM run_queue rq
                     JOIN run r ON r.id = rq.id
                     WHERE r.workspace_id = $1
                       AND r.team_owner = $2
                       AND rq.running = TRUE",
                    args.workspace_id, team_owner
                ).fetch_one(&mut *tx).await?.unwrap_or(0.0);
                let needed = args.cpus.unwrap_or(1.0) as f64;
                if used_cpus + needed > max_c as f64 {
                    return Err(anyhow::anyhow!(
                        "team '{}' CPU quota exceeded ({:.1}/{:.1} cpus, run needs {:.1})",
                        team_owner, used_cpus, max_c, needed
                    ));
                }
            }
            // 檢查 RAM 上限（佔用 memory_mb 總量）
            if let Some(max_m) = q.max_memory_mb {
                let used_mem: i64 = sqlx::query_scalar!(
                    "SELECT COALESCE(SUM(r.memory_mb), 0) FROM run_queue rq
                     JOIN run r ON r.id = rq.id
                     WHERE r.workspace_id = $1
                       AND r.team_owner = $2
                       AND rq.running = TRUE",
                    args.workspace_id, team_owner
                ).fetch_one(&mut *tx).await?.unwrap_or(0);
                let needed = args.memory_mb.unwrap_or(512) as i64;
                if used_mem + needed > max_m {
                    return Err(anyhow::anyhow!(
                        "team '{}' memory quota exceeded ({}/{} MB, run needs {} MB)",
                        team_owner, used_mem, max_m, needed
                    ));
                }
            }
            // 檢查每日上限
            if let Some(max_daily) = q.max_daily_runs {
                let today_count: i64 = sqlx::query_scalar!(
                    "SELECT COUNT(*) FROM run r
                     WHERE r.workspace_id = $1
                       AND r.team_owner = $2
                       AND r.created_at >= CURRENT_DATE",
                    args.workspace_id, team_owner
                ).fetch_one(&mut *tx).await?.unwrap_or(0);
                if today_count >= max_daily as i64 {
                    return Err(anyhow::anyhow!(
                        "team '{}' daily run limit reached ({}/{})",
                        team_owner, today_count, max_daily
                    ));
                }
            }
        }
    }

    // 1. 插入 run（不可變定義）
    sqlx::query!(
        "INSERT INTO run (id, workspace_id, kind, script_hash, script_path,
         raw_code, language, args, tag, parent_run, root_run, flow_step_id,
         team_owner, created_by, trace_id, span_id)
         VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16)",
        run_id, args.workspace_id, args.kind.as_str(),
        args.script_hash, args.script_path,
        args.raw_code, args.language.map(|l| l.as_str()),
        args.args, args.tag,
        args.parent_run, args.root_run, args.flow_step_id,
        args.team_owner,
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

    // 3. 日誌由 DbLogLayer（tracing subscriber）自動寫入 run_log，無需手動 INSERT

    // 4. LISTEN/NOTIFY（比純 polling 快 10x）
    sqlx::query("SELECT pg_notify('new_job', $1)")
        .bind(job_id.to_string())
        .execute(&mut *tx).await?;

    tx.commit().await?;
    Ok(job_id)
}

// crates/queue/src/pull.rs

/// FOR UPDATE SKIP LOCKED — 搶 job（含全域並發控制 + L6 資源可用性）
/// available_cpus/mem/disk: 此 Worker 目前可用的資源（由 ResourceManager 提供）
pub async fn pull_job(
    db: &PgPool, worker_name: &str, tags: &[String],
    available_cpus: f32, available_memory_mb: i64, available_disk_mb: i64,
) -> Result<Option<PulledJob>> {
    // 1. L2: 檢查全域並發上限
    let global_limit = sqlx::query_scalar!(
        "SELECT max_concurrent_jobs FROM worker_config LIMIT 1"
    ).fetch_optional(db).await?.flatten();

    if let Some(limit) = global_limit {
        let running: i64 = sqlx::query_scalar!(
            "SELECT COUNT(*) FROM job_queue WHERE running = TRUE"
        ).fetch_one(db).await?.unwrap_or(0);

        if running >= limit as i64 {
            return Ok(None); // 全域已滿，等下一輪
        }
    }

    // 2. L3 + L5 + L6: tag 級 + 團隊級 + 資源可用性
    let row = sqlx::query_as!(PulledJob,
        r#"
        WITH next_job AS (
            SELECT jq.id, jq.tag, j.cpus, j.memory_mb, j.disk_mb
            FROM job_queue jq
            JOIN job j ON j.id = jq.id
            WHERE jq.running = FALSE
              AND jq.scheduled_for <= now()
              AND jq.canceled_by IS NULL      -- 跳過已取消的 run（11-run-operations.md）
              AND jq.tag = ANY($1)
              -- L6a: CPU 可用性
              AND j.cpus <= $3
              -- L6b: RAM 可用性
              AND j.memory_mb <= $4
              -- L6c: Disk 可用性
              AND j.disk_mb <= $5
              -- L3: tag 級並發控制
              AND NOT EXISTS (
                  SELECT 1 FROM concurrency_limit cl
                  WHERE cl.tag = jq.tag
                    AND (SELECT COUNT(*) FROM job_queue jq2
                         WHERE jq2.tag = jq.tag AND jq2.running = TRUE) >= cl.max_concurrent
              )
              -- L5a: 團隊級並發控制（team_quota.max_concurrent_runs）
              AND (
                  r.team_owner IS NULL
                  OR NOT EXISTS (
                      SELECT 1 FROM team_quota tq
                      WHERE tq.workspace_id = r.workspace_id
                        AND tq.team_name = r.team_owner
                        AND tq.max_concurrent_runs IS NOT NULL
                        AND (SELECT COUNT(*) FROM run_queue rq3
                             JOIN run r3 ON r3.id = rq3.id
                             WHERE r3.workspace_id = r.workspace_id
                               AND r3.team_owner = r.team_owner
                               AND rq3.running = TRUE) >= tq.max_concurrent_runs
                  )
              )
              -- L5b: 團隊級 CPU 控制（team_quota.max_cpus）
              AND (
                  r.team_owner IS NULL
                  OR NOT EXISTS (
                      SELECT 1 FROM team_quota tq
                      WHERE tq.workspace_id = r.workspace_id
                        AND tq.team_name = r.team_owner
                        AND tq.max_cpus IS NOT NULL
                        AND (
                            SELECT COALESCE(SUM(r4.cpus::DOUBLE PRECISION), 0)
                            FROM run_queue rq4
                            JOIN run r4 ON r4.id = rq4.id
                            WHERE r4.workspace_id = r.workspace_id
                              AND r4.team_owner = r.team_owner
                              AND rq4.running = TRUE
                        ) + r.cpus::DOUBLE PRECISION > tq.max_cpus::DOUBLE PRECISION
                  )
              )
              -- L5c: 團隊級 RAM 控制（team_quota.max_memory_mb）
              AND (
                  r.team_owner IS NULL
                  OR NOT EXISTS (
                      SELECT 1 FROM team_quota tq
                      WHERE tq.workspace_id = r.workspace_id
                        AND tq.team_name = r.team_owner
                        AND tq.max_memory_mb IS NOT NULL
                        AND (
                            SELECT COALESCE(SUM(r5.memory_mb), 0)
                            FROM run_queue rq5
                            JOIN run r5 ON r5.id = rq5.id
                            WHERE r5.workspace_id = r.workspace_id
                              AND r5.team_owner = r.team_owner
                              AND rq5.running = TRUE
                        ) + r.memory_mb > tq.max_memory_mb
                  )
              )
            ORDER BY jq.priority DESC, jq.scheduled_for ASC
            LIMIT 1
            FOR UPDATE OF jq SKIP LOCKED
        )
        UPDATE job_queue
        SET running = TRUE, started_at = now(), worker = $2, last_ping = now()
        FROM next_job
        WHERE job_queue.id = next_job.id
        RETURNING job_queue.id, job_queue.tag,
                  next_job.cpus, next_job.memory_mb, next_job.disk_mb
        "#,
        tags, worker_name,
        available_cpus, available_memory_mb, available_disk_mb
    )
    .fetch_optional(db)
    .await?;

    if let Some(row) = row {
        // 更新 worker_ping 記錄正在跑的 job
        sqlx::query!(
            "UPDATE worker_ping SET current_job_id = $1 WHERE worker = $2",
            row.id, worker_name
        ).execute(db).await.ok();

        let job = sqlx::query_as!(QueuedJob, "SELECT * FROM job WHERE id = $1", row.id)
            .fetch_one(db).await?;
        Ok(Some(PulledJob { job, tag: row.tag }))
    } else {
        Ok(None)
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

    // NOTIFY：喚醒 run_wait_result 的輪詢（把 200ms 延遲降到 ~1ms）
    sqlx::query("SELECT pg_notify('job_completed', $1)")
        .bind(job_id.to_string())
        .execute(&mut *tx).await?;

    // 清除 worker 的 current_job_id + 累計完成數
    sqlx::query!(
        "UPDATE worker_ping SET current_job_id = NULL, jobs_completed = jobs_completed + 1
         WHERE current_job_id = $1", job_id
    ).execute(&mut *tx).await.ok();

    tx.commit().await?;
    Ok(())
}
```

### 1.4 ResourceManager 實作

```rust
// crates/worker/src/resource_manager.rs

use std::sync::Mutex as StdMutex;  // 非 async——鎖持有時間極短，避免 async mutex 開銷

/// Worker 的三維資源池
pub struct ResourceManager {
    total_cpus: f32,
    total_memory_mb: u64,
    total_disk_mb: u64,
    inner: StdMutex<ResourceState>,
    notify: tokio::sync::Notify,
}

struct ResourceState {
    used_cpus: f32,
    used_memory_mb: u64,
    used_disk_mb: u64,
    // Dedicated/Runner Group 永久預留的資源
    reserved_cpus: f32,
    reserved_memory_mb: u64,
    reserved_disk_mb: u64,
}

/// RAII guard — drop 時自動歸還資源（Normal job 用）
pub struct ResourceGuard {
    cpus: f32,
    memory_mb: u64,
    disk_mb: u64,
    manager: Arc<ResourceManager>,
}

/// 永久預留 — 不會自動歸還（Dedicated/Runner Group 用）
pub struct ResourceReservation {
    cpus: f32,
    memory_mb: u64,
    disk_mb: u64,
    manager: Arc<ResourceManager>,
}

impl ResourceManager {
    pub fn new(total_cpus: f32, total_memory_mb: u64, total_disk_mb: u64) -> Self {
        Self {
            total_cpus,
            total_memory_mb,
            total_disk_mb,
            inner: StdMutex::new(ResourceState {
                used_cpus: 0.0, used_memory_mb: 0, used_disk_mb: 0,
                reserved_cpus: 0.0, reserved_memory_mb: 0, reserved_disk_mb: 0,
            }),
            notify: tokio::sync::Notify::new(),
        }
    }

    /// 可供 Normal job 使用的資源 = total - reserved - used
    pub fn available(&self) -> (f32, u64, u64) {
        let s = self.inner.lock().unwrap();
        (
            self.total_cpus - s.reserved_cpus - s.used_cpus,
            self.total_memory_mb - s.reserved_memory_mb - s.used_memory_mb,
            self.total_disk_mb - s.reserved_disk_mb - s.used_disk_mb,
        )
    }

    /// 嘗試取得資源（非阻塞）— Normal job pull 後立即呼叫
    pub fn try_acquire(&self, cpus: f32, mem_mb: u64, disk_mb: u64) -> Option<ResourceGuard> {
        let mut s = self.inner.lock().unwrap();
        let free_cpu = self.total_cpus - s.reserved_cpus - s.used_cpus;
        let free_mem = self.total_memory_mb - s.reserved_memory_mb - s.used_memory_mb;
        let free_disk = self.total_disk_mb - s.reserved_disk_mb - s.used_disk_mb;
        if cpus <= free_cpu && mem_mb <= free_mem && disk_mb <= free_disk {
            s.used_cpus += cpus;
            s.used_memory_mb += mem_mb;
            s.used_disk_mb += disk_mb;
            Some(ResourceGuard { cpus, memory_mb: mem_mb, disk_mb, manager: self.into() })
        } else {
            None
        }
    }

    /// 永久預留資源（Dedicated / Runner Group 啟動時呼叫）
    pub fn reserve(&self, cpus: f32, mem_mb: u64, disk_mb: u64) -> Option<ResourceReservation> {
        let mut s = self.inner.lock().unwrap();
        let free_cpu = self.total_cpus - s.reserved_cpus - s.used_cpus;
        let free_mem = self.total_memory_mb - s.reserved_memory_mb - s.used_memory_mb;
        let free_disk = self.total_disk_mb - s.reserved_disk_mb - s.used_disk_mb;
        if cpus <= free_cpu && mem_mb <= free_mem && disk_mb <= free_disk {
            s.reserved_cpus += cpus;
            s.reserved_memory_mb += mem_mb;
            s.reserved_disk_mb += disk_mb;
            Some(ResourceReservation { cpus, memory_mb: mem_mb, disk_mb, manager: self.into() })
        } else {
            None
        }
    }

    pub async fn wait_for_release(&self) {
        self.notify.notified().await;
    }
}

impl Drop for ResourceGuard {
    fn drop(&mut self) {
        let mut s = self.manager.inner.lock().unwrap();
        s.used_cpus -= self.cpus;
        s.used_memory_mb -= self.memory_mb;
        s.used_disk_mb -= self.disk_mb;
        self.manager.notify.notify_waiters();
    }
}
// Normal job 完成 -> guard drop -> 資源回到池中 -> notify_waiters 喚醒主迴圈

impl Drop for ResourceReservation {
    fn drop(&mut self) {
        // Dedicated 停止時才會 drop → 歸還 reserved 資源
        let mut s = self.manager.inner.lock().unwrap();
        s.reserved_cpus -= self.cpus;
        s.reserved_memory_mb -= self.memory_mb;
        s.reserved_disk_mb -= self.disk_mb;
        self.manager.notify.notify_waiters();
    }
}
```

**為什麼改用 `std::Mutex` 而非 `tokio::Mutex`？**
鎖內只做加減法（幾個 ns），不含 await。`std::Mutex` 比 async mutex 快 10 倍，且可在 `Drop` 中使用（async Drop 不存在）。

### 1.5 Worker 主迴圈（整合 ResourceManager）

```rust
// crates/worker/src/worker.rs

use opentelemetry::trace::{Tracer, SpanKind};

pub async fn run_worker(
    db: PgPool, worker_name: String, tags: Vec<String>,
    config: WorkerConfig,
    sandbox_router: Arc<SandboxRouter>,
    tracer: opentelemetry::global::BoxedTracer,
) {
    let worker_dir = format!("/tmp/coveflow/{}", worker_name);
    tokio::fs::create_dir_all(&worker_dir).await.unwrap();

    // 啟動時資源驗證（自動偵測 + validate + clamp）
    let mut resolved_config = config.clone();
    resource_check::validate_and_resolve(&mut resolved_config, &worker_dir);

    // 初始化 ResourceManager
    let rm = Arc::new(ResourceManager::new(
        resolved_config.total_cpus.unwrap(),
        resolved_config.total_memory_mb.unwrap(),
        resolved_config.total_disk_mb.unwrap(),
    ));

    // 健康檢查：定期 ping（含資源狀態回報）
    let db2 = db.clone();
    let wn = worker_name.clone();
    let rm2 = rm.clone();
    tokio::spawn(async move {
        loop {
            let (free_cpu, free_mem, free_disk) = rm2.available();
            let total_cpus = resolved_config.total_cpus.unwrap();
            let total_mem = resolved_config.total_memory_mb.unwrap();
            let total_disk = resolved_config.total_disk_mb.unwrap();
            sqlx::query!(
                "INSERT INTO worker_ping (worker, ping_at, tags,
                 total_cpus, used_cpus, total_memory_mb, used_memory_mb,
                 total_disk_mb, used_disk_mb)
                 VALUES ($1, now(), $2, $3, $4, $5, $6, $7, $8)
                 ON CONFLICT (worker) DO UPDATE SET
                   ping_at = now(),
                   total_cpus = $3, used_cpus = $4,
                   total_memory_mb = $5, used_memory_mb = $6,
                   total_disk_mb = $7, used_disk_mb = $8",
                wn, &tags_for_ping,
                total_cpus, total_cpus - free_cpu,
                total_mem as i64, (total_mem - free_mem) as i64,
                total_disk as i64, (total_disk - free_disk) as i64,
            ).execute(&db2).await.ok();
            tokio::time::sleep(std::time::Duration::from_secs(15)).await;
        }
    });

    // 雙軌 Pull：LISTEN/NOTIFY 事件驅動 + 5 秒兜底輪詢
    let mut listener = sqlx::postgres::PgListener::connect_with(&db).await.unwrap();
    listener.listen("new_job").await.unwrap();

    loop {
        // L6: 檢查資源可用性
        let (free_cpu, free_mem, free_disk) = rm.available();
        if free_cpu <= 0.0 || free_mem == 0 || free_disk == 0 {
            // 所有資源都耗盡，等任一 job 完成
            rm.wait_for_release().await;
            continue;
        }

        // 拉取能塞進可用資源的 job
        match queue::pull_job(
            &db, &worker_name, &tags,
            free_cpu, free_mem as i64, free_disk as i64,
        ).await {
            Ok(Some(pulled)) => {
                let job = pulled.job;
                let guard = rm.try_acquire(
                    pulled.cpus, pulled.memory_mb as u64, pulled.disk_mb as u64,
                ).expect("resources were just checked");

                let job_dir = format!("{}/{}", worker_dir, job.id);
                tokio::fs::create_dir_all(&job_dir).await.unwrap();

                // OTel: 建立 span
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
                let cx = opentelemetry::Context::current_with_span(span);

                // 直接使用 job 宣告的資源
                let sandbox_resources = SandboxResources {
                    cpus: pulled.cpus,
                    memory: pulled.memory_mb as u64 * 1024 * 1024,
                    disk: pulled.disk_mb as u64 * 1024 * 1024,
                    timeout: config.job_timeout_secs,
                };

                let sandbox = sandbox_router.select(&pulled.tag, None);

                // 非同步執行 job（不阻塞 pull 迴圈）
                let db_clone = db.clone();
                tokio::spawn(async move {
                    let start = std::time::Instant::now();
                    let result = handle_job(&job, &db_clone, &job_dir, sandbox, &sandbox_resources).await;
                    let duration_ms = start.elapsed().as_millis() as i32;

                    match result {
                        Ok((value, mem_peak)) => {
                            let s3_key = maybe_upload_to_s3(&value).await;
                            queue::complete_job(&db_clone, job.id, true, value, duration_ms, mem_peak, s3_key.as_deref()).await.ok();
                            cx.span().set_status(opentelemetry::trace::StatusCode::Ok, "".into());
                        }
                        Err(e) => {
                            let error = serde_json::json!({"error": {"message": e.to_string()}});
                            queue::complete_job(&db_clone, job.id, false, error, duration_ms, 0, None).await.ok();
                            cx.span().set_status(opentelemetry::trace::StatusCode::Error, e.to_string());
                        }
                    }
                    cx.span().end();
                    tokio::fs::remove_dir_all(&job_dir).await.ok();

                    // Flow child job 完成 → 通知 flow engine
                    if let Some(parent_job) = job.parent_job {
                        update_flow_after_job_completion(&db_clone, parent_job, job.id).await.ok();
                    }

                    drop(guard); // RAII: 歸還資源到池中 → notify_waiters
                });

                continue; // 立即嘗試拉下一個 job
            }
            Ok(None) => {
                // 沒有 job → 等 NOTIFY 或 5 秒兜底
                tokio::select! {
                    notification = listener.recv() => {
                        match notification {
                            Ok(_) => {} // 收到 new_job 通知，立即回到 loop 拉 job
                            Err(e) => {
                                tracing::warn!("PG LISTEN error, reconnecting: {:?}", e);
                                if let Ok(new_listener) = sqlx::postgres::PgListener::connect_with(&db).await {
                                    listener = new_listener;
                                    listener.listen("new_job").await.ok();
                                }
                                tokio::time::sleep(std::time::Duration::from_secs(1)).await;
                            }
                        }
                    }
                    _ = tokio::time::sleep(std::time::Duration::from_secs(5)) => {
                        // 兜底輪詢：處理 scheduled_for 延遲排程、NOTIFY 漏掉等邊界
                    }
                }
            }
            Err(e) => {
                tracing::error!("Error pulling job: {:?}", e);
                tokio::time::sleep(std::time::Duration::from_secs(5)).await;
            }
        }
    }
}

async fn handle_job(
    job: &QueuedJob, db: &PgPool, job_dir: &str,
    sandbox: &dyn Sandbox, resources: &SandboxResources,
) -> Result<(serde_json::Value, i64)> {
    match job.kind.as_str() {
        "flow" | "flow_preview" => handle_flow_job(job, db, job_dir, sandbox).await,
        _ => {
            let (content, language, requirements) = get_job_content(job, db).await?;
            match language {
                ScriptLang::Python3 => handle_python_job(job, &content, job_dir, sandbox).await,
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
        .nest("/api/workspaces/:workspace_id", Router::new()
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
            // Jobs — 非同步（立即回傳 job id）
            .route("/jobs/run/p/*path", post(jobs::run_script_by_path))
            .route("/jobs/run/f/*path", post(jobs::run_flow))
            .route("/jobs/run/preview", post(jobs::run_preview))
            // Jobs — 同步（阻塞等結果回傳）
            .route("/jobs/run_wait_result/p/*path", post(jobs::run_wait_result_script))
            .route("/jobs/run_wait_result/f/*path", post(jobs::run_wait_result_flow))
            .route("/jobs/run_wait_result/preview", post(jobs::run_wait_result_preview))
            // Jobs — 查詢
            .route("/jobs/:id", get(jobs::get_job))
            .route("/jobs/:id/result", get(jobs::get_job_result))
            .route("/jobs/:id/logs/stream", get(sse::stream_job_logs))
            .route("/jobs/:id/flow_status", get(jobs::get_flow_status))
            .route("/jobs/list", get(jobs::list_jobs))
            // Run Operations（詳見 11-run-operations.md）
            .route("/runs/:id/cancel", post(runs::cancel_run))
            .route("/runs/:id/force-cancel", post(runs::force_cancel_run))
            .route("/runs/:id/rerun", post(runs::rerun))
            .route("/runs/:id/mark-success", post(runs::mark_success))
            .route("/runs/:id/mark-fail", post(runs::mark_fail))
            // Resources
            .route("/resources/types", get(resources::list_types).post(resources::create_type))
            .route("/resources/list", get(resources::list_resources))
            .route("/resources/get/p/*path", get(resources::get_resource))
            .route("/resources/get_value/p/*path", get(resources::get_resource_value))
            .route("/resources/create", post(resources::create_resource))
            .route("/resources/update/p/*path", put(resources::update_resource))
            .route("/resources/delete/p/*path", delete(resources::delete_resource))
            // Variables
            .route("/variables/list", get(resources::list_variables))
            .route("/variables/get/p/*path", get(resources::get_variable))
            .route("/variables/create", post(resources::create_variable))
            .route("/variables/update/p/*path", put(resources::update_variable))
            .route("/variables/delete/p/*path", delete(resources::delete_variable))
            // Data Preview（Phase 4）
            .route("/data/preview", post(data_preview::preview_query))
            // Concurrency Limits（Phase 3）
            .route("/concurrency_limits", get(concurrency::list_limits))
            .route("/concurrency_limits/:tag", put(concurrency::set_limit).delete(concurrency::delete_limit))
            // Deploy Approval Gate（Phase 3）
            .route("/approval_policies", get(deploy::list_policies).post(deploy::create_policy))
            .route("/approval_policies/p/*pattern", put(deploy::update_policy).delete(deploy::delete_policy))
            .route("/deploy_requests/create", post(deploy::create_deploy_request))
            .route("/deploy_requests/list", get(deploy::list_deploy_requests))
            .route("/deploy_requests/:id", get(deploy::get_deploy_request))
            .route("/deploy_requests/:id/diff", get(deploy::get_deploy_diff))
            .route("/deploy_requests/:id/approve", post(deploy::approve_deploy))
            .route("/deploy_requests/:id/reject", post(deploy::reject_deploy))
            .route("/deploy_requests/:id/deploy", post(deploy::execute_deploy))
            // File Storage（Phase 3）
            .route("/files/upload", post(files::upload_file))
            .route("/files/upload/*path", post(files::upload_file_to_path))
            .route("/files/download/*path", get(files::download_file))
            .route("/files/list", get(files::list_files))
            .route("/files/metadata/*path", get(files::get_file_metadata))
            .route("/files/preview/*path", get(files::preview_file))
            .route("/files/delete/*path", delete(files::delete_file))
            // Cluster Dashboard（Phase 3）
            .route("/workers/list", get(cluster::list_workers))
            .route("/workers/summary", get(cluster::cluster_summary))
            .route("/workers/team_usage", get(cluster::team_resource_usage))
            // Teams（團隊管理）
            .route("/teams/list", get(teams::list_teams))
            .route("/teams/create", post(teams::create_team))
            .route("/teams/get/:name", get(teams::get_team))
            .route("/teams/update/:name", put(teams::update_team))
            .route("/teams/delete/:name", delete(teams::delete_team))
            .route("/teams/:name/members", get(teams::list_members).post(teams::add_member))
            .route("/teams/:name/members/:email", delete(teams::remove_member))
            // Folders（路徑 ACL）
            .route("/folders/list", get(folders::list_folders))
            .route("/folders/create", post(folders::create_folder))
            .route("/folders/get/:name", get(folders::get_folder))
            .route("/folders/update/:name", put(folders::update_folder))
            .route("/folders/delete/:name", delete(folders::delete_folder))
            .route("/folders/:name/acl", put(folders::update_folder_acl))
            // Team Quotas（團隊資源配額，admin only）
            .route("/team_quotas/list", get(teams::list_quotas))
            .route("/team_quotas/:team", get(teams::get_quota).put(teams::set_quota).delete(teams::delete_quota))
            .layer(auth_middleware)
        )
        .with_state(AppState { db, sandbox })
}
```

#### Axum 架構：Extractor / Handler / Middleware

Axum 是 Tokio 團隊開發的 web framework，採用 **Type-Directed Dispatch**（型別導向分派）設計模式：
**型別本身就是指令，告訴框架要做什麼**。所有提取、驗證、組裝都在編譯期完成，零 runtime 成本。

##### 設計模式：Type-Directed Dispatch

傳統框架（Flask/Express）要手動從 request 取資料，忘了取就 runtime 報錯：

```python
# Python Flask — 手動從 request 拿東西
@app.route("/login", methods=["POST"])
def login():
    db = get_db()                    # 手動取 DB
    body = request.get_json()        # 手動解析 body
    workspace = request.args["ws"]   # 手動取 query param
    # 忘了取？runtime 才會炸
```

Axum 只要宣告參數型別，框架自動提取。少寫或寫錯 → 編譯期直接報錯：

```rust
async fn login(
    State(db): State<PgPool>,        // 寫了這個型別 → 框架知道要注入 DB
    Json(body): Json<LoginRequest>,  // 寫了這個型別 → 框架知道要解析 JSON
    Path(ws): Path<String>,          // 寫了這個型別 → 框架知道要從 URL 取
) -> Result<Json<LoginResponse>, ApiError> { ... }
```

##### 三個核心概念

**1. Handler（處理函式）**

任何滿足 `Handler` trait 的 async function。參數是 Extractor，回傳值實作 `IntoResponse`：

```rust
// Handler：就是普通的 async fn
async fn create_run(
    State(state): State<AppState>,          // Extractor 1: 從 app state 提取
    Path(workspace_id): Path<String>,       // Extractor 2: 從 URL path 提取
    Extension(user): Extension<AuthedUser>, // Extractor 3: 從 middleware 注入
    Json(req): Json<CreateRunRequest>,      // Extractor 4: 從 request body 反序列化
) -> Result<Json<RunResponse>, ApiError> {  // 回傳值自動轉成 HTTP response
    // state.db 就是 PgPool
    // workspace_id 就是 String
    // user 就是 AuthedUser
    // req 就是 CreateRunRequest
}
```

回傳值支援多種 tuple 組合，自動轉成 HTTP response：

```rust
StatusCode                                       // 只有狀態碼（空 body）
String                                           // text/plain body
Json<T>                                          // JSON body（預設 200）
(StatusCode, Json<T>)                            // 狀態碼 + JSON body
(StatusCode, [(HeaderName, &str)], Json<T>)      // 狀態碼 + headers + body
```

**2. Extractor（提取器）**

每個 Extractor 實作 `FromRequestParts` trait，告訴 Axum「怎麼從 request 中把自己提取出來」：

```rust
// Axum 內部的 trait 定義
trait FromRequestParts<S> {
    type Rejection: IntoResponse;  // 提取失敗時的錯誤型別
    async fn from_request_parts(parts: &mut Parts, state: &S) -> Result<Self, Self::Rejection>;
}

// 每個 Extractor 各自實作
impl FromRequestParts for State<S>       { /* 從 app state 拿 */ }
impl FromRequestParts for Path<String>   { /* 從 URL 解析 {param} */ }
impl FromRequestParts for Extension<T>   { /* 從 middleware 注入的資料拿 */ }
impl FromRequest      for Json<T>        { /* 從 body 反序列化 */ }
```

Axum 用 macro 對 0~16 個參數自動生成 Handler 實作（編譯期展開）：

```rust
// 編譯器看到 handler 的參數型別，自動生成提取程式碼（概念上）：
async fn call(request: Request, state: S) -> Response {
    let (mut parts, body) = request.into_parts();
    // 按參數順序，依次呼叫每個型別的 from_request_parts
    let arg1 = State::from_request_parts(&mut parts, &state).await?;
    let arg2 = Path::from_request_parts(&mut parts, &state).await?;
    let arg3 = Extension::from_request_parts(&mut parts, &state).await?;
    let arg4 = Json::from_request(Request::from_parts(parts, body), &state).await?;
    handler(arg1, arg2, arg3, arg4).await.into_response()
}
```

**為什麼參數要「包起來」？** `State(db): State<PgPool>` 是 Rust 的解構模式。`State` 是 tuple struct：

```rust
pub struct State<S>(pub S);

// 這兩種寫法等價：
async fn handler(State(db): State<PgPool>) { /* 直接用 db */ }
async fn handler(state: State<PgPool>) { let db = state.0; /* 多一行 */ }
```

**可自訂 Extractor**：實作 `FromRequestParts` 就能建立新的 extractor：

```rust
// 自訂 Extractor：從 header 取 request ID
struct RequestId(String);

impl<S: Send + Sync> FromRequestParts<S> for RequestId {
    type Rejection = ApiError;
    async fn from_request_parts(parts: &mut Parts, _: &S) -> Result<Self, Self::Rejection> {
        let id = parts.headers.get("X-Request-Id")
            .and_then(|v| v.to_str().ok())
            .unwrap_or("unknown").to_string();
        Ok(RequestId(id))
    }
}

// 之後任何 handler 直接加這個型別就能用，不需要改框架或註冊
async fn some_handler(RequestId(id): RequestId) {
    tracing::info!(request_id = %id, "handling request");
}
```

**3. Middleware（中介層）**

Middleware 也是 async function，透過 `Extension` 在 request 中注入資料給後續 handler。
固定參數是 `req: Request` 和 `next: Next`，可選加 `State`、`Path` 等 extractor：

```rust
// Middleware：JWT 驗證 → 查 DB → 注入 AuthedUser
async fn require_auth(
    State(db): State<PgPool>,              // 可選：需要 DB 就加 State
    Path(workspace_id): Path<String>,      // 可選：需要 URL 參數就加 Path
    mut req: Request,                      // 固定：原始 HTTP request
    next: Next,                            // 固定：呼叫下一層
) -> Result<Response, ApiError> {
    let user = AuthedUser { /* ... 從 JWT + DB 建構 */ };
    req.extensions_mut().insert(user);  // 注入到 request 的 extensions
    Ok(next.run(req).await)             // 繼續往下走
}

// Handler：透過 Extension 提取 middleware 注入的資料
async fn handler(Extension(user): Extension<AuthedUser>) { /* 直接用 user */ }
```

##### Router 組裝

```rust
Router::new()
    // 公開路由（不經過 auth middleware）
    .route("/api/auth/login", post(auth::login))
    .route("/api/auth/signup", post(auth::signup))
    // Workspace 路由（經過 auth middleware）
    .nest("/api/workspaces/{workspace_id}", Router::new()
        .route("/scripts/create", post(scripts::create_script))
        .route("/runs/list", get(runs::list_runs))
        .layer(auth_middleware)  // 只套用在這個子 Router
    )
    .with_state(db)  // 注入 PgPool，讓所有 handler 透過 State 提取
```

- `.nest(prefix, sub_router)` — 子 Router 的所有 route 自動加上 prefix，middleware 也只套用在子 Router 內
- `.with_state(db)` — 注入共用資源，handler 透過 `State(db): State<PgPool>` 提取
- `.layer(middleware)` — 掛載 middleware，只影響同一層的 route

##### State 擴充：FromRef

目前 CoveFlow 的 state 是 `PgPool`。未來需要共用更多資源時，可以包成 struct：

```rust
#[derive(Clone)]
struct AppState { db: PgPool, sandbox: Arc<SandboxRouter> }

// 實作 FromRef：告訴 Axum 怎麼從 AppState 取出 PgPool
impl FromRef<AppState> for PgPool {
    fn from_ref(state: &AppState) -> PgPool { state.db.clone() }
}

// 改 .with_state(AppState { db, sandbox })
// 所有現有的 State(db): State<PgPool> 都不用改——Axum 自動透過 FromRef 提取
```

##### 與其他框架的比較

| 框架 | 機制 | 檢查時機 | 成本 |
|------|------|---------|------|
| **Spring (Java)** | `@Autowired` 依賴注入 | Runtime 反射 | 啟動慢 |
| **Angular (TS)** | Constructor DI | Runtime DI container | Runtime |
| **FastAPI (Python)** | Type hints + `Depends()` | Runtime 解析 | Runtime |
| **Axum (Rust)** | Trait + 泛型 | **編譯期**完成 | **零成本** |

FastAPI 的設計最接近 Axum（都是看參數型別決定行為），但 FastAPI 是 runtime 解析，Axum 是編譯期完成。

##### 為什麼 auth middleware 查 DB 是標準做法？

幾乎所有框架都在 middleware 查 DB：Windmill（PG）、Airflow（Flask+SQLAlchemy）、Django（ORM）、Rails（Devise）。
原因：(1) PgPool 連線池取連線 O(1)，(2) PRIMARY KEY 查詢 < 0.1ms，(3) 權限變更即時生效不需等 JWT 過期。
替代方案（JWT 塞角色 / Redis cache）增加複雜度但對 < 1000 QPS 的內部工具沒有實際收益。

#### JWT 機制與前後端互動

**JWT 結構**

JWT（JSON Web Token）是一個三段式的 Base64 字串，用 `.` 分隔：

```
eyJhbGciOiJIUzI1NiJ9.eyJlbWFpbCI6ImFsaWNlQGV4YW1wbGUuY29tIiwiZXhwIjoxNzE1MjAwMDAwfQ.SIGNATURE
├── Header ──────────┤├── Payload（Claims）───────────────────────────────────────────────┤├── Signature ┤
```

- **Header**（明文 Base64，任何人可讀）：`{"alg": "HS256"}` — 簽名演算法
- **Payload**（明文 Base64，任何人可讀）：`{"email": "alice@example.com", "exp": 1715200000}` — 就是 Claims struct
- **Signature**：`HMAC-SHA256(Header + "." + Payload, JWT_SECRET)` — 驗證用

重點：Payload **不加密**，任何人都能 Base64 decode 看到內容。JWT 只保證**不可篡改**——改了任何一個字元，Signature 就對不上。

JWT 規範（RFC 7519）定義的標準欄位：

| 欄位 | 全名 | 用途 | CoveFlow |
|------|------|------|----------|
| `sub` | Subject | 代表誰 | 用 `email` 欄位取代 |
| `exp` | Expiration | 過期時間 | ✅ 24 小時 |
| `iss` | Issuer | 誰簽發的 | 暫不使用（單系統不需要） |
| `aud` | Audience | 給誰用的 | 暫不使用（單系統不需要） |

**JWT_SECRET 的角色**

```
簽發（login）：Claims + JWT_SECRET → Signature → token 發給 client
驗證（middleware）：token + JWT_SECRET → 重算 Signature → 比對
  一致 → 沒被篡改，信任 Claims
  不一致 → 被竄改或密鑰錯誤 → 401
```

JWT_SECRET 洩漏 = 任何人都能偽造合法 token。正式環境必須設定強密鑰。

**Validation::default()** 自動驗證 `exp`（過期）和 `alg`（演算法），對 CoveFlow 目前夠用。

**前後端互動流程**

```
┌──────────┐                              ┌──────────┐                    ┌────┐
│  Browser │                              │   API    │                    │ DB │
└────┬─────┘                              └────┬─────┘                    └─┬──┘
     │                                         │                            │
     │  1. POST /api/auth/login                │                            │
     │     { email, password }                 │                            │
     │────────────────────────────────────────>│                            │
     │                                         │  2. SELECT password_hash   │
     │                                         │───────────────────────────>│
     │                                         │  3. Argon2 驗證密碼         │
     │                                         │     通過 → 簽發 JWT token  │
     │  4. 200 { token: "eyJhbG..." }          │                            │
     │<────────────────────────────────────────│                            │
     │                                         │                            │
     │  ── 前端存 token（見下方儲存方式）──       │                            │
     │                                         │                            │
     │  5. GET /api/workspaces/acme/runs/list  │                            │
     │     Authorization: Bearer eyJhbG...     │                            │
     │────────────────────────────────────────>│                            │
     │                                         │  6. require_auth middleware │
     │                                         │     a. decode JWT → email   │
     │                                         │     b. 查 workspace_member  │
     │                                         │     c. 查 team_member       │
     │                                         │     d. 查 folder_acl        │
     │                                         │     e. 注入 AuthedUser      │
     │                                         │  7. Handler 執行            │
     │  8. 200 { runs: [...] }                 │                            │
     │<────────────────────────────────────────│                            │
     │                                         │                            │
     │  ── 24 小時後 token 過期 ──              │                            │
     │                                         │                            │
     │  9. 請求帶過期 token                     │                            │
     │────────────────────────────────────────>│                            │
     │                                         │  10. decode 失敗（exp 過期）│
     │  11. 401 → 前端跳轉登入頁               │                            │
     │<────────────────────────────────────────│                            │
```

**Token 架構：Access Token + Refresh Token**

CoveFlow 使用 **短效 Access Token（15 min JWT）+ 長效 Refresh Token（7 天 UUID, HttpOnly Cookie）** 雙 token 架構，支援 token rotation 和 DB-backed revocation。

詳細設計（schema、token flow、cookie 設定、rotation 策略、安全分析）見 **[PRD/12-refresh-token.md](./12-refresh-token.md)**。

#### Auth（JWT + Argon2 + RBAC）

```rust
// crates/api/src/auth.rs

use argon2::{Argon2, PasswordHash, PasswordVerifier, PasswordHasher};
use jsonwebtoken::{encode, decode, Header, Validation, EncodingKey, DecodingKey};

#[derive(serde::Serialize, serde::Deserialize)]
struct Claims { email: String, exp: u64 }

/// Folder access level: Owner > Writer > Reader
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub enum FolderRole {
    Reader,
    Writer,
    Owner,
}

/// Authenticated user context, injected by require_auth middleware into every request.
/// The middleware queries DB for workspace membership, teams, and folder ACL.
#[derive(Clone, Debug)]
pub struct AuthedUser {
    pub email: String,
    pub workspace_id: String,
    pub is_admin: bool,
    /// Teams the user belongs to, e.g. ["ml-team", "data-eng"]
    pub teams: Vec<String>,
    /// Permission subjects for ACL matching
    /// e.g. ["users/alice", "teams/ml-team", "teams/data-eng"]
    pub perm_subjects: Vec<String>,
    /// Accessible folders -> highest role among all matching subjects
    pub folders: HashMap<String, FolderRole>,
}

impl AuthedUser {
    /// Check write permission for a path
    pub fn can_write(&self, path: &str) -> bool {
        if self.is_admin { return true; }
        // Personal path: users/alice/... -> only alice can write
        if let Some(owner) = path.strip_prefix("users/") {
            let owner_email_part = owner.split('/').next().unwrap_or("");
            return self.email.starts_with(owner_email_part);
        }
        // Folder path: folders/ml-team/... -> check folder_acl role
        if let Some(folder_name) = path.strip_prefix("folders/") {
            let folder_name = folder_name.split('/').next().unwrap_or("");
            return matches!(
                self.folders.get(folder_name),
                Some(FolderRole::Writer | FolderRole::Owner)
            );
        }
        false
    }

    /// Check read permission for a path
    pub fn can_read(&self, path: &str) -> bool {
        if self.is_admin { return true; }
        if let Some(owner) = path.strip_prefix("users/") {
            let owner_email_part = owner.split('/').next().unwrap_or("");
            return self.email.starts_with(owner_email_part);
        }
        if let Some(folder_name) = path.strip_prefix("folders/") {
            let folder_name = folder_name.split('/').next().unwrap_or("");
            return self.folders.contains_key(folder_name); // any role grants read
        }
        false
    }

    /// Check if user is folder owner
    pub fn is_folder_owner(&self, folder_name: &str) -> bool {
        if self.is_admin { return true; }
        matches!(self.folders.get(folder_name), Some(FolderRole::Owner))
    }

    /// Require owner permission, return 403 if denied
    pub fn require_owner(&self, path: &str) -> Result<(), ApiError> {
        if self.is_admin { return Ok(()); }
        if let Some(folder_name) = path.strip_prefix("folders/") {
            let folder_name = folder_name.split('/').next().unwrap_or("");
            if self.is_folder_owner(folder_name) { return Ok(()); }
        }
        Err(ApiError::Forbidden(format!("no owner access to '{}'", path)))
    }

    /// Require write permission, return 403 if denied
    pub fn require_writer(&self, path: &str) -> Result<(), ApiError> {
        if self.can_write(path) { Ok(()) }
        else { Err(ApiError::Forbidden(format!("no write access to '{}'", path))) }
    }

    /// Require read permission, return 403 if denied
    pub fn require_reader(&self, path: &str) -> Result<(), ApiError> {
        if self.can_read(path) { Ok(()) }
        else { Err(ApiError::Forbidden(format!("no read access to '{}'", path))) }
    }
}

/// Auth middleware: decode JWT -> query DB for teams + folder ACL -> inject Extension
pub async fn require_auth(
    State(db): State<PgPool>,
    Path(workspace_id): Path<String>,
    mut req: Request,
    next: Next,
) -> Result<Response, ApiError> {
    let token = req.headers().get("Authorization")
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.strip_prefix("Bearer "))
        .ok_or(ApiError::Unauthorized)?;

    let claims = decode::<Claims>(token, &DecodingKey::from_secret(JWT_SECRET.as_bytes()), &Validation::default())
        .map_err(|_| ApiError::Unauthorized)?
        .claims;

    // 1. Verify workspace membership and role
    let member = sqlx::query!(
        "SELECT role FROM workspace_member WHERE workspace_id = $1 AND email = $2",
        workspace_id, claims.email
    ).fetch_optional(&db).await?
     .ok_or(ApiError::Forbidden("not a member of this workspace".into()))?;

    let is_admin = member.role == "admin";

    // 2. Load user's teams
    let teams: Vec<String> = sqlx::query_scalar!(
        "SELECT team_name FROM team_member WHERE workspace_id = $1 AND email = $2",
        workspace_id, claims.email
    ).fetch_all(&db).await?;

    // 3. Build permission subjects list
    let mut perm_subjects = vec![format!("users/{}", claims.email)];
    for t in &teams {
        perm_subjects.push(format!("teams/{}", t));
    }

    // 4. Load folder ACL (simple SELECT from normalized folder_acl table)
    let acl_rows = sqlx::query!(
        "SELECT folder_name, role FROM folder_acl
         WHERE workspace_id = $1 AND subject = ANY($2)",
        workspace_id, &perm_subjects
    ).fetch_all(&db).await?;

    let mut folders: HashMap<String, FolderRole> = HashMap::new();
    for row in acl_rows {
        let role = match row.role.as_str() {
            "owner" => FolderRole::Owner,
            "writer" => FolderRole::Writer,
            _ => FolderRole::Reader,
        };
        // If multiple subjects grant access, keep the highest role
        let current = folders.entry(row.folder_name).or_insert(FolderRole::Reader);
        if role > *current { *current = role; }
    }

    let user = AuthedUser {
        email: claims.email,
        workspace_id,
        is_admin,
        teams,
        perm_subjects,
        folders,
    };

    req.extensions_mut().insert(user);
    Ok(next.run(req).await)
}

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
    // 權限檢查：必須有該路徑的寫入權限
    user.require_writer(&req.path)?;

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
        "INSERT INTO script (workspace_id, hash, path, content, language, schema, parent_hashes, requirements, created_by)
         VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)",
        workspace_id, hash, req.path, req.content,
        req.language.as_str(), schema, &parent_hashes, &req.requirements, user.email
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
    // 權限檢查：必須有該路徑的讀取權限才能執行
    user.require_reader(&path)?;

    let script = sqlx::query_as!(Script,
        "SELECT * FROM script WHERE workspace_id = $1 AND path = $2
         ORDER BY created_at DESC LIMIT 1",
        workspace_id, path
    ).fetch_optional(&state.db).await?.ok_or(ApiError::NotFound)?;

    // 從 path 推導 team_owner（folders/ml-team/xxx → "ml-team"，users/alice/xxx → NULL）
    let team_owner = extract_team_owner(&path);

    let run_id = queue::submit_run(&state.db, NewRun {
        workspace_id: &workspace_id,
        kind: "script",
        script_hash: Some(&script.hash),
        script_path: Some(&script.path),
        language: Some(script.language),
        args: Some(args),
        tag: "default",
        team_owner: team_owner.as_deref(),
        requirements: script.requirements.clone(),
        created_by: &user.email,
        trace_id: current_trace_id(),
        span_id: current_span_id(),
        ..Default::default()
    }).await?;

    Ok(Json(RunCreated { id: run_id }))
}

/// 從路徑推導 team owner
/// "folders/ml-team/my_script" → Some("ml-team")
/// "users/alice/my_script" → None
fn extract_team_owner(path: &str) -> Option<String> {
    path.strip_prefix("folders/")
        .and_then(|rest| rest.split('/').next())
        .map(|s| s.to_string())
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
        requirements: req.requirements.unwrap_or_default(),
        created_by: &user.email,
        trace_id: current_trace_id(),
        span_id: current_span_id(),
        ..Default::default()
    }).await?;

    Ok(Json(JobCreated { id: job_id }))
}
```

#### 同步執行（`run_wait_result`）

外部系統把 CoveFlow 當 API 用的核心端點——HTTP 阻塞直到 job 完成，直接回傳結果。

```rust
// crates/api/src/jobs.rs

/// POST /api/workspaces/{ws}/jobs/run_wait_result/p/{path}
/// 推入 job → 阻塞等完成 → 回傳結果（或超時）
pub async fn run_wait_result_script(
    State(state): State<AppState>,
    Path((workspace_id, path)): Path<(String, String)>,
    Extension(user): Extension<AuthedUser>,
    Query(params): Query<WaitResultParams>,
    Json(args): Json<serde_json::Value>,
) -> Result<Response, ApiError> {
    // 佇列反壓：防止大量同步呼叫擠爆 queue
    check_queue_too_long(&state.db, params.queue_limit).await?;

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
        ..Default::default()
    }).await?;

    run_wait_result_internal(&state.db, &workspace_id, job_id, params.timeout, None).await
}

#[derive(Deserialize)]
pub struct WaitResultParams {
    pub timeout: Option<u64>,        // 秒，預設 600
    pub queue_limit: Option<i64>,    // 佇列上限，超過直接 503
}

/// 核心：NOTIFY 事件驅動 + 兜底輪詢，等待 job 完成
///
/// 與 Worker 的雙軌制對稱：
///   Worker pull：  LISTEN new_job + 5s 兜底
///   Wait result：  LISTEN job_completed + 200ms 兜底
async fn run_wait_result_internal(
    db: &PgPool, workspace_id: &str, job_id: Uuid,
    timeout_override: Option<u64>,
    early_return_node: Option<&str>,  // Flow 的 early return 節點 ID
) -> Result<Response, ApiError> {
    // RAII Guard：HTTP 連線斷開時自動取消 job
    let mut guard = WaitResultGuard { done: false, id: job_id, db: db.clone(), w_id: workspace_id.to_string() };

    let timeout_secs = timeout_override.unwrap_or(600);
    let deadline = tokio::time::Instant::now() + Duration::from_secs(timeout_secs);

    // 主要：LISTEN job_completed（complete_job 會 NOTIFY）
    let mut listener = sqlx::postgres::PgListener::connect_with(db).await?;
    listener.listen("job_completed").await?;

    // 兜底輪詢間隔
    const POLL_INTERVAL_MS: u64 = 200;

    loop {
        // 查詢完成結果
        let row = sqlx::query!(
            "SELECT result, success FROM job_completed
             WHERE id = $1 AND workspace_id = $2",
            job_id, workspace_id
        ).fetch_optional(db).await?;

        if let Some(r) = row {
            guard.done = true;
            return result_to_response(r.success, r.result);
        }

        // 如果是 Flow + early_return，檢查指定節點是否已完成
        if let Some(node_id) = early_return_node {
            if let Some(result) = check_early_return(db, workspace_id, job_id, node_id).await? {
                guard.done = true;  // 不取消 job，讓剩餘步驟背景跑完
                return result_to_response(true, Some(result));
            }
        }

        // 超時檢查
        if tokio::time::Instant::now() >= deadline {
            guard.done = true;
            return Err(ApiError::Timeout(format!("timeout after {}s", timeout_secs)));
        }

        // 等待：NOTIFY 喚醒（~1ms）或兜底輪詢（200ms）
        tokio::select! {
            notification = listener.recv() => {
                if let Ok(n) = notification {
                    // 只處理自己的 job_id 的通知
                    if n.payload() == job_id.to_string() {
                        continue; // 立即查詢結果
                    }
                }
                // 其他 job 的通知，忽略，繼續等
            }
            _ = tokio::time::sleep(Duration::from_millis(POLL_INTERVAL_MS)) => {
                // 兜底：處理 NOTIFY 漏掉、listener 斷線等邊界
            }
        }
    }
}

/// 檢查 Flow 的特定節點是否已完成（用於 Early Return）
async fn check_early_return(
    db: &PgPool, workspace_id: &str, flow_job_id: Uuid, node_id: &str,
) -> Result<Option<serde_json::Value>, ApiError> {
    let status = sqlx::query_scalar!(
        "SELECT flow_status FROM job_flow_status WHERE id = $1", flow_job_id
    ).fetch_optional(db).await?;

    if let Some(status_json) = status {
        let status: FlowStatus = serde_json::from_value(status_json)?;
        for module in &status.modules {
            if let FlowStatusModule::Success { id, result, .. } = module {
                if id == node_id {
                    return Ok(Some(result.clone()));
                }
            }
        }
    }
    Ok(None)
}

/// RAII Guard：HTTP 連線斷開時自動取消尚未完成的 job
struct WaitResultGuard {
    done: bool,
    id: Uuid,
    db: PgPool,
    w_id: String,
}

impl Drop for WaitResultGuard {
    fn drop(&mut self) {
        if !self.done {
            let id = self.id;
            let db = self.db.clone();
            let w_id = self.w_id.clone();
            tokio::spawn(async move {
                let _ = sqlx::query!(
                    "UPDATE job_queue SET canceled_by = 'http_disconnect', canceled_reason = 'client disconnected'
                     WHERE id = $1 AND workspace_id = $2",
                    id, w_id
                ).execute(&db).await;
            });
        }
    }
}

/// 佇列反壓：同步呼叫過多時拒絕新請求
async fn check_queue_too_long(db: &PgPool, limit: Option<i64>) -> Result<(), ApiError> {
    if let Some(limit) = limit {
        let count: i64 = sqlx::query_scalar!(
            "SELECT COUNT(*) FROM job_queue WHERE canceled_by IS NULL AND scheduled_for <= NOW()"
        ).fetch_one(db).await?.unwrap_or(0);

        if count > limit {
            return Err(ApiError::ServiceUnavailable(
                format!("queue too long: {} > {} (try again later)", count, limit)
            ));
        }
    }
    Ok(())
}

/// 將 job 結果轉成 HTTP Response（支援自訂 status code / content-type / headers）
fn result_to_response(success: bool, result: Option<serde_json::Value>) -> Result<Response, ApiError> {
    let result = result.unwrap_or(serde_json::Value::Null);

    // 支援 WindmillCompositeResult 格式：script 可以控制 HTTP 回應
    // { "windmill_status_code": 201, "windmill_content_type": "text/plain",
    //   "windmill_headers": {"X-Custom": "val"}, "result": { ... } }
    if let Some(obj) = result.as_object() {
        if obj.contains_key("windmill_status_code") || obj.contains_key("windmill_content_type") {
            let status = obj.get("windmill_status_code")
                .and_then(|v| v.as_u64())
                .unwrap_or(if success { 200 } else { 500 }) as u16;
            let content_type = obj.get("windmill_content_type")
                .and_then(|v| v.as_str())
                .unwrap_or("application/json");
            let body = obj.get("result").unwrap_or(&result);

            let mut response = Response::builder()
                .status(status)
                .header("content-type", content_type);

            if let Some(headers) = obj.get("windmill_headers").and_then(|v| v.as_object()) {
                for (k, v) in headers {
                    if let Some(v_str) = v.as_str() {
                        response = response.header(k.as_str(), v_str);
                    }
                }
            }

            return Ok(response.body(serde_json::to_string(body)?.into())?);
        }
    }

    let status = if success { 200 } else { 500 };
    Ok(Response::builder()
        .status(status)
        .header("content-type", "application/json")
        .body(serde_json::to_string(&result)?.into())?)
}
```

#### SSE 日誌串流（chunk-based，詳見 10-log-and-ha.md §1.4.6）

```rust
// crates/api/src/sse.rs

use axum::response::sse::{Event, Sse};
use futures::stream::Stream;

/// SSE 日誌串流 — 基於 chunk 的增量推送
/// 前端持有 last_chunk_id，每次只拉新 chunk
pub async fn stream_job_logs(
    State(state): State<AppState>,
    Path((workspace_id, job_id)): Path<(String, Uuid)>,
    Query(params): Query<LogStreamParams>,
) -> Sse<impl Stream<Item = Result<Event, axum::Error>>> {
    let db = state.db.clone();
    let min_level = params.level.unwrap_or(0); // 0 = TRACE, show all

    let stream = async_stream::stream! {
        let mut last_chunk_id: i64 = params.after_chunk.unwrap_or(0);

        loop {
            // 拉取新 chunks（利用 max_level 做 DB 層跳過）
            let chunks = sqlx::query_as!(
                LogChunkRow,
                r#"SELECT id, seq, created_at, min_level, max_level, line_count, entries
                   FROM run_log
                   WHERE run_id = $1 AND id > $2 AND max_level >= $3
                   ORDER BY seq ASC
                   LIMIT 50"#,
                job_id, last_chunk_id, min_level as i16,
            ).fetch_all(&db).await;

            if let Ok(rows) = chunks {
                for row in &rows {
                    let payload = serde_json::json!({
                        "chunk_id": row.id,
                        "seq": row.seq,
                        "entries": row.entries,
                    });
                    yield Ok(Event::default().event("log").data(payload.to_string()));
                    last_chunk_id = row.id;
                }
            }

            // 檢查 job 是否完成
            let completed = sqlx::query_scalar!(
                "SELECT EXISTS(SELECT 1 FROM job_completed WHERE id = $1)", job_id
            ).fetch_one(&db).await;

            if let Ok(Some(true)) = completed {
                // 最後再拉一次，確保不漏 chunk
                let final_chunks = sqlx::query_as!(
                    LogChunkRow,
                    r#"SELECT id, seq, created_at, min_level, max_level, line_count, entries
                       FROM run_log
                       WHERE run_id = $1 AND id > $2 AND max_level >= $3
                       ORDER BY seq ASC"#,
                    job_id, last_chunk_id, min_level as i16,
                ).fetch_all(&db).await;

                if let Ok(rows) = final_chunks {
                    for row in &rows {
                        let payload = serde_json::json!({
                            "chunk_id": row.id, "seq": row.seq, "entries": row.entries,
                        });
                        yield Ok(Event::default().event("log").data(payload.to_string()));
                    }
                }

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

            tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;
        }
    };

    Sse::new(stream)
}

#[derive(serde::Deserialize)]
pub struct LogStreamParams {
    pub after_chunk: Option<i64>,
    pub level: Option<i16>,
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
    requirements = $bindable([]),
    onRun,
  }: {
    content: string
    language?: string
    requirements?: string[]
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
  {#if language === 'python3' || language === 'typescript'}
    <div class="requirements-input">
      <label>Dependencies (one per line)</label>
      <textarea
        placeholder="pandas>=2.0&#10;requests&#10;numpy"
        oninput={(e) => {
          requirements = e.target.value.split('\n').map(s => s.trim()).filter(Boolean)
        }}
        value={requirements.join('\n')}
      ></textarea>
    </div>
  {/if}
  <div bind:this={editorContainer} class="editor-container"></div>
</div>

<style>
  .editor-wrapper { display: flex; flex-direction: column; height: 100%; }
  .toolbar { display: flex; gap: 8px; padding: 8px; background: #1e1e1e; border-bottom: 1px solid #333; }
  .editor-container { flex: 1; }
</style>
```

#### LogViewer.svelte（chunk-based，詳見 10-log-and-ha.md §1.4.6）

```svelte
<!-- src/lib/components/LogViewer.svelte -->
<script lang="ts">
  import { onMount } from 'svelte'

  interface LogEntry { ts: string; level: number; msg: string; node_id?: string }
  interface LogChunk { chunk_id: number; seq: number; entries: LogEntry[] }

  const LEVEL_LABELS: Record<number, string> = { 1: 'TRACE', 2: 'DEBUG', 3: 'INFO', 4: 'WARN', 5: 'ERROR' }
  const LEVEL_COLORS: Record<number, string> = { 4: '#cca700', 5: '#f44747' }

  let {
    jobId, workspaceId, minLevel = 3, onResult,
  }: {
    jobId: string
    workspaceId: string
    minLevel?: number
    onResult?: (result: { success: boolean; result: any }) => void
  } = $props()

  let lines = $state<LogEntry[]>([])
  let status = $state<'running' | 'success' | 'failure'>('running')
  let logContainer: HTMLPreElement

  $effect(() => {
    if (!jobId) return
    const url = `/api/workspaces/${workspaceId}/jobs/${jobId}/logs/stream?level=${minLevel}`
    const eventSource = new EventSource(url)

    eventSource.addEventListener('log', (e) => {
      const chunk: LogChunk = JSON.parse(e.data)
      // client-side filtering: only show entries >= minLevel
      const filtered = chunk.entries.filter(entry => entry.level >= minLevel)
      lines = [...lines, ...filtered]
      requestAnimationFrame(() => {
        if (logContainer) logContainer.scrollTop = logContainer.scrollHeight
      })
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
  <div class="toolbar">
    <div class="status-bar" class:success={status === 'success'} class:failure={status === 'failure'}>
      {#if status === 'running'}Running...{:else if status === 'success'}Completed{:else}Failed{/if}
    </div>
    <span class="line-count">{lines.length} lines</span>
  </div>
  <pre bind:this={logContainer} class="logs">{#each lines as line}<span
    style:color={LEVEL_COLORS[line.level] ?? '#d4d4d4'}
  >{line.ts} [{LEVEL_LABELS[line.level] ?? '?'}] {line.msg}
</span>{/each}</pre>
</div>

<style>
  .log-viewer { display: flex; flex-direction: column; height: 100%; background: #1e1e1e; color: #d4d4d4; font-family: 'Fira Code', monospace; }
  .toolbar { display: flex; justify-content: space-between; padding: 4px 12px; background: #333; font-size: 12px; }
  .status-bar.success { color: #4ec9b0; }
  .status-bar.failure { color: #f44747; }
  .line-count { color: #888; }
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
            .unwrap_or_else(|_| "postgres://postgres:changeme@localhost:5432/coveflow".into())
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

    // 5. Worker（Resource-based：1 個 Worker process，多 job 按資源並行）
    let worker_config = config.clone();
    let db_worker = db.clone();
    let sandbox_worker = sandbox.clone();
    let worker_handle = tokio::spawn(async move {
        let tracer = global::tracer("coveflow-worker");
        worker::run_worker(
            db_worker,
            config.worker_name.clone(),
            config.tags.clone(),
            worker_config,
            sandbox_worker,
            tracer,
        ).await;
    });

    // 6. 等待結束
    tokio::select! {
        _ = server => {},
        _ = worker_handle => {},
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
      POSTGRES_DB: coveflow
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
6. 同步執行：POST run_wait_result/p/{path} → 阻塞 → 拿到結果 JSON（不用再查 job id）
7. 同步超時：設 timeout=1 + 放一個 sleep(5) script → 確認收到 timeout 錯誤
8. 斷線取消：curl 發 run_wait_result 後 Ctrl+C → 確認 job 被 canceled_by='http_disconnect'
9. 佇列反壓：設 queue_limit=2 → 同時發 5 個 sync 請求 → 後 3 個收到 503

# Run Operations（詳見 11-run-operations.md §9 驗證方式）
10. Cancel：sleep(60) script → Run → Cancel → 確認 worker 收到 SIGINT → run 標記 canceled
11. Cancel（queued）：建立 scheduled_for=未來 的 run → Cancel → 確認直接完成
12. Force Cancel：停掉 worker → running run 卡住 → Force Cancel → 確認標記失敗
13. Rerun：已完成 run → Rerun → 確認新 run 使用相同參數，rerun_of 正確
14. Mark Success/Fail：running run → Mark Success → 確認 run_completed.marked_by 有值
15. 邊界：已完成 run → Cancel → AlreadyCompleted；不存在 run → NotFound
```

### 1.11 Resource-based 部署指南

**Worker 啟動時資源驗證（自動偵測 + validate + clamp）**

三種模式：

| 部署方式 | CPU/RAM/Disk 設定 | 行為 |
|---------|-----------------|------|
| 全部省略（推薦） | `total_cpus = auto` | 自動偵測 - reserved |
| 手動指定 | `total_cpus = 6` | 使用指定值，超過則 warn + clamp |
| Container/K8s | 省略 | 自動讀 cgroup quota（最準確） |

```
Worker 啟動時自動偵測可用資源：

  detected_cpu = 8 vCPU          # sysinfo / cgroup v2 (cpu.max)
  detected_mem = 32768 MB        # sysinfo / cgroup v2 (memory.max)
  detected_disk = 102400 MB      # statvfs(job_dir)

  reserved_cpu = 1               # config [worker.reserved] cpu
  reserved_mem = 1024 MB         # config [worker.reserved] memory_mb
  reserved_disk = 0              # config [worker.reserved] disk_mb

  available_cpu = 8 - 1 = 7.0 CPUs
  available_mem = 32768 - 1024 = 31744 MB
  available_disk = 102400 - 0 = 102400 MB

  → ResourceManager::new(7.0, 31744, 102400)
  → 同時可跑多個 job，只要 CPU+RAM+Disk 三維都不超額
```

**各部署模式的建議值**：

```
┌─ 部署方式 ──────────────────── reserved.cpu ── reserved.memory_mb ────┐
│                                                                        │
│ 1. 單機 all-in-one                                                     │
│    coveflow --mode all                     1             1024          │
│                                                                        │
│ 2. 單機分 process                                                      │
│    coveflow --mode server  (PID 1001)                                  │
│    coveflow --mode worker  (PID 1002)      1             1024          │
│                                                                        │
│ 3. Worker 獨立機器 / Container / Pod                                    │
│    機器上只有 Worker，沒有 API Server       0             256           │
│                                                                        │
│ 4. 多 Worker 同機                                                      │
│    每個 Worker 手動設定 total_cpus / total_memory_mb                    │
│    確保 N 個 Worker 的總和 <= 機器資源      0 (手動算)     256 (每個)    │
└────────────────────────────────────────────────────────────────────────┘
```

**Resource Dashboard（前端 ClusterDashboard 整合）**：

```
┌── Worker Dashboard ──────────────────────────────────────────────────────┐
│                                                                          │
│  Worker 1 (default, gpu)                                                 │
│    CPU  [████░░░░░░] 4.0 / 8.0 cores                                    │
│    RAM  [██████░░░░] 12.0 / 32.0 GB                                     │
│    Disk [██░░░░░░░░]  8.0 / 100.0 GB                                    │
│    ├─ [Dedicated] predict_api   2.0 cpu  4.0 GB  2.0 GB  python3  4h12m │
│    ├─ [Normal]    job-abc       1.0 cpu  4.0 GB  2.0 GB  python3  12s   │
│    ├─ [Normal]    job-def       1.0 cpu  4.0 GB  4.0 GB  python3  45s   │
│    └─ (free: 4.0 cpu, 20.0 GB RAM, 92.0 GB disk)                        │
│                                                                          │
│  Worker 2 (default)                                                      │
│    CPU  [██░░░░░░░░] 1.0 / 4.0 cores                                    │
│    RAM  [████░░░░░░] 2.0 / 16.0 GB                                      │
│    Disk [█░░░░░░░░░]  1.0 / 50.0 GB                                     │
│    ├─ [Normal]    job-ghi       1.0 cpu  2.0 GB  1.0 GB  typescript  2s │
│    └─ (free: 3.0 cpu, 14.0 GB RAM, 49.0 GB disk)                        │
│                                                                          │
│  Cluster: 12.0 total cpu | 5.0 used | 42% utilization                   │
└──────────────────────────────────────────────────────────────────────────┘
```

**漸進式導入路徑**：

```
Phase 1-3（保持簡單）：
  default_job_cpus = 1, default_job_memory_mb = 512
  每個 job 請求少量資源（與傳統「1 worker = 1 job」接近）
  但 ResourceManager 已存在於程式碼中

Phase 3+（啟用多 job 並行）：
  Worker 自動偵測 8 CPU, 32GB RAM
  Job A: 2 cpu, 4GB → Job B: 4 cpu, 8GB → Job C: 1 cpu, 2GB → 三個同時跑
  尚無 Dedicated/Runner Group

Phase 4（加入 Dedicated）：
  Dedicated/Runner Group 啟動時 reserve(cpu, mem, disk)
  剩餘資源供 Normal job 使用
  完整組合矩陣可用
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
    pub early_return: Option<EarlyReturn>,  // 同步模式下此節點完成即回傳 HTTP
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

/// 同步執行時的 Early Return 設定
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EarlyReturn {
    /// 此節點完成後立即回傳 HTTP 回應，剩餘步驟背景繼續執行
    pub enabled: bool,
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
    const resp = await fetch(`/api/workspaces/${workspaceId}/flows/files/p/${flowPath}`)
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
        await fetch(`/api/workspaces/${workspaceId}/flows/files/p/${flowPath}`, {
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
    fetch(`/api/workspaces/${workspaceId}/flows/revisions/p/${flowPath}`)
      .then(r => r.json())
      .then(r => revisions = r)
  })

  async function showDiff(from: number, to: number) {
    const resp = await fetch(`/api/workspaces/${workspaceId}/flows/diff/p/${flowPath}?from=${from}&to=${to}`)
    diffResult = await resp.json()
  }

  async function rollback(targetRevision: number) {
    if (!confirm(`Rollback to revision ${targetRevision}?`)) return
    await fetch(`/api/workspaces/${workspaceId}/flows/rollback/p/${flowPath}/rev/${targetRevision}`, { method: 'POST' })
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
      <div class="diff-header">
        <h4>Diff: v{diffResult.from} → v{diffResult.to}</h4>
        <div class="diff-mode-toggle">
          <button class:active={diffMode === 'side'} onclick={() => diffMode = 'side'}>Side by Side</button>
          <button class:active={diffMode === 'unified'} onclick={() => diffMode = 'unified'}>Unified</button>
          <button class:active={diffMode === 'dag'} onclick={() => diffMode = 'dag'}>DAG Diff</button>
        </div>
      </div>

      {#if diffMode === 'side'}
        <!-- Side-by-side：像 GitHub PR 的左右對照 -->
        <div class="side-by-side">
          <div class="diff-panel old">
            <div class="panel-header">v{diffResult.from}</div>
            {#each diffResult.changes as change}
              <div class="diff-line" class:removed={change.type === 'removed'} class:modified={change.type === 'modified'}>
                <span class="path">{change.path}</span>
                <pre class="value">{JSON.stringify(change.old_value, null, 2)}</pre>
              </div>
            {/each}
          </div>
          <div class="diff-panel new">
            <div class="panel-header">v{diffResult.to}</div>
            {#each diffResult.changes as change}
              <div class="diff-line" class:added={change.type === 'added'} class:modified={change.type === 'modified'}>
                <span class="path">{change.path}</span>
                <pre class="value">{JSON.stringify(change.new_value, null, 2)}</pre>
              </div>
            {/each}
          </div>
        </div>
      {:else if diffMode === 'unified'}
        <!-- Unified：單欄 +/- 顯示 -->
        <div class="unified-diff">
          {#each diffResult.changes as change}
            {#if change.type === 'removed'}
              <div class="diff-line removed">- {change.path}: {JSON.stringify(change.old_value)}</div>
            {:else if change.type === 'added'}
              <div class="diff-line added">+ {change.path}: {JSON.stringify(change.new_value)}</div>
            {:else if change.type === 'modified'}
              <div class="diff-line removed">- {change.path}: {JSON.stringify(change.old_value)}</div>
              <div class="diff-line added">+ {change.path}: {JSON.stringify(change.new_value)}</div>
            {/if}
          {/each}
        </div>
      {:else}
        <!-- DAG Diff：視覺化顯示哪些步驟被新增/刪除/修改 -->
        <div class="dag-diff">
          {#each diffResult.module_changes as mod}
            <div class="module-change" class:added={mod.type === 'added'}
              class:removed={mod.type === 'removed'} class:modified={mod.type === 'modified'}>
              <span class="module-id">{mod.id}</span>
              <span class="module-type">{mod.type}</span>
              {#if mod.type === 'modified'}
                <span class="module-detail">{mod.changed_fields.join(', ')}</span>
              {/if}
            </div>
          {/each}
        </div>
      {/if}
    </div>
  {/if}
</div>

<style>
  .diff-mode-toggle { display: flex; gap: 4px; }
  .diff-mode-toggle button { padding: 4px 8px; border: 1px solid #555; background: #2d2d2d; color: #ccc; cursor: pointer; }
  .diff-mode-toggle button.active { background: #007acc; color: #fff; }
  .side-by-side { display: grid; grid-template-columns: 1fr 1fr; gap: 1px; background: #333; }
  .diff-panel { background: #1e1e1e; padding: 8px; overflow-x: auto; }
  .panel-header { font-weight: bold; padding: 4px 0; border-bottom: 1px solid #444; margin-bottom: 8px; }
  .diff-line.removed { background: rgba(244, 71, 71, 0.15); }
  .diff-line.added { background: rgba(78, 201, 176, 0.15); }
  .diff-line.modified { background: rgba(220, 220, 100, 0.15); }
  .path { color: #888; font-size: 12px; }
  .module-change { display: flex; gap: 8px; padding: 6px; border-bottom: 1px solid #333; }
  .module-change.added { border-left: 3px solid #4ec9b0; }
  .module-change.removed { border-left: 3px solid #f44747; }
  .module-change.modified { border-left: 3px solid #dcdcaa; }
</style>
```

**Diff 後端 API 回傳結構**（支援三種 diff 模式）：

```rust
#[derive(Serialize)]
pub struct FlowDiff {
    pub from: i32,
    pub to: i32,
    /// JSON path 級別的變更（用於 side-by-side 和 unified 模式）
    pub changes: Vec<DiffChange>,
    /// 模組級別的變更（用於 DAG diff 模式）
    pub module_changes: Vec<ModuleChange>,
}

#[derive(Serialize)]
pub struct DiffChange {
    pub path: String,              // e.g. "modules[1].value.content"
    pub r#type: String,            // "added", "removed", "modified"
    pub old_value: Option<serde_json::Value>,
    pub new_value: Option<serde_json::Value>,
}

#[derive(Serialize)]
pub struct ModuleChange {
    pub id: String,                // e.g. "b"
    pub r#type: String,            // "added", "removed", "modified", "unchanged"
    pub changed_fields: Vec<String>, // e.g. ["content", "input_transforms.url"]
}
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
    // 權限檢查：必須有該路徑的寫入權限
    user.require_writer(&path)?;

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
POST   /api/workspaces/{ws}/flows/save/p/{path}               // 儲存（自動建新 revision）
GET    /api/workspaces/{ws}/flows/list                          // 列出 flows（含最新 revision）
GET    /api/workspaces/{ws}/flows/get/p/{path}                  // 取得最新版
GET    /api/workspaces/{ws}/flows/get/p/{path}/rev/{rev}        // 取得指定版本
GET    /api/workspaces/{ws}/flows/revisions/p/{path}            // 版本歷史
GET    /api/workspaces/{ws}/flows/diff/p/{path}?from=2&to=3     // Diff 兩版本
POST   /api/workspaces/{ws}/flows/rollback/p/{path}/rev/{rev}   // Rollback

// === Flow 工作區檔案 ===
GET    /api/workspaces/{ws}/flows/files/p/{path}                // 列出檔案
GET    /api/workspaces/{ws}/flows/files/p/{path}/f/{file_path}  // 讀取檔案
PUT    /api/workspaces/{ws}/flows/files/p/{path}                // 建立/更新檔案
DELETE /api/workspaces/{ws}/flows/files/p/{path}/f/{file_path}  // 刪除檔案

// === Job 執行 ===
POST   /api/workspaces/{ws}/jobs/run/f/{path}                   // 執行 flow（最新版）
POST   /api/workspaces/{ws}/jobs/run/f/{path}/rev/{rev}         // 執行指定版本
GET    /api/workspaces/{ws}/jobs/{id}/flow_status                // 查詢 flow 執行狀態
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

7. Early Return（同步執行 Flow）：
   - 建立 3 步驟 Flow：A(推論 50ms) → B(存 DB 200ms) → C(寄通知 500ms)
   - Step A 設 `early_return: { enabled: true }`
   - POST run_wait_result/f/{path} → Step A 完成即回傳結果（~55ms）
   - 確認 Step B, C 在背景繼續完成
```

---

## Phase 3：排程與進階 Flow（Week 7-9）

### 目標

排程、大資料傳遞、內建節點、部署審核、檔案儲存、集群監控——讓 Flow 成為完整的工作流系統

### 3.1 Cron 排程系統

#### Multi-instance Deployment Strategy

Every worker instance runs its own `schedule_loop`. Multiple instances can safely run concurrently because `FOR UPDATE SKIP LOCKED` on the `schedule` table ensures each due schedule row is processed by exactly one instance per tick. No external coordination (etcd, Consul, Zookeeper) is required — PostgreSQL row-level locking is the sole synchronization mechanism.

```
Worker 1 scheduler: locks schedule A → push job → update next_trigger_at
Worker 2 scheduler: locks schedule B → push job → update next_trigger_at
Worker 3 scheduler: all rows locked  → SKIP LOCKED → idle this tick
```

This approach scales naturally: with N workers and M due schedules, the work is distributed across instances. Each worker processes a subset of schedules per tick, reducing per-instance load as the cluster grows.

#### Schema

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

#### Scheduler Loop

```rust
// Runs inside every worker instance. FOR UPDATE SKIP LOCKED ensures
// each schedule row is processed by exactly one worker per tick.
async fn schedule_loop(db: PgPool, cancel: CancellationToken) {
    loop {
        // Each schedule row is locked by at most one worker.
        // Other workers skip already-locked rows and process the rest.
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

            if let Err(e) = result {
                tracing::error!(schedule_id = %schedule.id, error = %e,
                    "failed to push scheduled job, will retry next tick");
                continue; // next_trigger_at unchanged → retried next tick
            }

            let next = cron::Schedule::from_str(&schedule.cron_expr)
                .ok().and_then(|s| s.upcoming(chrono::Utc).next());

            sqlx::query!(
                "UPDATE schedule SET last_triggered_at = now(), next_trigger_at = $1 WHERE id = $2",
                next, schedule.id
            ).execute(&db).await.ok();
        }

        tokio::select! {
            _ = tokio::time::sleep(std::time::Duration::from_secs(5)) => {},
            _ = cancel.cancelled() => break,
        }
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
            // Use tracing macro — DbLogLayer subscriber auto-writes to run_log
            match level {
                LogLevel::Debug => tracing::debug!(run_id = %job_id, "{}", msg_str),
                LogLevel::Info  => tracing::info!(run_id = %job_id, "{}", msg_str),
                LogLevel::Warn  => tracing::warn!(run_id = %job_id, "{}", msg_str),
                LogLevel::Error => tracing::error!(run_id = %job_id, "{}", msg_str),
            }
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
path: "folders/data-team/daily_etl"
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
GET  /api/workspaces/{ws}/flows/export/p/{path}?format=yaml   → YAML 字串
GET  /api/workspaces/{ws}/flows/export/p/{path}?format=json   → JSON
POST /api/workspaces/{ws}/flows/import                         → 匯入
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
PUT    /api/workspaces/{ws}/concurrency_limits/{tag}   // 設定限制（max_concurrent）
GET    /api/workspaces/{ws}/concurrency_limits          // 列出所有限制
DELETE /api/workspaces/{ws}/concurrency_limits/{tag}    // 刪除限制
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

### 3.7 Resource 系統實作

Schema 在 Phase 1 已建立（`resource_type`, `resource`, `variable` 三表），此處實作完整功能。

#### 加密模組

```rust
// crates/worker/src/crypto.rs

use aes_gcm::{Aes256Gcm, Key, Nonce, aead::{Aead, KeyInit, OsRng}};
use aes_gcm::aead::rand_core::RngCore;

/// 加密 resource/variable 的值
pub fn encrypt_value(plaintext: &serde_json::Value, key: &[u8; 32]) -> Vec<u8> {
    let cipher = Aes256Gcm::new(Key::<Aes256Gcm>::from_slice(key));
    let mut nonce_bytes = [0u8; 12];
    OsRng.fill_bytes(&mut nonce_bytes);
    let nonce = Nonce::from_slice(&nonce_bytes);

    let json_bytes = serde_json::to_vec(plaintext).unwrap();
    let ciphertext = cipher.encrypt(nonce, json_bytes.as_ref()).unwrap();

    // 格式：nonce(12) + ciphertext
    let mut result = Vec::with_capacity(12 + ciphertext.len());
    result.extend_from_slice(&nonce_bytes);
    result.extend_from_slice(&ciphertext);
    result
}

pub fn decrypt_value(encrypted: &[u8], key: &[u8; 32]) -> serde_json::Value {
    let cipher = Aes256Gcm::new(Key::<Aes256Gcm>::from_slice(key));
    let (nonce_bytes, ciphertext) = encrypted.split_at(12);
    let nonce = Nonce::from_slice(nonce_bytes);

    let plaintext = cipher.decrypt(nonce, ciphertext).unwrap();
    serde_json::from_slice(&plaintext).unwrap()
}
```

#### Resource CRUD API

```rust
// crates/api/src/resources.rs

pub async fn create_resource(
    State(state): State<AppState>,
    Path(workspace_id): Path<String>,
    Json(req): Json<CreateResourceRequest>,
) -> Result<Json<()>, ApiError> {
    // 驗證 resource_type 的 JSON Schema
    let rt = sqlx::query_as!(ResourceType,
        "SELECT schema FROM resource_type WHERE workspace_id = $1 AND name = $2",
        workspace_id, req.resource_type
    ).fetch_optional(&state.db).await?.ok_or(ApiError::NotFound)?;

    validate_json_schema(&rt.schema, &req.value)?;

    // AES-256-GCM 加密
    let encrypted = crypto::encrypt_value(&req.value, &state.encryption_key);

    sqlx::query!(
        "INSERT INTO resource (workspace_id, path, resource_type, value_encrypted, description, created_by)
         VALUES ($1, $2, $3, $4, $5, $6)",
        workspace_id, req.path, req.resource_type, encrypted, req.description, user.email
    ).execute(&state.db).await?;

    Ok(Json(()))
}

/// GET /resources/get_value/p/{path}
/// Worker 呼叫此端點取得解密後的值（僅限內部）
pub async fn get_resource_value(
    State(state): State<AppState>,
    Path((workspace_id, path)): Path<(String, String)>,
) -> Result<Json<serde_json::Value>, ApiError> {
    let resource = sqlx::query!(
        "SELECT value_encrypted FROM resource WHERE workspace_id = $1 AND path = $2",
        workspace_id, path
    ).fetch_optional(&state.db).await?.ok_or(ApiError::NotFound)?;

    let value = crypto::decrypt_value(&resource.value_encrypted, &state.encryption_key);
    Ok(Json(value))
}
```

#### Worker Args 引用替換

核心：Worker 執行 job 前，遞迴遍歷 `args` JSON，把 `$res:path` 和 `$var:path` 替換成實際值。

```rust
// crates/worker/src/resolve_args.rs

/// 遞迴解析 job args 中的 $res: 和 $var: 引用
pub async fn resolve_args(
    args: &mut serde_json::Value,
    db: &PgPool,
    workspace_id: &str,
    encryption_key: &[u8; 32],
) -> Result<()> {
    match args {
        serde_json::Value::String(s) => {
            if let Some(res_path) = s.strip_prefix("$res:") {
                // $res:users/admin/prod_db → 從 resource 表取值、解密
                let resource = sqlx::query!(
                    "SELECT value_encrypted FROM resource WHERE workspace_id = $1 AND path = $2",
                    workspace_id, res_path
                ).fetch_optional(db).await?.ok_or(Error::ResourceNotFound(res_path.into()))?;

                let mut value = crypto::decrypt_value(&resource.value_encrypted, encryption_key);
                // 遞迴：resource 值內部也可能有 $var: 引用
                resolve_args(&mut value, db, workspace_id, encryption_key).await?;
                *args = value;
            } else if let Some(var_path) = s.strip_prefix("$var:") {
                // $var:users/admin/api_key → 從 variable 表取值、解密
                let variable = sqlx::query!(
                    "SELECT value_encrypted FROM variable WHERE workspace_id = $1 AND path = $2",
                    workspace_id, var_path
                ).fetch_optional(db).await?.ok_or(Error::VariableNotFound(var_path.into()))?;

                *args = crypto::decrypt_value(&variable.value_encrypted, encryption_key);
            }
        }
        serde_json::Value::Object(map) => {
            for (_, v) in map.iter_mut() {
                Box::pin(resolve_args(v, db, workspace_id, encryption_key)).await?;
            }
        }
        serde_json::Value::Array(arr) => {
            for v in arr.iter_mut() {
                Box::pin(resolve_args(v, db, workspace_id, encryption_key)).await?;
            }
        }
        _ => {} // Number, Bool, Null — 不處理
    }
    Ok(())
}
```

#### Worker 整合

`create_args_and_out_file` 中呼叫 `resolve_args`：

```rust
// crates/worker/src/common.rs（修改既有函數）

pub async fn create_args_and_out_file(
    job: &QueuedJob, job_dir: &str, db: &PgPool, encryption_key: &[u8; 32],
) -> Result<()> {
    let mut args = job.args.clone().unwrap_or(serde_json::json!({}));

    // ★ 核心：解析 $res: 和 $var: 引用
    resolve_args(&mut args, db, &job.workspace_id, encryption_key).await?;

    // 寫入 args.json（使用者的 wrapper.py 會讀這個檔案）
    let args_str = serde_json::to_string(&args)?;
    tokio::fs::write(format!("{}/args.json", job_dir), args_str).await?;
    tokio::fs::write(format!("{}/result.json", job_dir), "{}").await?;
    Ok(())
}
```

#### 前端 Resource 管理

```
/resources 頁面：
  ├── 左側：Resource 列表（按 type 篩選 + 搜尋）
  ├── 右側：Resource 編輯器
  │   ├── 選擇 resource_type → 自動生成 JSON Schema 表單
  │   ├── 密碼欄位顯示 ●●●●●●（不回傳明文到前端）
  │   └── 測試連線按鈕（可選）
  └── Resource Type 管理：建立自訂類型 + JSON Schema 編輯器

Script Editor 整合：
  ├── 參數欄位旁邊有「Link Resource」按鈕
  └── 選擇 resource → 自動填入 "$res:users/admin/prod_db"
```

#### 內建 Resource Type

```sql
-- 預設 resource type（初次啟動自動建立）
INSERT INTO resource_type (workspace_id, name, schema, description) VALUES
('admins', 'postgres', '{
    "type": "object",
    "properties": {
        "host": {"type": "string"},
        "port": {"type": "integer", "default": 5432},
        "dbname": {"type": "string"},
        "user": {"type": "string"},
        "password": {"type": "string"}
    },
    "required": ["host", "dbname", "user", "password"]
}', 'PostgreSQL connection'),
('admins', 'mysql', '...', 'MySQL connection'),
('admins', 'openai', '{
    "type": "object",
    "properties": {
        "api_key": {"type": "string"},
        "organization": {"type": "string"}
    },
    "required": ["api_key"]
}', 'OpenAI API credentials'),
('admins', 's3', '{
    "type": "object",
    "properties": {
        "endpoint": {"type": "string"},
        "region": {"type": "string"},
        "access_key_id": {"type": "string"},
        "secret_access_key": {"type": "string"},
        "bucket": {"type": "string"}
    },
    "required": ["access_key_id", "secret_access_key", "bucket"]
}', 'S3-compatible storage');
```

### 3.8 Webhook Trigger + Trigger Trait 框架

**現狀問題**：目前只有 Cron 排程，沒有辦法從外部事件觸發 Flow/Script。Webhook 是最基本的觸發方式——外部系統 POST 一個 HTTP 請求就啟動 job。

**設計原則**：用 Trigger trait 抽象，Phase 3 先實作 Webhook + Cron 兩種，未來（Phase 4+）再接 Kafka、MQTT 等，只需實作 trait 即可。

#### Schema

```sql
CREATE TABLE webhook_trigger (
    workspace_id VARCHAR(50) NOT NULL REFERENCES workspace(id),
    path VARCHAR(255) NOT NULL,          -- trigger 的識別路徑
    target_type VARCHAR(10) NOT NULL,    -- "script" 或 "flow"
    target_path VARCHAR(255) NOT NULL,   -- 要觸發的 script/flow path
    enabled BOOLEAN NOT NULL DEFAULT TRUE,
    -- 認證方式
    auth_method VARCHAR(20) NOT NULL DEFAULT 'token',  -- token, hmac, none
    auth_token_hash CHAR(64),            -- SHA256 of bearer token
    hmac_secret VARCHAR(255),            -- for HMAC-SHA256 signature verification
    -- 執行模式
    request_type VARCHAR(10) NOT NULL DEFAULT 'async',  -- async, sync, sse
    sync_timeout_secs INTEGER DEFAULT 60,               -- sync 模式超時
    -- 其他
    args_transform TEXT,                 -- JS 表達式：將 HTTP body 轉成 job args
    created_by VARCHAR(255) NOT NULL,
    PRIMARY KEY (workspace_id, path)
);
```

#### Trigger Trait

```rust
// crates/worker/src/trigger.rs

#[async_trait]
pub trait Trigger: Send + Sync {
    fn name(&self) -> &str;

    /// 啟動監聽（長期執行的 background task）
    async fn start(&self, db: PgPool, handler: Arc<TriggerHandler>) -> Result<()>;

    /// 健康檢查
    async fn health_check(&self) -> Result<()>;
}

/// 所有 trigger 共用的處理邏輯：收到事件 → push job
pub struct TriggerHandler {
    db: PgPool,
}

impl TriggerHandler {
    pub async fn fire(
        &self, workspace_id: &str, target_type: &str, target_path: &str,
        args: serde_json::Value, trigger_kind: &str,
    ) -> Result<Uuid> {
        let kind = match target_type {
            "script" => JobKind::Script,
            "flow" => JobKind::Flow,
            _ => return Err(Error::BadRequest("invalid target_type")),
        };
        queue::push_job(&self.db, PushJobArgs {
            workspace_id, kind,
            script_path: if target_type == "script" { Some(target_path) } else { None },
            flow_path: if target_type == "flow" { Some(target_path) } else { None },
            args: Some(args),
            created_by: &format!("trigger:{}", trigger_kind),
            ..Default::default()
        }).await
    }
}
```

#### Webhook 實作

```rust
// crates/api/src/webhooks.rs

/// Webhook 支援三種執行模式
#[derive(Deserialize, Default)]
pub enum RequestType { #[default] Async, Sync, SyncSse }

/// POST /api/workspaces/{ws}/webhooks/{path}?mode=sync
/// 外部系統呼叫此 endpoint 觸發 job
pub async fn handle_webhook(
    State(state): State<AppState>,
    Path((workspace_id, webhook_path)): Path<(String, String)>,
    Query(params): Query<WebhookQueryParams>,
    headers: HeaderMap,
    Json(body): Json<serde_json::Value>,
) -> Result<Response, ApiError> {
    let trigger = sqlx::query_as!(WebhookTrigger,
        "SELECT * FROM webhook_trigger
         WHERE workspace_id = $1 AND path = $2 AND enabled = TRUE",
        workspace_id, webhook_path
    ).fetch_optional(&state.db).await?.ok_or(ApiError::NotFound)?;

    // 驗證認證
    match trigger.auth_method.as_str() {
        "token" => {
            let token = headers.get("Authorization")
                .and_then(|v| v.to_str().ok())
                .and_then(|v| v.strip_prefix("Bearer "))
                .ok_or(ApiError::Unauthorized)?;
            let hash = sha256(token);
            if hash != trigger.auth_token_hash.unwrap_or_default() {
                return Err(ApiError::Unauthorized);
            }
        }
        "hmac" => {
            let signature = headers.get("X-Signature-256")
                .and_then(|v| v.to_str().ok())
                .ok_or(ApiError::Unauthorized)?;
            verify_hmac_sha256(&trigger.hmac_secret.unwrap(), &body, signature)?;
        }
        "none" => {} // 公開 webhook
        _ => return Err(ApiError::BadRequest("unknown auth method")),
    }

    // 轉換 args（可選的 JS 表達式）
    let args = if let Some(transform) = &trigger.args_transform {
        jseval::eval_js_expr(transform, &body)?
    } else {
        body
    };

    let job_id = state.trigger_handler.fire(
        &workspace_id, &trigger.target_type, &trigger.target_path,
        args, "webhook",
    ).await?;

    // 根據模式決定回傳方式
    match params.mode.unwrap_or_default() {
        RequestType::Async => Ok(Json(JobCreated { id: job_id }).into_response()),
        RequestType::Sync => {
            // 阻塞等待結果（復用 run_wait_result 機制）
            run_wait_result_internal(
                &state.db, &workspace_id, job_id, params.timeout, None,
            ).await
        }
        RequestType::SyncSse => {
            // SSE 串流日誌（chunk-based）+ 最終結果（復用 stream_job_logs 機制）
            Ok(stream_job_logs_response(&state.db, &workspace_id, job_id).await)
        }
    }
}

#[derive(Deserialize)]
pub struct WebhookQueryParams {
    pub mode: Option<RequestType>,   // async(預設), sync, sse
    pub timeout: Option<u64>,        // sync 模式的超時秒數
}
```

#### Cron 也套用 Trigger Trait

Phase 3.1 已有的 Cron 排程，現在也納入 Trigger 框架：

```rust
pub struct CronTrigger;

#[async_trait]
impl Trigger for CronTrigger {
    fn name(&self) -> &str { "cron" }

    async fn start(&self, db: PgPool, handler: Arc<TriggerHandler>) -> Result<()> {
        // 就是原本的 schedule_loop，改用 handler.fire() 推 job
        tokio::spawn(async move { schedule_loop(db, handler).await });
        Ok(())
    }

    async fn health_check(&self) -> Result<()> { Ok(()) }
}
```

**未來擴展**（Phase 4+）只需新增 trait 實作：

```
Phase 3:  CronTrigger + WebhookTrigger（已實作）
Phase 4+: KafkaTrigger / MqttTrigger / NatsTrigger / PostgresCdcTrigger
          → 各自實作 Trigger trait，不改核心程式碼
```

#### 新增 API

```
// Webhook 管理
POST   /api/workspaces/{ws}/webhook_triggers/create          // 建立 webhook trigger
GET    /api/workspaces/{ws}/webhook_triggers/list             // 列出
DELETE /api/workspaces/{ws}/webhook_triggers/{path}           // 刪除
PUT    /api/workspaces/{ws}/webhook_triggers/{path}/toggle    // 啟用/停用

// Webhook 觸發（外部呼叫）
POST   /api/workspaces/{ws}/webhooks/{path}                   // 觸發 job
```

### 3.9 Deploy Approval Gate 實作

#### 核心邏輯：deploy.rs

```rust
// crates/api/src/deploy.rs

use axum::{extract::*, response::Json};
use sqlx::PgPool;
use uuid::Uuid;

// ============================================================
// Approval Policy CRUD
// ============================================================

/// 建立審核政策
pub async fn create_policy(
    Path(workspace_id): Path<String>,
    State(db): State<PgPool>,
    claims: AuthClaims,
    Json(req): Json<CreatePolicyRequest>,
) -> Result<Json<ApprovalPolicy>, ApiError> {
    // 只有 admin 可以建立 policy
    require_admin(&db, &workspace_id, &claims.email).await?;

    let policy = sqlx::query_as!(ApprovalPolicy, r#"
        INSERT INTO approval_policy (workspace_id, path_pattern, min_approvals, approvers, auto_deploy)
        VALUES ($1, $2, $3, $4, $5)
        RETURNING *
    "#, workspace_id, req.path_pattern, req.min_approvals, &req.approvers, req.auto_deploy)
    .fetch_one(&db).await?;

    Ok(Json(policy))
}

/// 列出所有審核政策
pub async fn list_policies(
    Path(workspace_id): Path<String>,
    State(db): State<PgPool>,
) -> Result<Json<Vec<ApprovalPolicy>>, ApiError> {
    let policies = sqlx::query_as!(ApprovalPolicy,
        "SELECT * FROM approval_policy WHERE workspace_id = $1 ORDER BY path_pattern",
        workspace_id
    ).fetch_all(&db).await?;
    Ok(Json(policies))
}

// ============================================================
// Deploy Request 建立 + 查詢
// ============================================================

/// 發起部署請求
pub async fn create_deploy_request(
    Path(workspace_id): Path<String>,
    State(db): State<PgPool>,
    claims: AuthClaims,
    Json(req): Json<CreateDeployRequest>,
) -> Result<Json<DeployRequest>, ApiError> {
    // 1. 檢查是否有匹配的 approval_policy
    let policy = find_matching_policy(&db, &workspace_id, &req.target_path).await?;

    match policy {
        None => {
            // 無 policy → 直接部署（零阻力）
            do_deploy(&db, &workspace_id, &req.target_path, &req.target_kind, &req.draft_value).await?;
            Ok(Json(DeployRequest {
                id: Uuid::new_v4(),
                status: "deployed".to_string(),
                // ... 其餘欄位
            }))
        }
        Some(policy) => {
            // 有 policy → 建立 deploy_request，等待審核
            let previous_hash = get_current_hash(&db, &workspace_id, &req.target_path, &req.target_kind).await?;

            let deploy_req = sqlx::query_as!(DeployRequest, r#"
                INSERT INTO deploy_request
                    (workspace_id, target_path, target_kind, draft_value, previous_hash, requested_by)
                VALUES ($1, $2, $3, $4, $5, $6)
                RETURNING *
            "#, workspace_id, req.target_path, req.target_kind,
                req.draft_value, previous_hash, claims.email)
            .fetch_one(&db).await?;

            // 發送通知給 approvers（in-app + 可選 webhook）
            notify_approvers(&db, &workspace_id, &policy.approvers, &deploy_req).await?;

            Ok(Json(deploy_req))
        }
    }
}

/// 找到匹配的 approval_policy（最具體的 pattern 優先）
async fn find_matching_policy(
    db: &PgPool, workspace_id: &str, path: &str,
) -> Result<Option<ApprovalPolicy>, ApiError> {
    // 用 SQL 的 LIKE 匹配，path_pattern 中的 * 轉成 %
    // 如果多個 pattern 都匹配，取最長的（最具體）
    let policy = sqlx::query_as!(ApprovalPolicy, r#"
        SELECT * FROM approval_policy
        WHERE workspace_id = $1
          AND $2 LIKE replace(path_pattern, '*', '%')
        ORDER BY length(path_pattern) DESC
        LIMIT 1
    "#, workspace_id, path)
    .fetch_optional(db).await?;
    Ok(policy)
}

// ============================================================
// 審核 + 部署
// ============================================================

/// 審核通過
pub async fn approve_deploy(
    Path((workspace_id, request_id)): Path<(String, Uuid)>,
    State(db): State<PgPool>,
    claims: AuthClaims,
    Json(req): Json<ApprovalDecision>,
) -> Result<Json<DeployRequest>, ApiError> {
    let deploy_req = get_deploy_request(&db, &request_id).await?;

    // 檢查是否為合格的 approver
    let policy = find_matching_policy(&db, &workspace_id, &deploy_req.target_path).await?
        .ok_or(ApiError::BadRequest("no matching policy"))?;
    ensure_is_approver(&db, &workspace_id, &claims.email, &policy.approvers).await?;

    // 記錄 approval
    sqlx::query!(r#"
        INSERT INTO deploy_approval (deploy_request_id, approver, decision, comment)
        VALUES ($1, $2, 'approved', $3)
        ON CONFLICT (deploy_request_id, approver)
        DO UPDATE SET decision = 'approved', comment = $3, decided_at = now()
    "#, request_id, claims.email, req.comment.unwrap_or_default())
    .execute(&db).await?;

    // 檢查是否達到 min_approvals
    let approval_count: i64 = sqlx::query_scalar!(
        "SELECT COUNT(*) FROM deploy_approval WHERE deploy_request_id = $1 AND decision = 'approved'",
        request_id
    ).fetch_one(&db).await?.unwrap_or(0);

    let new_status = if approval_count >= policy.min_approvals as i64 {
        if policy.auto_deploy {
            // 自動部署
            do_deploy(&db, &workspace_id, &deploy_req.target_path,
                      &deploy_req.target_kind, &deploy_req.draft_value).await?;
            sqlx::query!(
                "UPDATE deploy_request SET status = 'deployed', deployed_at = now() WHERE id = $1",
                request_id
            ).execute(&db).await?;
            "deployed"
        } else {
            sqlx::query!(
                "UPDATE deploy_request SET status = 'approved' WHERE id = $1",
                request_id
            ).execute(&db).await?;
            "approved"  // 等待手動按下 deploy
        }
    } else {
        "pending"
    };

    let updated = get_deploy_request(&db, &request_id).await?;
    Ok(Json(updated))
}

/// 駁回
pub async fn reject_deploy(
    Path((workspace_id, request_id)): Path<(String, Uuid)>,
    State(db): State<PgPool>,
    claims: AuthClaims,
    Json(req): Json<ApprovalDecision>,
) -> Result<Json<DeployRequest>, ApiError> {
    let deploy_req = get_deploy_request(&db, &request_id).await?;
    let policy = find_matching_policy(&db, &workspace_id, &deploy_req.target_path).await?
        .ok_or(ApiError::BadRequest("no matching policy"))?;
    ensure_is_approver(&db, &workspace_id, &claims.email, &policy.approvers).await?;

    sqlx::query!(r#"
        INSERT INTO deploy_approval (deploy_request_id, approver, decision, comment)
        VALUES ($1, $2, 'rejected', $3)
        ON CONFLICT (deploy_request_id, approver)
        DO UPDATE SET decision = 'rejected', comment = $3, decided_at = now()
    "#, request_id, claims.email, req.comment.unwrap_or_default())
    .execute(&db).await?;

    // 任何人 reject → 整個 request 變 rejected
    sqlx::query!("UPDATE deploy_request SET status = 'rejected' WHERE id = $1", request_id)
        .execute(&db).await?;

    let updated = get_deploy_request(&db, &request_id).await?;
    Ok(Json(updated))
}

/// 手動部署（status = approved 後才能呼叫）
pub async fn execute_deploy(
    Path((workspace_id, request_id)): Path<(String, Uuid)>,
    State(db): State<PgPool>,
    claims: AuthClaims,
) -> Result<Json<DeployRequest>, ApiError> {
    let deploy_req = get_deploy_request(&db, &request_id).await?;

    if deploy_req.status != "approved" {
        return Err(ApiError::BadRequest("deploy request is not approved"));
    }

    do_deploy(&db, &workspace_id, &deploy_req.target_path,
              &deploy_req.target_kind, &deploy_req.draft_value).await?;

    sqlx::query!(
        "UPDATE deploy_request SET status = 'deployed', deployed_at = now() WHERE id = $1",
        request_id
    ).execute(&db).await?;

    let updated = get_deploy_request(&db, &request_id).await?;
    Ok(Json(updated))
}

/// 查看 diff（舊版 vs 新版）
pub async fn get_deploy_diff(
    Path((_workspace_id, request_id)): Path<(String, Uuid)>,
    State(db): State<PgPool>,
) -> Result<Json<DeployDiff>, ApiError> {
    let deploy_req = get_deploy_request(&db, &request_id).await?;

    let current_value = if let Some(ref hash) = deploy_req.previous_hash {
        // 根據 target_kind 取得舊版本內容
        match deploy_req.target_kind.as_str() {
            "flow" => get_flow_value_by_hash(&db, hash).await?,
            "script" => get_script_value_by_hash(&db, hash).await?,
            _ => None,
        }
    } else {
        None  // 新建，無舊版本
    };

    Ok(Json(DeployDiff {
        current: current_value,
        proposed: deploy_req.draft_value.clone(),
        target_path: deploy_req.target_path,
        target_kind: deploy_req.target_kind,
    }))
}

// ============================================================
// 內部輔助
// ============================================================

/// 實際執行部署：將 draft_value 寫入正式表
async fn do_deploy(
    db: &PgPool, workspace_id: &str, path: &str,
    kind: &str, value: &serde_json::Value,
) -> Result<(), ApiError> {
    match kind {
        "flow" => {
            // 呼叫 flows::save_flow 的內部邏輯（建立新 revision）
            flows::save_flow_internal(db, workspace_id, path, value).await?;
        }
        "script" => {
            // 呼叫 scripts::create_script 的內部邏輯（建立新 hash）
            scripts::create_script_internal(db, workspace_id, path, value).await?;
        }
        _ => return Err(ApiError::BadRequest("invalid target_kind")),
    }
    Ok(())
}

/// 檢查使用者是否為合格的 approver（支援 team 展開）
async fn ensure_is_approver(
    db: &PgPool, workspace_id: &str, email: &str, approvers: &[String],
) -> Result<(), ApiError> {
    for approver in approvers {
        if approver.starts_with("users/") && approver == format!("users/{email}") {
            return Ok(());
        }
        if approver.starts_with("teams/") {
            // 查 team membership
            let team_name = &approver["teams/".len()..];
            let is_member = sqlx::query_scalar!(
                "SELECT EXISTS(SELECT 1 FROM team_member WHERE workspace_id = $1 AND team_name = $2 AND email = $3)",
                workspace_id, team_name, email
            ).fetch_one(db).await?.unwrap_or(false);
            if is_member { return Ok(()); }
        }
    }
    Err(ApiError::Forbidden("you are not an approved reviewer"))
}

// ============================================================
// 型別定義
// ============================================================

#[derive(Deserialize)]
pub struct CreatePolicyRequest {
    pub path_pattern: String,      // "folders/production/*"
    pub min_approvals: i32,        // 1
    pub approvers: Vec<String>,    // ["users/alice", "teams/sre-team"]
    pub auto_deploy: Option<bool>, // 達到 min_approvals 後自動部署？
}

#[derive(Deserialize)]
pub struct CreateDeployRequest {
    pub target_path: String,           // "folders/production/credit_scoring"
    pub target_kind: String,           // "flow" | "script"
    pub draft_value: serde_json::Value, // 新版本 JSON
}

#[derive(Deserialize)]
pub struct ApprovalDecision {
    pub comment: Option<String>,
}

#[derive(Serialize)]
pub struct DeployDiff {
    pub current: Option<serde_json::Value>,   // 舊版（None = 新建）
    pub proposed: serde_json::Value,          // 新版
    pub target_path: String,
    pub target_kind: String,
}
```

#### Flow/Script 儲存整合

既有的 `flows::save_flow` 和 `scripts::create_script` 需要加入 approval gate 檢查：

```rust
// crates/api/src/flows.rs — save_flow 修改

pub async fn save_flow(
    Path((workspace_id, path)): Path<(String, String)>,
    State(db): State<PgPool>,
    claims: AuthClaims,
    Json(req): Json<SaveFlowRequest>,
) -> Result<Json<SaveFlowResponse>, ApiError> {
    // 檢查是否有匹配的 approval_policy
    let policy = deploy::find_matching_policy(&db, &workspace_id, &path).await?;

    match policy {
        None => {
            // 無 policy → 直接儲存（原有邏輯）
            let result = save_flow_internal(&db, &workspace_id, &path, &req.value).await?;
            Ok(Json(SaveFlowResponse { deployed: true, deploy_request_id: None, revision: result }))
        }
        Some(_policy) => {
            // 有 policy → 只存 draft，不寫入正式版本
            // 自動建立 deploy_request
            let deploy_req = deploy::create_deploy_request_internal(
                &db, &workspace_id, &path, "flow",
                &req.value, &claims.email,
            ).await?;
            Ok(Json(SaveFlowResponse {
                deployed: false,
                deploy_request_id: Some(deploy_req.id),
                revision: None,
            }))
        }
    }
}
```

#### 前端 DeployGate.svelte

```
DeployGate 面板（出現在 FlowEditor / ScriptEditor 的右側）：

┌─────────────────────────────────────┐
│ 📋 Deploy Request #abc123           │
│                                     │
│ Status: ⏳ Pending (1/2 approvals)  │
│ Requested by: users/bob             │
│ Created: 2025-01-15 14:30           │
│                                     │
│ ─── Diff ───                        │
│ ┌─────────────────────────────────┐ │
│ │ - step_a: return x * 2         │ │
│ │ + step_a: return x * 3         │ │
│ │   step_b: (unchanged)          │ │
│ └─────────────────────────────────┘ │
│                                     │
│ ─── Approvals ───                   │
│ ✅ users/alice: "LGTM"              │
│ ⏳ teams/sre-team: (waiting)        │
│                                     │
│ 💬 Comment:                         │
│ ┌─────────────────────────────────┐ │
│ │                                 │ │
│ └─────────────────────────────────┘ │
│                                     │
│ [✅ Approve]  [❌ Reject]           │
│                                     │
│ ─── History ───                     │
│ • #abc122 deployed 2025-01-14       │
│ • #abc121 rejected 2025-01-13       │
└─────────────────────────────────────┘

管理者設定頁（/settings/approval-policies）：

┌──────────────────────────────────────────┐
│ Approval Policies                        │
│                                          │
│ ┌──────────────┬───────┬──────────────┐  │
│ │ Path Pattern │ Min   │ Approvers    │  │
│ ├──────────────┼───────┼──────────────┤  │
│ │ folders/prod/*    │ 2  │ teams/sre    │  │
│ │ folders/finance/* │ 1  │ users/alice  │  │
│ │ folders/staging/* │ 1  │ teams/dev-lead│  │
│ └──────────────┴───────┴──────────────┘  │
│                                          │
│ [+ Add Policy]                           │
└──────────────────────────────────────────┘
```

### 3.10 File Storage 實作

#### 儲存抽象層

```rust
// crates/object-store/src/lib.rs

use std::path::{Path, PathBuf};
use tokio::io::AsyncRead;

/// 雙模式檔案儲存
pub enum FileStorage {
    S3 { client: aws_sdk_s3::Client, bucket: String },
    Local { base_dir: PathBuf },
}

/// 檔案參考（存在 job args JSONB 中）
#[derive(Serialize, Deserialize, Clone)]
#[serde(untagged)]
pub enum FileRef {
    S3 { s3: String },               // { "s3": "uploads/2025/data.csv" }
    Local { local: String },          // { "local": "data.csv" }
}

impl FileStorage {
    /// 從 workspace_settings 建立
    pub async fn from_workspace(db: &PgPool, workspace_id: &str) -> Result<Self, Error> {
        let settings = sqlx::query_as!(WorkspaceSettings,
            "SELECT * FROM workspace_settings WHERE workspace_id = $1",
            workspace_id
        ).fetch_optional(db).await?;

        match settings.map(|s| s.file_storage_mode.as_str()) {
            Some("s3") => {
                let s = settings.unwrap();
                let config = aws_config::defaults(BehaviorVersion::latest())
                    .endpoint_url(s.s3_endpoint.unwrap_or_default())
                    .region(Region::new(s.s3_region.unwrap_or("us-east-1".into())))
                    .credentials_provider(Credentials::new(
                        decrypt(&s.s3_access_key_encrypted)?,
                        decrypt(&s.s3_secret_key_encrypted)?,
                        None, None, "workspace"
                    ))
                    .load().await;
                Ok(Self::S3 { client: aws_sdk_s3::Client::new(&config), bucket: s.s3_bucket.unwrap() })
            }
            _ => {
                let dir = settings
                    .and_then(|s| s.local_data_dir)
                    .unwrap_or_else(|| "/data/coveflow/files".into());
                let base = PathBuf::from(dir).join(workspace_id);
                tokio::fs::create_dir_all(&base).await?;
                Ok(Self::Local { base_dir: base })
            }
        }
    }

    /// 上傳檔案
    pub async fn upload(&self, path: &str, body: impl AsyncRead + Send + 'static, size: Option<u64>) -> Result<FileRef, Error> {
        match self {
            Self::S3 { client, bucket } => {
                let key = format!("files/{}", path);
                let stream = ByteStream::from_reader(body, size);
                client.put_object().bucket(bucket).key(&key)
                    .body(stream).send().await?;
                Ok(FileRef::S3 { s3: key })
            }
            Self::Local { base_dir } => {
                let dest = base_dir.join(path);
                tokio::fs::create_dir_all(dest.parent().unwrap()).await?;
                let mut file = tokio::fs::File::create(&dest).await?;
                tokio::io::copy(&mut tokio::io::BufReader::new(body), &mut file).await?;
                Ok(FileRef::Local { local: path.to_string() })
            }
        }
    }

    /// 下載到 job_dir（Worker 呼叫）
    pub async fn download_to(&self, file_ref: &FileRef, dest: &Path) -> Result<(), Error> {
        tokio::fs::create_dir_all(dest.parent().unwrap()).await?;
        match (self, file_ref) {
            (Self::S3 { client, bucket }, FileRef::S3 { s3: key }) => {
                let resp = client.get_object().bucket(bucket).key(key).send().await?;
                let mut file = tokio::fs::File::create(dest).await?;
                let mut stream = resp.body.into_async_read();
                tokio::io::copy(&mut stream, &mut file).await?;
            }
            (Self::Local { base_dir }, FileRef::Local { local: path }) => {
                let src = base_dir.join(path);
                tokio::fs::copy(&src, dest).await?;
            }
            _ => return Err(Error::Mismatch("file ref type doesn't match storage mode")),
        }
        Ok(())
    }

    /// 列出檔案
    pub async fn list(&self, prefix: &str, limit: usize) -> Result<Vec<FileMetadata>, Error> {
        match self {
            Self::S3 { client, bucket } => {
                let resp = client.list_objects_v2().bucket(bucket)
                    .prefix(format!("files/{}", prefix))
                    .max_keys(limit as i32)
                    .send().await?;
                Ok(resp.contents().iter().map(|obj| FileMetadata {
                    path: obj.key().unwrap_or("").strip_prefix("files/").unwrap_or("").to_string(),
                    size: obj.size().unwrap_or(0) as u64,
                    last_modified: obj.last_modified().map(|t| t.to_string()),
                }).collect())
            }
            Self::Local { base_dir } => {
                // 遞迴列出 base_dir/prefix 下的檔案
                list_local_files(base_dir, prefix, limit).await
            }
        }
    }

    /// 預覽檔案（前 N 行 / 前 N bytes）
    pub async fn preview(&self, file_ref: &FileRef, max_bytes: usize) -> Result<FilePreview, Error> {
        let bytes = self.read_partial(file_ref, max_bytes).await?;
        let ext = file_ref.extension();

        match ext {
            "csv" => {
                let mut rdr = csv::ReaderBuilder::new().from_reader(&bytes[..]);
                let headers = rdr.headers()?.clone();
                let rows: Vec<Vec<String>> = rdr.records().take(100)
                    .filter_map(|r| r.ok())
                    .map(|r| r.iter().map(|s| s.to_string()).collect())
                    .collect();
                Ok(FilePreview::Table { headers: headers.iter().map(|s| s.to_string()).collect(), rows })
            }
            "json" => {
                let text = String::from_utf8_lossy(&bytes);
                Ok(FilePreview::Json(text.to_string()))
            }
            _ => {
                let text = String::from_utf8_lossy(&bytes[..max_bytes.min(bytes.len())]);
                Ok(FilePreview::Text(text.to_string()))
            }
        }
    }
}

#[derive(Serialize)]
pub struct FileMetadata {
    pub path: String,
    pub size: u64,
    pub last_modified: Option<String>,
}

#[derive(Serialize)]
pub enum FilePreview {
    Table { headers: Vec<String>, rows: Vec<Vec<String>> },
    Json(String),
    Text(String),
}
```

#### API 端點

```rust
// crates/api/src/files.rs

/// 上傳檔案（streaming，支援大檔案）
pub async fn upload_file(
    Path(workspace_id): Path<String>,
    State(state): State<AppState>,
    Query(params): Query<UploadParams>,
    body: axum::body::Body,
) -> Result<Json<FileRef>, ApiError> {
    // 檢查檔案大小限制
    let settings = get_workspace_settings(&state.db, &workspace_id).await?;
    if let Some(size) = params.size {
        if size > settings.max_file_size as u64 {
            return Err(ApiError::PayloadTooLarge(format!(
                "file size {} exceeds limit {}", size, settings.max_file_size
            )));
        }
    }

    let storage = FileStorage::from_workspace(&state.db, &workspace_id).await?;
    let path = params.path.unwrap_or_else(|| {
        format!("uploads/{}/{}", chrono::Utc::now().format("%Y/%m/%d"), Uuid::new_v4())
    });

    let reader = StreamReader::new(body.into_data_stream().map_err(|e| {
        std::io::Error::new(std::io::ErrorKind::Other, e)
    }));

    let file_ref = storage.upload(&path, reader, params.size).await?;
    Ok(Json(file_ref))
}

/// 下載檔案
pub async fn download_file(
    Path((workspace_id, path)): Path<(String, String)>,
    State(state): State<AppState>,
) -> Result<impl IntoResponse, ApiError> {
    let storage = FileStorage::from_workspace(&state.db, &workspace_id).await?;
    let bytes = storage.read_full(&FileRef::from_path(&path, &storage)).await?;
    let content_type = mime_guess::from_path(&path).first_or_octet_stream();
    Ok(([(header::CONTENT_TYPE, content_type.to_string())], bytes))
}

/// 預覽檔案（前 100 行 or 前 64KB）
pub async fn preview_file(
    Path((workspace_id, path)): Path<(String, String)>,
    State(state): State<AppState>,
) -> Result<Json<FilePreview>, ApiError> {
    let storage = FileStorage::from_workspace(&state.db, &workspace_id).await?;
    let file_ref = FileRef::from_path(&path, &storage);
    let preview = storage.preview(&file_ref, 65536).await?;
    Ok(Json(preview))
}

#[derive(Deserialize)]
pub struct UploadParams {
    pub path: Option<String>,    // 自訂路徑，否則自動生成
    pub size: Option<u64>,       // Content-Length（用於預檢大小限制）
}
```

#### Worker 整合：自動解析 FileRef

```rust
// crates/worker/src/resolve_args.rs — 新增 resolve_file_refs

/// 掃描 args 中的 FileRef，下載到 job_dir/input/
pub async fn resolve_file_refs(
    args: &mut serde_json::Value,
    job_dir: &Path,
    storage: &FileStorage,
) -> Result<(), Error> {
    let input_dir = job_dir.join("input");
    tokio::fs::create_dir_all(&input_dir).await?;

    resolve_file_refs_recursive(args, &input_dir, storage).await
}

async fn resolve_file_refs_recursive(
    value: &mut serde_json::Value,
    input_dir: &Path,
    storage: &FileStorage,
) -> Result<(), Error> {
    match value {
        serde_json::Value::Object(map) => {
            // 檢查是否為 FileRef（有 "s3" 或 "local" key）
            if let Some(file_ref) = try_parse_file_ref(map) {
                let filename = file_ref.filename();
                let dest = input_dir.join(&filename);
                storage.download_to(&file_ref, &dest).await?;

                // 替換為本地路徑，讓 script 直接 open()
                *value = serde_json::json!({
                    "path": format!("input/{}", filename),
                    "original_ref": file_ref,
                });
                return Ok(());
            }
            // 遞迴處理子欄位
            for (_k, v) in map.iter_mut() {
                resolve_file_refs_recursive(v, input_dir, storage).await?;
            }
        }
        serde_json::Value::Array(arr) => {
            for v in arr.iter_mut() {
                resolve_file_refs_recursive(v, input_dir, storage).await?;
            }
        }
        _ => {}
    }
    Ok(())
}
```

#### 前端 FileUpload + FileBrowser

```
FileUpload 元件（拖拉上傳）：
┌──────────────────────────────────────┐
│  📁 拖拉檔案到這裡，或點擊選擇       │
│                                      │
│  ┌────────────────────────────────┐  │
│  │ data.csv        45.2 MB       │  │
│  │ ████████████████░░░░  78%     │  │
│  └────────────────────────────────┘  │
│  ┌────────────────────────────────┐  │
│  │ model.pkl       12.1 MB  ✅   │  │
│  └────────────────────────────────┘  │
└──────────────────────────────────────┘

FileBrowser 頁面（/files）：
┌──────────────────────────────────────────────┐
│ 📂 Files                    [Upload] [🔄]    │
│                                              │
│ 📂 uploads/                                  │
│   📂 2025/01/                                │
│     📄 data.csv          45.2 MB  2025-01-15 │
│     📄 model.pkl         12.1 MB  2025-01-14 │
│     📄 config.json        1.2 KB  2025-01-14 │
│ 📂 results/                                  │
│   📄 output_abc123.json  2.3 MB  2025-01-15  │
│                                              │
│ ─── Preview ───                              │
│ ┌────────────────────────────────────────┐   │
│ │ name  │ age │ score │ city            │   │
│ │ Alice │ 30  │ 92.5  │ Taipei          │   │
│ │ Bob   │ 25  │ 88.0  │ Tokyo           │   │
│ │ ...   │ ... │ ...   │ ...             │   │
│ │        (showing 100 of 50,000 rows)    │   │
│ └────────────────────────────────────────┘   │
└──────────────────────────────────────────────┘

Storage 設定（/settings）：
┌──────────────────────────────────────────────┐
│ File Storage Settings                        │
│                                              │
│ Mode: ( ) Local  (•) S3                      │
│                                              │
│ S3 Endpoint: [http://minio:9000         ]    │
│ Bucket:      [coveflow-files            ]    │
│ Region:      [us-east-1                 ]    │
│ Access Key:  [********************      ]    │
│ Secret Key:  [********************      ]    │
│                                              │
│ Max File Size: [100 MB ▼]                    │
│                                              │
│ [Test Connection]  [Save]                    │
└──────────────────────────────────────────────┘
```

### 3.11 Cluster Resource Dashboard 實作

#### Worker 資源回報

```rust
// crates/worker/src/worker.rs — worker ping 擴充

use sysinfo::System;
use nix::sys::statvfs;

/// 收集 worker 資源指標
fn collect_worker_metrics(job_dir: &str) -> WorkerMetrics {
    let mut sys = System::new();
    sys.refresh_cpu_all();
    sys.refresh_memory();

    // CPU 配額（cgroup v2 優先，fallback sysinfo）
    let vcpus = read_cgroup_cpu_quota()
        .unwrap_or_else(|| sys.cpus().len() as i32);

    // 記憶體（cgroup v2 優先，fallback sysinfo）
    let memory_total = read_cgroup_memory_limit()
        .unwrap_or_else(|| sys.total_memory() as i64);
    let memory_usage = read_cgroup_memory_usage()
        .unwrap_or_else(|| sys.used_memory() as i64);

    // CPU 使用率（/proc/stat delta）
    let cpu_usage_percent = calculate_cpu_usage_percent();

    // 磁碟（statvfs on job_dir）
    let (disk_total, disk_usage) = match statvfs::statvfs(job_dir) {
        Ok(stat) => {
            let total = stat.blocks() * stat.block_size() as u64;
            let avail = stat.blocks_available() * stat.block_size() as u64;
            (total as i64, (total - avail) as i64)
        }
        Err(_) => (0, 0),
    };

    WorkerMetrics { vcpus, memory_total, memory_usage, cpu_usage_percent, disk_total, disk_usage }
}

fn read_cgroup_cpu_quota() -> Option<i32> {
    // cgroup v2: /sys/fs/cgroup/cpu.max → "quota period"
    let content = std::fs::read_to_string("/sys/fs/cgroup/cpu.max").ok()?;
    let parts: Vec<&str> = content.trim().split(' ').collect();
    if parts[0] == "max" { return None; }
    let quota: i64 = parts[0].parse().ok()?;
    let period: i64 = parts[1].parse().ok()?;
    Some((quota / period) as i32)
}

fn read_cgroup_memory_limit() -> Option<i64> {
    // cgroup v2: /sys/fs/cgroup/memory.max
    let content = std::fs::read_to_string("/sys/fs/cgroup/memory.max").ok()?;
    if content.trim() == "max" { return None; }
    content.trim().parse().ok()
}

fn read_cgroup_memory_usage() -> Option<i64> {
    // cgroup v2: /sys/fs/cgroup/memory.current
    std::fs::read_to_string("/sys/fs/cgroup/memory.current").ok()?.trim().parse().ok()
}

/// Worker ping 更新（每 15 秒）
async fn update_worker_ping(
    db: &PgPool, worker_name: &str, job_dir: &str,
    current_job_id: Option<Uuid>, occupancy: &OccupancyTracker,
) {
    let m = collect_worker_metrics(job_dir);
    let (o15, o5, o30) = occupancy.rates();

    sqlx::query!(r#"
        INSERT INTO worker_ping
            (worker, ping_at, vcpus, memory_total, disk_total,
             cpu_usage_percent, memory_usage, disk_usage,
             current_job_id, occupancy_15s, occupancy_5m, occupancy_30m)
        VALUES ($1, now(), $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
        ON CONFLICT (worker) DO UPDATE SET
            ping_at = now(), vcpus = $2, memory_total = $3, disk_total = $4,
            cpu_usage_percent = $5, memory_usage = $6, disk_usage = $7,
            current_job_id = $8, occupancy_15s = $9, occupancy_5m = $10, occupancy_30m = $11
    "#, worker_name, m.vcpus, m.memory_total, m.disk_total,
        m.cpu_usage_percent, m.memory_usage, m.disk_usage,
        current_job_id, o15, o5, o30)
    .execute(db).await.ok();
}
```

#### API 端點

```rust
// crates/api/src/cluster.rs

/// 列出所有 worker 及其資源狀態
pub async fn list_workers(
    Path(workspace_id): Path<String>,
    State(db): State<PgPool>,
) -> Result<Json<Vec<WorkerInfo>>, ApiError> {
    let workers = sqlx::query_as!(WorkerInfo, r#"
        SELECT
            worker, tags, ip, sandbox_mode, current_job_id, jobs_completed,
            vcpus, memory_total, disk_total,
            cpu_usage_percent, memory_usage, disk_usage,
            occupancy_15s, occupancy_5m, occupancy_30m,
            EXTRACT(EPOCH FROM (now() - ping_at))::int AS last_ping_secs,
            CASE WHEN ping_at > now() - interval '30 seconds' THEN 'online' ELSE 'offline' END AS status
        FROM worker_ping
        ORDER BY worker
    "#).fetch_all(&db).await?;
    Ok(Json(workers))
}

/// 集群彙總
pub async fn cluster_summary(
    Path(workspace_id): Path<String>,
    State(db): State<PgPool>,
) -> Result<Json<ClusterSummary>, ApiError> {
    let summary = sqlx::query_as!(ClusterSummary, r#"
        SELECT
            COUNT(*) FILTER (WHERE ping_at > now() - interval '30 seconds') AS online_workers,
            COUNT(*) AS total_workers,
            COALESCE(SUM(vcpus), 0) AS total_vcpus,
            COALESCE(SUM(memory_total), 0) AS total_memory,
            COALESCE(SUM(disk_total), 0) AS total_disk,
            COALESCE(AVG(cpu_usage_percent), 0) AS avg_cpu_usage,
            COALESCE(SUM(memory_usage), 0) AS total_memory_usage,
            COALESCE(SUM(disk_usage), 0) AS total_disk_usage,
            COUNT(current_job_id) AS active_jobs
        FROM worker_ping
    "#).fetch_one(&db).await?;
    Ok(Json(summary))
}

#[derive(Serialize)]
pub struct ClusterSummary {
    pub online_workers: i64,
    pub total_workers: i64,
    pub total_vcpus: i64,
    pub total_memory: i64,       // bytes
    pub total_disk: i64,         // bytes
    pub avg_cpu_usage: f32,      // %
    pub total_memory_usage: i64, // bytes
    pub total_disk_usage: i64,   // bytes
    pub active_jobs: i64,
}
```

#### 前端 Cluster Dashboard

```
ClusterDashboard 頁面（/workers）：

┌──────────────────────────────────────────────────────────────┐
│ 🖥️ Cluster Overview                    Last updated: 3s ago  │
│                                                              │
│ ┌──────────┐ ┌──────────┐ ┌──────────┐ ┌──────────────────┐ │
│ │ Workers  │ │ CPU      │ │ Memory   │ │ Disk             │ │
│ │ 6/8      │ │ 38%      │ │ 24/64 GB │ │ 120/500 GB       │ │
│ │ online   │ │ avg      │ │ used     │ │ used             │ │
│ └──────────┘ └──────────┘ └──────────┘ └──────────────────┘ │
│                                                              │
│ Active Jobs: 12 │ Queue Depth: 3 │ Throughput: 45 jobs/min   │
│                                                              │
│ ┌────────┬──────┬────────────┬────────────┬─────────┬──────┐ │
│ │ Worker │ Tags │ CPU        │ Memory     │ Disk    │Status│ │
│ ├────────┼──────┼────────────┼────────────┼─────────┼──────┤ │
│ │ w-01   │ gpu  │ █████░ 62% │ ██████ 12G │ ███░ 45G│ 3/4 │ │
│ │ w-02   │ api  │ ██░░░░ 25% │ ███░░░  6G │ █░░ 20G │ 1/4 │ │
│ │ w-03   │ api  │ ░░░░░░  3% │ █░░░░░  2G │ █░░ 15G │idle │ │
│ │ w-04   │ gpu  │ ████░░ 55% │ █████░ 10G │ ██░ 30G │ 2/4 │ │
│ │ w-05   │ def  │ ███░░░ 40% │ ████░░  8G │ █░░ 18G │ 2/4 │ │
│ │ w-06   │ def  │ █░░░░░ 12% │ ██░░░░  4G │ █░░ 12G │ 1/4 │ │
│ │ w-07   │ def  │ ─ offline ─│────────────│─────────│ ❌  │ │
│ │ w-08   │ def  │ ─ offline ─│────────────│─────────│ ❌  │ │
│ └────────┴──────┴────────────┴────────────┴─────────┴──────┘ │
│                                                              │
│ ─── Per Tag Summary ───                                      │
│ ┌──────┬─────────┬──────┬────────┐                           │
│ │ Tag  │ Workers │ Jobs │ Avg CPU│                           │
│ ├──────┼─────────┼──────┼────────┤                           │
│ │ gpu  │ 2       │ 5    │ 58%    │                           │
│ │ api  │ 2       │ 1    │ 14%    │                           │
│ │ def  │ 2 (4)   │ 3    │ 26%    │                           │
│ └──────┴─────────┴──────┴────────┘                           │
└──────────────────────────────────────────────────────────────┘
```

### 3.12 Team + Folder + ACL + Quota 實作

**核心邏輯**：團隊管理、路徑級 ACL（正規化 folder_acl 表）、配額執行。
Schema 已在 1.1 定義，Auth middleware 已在 1.6 整合。
本節實作 CRUD API 和進階整合邏輯。

```rust
// crates/api/src/teams.rs

use crate::auth::AuthedUser;

/// 列出 workspace 中所有 team
pub async fn list_teams(
    State(state): State<AppState>,
    Path(workspace_id): Path<String>,
    Extension(user): Extension<AuthedUser>,
) -> Result<Json<Vec<TeamInfo>>, ApiError> {
    let teams = sqlx::query_as!(TeamInfo,
        r#"SELECT t.name, t.summary,
                  (SELECT array_agg(m.email) FROM team_member m
                   WHERE m.workspace_id = t.workspace_id AND m.team_name = t.name) as "members: Vec<String>"
           FROM team t
           WHERE t.workspace_id = $1
           ORDER BY t.name"#,
        workspace_id
    ).fetch_all(&state.db).await?;
    Ok(Json(teams))
}

/// Create team (admin only). Also creates a same-name folder with the team as owner.
pub async fn create_team(
    State(state): State<AppState>,
    Path(workspace_id): Path<String>,
    Extension(user): Extension<AuthedUser>,
    Json(req): Json<CreateTeamRequest>,
) -> Result<StatusCode, ApiError> {
    if !user.is_admin {
        return Err(ApiError::Forbidden("admin only".into()));
    }

    let mut tx = state.db.begin().await?;

    sqlx::query!(
        "INSERT INTO team (workspace_id, name, summary) VALUES ($1, $2, $3)",
        workspace_id, req.name, req.summary.unwrap_or_default()
    ).execute(&mut *tx).await?;

    // Auto-create same-name folder (convention: team "ml-team" -> folder "ml-team")
    sqlx::query!(
        "INSERT INTO folder (workspace_id, name, display_name)
         VALUES ($1, $2, $3)
         ON CONFLICT DO NOTHING",
        workspace_id, req.name, req.name,
    ).execute(&mut *tx).await?;

    // Grant the team owner role on its folder + manager role on team_acl
    let subject = format!("teams/{}", req.name);
    sqlx::query!(
        "INSERT INTO folder_acl (workspace_id, folder_name, subject, role)
         VALUES ($1, $2, $3, 'owner')
         ON CONFLICT DO NOTHING",
        workspace_id, req.name, subject,
    ).execute(&mut *tx).await?;

    sqlx::query!(
        "INSERT INTO team_acl (workspace_id, team_name, subject, role)
         VALUES ($1, $2, $3, 'manager')
         ON CONFLICT DO NOTHING",
        workspace_id, req.name, subject,
    ).execute(&mut *tx).await?;

    tx.commit().await?;
    Ok(StatusCode::CREATED)
}

/// Check if user can manage this team (admin or has 'manager' role in team_acl)
async fn require_team_manager(
    user: &AuthedUser,
    db: &PgPool,
    workspace_id: &str,
    team_name: &str,
) -> Result<(), ApiError> {
    if user.is_admin {
        return Ok(());
    }
    let has_access = sqlx::query_scalar!(
        "SELECT EXISTS(
            SELECT 1 FROM team_acl
            WHERE workspace_id = $1 AND team_name = $2 AND subject = ANY($3)
        ) as \"exists!: bool\"",
        workspace_id, team_name, &user.perm_subjects
    ).fetch_one(db).await?;

    if has_access {
        Ok(())
    } else {
        Err(ApiError::Forbidden("no permission to manage this team".into()))
    }
}

/// Add member to team (admin or team manager via team_acl)
pub async fn add_member(
    State(state): State<AppState>,
    Path((workspace_id, team_name)): Path<(String, String)>,
    Extension(user): Extension<AuthedUser>,
    Json(req): Json<AddMemberRequest>,
) -> Result<StatusCode, ApiError> {
    require_team_manager(&user, &state.db, &workspace_id, &team_name).await?;

    sqlx::query!(
        "INSERT INTO team_member (workspace_id, email, team_name)
         VALUES ($1, $2, $3) ON CONFLICT DO NOTHING",
        workspace_id, req.email, team_name
    ).execute(&state.db).await?;
    Ok(StatusCode::CREATED)
}

/// Remove member from team (admin or team manager via team_acl)
pub async fn remove_member(
    State(state): State<AppState>,
    Path((workspace_id, team_name, email)): Path<(String, String, String)>,
    Extension(user): Extension<AuthedUser>,
) -> Result<StatusCode, ApiError> {
    require_team_manager(&user, &state.db, &workspace_id, &team_name).await?;
    sqlx::query!(
        "DELETE FROM team_member WHERE workspace_id = $1 AND email = $2 AND team_name = $3",
        workspace_id, email, team_name
    ).execute(&state.db).await?;
    Ok(StatusCode::NO_CONTENT)
}

// crates/api/src/folders.rs

/// List folders visible to the user
pub async fn list_folders(
    State(state): State<AppState>,
    Path(workspace_id): Path<String>,
    Extension(user): Extension<AuthedUser>,
) -> Result<Json<Vec<FolderInfo>>, ApiError> {
    if user.is_admin {
        // Admin sees all folders with their ACL entries
        let folders = sqlx::query_as!(FolderInfo,
            "SELECT f.name, f.display_name,
                    COALESCE(
                        (SELECT json_agg(json_build_object('subject', a.subject, 'role', a.role))
                         FROM folder_acl a
                         WHERE a.workspace_id = f.workspace_id AND a.folder_name = f.name),
                        '[]'::json
                    ) as \"acl!: serde_json::Value\"
             FROM folder f WHERE f.workspace_id = $1
             ORDER BY f.name",
            workspace_id
        ).fetch_all(&state.db).await?;
        return Ok(Json(folders));
    }

    // Non-admin: only see folders they have access to (pre-computed in auth middleware)
    let visible_names: Vec<&str> = user.folders.keys().map(|s| s.as_str()).collect();
    let folders = sqlx::query_as!(FolderInfo,
        "SELECT f.name, f.display_name,
                COALESCE(
                    (SELECT json_agg(json_build_object('subject', a.subject, 'role', a.role))
                     FROM folder_acl a
                     WHERE a.workspace_id = f.workspace_id AND a.folder_name = f.name),
                    '[]'::json
                ) as \"acl!: serde_json::Value\"
         FROM folder f
         WHERE f.workspace_id = $1 AND f.name = ANY($2)",
        workspace_id, &visible_names
    ).fetch_all(&state.db).await?;
    Ok(Json(folders))
}

#[derive(serde::Deserialize)]
pub struct AclEntry {
    pub subject: String,
    pub role: String,  // "owner", "writer", "reader"
}

#[derive(serde::Deserialize)]
pub struct CreateFolderRequest {
    pub name: String,
    pub display_name: Option<String>,
    pub acl: Option<Vec<AclEntry>>,
}

/// Create folder (admin only) with optional initial ACL entries
pub async fn create_folder(
    State(state): State<AppState>,
    Path(workspace_id): Path<String>,
    Extension(user): Extension<AuthedUser>,
    Json(req): Json<CreateFolderRequest>,
) -> Result<StatusCode, ApiError> {
    if !user.is_admin {
        return Err(ApiError::Forbidden("admin only".into()));
    }

    let mut tx = state.db.begin().await?;

    sqlx::query!(
        "INSERT INTO folder (workspace_id, name, display_name) VALUES ($1, $2, $3)",
        workspace_id, req.name,
        req.display_name.as_deref().unwrap_or(&req.name),
    ).execute(&mut *tx).await?;

    // Insert initial ACL entries
    if let Some(acl) = &req.acl {
        for entry in acl {
            sqlx::query!(
                "INSERT INTO folder_acl (workspace_id, folder_name, subject, role)
                 VALUES ($1, $2, $3, $4)",
                workspace_id, req.name, entry.subject, entry.role,
            ).execute(&mut *tx).await?;
        }
    }

    tx.commit().await?;
    Ok(StatusCode::CREATED)
}

#[derive(serde::Deserialize)]
pub struct UpdateFolderAclRequest {
    /// ACL entries to set (upsert). Omitted subjects are unchanged.
    pub set: Option<Vec<AclEntry>>,
    /// Subjects to remove from ACL entirely.
    pub remove: Option<Vec<String>>,
}

/// Update folder ACL (folder owners or admin only)
pub async fn update_folder_acl(
    State(state): State<AppState>,
    Path((workspace_id, folder_name)): Path<(String, String)>,
    Extension(user): Extension<AuthedUser>,
    Json(req): Json<UpdateFolderAclRequest>,
) -> Result<StatusCode, ApiError> {
    // Check folder ownership using AuthedUser (no extra DB query needed)
    if !user.is_folder_owner(&folder_name) {
        return Err(ApiError::Forbidden(
            "only folder owners or admins can update ACL".into()
        ));
    }

    // Verify folder exists
    sqlx::query!(
        "SELECT 1 as _e FROM folder WHERE workspace_id = $1 AND name = $2",
        workspace_id, folder_name
    ).fetch_optional(&state.db).await?
     .ok_or(ApiError::NotFound)?;

    let mut tx = state.db.begin().await?;

    // Upsert ACL entries
    if let Some(entries) = &req.set {
        for entry in entries {
            sqlx::query!(
                "INSERT INTO folder_acl (workspace_id, folder_name, subject, role)
                 VALUES ($1, $2, $3, $4)
                 ON CONFLICT (workspace_id, folder_name, subject)
                 DO UPDATE SET role = EXCLUDED.role",
                workspace_id, folder_name, entry.subject, entry.role,
            ).execute(&mut *tx).await?;
        }
    }

    // Remove subjects
    if let Some(subjects) = &req.remove {
        sqlx::query!(
            "DELETE FROM folder_acl
             WHERE workspace_id = $1 AND folder_name = $2 AND subject = ANY($3)",
            workspace_id, folder_name, subjects,
        ).execute(&mut *tx).await?;
    }

    tx.commit().await?;
    Ok(StatusCode::OK)
}

// crates/api/src/teams.rs（配額相關）

/// 設定團隊配額（admin only）
pub async fn set_quota(
    State(state): State<AppState>,
    Path((workspace_id, team_name)): Path<(String, String)>,
    Extension(user): Extension<AuthedUser>,
    Json(req): Json<SetQuotaRequest>,
) -> Result<StatusCode, ApiError> {
    if !user.is_admin {
        return Err(ApiError::Forbidden("admin only".into()));
    }
    sqlx::query!(
        "INSERT INTO team_quota (workspace_id, team_name, max_concurrent_runs,
         max_cpus, max_memory_mb,
         max_daily_runs, max_storage_bytes, max_run_timeout_secs)
         VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
         ON CONFLICT (workspace_id, team_name) DO UPDATE SET
           max_concurrent_runs = EXCLUDED.max_concurrent_runs,
           max_cpus = EXCLUDED.max_cpus,
           max_memory_mb = EXCLUDED.max_memory_mb,
           max_daily_runs = EXCLUDED.max_daily_runs,
           max_storage_bytes = EXCLUDED.max_storage_bytes,
           max_run_timeout_secs = EXCLUDED.max_run_timeout_secs",
        workspace_id, team_name,
        req.max_concurrent_runs, req.max_cpus, req.max_memory_mb,
        req.max_daily_runs, req.max_storage_bytes, req.max_run_timeout_secs,
    ).execute(&state.db).await?;
    Ok(StatusCode::OK)
}

/// 查詢配額使用量（admin + team member）
pub async fn get_quota(
    State(state): State<AppState>,
    Path((workspace_id, team_name)): Path<(String, String)>,
    Extension(user): Extension<AuthedUser>,
) -> Result<Json<QuotaUsage>, ApiError> {
    if !user.is_admin && !user.teams.contains(&team_name) {
        return Err(ApiError::Forbidden("not a member of this team".into()));
    }

    let quota = sqlx::query_as!(TeamQuota,
        "SELECT * FROM team_quota WHERE workspace_id = $1 AND team_name = $2",
        workspace_id, team_name
    ).fetch_optional(&state.db).await?;

    // 查詢目前使用量
    let running_runs: i64 = sqlx::query_scalar!(
        "SELECT COUNT(*) FROM run_queue rq
         JOIN run r ON r.id = rq.id
         WHERE r.workspace_id = $1 AND r.team_owner = $2 AND rq.running = TRUE",
        workspace_id, team_name
    ).fetch_one(&state.db).await?.unwrap_or(0);

    // 查詢目前佔用的 CPU 量
    let used_cpus: f64 = sqlx::query_scalar!(
        "SELECT COALESCE(SUM(r.cpus::DOUBLE PRECISION), 0) FROM run_queue rq
         JOIN run r ON r.id = rq.id
         WHERE r.workspace_id = $1 AND r.team_owner = $2 AND rq.running = TRUE",
        workspace_id, team_name
    ).fetch_one(&state.db).await?.unwrap_or(0.0);

    // 查詢目前佔用的 RAM (MB)
    let used_memory_mb: i64 = sqlx::query_scalar!(
        "SELECT COALESCE(SUM(r.memory_mb), 0) FROM run_queue rq
         JOIN run r ON r.id = rq.id
         WHERE r.workspace_id = $1 AND r.team_owner = $2 AND rq.running = TRUE",
        workspace_id, team_name
    ).fetch_one(&state.db).await?.unwrap_or(0);

    let today_runs: i64 = sqlx::query_scalar!(
        "SELECT COUNT(*) FROM run r
         WHERE r.workspace_id = $1 AND r.team_owner = $2
           AND r.created_at >= CURRENT_DATE",
        workspace_id, team_name
    ).fetch_one(&state.db).await?.unwrap_or(0);

    // 檔案儲存用量（走 File Storage API）
    let storage_bytes: i64 = sqlx::query_scalar!(
        "SELECT COALESCE(SUM(length(content)), 0) FROM flow_file
         WHERE workspace_id = $1 AND flow_path LIKE $2",
        workspace_id, format!("folders/{}/%", team_name)
    ).fetch_one(&state.db).await?.unwrap_or(0);

    Ok(Json(QuotaUsage {
        quota,
        current_running_runs: running_runs,
        current_used_cpus: used_cpus,
        current_used_memory_mb: used_memory_mb,
        current_daily_runs: today_runs,
        current_storage_bytes: storage_bytes,
    }))
}
```

**Resource / Variable 的 Folder ACL 整合：**

Resource 和 Variable 都使用 `path` 欄位（如 `folders/ml-team/prod_db`），因此權限控制直接複用 `AuthedUser.can_read(path)` / `can_write(path)`。

```rust
// crates/api/src/resources.rs（部分修改）

pub async fn get_resource_value(
    State(state): State<AppState>,
    Path((workspace_id, path)): Path<(String, String)>,
    Extension(user): Extension<AuthedUser>,
) -> Result<Json<serde_json::Value>, ApiError> {
    // 讀取加密值需要路徑讀取權限
    user.require_reader(&path)?;
    // ... 解密邏輯不變 ...
}

pub async fn create_resource(
    State(state): State<AppState>,
    Path(workspace_id): Path<String>,
    Extension(user): Extension<AuthedUser>,
    Json(req): Json<CreateResourceRequest>,
) -> Result<StatusCode, ApiError> {
    // 建立資源需要路徑寫入權限
    user.require_writer(&req.path)?;
    // ... 建立邏輯不變 ...
}
```

**File Storage 的團隊儲存配額：**

```rust
// crates/api/src/files.rs（部分修改）

pub async fn upload_file(/* ... */) -> Result<Json<FileRef>, ApiError> {
    // ... 既有邏輯 ...

    // 團隊儲存配額檢查
    if let Some(team_name) = extract_team_owner(&req.path) {
        let quota = sqlx::query!(
            "SELECT max_storage_bytes FROM team_quota
             WHERE workspace_id = $1 AND team_name = $2",
            workspace_id, team_name
        ).fetch_optional(&state.db).await?;

        if let Some(q) = quota {
            if let Some(max_bytes) = q.max_storage_bytes {
                let current: i64 = get_team_storage_usage(&state.db, &workspace_id, &team_name).await?;
                if current + file_size as i64 > max_bytes {
                    return Err(ApiError::PayloadTooLarge(format!(
                        "team '{}' storage quota exceeded ({}/{} bytes)",
                        team_name, current, max_bytes
                    )));
                }
            }
        }
    }
    // ... 上傳邏輯不變 ...
}
```

**Cluster Dashboard 的團隊資源用量視角：**

```rust
// crates/api/src/cluster.rs（新增 endpoint）

/// 團隊資源用量摘要（哪個團隊用了多少 CPU 時間 / Run 數 / 儲存）
pub async fn team_resource_usage(
    State(state): State<AppState>,
    Path(workspace_id): Path<String>,
    Extension(user): Extension<AuthedUser>,
) -> Result<Json<Vec<TeamResourceUsage>>, ApiError> {
    if !user.is_admin {
        return Err(ApiError::Forbidden("admin only".into()));
    }

    let usage = sqlx::query_as!(TeamResourceUsage,
        r#"SELECT
             r.team_owner as "team_name!",
             COUNT(DISTINCT r.id) as "total_runs!: i64",
             COUNT(DISTINCT rq.id) FILTER (WHERE rq.running = TRUE) as "running_runs!: i64",
             COALESCE(SUM(rc.duration_ms), 0) as "total_duration_ms!: i64",
             COALESCE(AVG(rc.duration_ms), 0) as "avg_duration_ms!: f64"
           FROM run r
           LEFT JOIN run_queue rq ON rq.id = r.id
           LEFT JOIN run_completed rc ON rc.id = r.id
           WHERE r.workspace_id = $1
             AND r.team_owner IS NOT NULL
             AND r.created_at >= now() - interval '24 hours'
           GROUP BY r.team_owner
           ORDER BY "total_runs!: i64" DESC"#,
        workspace_id
    ).fetch_all(&state.db).await?;

    Ok(Json(usage))
}
```

**前端 UI 設計：**

```
┌── Groups & Folders Settings ──────────────────────────────────┐
│                                                                │
│  ┌─ Groups ─────────────────────────────────────────────────┐  │
│  │ [+ New Group]                                            │  │
│  │ ┌────────────┬──────────┬─────────┬───────────────────┐  │  │
│  │ │ Group      │ Members  │ Quota   │ Usage             │  │  │
│  │ ├────────────┼──────────┼─────────┼───────────────────┤  │  │
│  │ │ ml-team    │ 5        │ 10 conc │ ██████░░ 6/10     │  │  │
│  │ │ data-eng   │ 3        │ 20 conc │ ██░░░░░░ 4/20     │  │  │
│  │ │ sre        │ 2        │ ∞       │ █░░░░░░░ 2        │  │  │
│  │ └────────────┴──────────┴─────────┴───────────────────┘  │  │
│  └──────────────────────────────────────────────────────────┘  │
│                                                                │
│  ┌─ Folders (Path ACL) ─────────────────────────────────────┐  │
│  │ [+ New Folder]                                           │  │
│  │ ┌────────────┬──────────┬────────────────────────────┐   │  │
│  │ │ Folder     │ Owners   │ Permissions                │   │  │
│  │ ├────────────┼──────────┼────────────────────────────┤   │  │
│  │ │ ml-team    │ teams/sre│ teams/ml-team: RW, users/bob: R│  │  │
│  │ │ production │ teams/sre│ teams/sre: RW, teams/dev: R   │  │  │
│  │ │ shared     │ (admin)  │ teams/all: RW                 │  │  │
│  │ └────────────┴──────────┴────────────────────────────┘   │  │
│  └──────────────────────────────────────────────────────────┘  │
│                                                                │
│  ┌─ Team Resource Usage (24h) ──────────────────────────────┐  │
│  │ ┌────────────┬──────┬─────────┬──────────┬────────────┐  │  │
│  │ │ Group      │ Jobs │ Running │ Avg Dur  │ Storage    │  │  │
│  │ ├────────────┼──────┼─────────┼──────────┼────────────┤  │  │
│  │ │ ml-team    │ 156  │ 6       │ 45.2s    │ 2.3 GB     │  │  │
│  │ │ data-eng   │ 89   │ 4       │ 12.1s    │ 800 MB     │  │  │
│  │ │ sre        │ 23   │ 2       │ 3.4s     │ 100 MB     │  │  │
│  │ └────────────┴──────┴─────────┴──────────┴────────────┘  │  │
│  └──────────────────────────────────────────────────────────┘  │
└────────────────────────────────────────────────────────────────┘
```

### 3.13 驗證方式

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
10. Resource: 建立 postgres resource → script 參數用 $res:path → 執行後拿到解密的 dict
11. Variable: 建立 secret variable → resource 內引用 $var:path → 確認嵌套替換正確
12. 加密驗證: 直接查 DB → 確認 value_encrypted 是亂碼（非明文）
13. Webhook sync + resource: webhook 觸發的 job 也能正確解析 $res: 引用
14. Deploy Gate 無 policy: folders/sandbox/test flow → 直接 deploy，無需審核
15. Deploy Gate 有 policy: 設 folders/prod/* min=2 → 修改 flow → 自動建立 deploy_request
16. Deploy Gate 審核流程: approver A approve → 仍 pending (1/2) → approver B approve → status=approved
17. Deploy Gate 駁回: 任一 approver reject → status=rejected，附 comment
18. Deploy Gate diff: 確認 /diff API 回傳正確的舊版 vs 新版差異
19. Deploy Gate auto_deploy: 設 auto_deploy=true → 達到 min_approvals 後自動部署（不需手動按 deploy）
20. File Upload S3: 設 storage_mode=s3 → 上傳 CSV → 確認 S3 有檔案 → 下載比對一致
21. File Upload Local: 設 storage_mode=local → 上傳 JSON → 確認本地目錄有檔案
22. File + Script: 上傳 data.csv → Script 參數引用 FileRef → 確認 Worker 自動下載到 job_dir/input/
23. File Preview: 上傳 CSV → GET /files/preview → 確認回傳前 100 行表格資料
24. File Size Limit: 設 max_file_size=1MB → 上傳 2MB 檔案 → 確認回傳 413
25. Cluster Dashboard: 啟動 3 個 worker → GET /workers/list → 確認 3 個都顯示 CPU/記憶體/磁碟
26. Cluster Summary: GET /workers/summary → 確認 total_vcpus/total_memory/total_disk 正確加總
27. Worker Offline: 停止 1 個 worker → 等 30 秒 → 確認 status 變 offline
28. Sandbox Disk Limit: nsjail tmpfs_size=50MB → 寫 100MB → 確認 ENOSPC 錯誤
29. Team CRUD: 建立 "ml-team" team → 加成員 alice, bob → 確認 list_teams 回傳正確
30. Folder ACL 讀寫: alice 屬 ml-team, bob 屬 data-eng → folders/ml-team/ script → alice 可寫, bob 不可寫
31. Folder ACL 唯讀: 設 teams/data-eng → false（唯讀）→ bob 可讀 folders/ml-team/ 的 script，但不能修改
32. 個人路徑隔離: alice 建 users/alice/my_script → bob 無法讀取或執行
33. Admin 無視 ACL: admin 可存取任何路徑下的 script/flow/resource
34. 團隊並發配額: 設 ml-team max_concurrent_runs=2 → 同時推 5 個 folders/ml-team/ run → 最多 2 個同時跑
35. 團隊每日配額: 設 ml-team max_daily_runs=10 → 跑到第 11 個 → 確認被拒絕
36. 配額使用量 API: GET /team_quotas/ml-team → 確認 current_running_runs + current_used_cpus + current_used_memory_mb + current_daily_runs 正確
42. 團隊 CPU 配額: 設 ml-team max_cpus=4 → 推 cpus=2 的 run 3 個 → 最多 2 個同時跑（佔 4 cpus），第 3 個延後
43. CPU 配額 push 拒絕: 設 ml-team max_cpus=4 → 已有 3 cpus running → 推 cpus=2 → 被 submit_run reject
44. 併發+CPU 雙重限制: 設 max_concurrent=5, max_cpus=3 → 推 5 個 1-cpu run → 最多 3 個跑（受 CPU 限制）
45. RAM 配額: 設 ml-team max_memory_mb=8192 → 推 memory_mb=4096 的 run 3 個 → 最多 2 個同時跑（佔 8192 MB），第 3 個延後
37. 自動建 folder: 建立 team "sre" → 確認 folder "sre" 自動建立且 teams/sre 有讀寫權
38. Resource ACL: 建 folders/ml-team/prod_db resource → alice 可讀, bob（非 ml-team）不可讀
39. File Storage 配額: 設 ml-team max_storage_bytes=100MB → 上傳 110MB → 確認被拒絕
40. 團隊資源用量: GET /workers/team_usage → 確認每個 team 的 run 數、平均耗時、儲存量
41. Deploy Gate + ACL: folders/production/ 需審核 → 非 production folder member 無法建 deploy_request
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

### 4.4 Dedicated Worker + Runner Groups（消除冷啟動）

每個 job 都 spawn 新子程序，冷啟動佔比高。兩種模式從一開始就設計好。
與**同步執行模式**搭配使用時效果最大——`run_wait_result` + Dedicated Worker = **~5ms 開銷**，
把 CoveFlow 變成一個延遲媲美直接呼叫函式的 API 服務：

```
Normal 模式：  spawn python3 → import → main() → exit    ~65ms（冷啟動 60ms + 執行 5ms）
Dedicated：    python3 常駐 → loop { 收 job → main() }   ~5ms（13x 加速）
Runner Group： python3 常駐 + 多 script 共用同一 runtime  ~5ms + 記憶體省 N 倍
```

**兩種 Dedicated 模式的差異：**

| | Dedicated（1:1） | Runner Group（N:1） |
|---|---|---|
| 對應關係 | 1 個 script = 1 個常駐程序 | N 個 script 共用 1 個常駐程序 |
| 記憶體 | 每個 script 獨立載入依賴 | 同 group 的 script 共享依賴 |
| 隔離性 | 完全隔離（不同程序） | 弱（同程序，全域狀態共享） |
| 適用場景 | 單一高頻 script | 同團隊、同依賴的多個 script |

**Runner Group 不適合與 K8s Pod Sandbox 組合**——Dedicated 的意義是「不重啟 runtime」，每 job 開新 Pod 就失去意義。

**組合矩陣：**

| 執行模型 | None | nsjail | K8s Pod | Firecracker (Phase 4+) |
|---------|------|--------|---------|----------------------|
| Normal | 開發 | VM 生產 | 重型/GPU | 多租戶 SaaS |
| Dedicated 1:1 | 開發 | VM 高頻 | 不適用 | 不適用 |
| Runner Group | 開發 | VM 高頻 | 不適用 | 不適用 |

#### Schema

```sql
-- Runner Group 定義
CREATE TABLE runner_group (
    workspace_id VARCHAR(50) NOT NULL REFERENCES workspace(id),
    id VARCHAR(100) NOT NULL,            -- "data-team-pandas"
    name VARCHAR(255) NOT NULL,
    language VARCHAR(20) NOT NULL,       -- python3, typescript
    dependencies TEXT[],                 -- ["pandas", "numpy", "requests"]
    max_concurrent INTEGER DEFAULT 8,   -- 同時服務幾個 job
    idle_timeout_secs INTEGER DEFAULT 300,  -- 閒置多久回收
    created_by VARCHAR(255) NOT NULL,
    PRIMARY KEY (workspace_id, id)
);

-- Script 可以指定歸屬哪個 Runner Group
ALTER TABLE script ADD COLUMN runner_group_id VARCHAR(100);
```

#### Rust 實作

```rust
// crates/worker/src/dedicated.rs

use tokio::sync::mpsc;

/// 單一 script 的 Dedicated Runner（1:1 模式）
pub struct DedicatedRunner {
    tx: mpsc::Sender<DedicatedJob>,
    handle: tokio::task::JoinHandle<()>,
    language: ScriptLang,
    script_path: String,
}

/// Runner Group（N:1 模式）— 多個 script 共用一個 runtime
pub struct RunnerGroup {
    tx: mpsc::Sender<GroupJob>,
    handle: tokio::task::JoinHandle<()>,
    language: ScriptLang,
    group_id: String,
    loaded_scripts: std::sync::Arc<tokio::sync::RwLock<HashSet<String>>>,
}

struct DedicatedJob {
    args: serde_json::Value,
    result_tx: tokio::sync::oneshot::Sender<Result<serde_json::Value>>,
}

struct GroupJob {
    script_path: String,    // 要執行哪個 script
    script_content: String, // 第一次載入時需要
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

impl RunnerGroup {
    pub async fn spawn(
        language: ScriptLang, group_id: &str, dependencies: &[String],
        job_dir: &str, sandbox: &dyn Sandbox,
    ) -> Result<Self> {
        let (tx, mut rx) = mpsc::channel::<GroupJob>(64);
        let loaded = std::sync::Arc::new(tokio::sync::RwLock::new(HashSet::new()));

        // Runner Group 的 Python 常駐程序：用 importlib 動態載入不同 script
        let runner_code = match language {
            ScriptLang::Python3 => r#"
import json, sys, importlib, importlib.util, os

loaded_modules = {}

while True:
    line = sys.stdin.readline()
    if not line:
        break
    request = json.loads(line)
    script_name = request["script"]
    args = request["args"]

    # 動態載入 script（第一次寫入檔案 + import，之後用快取）
    if script_name not in loaded_modules:
        if "content" in request:
            path = f"/tmp/group/{script_name}"
            os.makedirs(os.path.dirname(path), exist_ok=True)
            with open(path, "w") as f:
                f.write(request["content"])
        spec = importlib.util.spec_from_file_location(script_name, f"/tmp/group/{script_name}")
        mod = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(mod)
        loaded_modules[script_name] = mod

    try:
        result = loaded_modules[script_name].main(**args)
        print(json.dumps({"ok": result}), flush=True)
    except Exception as e:
        print(json.dumps({"err": str(e)}), flush=True)
"#.to_string(),
            _ => return Err(Error::UnsupportedLanguage(language)),
        };

        let handle = tokio::spawn(async move {
            while let Some(job) = rx.recv().await {
                // 寫 {script, content?, args} 到 stdin → 讀 result → 回傳
            }
        });

        Ok(Self { tx, handle, language, group_id: group_id.to_string(), loaded_scripts: loaded })
    }

    pub async fn execute(
        &self, script_path: &str, script_content: &str, args: serde_json::Value,
    ) -> Result<serde_json::Value> {
        let (result_tx, result_rx) = tokio::sync::oneshot::channel();
        self.tx.send(GroupJob {
            script_path: script_path.to_string(),
            script_content: script_content.to_string(),
            args, result_tx,
        }).await?;
        result_rx.await?
    }
}

/// Worker 主迴圈中選擇執行方式
async fn dispatch_job(job: &QueuedJob, runners: &RunnerManager, sandbox: &dyn Sandbox) {
    if let Some(group_id) = &job.runner_group_id {
        // Runner Group 模式
        let group = runners.get_or_create_group(group_id).await;
        let result = group.execute(&job.script_path, &job.content, job.args.clone()).await;
    } else if runners.has_dedicated(&job.script_path) {
        // Dedicated 1:1 模式
        let runner = runners.get_dedicated(&job.script_path);
        let result = runner.execute(job.args.clone()).await;
    } else {
        // Normal 模式：spawn 新子程序
        handle_job(job, sandbox).await;
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

// POST /api/workspaces/{ws}/data/preview
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
      const resp = await fetch(`/api/workspaces/${workspaceId}/data/preview`, {
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
- 更多 Trigger 類型：Kafka、MQTT、PostgreSQL CDC、S3 事件（基於 Phase 3 的 Trigger Trait 擴展）
- App Builder（低代碼 UI）
- Firecracker microVM 沙箱完整實作（Sandbox trait 已預留，Phase 4+ 實作 FirecrackerSandbox）
- CPU Pinning（per-job CPU affinity，NUMA 感知，ML inference 場景）
- Marketplace（分享自訂節點和 Flow 範本）
- Software-Defined Assets（學 Dagster，將 data lineage 做為 first-class 概念）

---

## 附錄 A：Sandbox 三模式詳細實作

### A.1 nsjail（Phase 1 預設）

見 [04-worker-executor.md](./04-worker-executor.md) 的詳細分析。nsjail 提供 cgroup + namespace 級隔離，是 VM/Bare Metal 部署的首選。

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

        // Resource-based：用 SandboxResources 設定 cgroup 限制
        let nsjail_timeout = ctx.timeout_secs + 15;
        let config_content = template
            .replace("{JOB_DIR}", &ctx.job_dir)
            .replace("{TIMEOUT}", &nsjail_timeout.to_string())
            .replace("{CLONE_NEWUSER}", "true")
            .replace("{TMPFS_SIZE}", &ctx.resource_limits.disk.to_string())
            .replace("{MEMORY_LIMIT}", &ctx.resource_limits.memory.to_string())
            .replace("{CPU_MS_PER_SEC}", &((ctx.resource_limits.cpus * 1000.0) as u64).to_string());

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

### A.2 K8s Pod（Phase 3+）

```rust
use k8s_openapi::api::core::v1::Pod;
use kube::{Api, Client, api::PostParams};

pub struct K8sPodSandbox { client: Client, config: K8sPodConfig }

#[async_trait]
impl Sandbox for K8sPodSandbox {
    async fn execute(&self, ctx: &SandboxContext) -> Result<SandboxResult, SandboxError> {
        let pods: Api<Pod> = Api::namespaced(self.client.clone(), &self.config.namespace);
        let image = ctx.custom_image.as_deref().unwrap_or(&self.config.default_image);
        let pod_name = format!("cf-job-{}", ctx.job_id);

        // Resource-based：用 SandboxResources 設定 Pod resource requests & limits
        let cpu_limit = format!("{}m", (ctx.resource_limits.cpus * 1000.0) as u64);
        let mem_limit = format!("{}Mi", ctx.resource_limits.memory / 1024 / 1024);
        let disk_limit = format!("{}Mi", ctx.resource_limits.disk / 1024 / 1024);

        // requests = limits × ratio（ratio=1.0 → Guaranteed QoS）
        let ratio = self.config.request_ratio;
        let cpu_req = format!("{}m", (ctx.resource_limits.cpus * 1000.0 * ratio) as u64);
        let mem_req = format!("{}Mi", (ctx.resource_limits.memory as f64 * ratio as f64) as u64 / 1024 / 1024);

        let pod: Pod = serde_json::from_value(serde_json::json!({
            "apiVersion": "v1", "kind": "Pod",
            "metadata": {
                "name": pod_name, "namespace": self.config.namespace,
                "labels": { "app": "coveflow", "job-id": ctx.job_id.to_string() }
            },
            "spec": {
                "restartPolicy": "Never",
                "serviceAccountName": self.config.service_account,
                "containers": [{
                    "name": "job", "image": image,
                    "command": [ctx.command.clone()], "args": ctx.args.clone(),
                    "env": ctx.env.iter().map(|(k, v)| serde_json::json!({"name": k, "value": v})).collect::<Vec<_>>(),
                    "resources": {
                        "requests": { "cpu": cpu_req, "memory": mem_req, "ephemeral-storage": disk_limit },
                        "limits":   { "cpu": cpu_limit, "memory": mem_limit, "ephemeral-storage": disk_limit },
                    },
                    "volumeMounts": [{ "name": "job-data", "mountPath": "/tmp/job" }]
                }],
                // emptyDir：ephemeral 可寫 volume，sizeLimit 對應 job 宣告的 disk
                // （原本用 ConfigMap，但 ConfigMap 唯讀且上限 1MB，不適合當工作目錄）
                "volumes": [{ "name": "job-data", "emptyDir": { "sizeLimit": disk_limit } }],
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

### A.3 Firecracker microVM（Phase 4+ Placeholder）

Firecracker 是 AWS Lambda 底層使用的 microVM 技術，啟動延遲 ~125ms，提供硬體虛擬化級別的隔離。

```rust
/// Phase 4+ 才實作，目前只有 struct + trait placeholder
pub struct FirecrackerSandbox { config: FirecrackerConfig }

#[async_trait]
impl Sandbox for FirecrackerSandbox {
    async fn execute(&self, ctx: &SandboxContext) -> Result<SandboxResult, SandboxError> {
        // Phase 4+ 實作：
        // 1. 建立 microVM（firecracker API socket）
        // 2. 設定 vCPU/memory（從 ctx.resource_limits）
        // 3. 掛載 rootfs + job_dir（virtio-blk）
        // 4. 啟動 VM → 執行程式碼 → 讀取結果
        // 5. 銷毀 VM
        Err(SandboxError::Unavailable(
            "Firecracker sandbox not yet implemented (Phase 4+)".into()
        ))
    }

    async fn health_check(&self) -> Result<(), SandboxError> {
        Err(SandboxError::Unavailable("Firecracker not yet implemented".into()))
    }

    fn name(&self) -> &str { "firecracker" }
}

#[derive(Debug, Clone, serde::Deserialize)]
pub struct FirecrackerConfig {
    pub firecracker_path: String,     // 預設 "firecracker"
    pub kernel_image: String,         // vmlinux 路徑
    pub rootfs_image: String,         // ext4 rootfs 路徑
    pub api_socket_dir: String,       // /tmp/coveflow/fc/
    pub vcpu_count: u32,              // 預設由 job.cpus 決定
    pub mem_size_mib: u32,            // 預設由 job.memory_mb 決定
}
```

### A.4 Config 型別定義

```rust
#[derive(Debug, Clone, serde::Deserialize)]
pub struct NsjailConfig {
    pub nsjail_path: String,      // 預設 "nsjail"
    pub clone_newnet: bool,       // 預設 false（網路隔離）
}

#[derive(Debug, Clone, serde::Deserialize)]
pub struct K8sPodConfig {
    pub namespace: String,        // "coveflow-jobs"
    pub default_image: String,
    pub request_ratio: f32,       // requests = limits × ratio，預設 1.0（Guaranteed QoS）
    pub service_account: Option<String>,
    pub node_selector: Option<std::collections::HashMap<String, String>>,
    pub image_pull_secrets: Vec<String>,
    pub auto_cleanup: bool,
}
// request_ratio 說明：
//   1.0 → requests = limits（Guaranteed QoS，生產預設）
//   0.5 → requests = limits × 0.5（Burstable QoS，允許超售）
//   原本的靜態 cpu_request / memory_request 已移除，
//   因為每個 job 資源需求不同，靜態值會導致 scheduler 錯誤排程

// NsjailConfig 不再需要 memory_limit / cpu_time_limit / tmpfs_size，
// 這些由 Resource-based 模型的 SandboxResources 動態提供：
//   ctx.resource_limits.memory  → nsjail cgroup 限制（直接來自 job.memory_mb）
//   ctx.resource_limits.disk    → nsjail tmpfs_size（直接來自 job.disk_mb）
//   ctx.resource_limits.cpus    → nsjail cgroup CPU quota（直接來自 job.cpus）
//   ctx.resource_limits.timeout → nsjail timeout
```

### A.5 設定檔範例

```toml
# coveflow.toml

[worker]
name = "worker-01"
tags = ["default"]

# 資源總量（省略 = 自動偵測 - reserved）
# total_cpus = 8             # 手動指定
# total_memory_mb = 32768    # 手動指定
# total_disk_mb = 102400     # 手動指定

# 每 job 預設上限（job 沒設定時用這個）
default_job_cpus = 1
default_job_memory_mb = 512
default_job_disk_mb = 1024
job_timeout_secs = 300

# 同機部署時的資源預留（省略時為 0）
[worker.reserved]
cpu = 1                          # 預留給 API Server / OS 的 CPU
memory_mb = 1024                 # 預留 memory (MB)
disk_mb = 0                      # 預留 disk (MB)

# === 沙箱模式 1：nsjail（VM/Bare Metal 預設）===
[worker.sandbox.nsjail]
nsjail_path = "nsjail"
clone_newnet = false

# === 沙箱模式 2：K8s Pod（Phase 3+）===
[worker.sandbox.k8s_pod]
namespace = "coveflow-jobs"
default_image = "coveflow/python-runner:3.12"
cpu_request = "100m"
memory_request = "256Mi"
ephemeral_storage_limit = "1Gi"
service_account = "coveflow-job-runner"
auto_cleanup = true

[worker.sandbox.k8s_pod.node_selector]
"node-type" = "compute"

# === 沙箱模式 3：Firecracker（Phase 4+，目前 placeholder）===
# [worker.sandbox.firecracker]
# firecracker_path = "firecracker"
# kernel_image = "/opt/coveflow/vmlinux"
# rootfs_image = "/opt/coveflow/rootfs.ext4"
```

### A.6 各模式適用場景

```
生產環境（VM / Bare Metal）：
  → nsjail（預設）+ k8s_pod（需要自訂 image / GPU 的 job）

K8s 叢集：
  → k8s_pod（所有 job 都走 Pod，或搭配 nsjail 混合使用）

多租戶 SaaS（Phase 4+，最強隔離需求）：
  → firecracker（硬體虛擬化，每 job 一個 microVM）

macOS / Windows 開發環境：
  → none（不隔離，開發時直接 spawn 子程序）
```

### A.7 擴展新模式的步驟

新增沙箱模式只需 4 步：

```
1. 新增 SandboxMode variant：
   enum SandboxMode { ..., NewMode(NewModeConfig) }

2. 實作 Sandbox trait：
   impl Sandbox for NewModeSandbox { ... }

3. 新增 Config：
   struct NewModeConfig { ... }

4. 在 SandboxRouter 加一個 field + match arm：
   pub struct SandboxRouter { ..., new_mode: Option<NewModeSandbox> }
   match tag { "new_mode" => ..., }

不需修改任何 executor（Python/TS/Bash）程式碼。
```

---

## Appendix B: Architecture Overview Diagram

```
┌──────────────────────────────────────────────────────────────────────────────────┐
│                            CLUSTER (CoveFlow Deployment)                         │
│                                                                                  │
│  ┌─ Workspace ───────────────────────────────────────────────────────────────┐   │
│  │                                                                           │   │
│  │  ┌─ Groups (Teams) ────────────────────────────────────────────────────┐  │   │
│  │  │  ml-team: [alice, bob]          data-eng: [charlie, dave]           │  │   │
│  │  │       │                                │                            │  │   │
│  │  │       ▼                                ▼                            │  │   │
│  │  │  ┌─ Quota ──────────┐           ┌─ Quota ──────────┐                │  │   │
│  │  │  │ max_conc: 10     │           │ max_conc: 20     │                │  │   │
│  │  │  │ max_daily: 500   │           │ max_daily: 1000  │                │  │   │
│  │  │  │ storage: 50GB    │           │ storage: 100GB   │                │  │   │
│  │  │  └────────┬─────────┘           └────────┬─────────┘                │  │   │
│  │  └───────────┼──────────────────────────────┼──────────────────────────┘  │   │
│  │              │ quota enforcement             │                            │   │
│  │              ▼                               ▼                            │   │
│  │  ┌─ Folders (Path ACL) ────────────────────────────────────────────────┐  │   │
│  │  │  folders/ml-team/      folders/data-eng/      folders/production/   │  │   │
│  │  │       │                      │                      │               │  │   │
│  │  │       ▼                      ▼                      ▼               │  │   │
│  │  │  ┌──────────┐          ┌──────────┐          ┌──────────┐           │  │   │
│  │  │  │ Flow A   │          │ Flow C   │          │ Flow E   │           │  │   │
│  │  │  │ Script B │          │ Script D │          │(approval)│           │  │   │
│  │  │  └────┬─────┘          └────┬─────┘          └────┬─────┘           │  │   │
│  │  └───────┼─────────────────────┼─────────────────────┼─────────────────┘  │   │
│  └──────────┼─────────────────────┼─────────────────────┼────────────────────┘   │
│             │ trigger              │                      │                      │
│             ▼                      ▼                      ▼                      │
│  ┌─ Job Queue (PostgreSQL FOR UPDATE SKIP LOCKED) ────────────────────────────┐  │
│  │                                                                            │  │
│  │  ┌─ Flow Job ──────────────────────────────┐                               │  │
│  │  │  id: uuid-001                           │                               │  │
│  │  │  kind: flow                             │                               │  │
│  │  │  team_owner: "ml-team" ──────> L5a concurrent + L5b cpu + L5c mem check │  │
│  │  │  tag: "default"                         │                               │  │
│  │  │  priority: 0                            │                               │  │
│  │  │       │                                 │                               │  │
│  │  │       │ Flow Engine expands steps       │                               │  │
│  │  │       ▼                                 │                               │  │
│  │  │  ┌─ Child Jobs ─────────────────────┐   │                               │  │
│  │  │  │ Step A (script)  -> uuid-002     │   │                               │  │
│  │  │  │ Step B (script)  -> uuid-003     │   │                               │  │
│  │  │  │ Step C (forloop) -> uuid-004     │   │                               │  │
│  │  │  │   ├─ iter[0] -> uuid-005         │   │                               │  │
│  │  │  │   ├─ iter[1] -> uuid-006         │   │                               │  │
│  │  │  │   └─ iter[2] -> uuid-007         │   │                               │  │
│  │  │  └──────────────────────────────────┘   │                               │  │
│  │  └─────────────────────────────────────────┘                               │  │
│  │                                                                            │  │
│  │  ┌── 6-Layer Concurrency Defense ──────────────────────────────────────┐   │  │
│  │  │ L1: Physical worker count (natural limit)                           │   │  │
│  │  │ L2: worker_config.max_concurrent_jobs (global cap)                  │   │  │
│  │  │ L3: concurrency_limit per tag (tag-level cap)                       │   │  │
│  │  │ L4: Worker backpressure (LISTEN/NOTIFY + poll interval)             │   │  │
│  │  │ L5: team_quota (team-level: max_concurrent_runs + max_cpus/memory)    │   │  │
│  │  │ L6: Worker resource availability (cpus/mem/disk <= available)       │   │  │
│  │  └─────────────────────────────────────────────────────────────────────┘   │  │
│  └────────────────────────────────────────────────────────────────────────────┘  │
│             │                                                                    │
│             │ SELECT ... FOR UPDATE SKIP LOCKED                                  │
│             ▼                                                                    │
│  ┌─ Worker Pool ──────────────────────────────────────────────────────────────┐  │
│  │                                                                            │  │
│  │  ┌─ Worker 1 ────────────────────┐  ┌─ Worker 2 ───────────┐  ┌─ Worker N ──────┐  │  │
│  │  │ tags: [default, gpu]          │  │ tags: [default]      │  │ tags: [heavy]   │  │  │
│  │  │ 8 cpu, 32GB, 100GB disk     │  │ 4 cpu, 16GB          │  │ 8 cpu, 64GB     │  │  │
│  │  │                               │  │                      │  │                 │  │  │
│  │  │ ┌─ ResourceManager ────────┐  │  │ ┌─ ResourceMgr ────┐ │  │                 │  │  │
│  │  │ │ CPU [████░░░░] 4/8 used  │  │  │ │ CPU [██░░] 1/4   │ │  │                 │  │  │
│  │  │ │ RAM [██░░░░░░] 8/32 GB   │  │  │ │ RAM [█░░░] 2/16  │ │  │                 │  │  │
│  │  │ │ Job A: 2cpu 4GB (Normal) │  │  │ │ Job D: 1cpu 2GB  │ │  │                 │  │  │
│  │  │ │ Job B: 2cpu 4GB (Normal) │  │  │ └──────────────────┘ │  │                 │  │  │
│  │  │ │ free: 4cpu 24GB          │  │  │                      │  │                 │  │  │
│  │  │ └──────────────────────────┘  │  │ ┌─ Sandbox ────────┐ │  │                 │  │  │
│  │  │                               │  │ │ mode: none       │ │  │                 │  │  │
│  │  │ ┌─ Sandbox ────────────────┐  │  │ │ (dev mode)       │ │  │                 │  │  │
│  │  │ │ mode: nsjail             │  │  │ └──────────────────┘ │  │                 │  │  │
│  │  │ │ resources: per-job       │  │  └──────────────────────┘  └─────────────────┘  │  │
│  │  │ └────────────┬─────────────┘  │                                                │  │
│  │  │              │                 │                                                │  │
│  │  │              ▼                 │                                                │  │
│  │  │  ┌─ Job Execution ──────────────────────────────────┐                           │  │
│  │  │  │ 1. rm.try_acquire(cpus, mem, disk)               │                           │  │
│  │  │  │ 2. mkdir job_dir                                 │                           │  │
│  │  │  │ 3. write code (main.py + wrapper.py)             │                           │  │
│  │  │  │ 4. resolve dependencies (pip install, cached)    │                           │  │
│  │  │  │ 5. resolve FileRefs ──> download from S3/local   │                           │  │
│  │  │  │ 6. spawn process in sandbox (resource_limits)    │                           │  │
│  │  │  │ 7. stdout/stderr -> tracing -> DbLogLayer -> run_log -> SSE -> UI │                           │  │
│  │  │  │ 8. read result.json ──> job_completed (or S3)    │                           │  │
│  │  │  │ 9. drop(ResourceGuard) -> release resources      │                           │  │
│  │  │  └──────────────────────────────────────────────────┘                           │  │
│  │  └────────────────────────────────┘                                                │  │
│  └────────────────────────────────────────────────────────────────────────────────────┘  │
│                                                                                          │
│  ┌─ Observability ────────────────────────────────────────────────────────────────────┐  │
│  │  worker_ping: every 15s reports resources (cpus/mem/disk used/total) -> Dashboard   │  │
│  │  team_resource_usage: per-team aggregation (runs, duration, storage)               │  │
│  │  OTel tracing: each job = 1 span, flow children inherit parent trace_id            │  │
│  └────────────────────────────────────────────────────────────────────────────────────┘  │
└──────────────────────────────────────────────────────────────────────────────────────────┘
```

### B.1 Entity Relationship Summary

| From | To | Cardinality | Key Field |
|------|----|-------------|-----------|
| Cluster | Worker | 1 : N | `worker_ping` table |
| Workspace | Team | 1 : N | `team.workspace_id` |
| Team | Folder | 1 : 1 (convention) | auto-created on team create |
| Team | Quota | 1 : 1 | `team_quota.team_name` |
| Folder | Flow / Script | 1 : N | `path` prefix `folders/{folder}/` |
| Flow | Root Run | 1 : N (per trigger) | `run.kind = flow` |
| Root Run | Child Runs | 1 : N (per step) | `run.parent_run` / `run.root_run` |
| Run | Worker | N : 1 (claimed) | `run_queue.worker` |
| Run | Team | N : 1 | `run.team_owner` |
| Worker | Resources | 1 : 1 | `worker_ping.total_cpus / used_cpus / total_memory_mb / used_memory_mb / total_disk_mb / used_disk_mb` |
| Worker | Sandbox | 1 : 1 | `worker_ping.sandbox_mode` |
| Run | Resources Required | 1 : 1 | `run.cpus` (default 1), `run.memory_mb` (default 512), `run.disk_mb` (default 1024) |
| Team Quota (concurrency) | Run Queue | throttle | L5a: `max_concurrent_runs` in `submit_run` + `claim_run` |
| Team Quota (cpus) | Run Queue | throttle | L5b: `max_cpus` — SUM(cpus) cap in `submit_run` + `claim_run` |
| Team Quota (memory) | Run Queue | throttle | L5c: `max_memory_mb` — SUM(memory_mb) cap in `submit_run` + `claim_run` |

### B.2 FAQ: Resource Model Clarifications

**Q: What is the Job Queue?**

Not a separate process. It is a PostgreSQL table (`job_queue`).

```
┌───────────────────────────────────────────────────┐
│ There is NO separate queue process (no Redis,     │
│ no RabbitMQ, no Kafka).                           │
│                                                   │
│ "Job Queue" = job_queue TABLE in PostgreSQL       │
│                                                   │
│ API Server:  INSERT INTO job_queue  (push)        │
│                    │                              │
│                    │  same PostgreSQL instance     │
│                    ▼                              │
│ Worker:      SELECT ... FOR UPDATE SKIP LOCKED    │
│              (pull, atomic, no race condition)     │
│                                                   │
│ + LISTEN/NOTIFY for instant wakeup (<1ms latency) │
└───────────────────────────────────────────────────┘
```

**Q: Who defines worker resources (vCPU, RAM, Disk)?**

The deployment environment, not the application. Workers auto-detect on startup.

```
┌─ Who defines what ────────────────────────────────────────┐
│                                                           │
│ Hardware (vCPU / RAM / Disk)                              │
│   -> Determined by whoever deploys the worker             │
│      (docker-compose, K8s manifest, bare metal)           │
│   -> Worker auto-detects on startup via sysinfo / cgroup  │
│   -> Reports to worker_ping table every 15s               │
│                                                           │
│ Sandbox limits (per-job ceiling)                          │
│   -> Defined in worker config (TOML)                      │
│   -> e.g. mem_limit=4GB, timeout=300s, disk_limit=1GB     │
│   -> Applies to EVERY job equally on that worker          │
│                                                           │
│ Tag routing (soft affinity, NOT ownership)                │
│   -> Worker starts with tags: ["default", "gpu"]          │
│   -> Job pushed with tag: "gpu"                           │
│   -> Only workers with "gpu" tag will pick it up          │
│   -> This is the closest to "dedicated workers"           │
└───────────────────────────────────────────────────────────┘
```

**Q: Does a Group own specific workers?**

No. All workers are shared. Group quota limits **concurrency + hardware usage**, not worker ownership.

```
WRONG (Kubernetes-style mental model):
  ml-team "owns" Worker 1, 2, 3       <- NOT how it works
  data-eng "owns" Worker 4, 5

CORRECT (CoveFlow model):
  All 20 workers (total 160 cpus, 640 GB RAM) are in a shared pool.
  Any worker can pick up any team's job.

  team_quota for ml-team:
    max_concurrent_runs = 10    -> at most 10 runs run at the same time
    max_cpus = 16               -> those runs can consume at most 16 cpus total
    max_memory_mb = 32768       -> those runs can consume at most 32 GB RAM total

  Neither setting reserves workers exclusively.
  If ml-team has 0 running jobs, all cluster resources
  are available to other teams.
```

To get dedicated workers per team (Phase 4+), use **tag routing**:

```
# Ops deploys 3 workers with team-specific tag
worker --tags "ml-team,default"    # Worker 1-3

# ml-team's jobs pushed with tag "ml-team"
# -> only Worker 1-3 pick them up

# This is Runner Groups (Phase 4.4), orthogonal to team_quota
```

**Q: Why does Group have a storage quota?**

To prevent one team from filling shared storage with large files (ML models, datasets). However, if file storage uses S3/MinIO (virtually unlimited), **storage quota is low priority** — can be deferred to Phase 4+.

| Quota field | Priority | Reason |
|-------------|----------|--------|
| `max_concurrent_jobs` | **High** | Prevents job count starvation across teams |
| `max_cpus` | **High** | Prevents CPU starvation — 限制團隊佔用的 CPU 總量 |
| `max_memory_mb` | **High** | Prevents RAM starvation — 限制團隊佔用的 RAM 總量 |
| `max_daily_jobs` | Medium | Cost control, abuse prevention |
| `max_storage_bytes` | Low | Only matters if using local disk, S3 is ~infinite |
| `max_job_timeout_secs` | Low | Safety net, global default usually sufficient |
