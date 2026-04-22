# 第八章：源碼地圖 — 每個篇章的對應原始碼

本章列出每個學習篇章對應的 Windmill 原始碼檔案，方便你直接閱讀和學習。

---

## Chapter 00 — 總覽 / 架構

| 源碼路徑 | 說明 |
|----------|------|
| `backend/src/main.rs` | 後端入口，啟動 Axum server、初始化 DB 連線池 |
| `backend/Cargo.toml` | Cargo workspace 定義，列出所有 crate 成員 |
| `frontend/src/app.html` | 前端 HTML 入口，SvelteKit body 注入點 |
| `frontend/src/hooks.ts` | SvelteKit hooks（SSR disabled） |
| `frontend/src/routes/+layout.svelte` | SvelteKit 根 layout，全域初始化 |

---

## Chapter 01 — 資料庫

### 核心 Migration

| 源碼路徑 | 說明 |
|----------|------|
| `backend/migrations/20220123221903_first.up.sql` | **最重要**：建立 workspace、script、flow、queue、completed_job、usr、password 等核心表 |
| `backend/migrations/20220316135622_raw_flow.up.sql` | Raw Flow 支援 |
| `backend/migrations/20220320122733_schedule_flow.up.sql` | Flow 排程 |
| `backend/migrations/` | 共 **1088 個** migration 檔案，追蹤所有 schema 演進 |

### 資料模型定義

| 源碼路徑 | 說明 |
|----------|------|
| `backend/windmill-common/src/db.rs` | DB 連線池設定、client 配置 |
| `backend/windmill-common/src/scripts.rs` | Script 資料模型（`ScriptHash`、`ScriptLang`、版本鏈） |
| `backend/windmill-common/src/flows.rs` | Flow 共用工具函式（11KB） |
| `backend/windmill-common/src/jobs.rs` | Job 資料模型（`QueuedJob`、`CompletedJob`） |
| `backend/windmill-common/src/users.rs` | 使用者/認證 schema |
| `backend/windmill-common/src/workspaces.rs` | Workspace schema |
| `backend/windmill-common/src/variables.rs` | 變數 schema（含 reserved variables） |
| `backend/windmill-common/src/schema.rs` | 核心 DB 型別定義和工具 |

---

## Chapter 02 — 後端 API

### Router 與路由註冊

| 源碼路徑 | 說明 |
|----------|------|
| `backend/windmill-api/src/lib.rs` | **核心**：Axum router 設定，所有 `/api/w/{workspace_id}/...` 路由註冊 |

### 認證系統

| 源碼路徑 | 說明 |
|----------|------|
| `backend/windmill-common/src/jwt.rs` | JWT 編碼/解碼（`encode_with_internal_secret`、`decode_with_internal_secret`） |
| `backend/windmill-common/src/auth.rs` | 核心認證邏輯、授權檢查、快取 |
| `backend/windmill-api-auth/src/auth.rs` | Auth middleware 實作 |
| `backend/windmill-api-auth/src/scopes.rs` | OAuth/權限 scope 定義 |
| `backend/windmill-api/src/db.rs` | `ApiAuthed` extractor（從 request 中提取已認證使用者） |

### API Handlers（CRUD）

| 源碼路徑 | 說明 |
|----------|------|
| `backend/windmill-api-scripts/src/scripts.rs` | Script CRUD endpoints（create/read/update/delete/list） |
| `backend/windmill-api-flows/src/flows.rs` | Flow CRUD endpoints |
| `backend/windmill-api-jobs/src/jobs.rs` | Job 建立與查詢 endpoints |
| `backend/windmill-api-jobs/src/execution.rs` | Job 執行邏輯 |
| `backend/windmill-api-jobs/src/types.rs` | Job 型別定義 |
| `backend/windmill-api/src/users.rs` | 使用者管理 API |
| `backend/windmill-api/src/workspaces.rs` | Workspace 管理 API |
| `backend/windmill-api/src/resources.rs` | 資源管理 API |
| `backend/windmill-api/src/variables.rs` | 變數管理 API |
| `backend/windmill-api/src/apps.rs` | App/Dashboard API |
| `backend/windmill-api/src/audit.rs` | 審計日誌 API |

### 即時串流與 OpenAPI

| 源碼路徑 | 說明 |
|----------|------|
| `backend/windmill-api-sse/src/lib.rs` | SSE 型別（`stream_offset`、`flow_stream_job_id`、`new_result_stream`） |
| `backend/windmill-api-openapi/src/lib.rs` | OpenAPI spec 生成（`generate_openapi_spec`） |
| `backend/windmill-api/openapi.yaml` | 生成的 OpenAPI specification |
| `backend/openapi-bundled.yaml` | 打包後的 OpenAPI spec |

---

## Chapter 03 — 任務佇列

### 核心 Queue 操作

| 源碼路徑 | 說明 |
|----------|------|
| `backend/windmill-queue/src/jobs.rs` | **核心（6458 行）**：所有 queue 操作集中在此 |

#### 重點函式位置

| 函式 | 行數 | 說明 |
|------|------|------|
| `add_completed_job()` | ~817 | Job 完成後寫入 completed_job 表 |
| `add_completed_job_error()` | ~743 | 失敗 job 的完成處理 |
| `push_init_job()` | - | 初始化 job 推入 queue |
| `pull()` | ~3251 | Worker 用 `FOR UPDATE SKIP LOCKED` 搶 job |
| `MiniPulledJob` struct | ~2308 | 拉取的 job 結構（含 `priority`、`tag`） |
| `custom_concurrency_key()` | ~3546 | 併發限制 key 計算 |
| `insert_concurrency_key()` | ~5934 | 插入併發追蹤紀錄 |

### 排程與路由

| 源碼路徑 | 說明 |
|----------|------|
| `backend/windmill-queue/src/schedule.rs` | Cron 排程：`push_scheduled_job()` (line 126) |
| `backend/windmill-queue/src/tags.rs` | Tag 路由：`per_workspace_tag()` |
| `backend/windmill-queue/src/flow_status.rs` | Flow 狀態更新 helper：`update_flow_status_in_progress()` |
| `backend/windmill-api-schedule/src/` | 排程 API endpoints |
| `backend/windmill-types/src/schedule.rs` | Schedule 資料結構 |

### 心跳與監控

| 源碼路徑 | 說明 |
|----------|------|
| `backend/windmill-common/src/worker.rs` | Worker 心跳 `update_ping_http()`、自訂 pull query `store_pull_query()` (line 523) |
| `backend/src/monitor.rs` | **監控器（143KB）** |

#### monitor.rs 重點函式

| 函式 | 行數 | 說明 |
|------|------|------|
| `handle_zombie_flows()` | ~2221 | Zombie flow 偵測 |
| `handle_zombie_jobs()` | ~2900 | Zombie job 偵測（心跳超時 5 分鐘） |

### 結果處理

| 源碼路徑 | 說明 |
|----------|------|
| `backend/windmill-worker/src/result_processor.rs` | Job 完成後的背景結果處理器：`handle_receive_completed_job()` (line 568) |
| `backend/windmill-api-jobs/src/concurrency_groups.rs` | 併發群組管理 API |

---

## Chapter 04 — Worker & Executor

### Worker 主迴圈

| 源碼路徑 | 說明 |
|----------|------|
| `backend/windmill-worker/src/worker.rs` | **核心（5526 行）** |

#### worker.rs 重點函式

| 函式 | 行數 | 說明 |
|------|------|------|
| `run_worker()` | ~1638 | Worker event loop 入口 |
| `handle_queued_job()` | ~3321 | 處理單一 job（dispatch 到對應 executor） |

### 各語言 Executor

| 源碼路徑 | 大小 | 入口函式 |
|----------|------|---------|
| `backend/windmill-worker/src/python_executor.rs` | 106KB | `handle_python_job()` (line 584)、`handle_python_reqs()` (line 2062) |
| `backend/windmill-worker/src/bun_executor.rs` | 146KB | `handle_bun_job()` (line 1256) |
| `backend/windmill-worker/src/deno_executor.rs` | 25KB | `handle_deno_job()` (line 235) |
| `backend/windmill-worker/src/go_executor.rs` | 22KB | `handle_go_job()` (line 88) |
| `backend/windmill-worker/src/bash_executor.rs` | 19KB | `handle_bash_job()` (line 69) |
| `backend/windmill-worker/src/rust_executor.rs` | 25KB | `handle_rust_job()` (line 611)、`build_rust_crate()` (line 455) |

#### 其他語言 Executor（共 25 種）

`backend/windmill-worker/src/` 下還有：`ansible`、`bigquery`、`csharp`、`duckdb`、`graphql`、`java`、`mssql`、`mysql`、`nu`、`oracledb`、`pg`、`php`、`pwsh`、`r`、`ruby`、`snowflake`、`wac` 等 executor。

### 子程序管理與沙箱

| 源碼路徑 | 說明 |
|----------|------|
| `backend/windmill-worker/src/handle_child.rs` | **子程序監控（38KB）**：`handle_child()` (line 104)、`run_future_with_polling_update_job_poller()` (line 703) |
| `backend/windmill-worker/nsjail/` | 19 個 nsjail 沙箱設定檔（每種語言一個 `.config.proto`） |

#### nsjail 設定範例

| 設定檔 | 對應語言 |
|--------|---------|
| `run.python3.config.proto` | Python — mount points、資源限制、環境設定 |
| `run.bash.config.proto` | Bash |
| `run.bun.config.proto` | Bun/JS |
| `run.go.config.proto` | Go |
| `run.rust.config.proto` | Rust |

### 快取與依賴

| 源碼路徑 | 說明 |
|----------|------|
| `backend/windmill-worker/src/global_cache.rs` | 全域快取：`load_cache()`、`save_cache()` (9.6KB) |
| `backend/windmill-worker/src/worker_lockfiles.rs` | 依賴鎖定檔處理：`handle_dependency_job()` (line 81)（101KB） |
| `backend/windmill-worker/src/prepare_deps.rs` | 獨立依賴準備：`prepare_deps_standalone()` (line 305) |

### Reserved Variables

| 源碼路徑 | 說明 |
|----------|------|
| `backend/windmill-worker/src/common.rs` | `get_reserved_variables()` (line 473)（59KB） |
| `backend/windmill-common/src/variables.rs` | 基礎 reserved variable 定義：`get_reserved_variables()` (line 230) |

包含：`WM_WORKSPACE`、`WM_JOB_ID`、`WM_FLOW_STEP_ID`、`WM_FLOW_JOB`、`WM_STATE_PATH_NEW`、`WM_RESULT_PATH` 等。

---

## Chapter 05 — Flow 引擎

### 資料模型

| 源碼路徑 | 說明 |
|----------|------|
| `backend/windmill-types/src/flows.rs` | **Flow 定義的所有型別** |

#### flows.rs 重點結構

| 結構/列舉 | 行數 | 說明 |
|-----------|------|------|
| `FlowValue` | ~157 | 完整的 Flow 定義（modules + 依賴） |
| `FlowModule` | ~418 | 單一步驟（id + value + input_transforms） |
| `InputTransform` | ~632 | Static / Javascript / Ai 三種轉換 |
| `FlowModuleValue` | ~830 | 步驟類型：Script、Flow、ForloopFlow、BranchOne、BranchAll、Identity、AIAgent |
| `Retry` | ~311 | 重試設定（constant + exponential） |
| `Suspend` | ~387 | 暫停/審批設定 |
| `Branch` | ~712 | 分支定義（條件 + 模組） |

### 狀態追蹤

| 源碼路徑 | 說明 |
|----------|------|
| `backend/windmill-types/src/flow_status.rs` | **Flow 執行狀態** |

#### flow_status.rs 重點結構

| 結構/列舉 | 行數 | 說明 |
|-----------|------|------|
| `FlowStatus` | ~20 | 完整的 flow 執行狀態 |
| `RetryStatus` | ~48 | 重試次數追蹤 |
| `BranchAllStatus` | ~80 | 並行分支狀態 |
| `BranchChosen` | ~90 | 選擇了哪個分支 |
| `FlowStatusModule` | ~211 | 步驟狀態（WaitingForPriorSteps → InProgress → Success/Failure） |

### 狀態機（核心邏輯）

| 源碼路徑 | 說明 |
|----------|------|
| `backend/windmill-worker/src/worker_flow.rs` | **核心狀態機（5576 行）** |

#### worker_flow.rs 重點函式

| 函式 | 行數 | 說明 |
|------|------|------|
| `update_flow_status_after_job_completion()` | ~147 | 步驟完成後更新狀態的入口 |
| `update_flow_status_after_job_completion_internal()` | ~381 | 內部狀態更新邏輯 |
| `retrieve_flow_jobs_results()` | ~2100 | 從 DB 抓取所有步驟結果 |
| `transform_input()` | ~2367 | 用 InputTransform 轉換輸入 |
| `push_next_flow_job()` | ~2675 | 推入下一步的 job |
| `compute_next_flow_transform()` | ~4463 | 決定下一步是什麼 |
| `get_transform_context()` | ~5474 | 建立步驟結果的 context（`IdContext`） |
| `script_to_payload()` | ~5374 | Script → job payload 轉換 |
| `get_previous_job_result()` | ~5545 | 取得前一步的結果 |

### 表達式求值

| 源碼路徑 | 說明 |
|----------|------|
| `backend/windmill-worker/src/js_eval.rs` | JavaScript 表達式求值包裝（6KB） |
| `backend/windmill-jseval/src/lib.rs` | JS 求值引擎核心 |

### 其他 Flow 工具

| 源碼路徑 | 說明 |
|----------|------|
| `backend/windmill-common/src/flows.rs` | Flow 工具函式（11KB） |
| `backend/windmill-common/src/flow_status.rs` | 共用 Flow 狀態型別 |
| `backend/windmill-queue/src/flow_status.rs` | `update_flow_status_in_progress()`、`get_step_of_flow_status()` (line 102) |

---

## Chapter 06 — 前端

### SvelteKit 路由結構

| 源碼路徑 | 說明 |
|----------|------|
| `frontend/src/routes/(root)/+layout.svelte` | 已認證路由的包裝 |
| `frontend/src/routes/(root)/(logged)/+layout.svelte` | 主要 layout（側邊欄、導覽列） |
| `frontend/src/routes/(root)/(logged)/+page.svelte` | 首頁/Dashboard |
| `frontend/src/routes/(root)/(logged)/scripts/add/+page.svelte` | Script 建立頁 |
| `frontend/src/routes/(root)/(logged)/scripts/edit/[...path]/+page.svelte` | Script 編輯頁 |
| `frontend/src/routes/(root)/(logged)/flows/add/+page.svelte` | Flow 建立頁 |
| `frontend/src/routes/(root)/(logged)/flows/edit/[...path]/+page.svelte` | Flow 編輯頁 |
| `frontend/src/routes/(root)/(logged)/flows/get/[...path]/+page.svelte` | Flow 檢視頁 |
| `frontend/src/routes/(root)/(logged)/apps/edit/[...path]/+page.svelte` | App Builder 頁 |
| `frontend/src/routes/(root)/(logged)/run/[...run]/+page.svelte` | Job 執行檢視 |
| `frontend/src/routes/(root)/(logged)/runs/[...path]/+page.svelte` | 執行歷史頁 |

### OpenAPI Client 生成

| 源碼路徑 | 說明 |
|----------|------|
| `frontend/package.json` | `openapi-ts` 指令（`@hey-api/openapi-ts` v0.43.0） |
| `frontend/src/lib/gen/` | **自動生成的 TypeScript client**（從 `openapi.yaml` 生成） |
| `backend/windmill-api/openapi.yaml` | 來源 OpenAPI specification |
| `backend/windmill-api/build_openapi.sh` | 重建 OpenAPI 文件的 script |

### Monaco Editor 整合

| 源碼路徑 | 說明 |
|----------|------|
| `frontend/src/lib/components/monacoLanguagesOptions.ts` | Monaco 語言設定（SQL、Python、TypeScript 等） |
| `frontend/src/lib/components/monaco_keybindings.ts` | Vim/VSCode 鍵盤設定 |
| `frontend/src/lib/components/copilot/chat/monaco-adapter.ts` | AI Copilot 整合層 |
| `frontend/src/lib/svelteMonarch.ts` | Svelte 語法高亮 |
| `frontend/src/lib/editorUtils.ts` | 編輯器通用工具 |
| `frontend/src/lib/components/debug/MonacoDebugger.svelte` | Monaco-based debugger |

### Flow Editor（視覺流程編輯器）

| 源碼路徑 | 說明 |
|----------|------|
| `frontend/src/lib/components/flows/FlowEditor.svelte` | **主要 Flow 編輯器**（畫布 + 面板） |
| `frontend/src/lib/components/flows/flowState.ts` | Flow 狀態管理 store |
| `frontend/src/lib/components/flows/content/FlowEditorPanel.svelte` | 右側設定面板 |
| `frontend/src/lib/components/flows/content/FlowModuleComponent.svelte` | 單一步驟元件 |
| `frontend/src/lib/components/flows/content/FlowModuleScript.svelte` | Script step 編輯 UI |
| `frontend/src/lib/components/flows/content/FlowInputs.svelte` | Flow 輸入定義 |
| `frontend/src/lib/components/flows/content/FlowSettings.svelte` | Flow 全域設定 |
| `frontend/src/lib/components/flows/content/FlowResult.svelte` | Flow 執行結果顯示 |
| `frontend/src/lib/components/flows/map/FlowModuleSchemaMap.svelte` | 視覺化 schema mapper |
| `frontend/src/lib/components/flows/pickers/FlowScriptPicker.svelte` | 步驟選擇器 |
| `frontend/src/lib/components/flows/header/FlowYamlEditor.svelte` | YAML 編輯器 |
| `frontend/src/lib/components/graph/FlowGraphV2.svelte` | **D3 流程圖視覺化** |

### Input Transform 編輯器

| 源碼路徑 | 說明 |
|----------|------|
| `frontend/src/lib/components/flows/propPicker/InputPickerInner.svelte` | 輸入值選擇/轉換器 |
| `frontend/src/lib/components/flows/propPicker/OutputPicker.svelte` | 輸出選擇器 |
| `frontend/src/lib/components/flows/content/FlowInputEditor.svelte` | Flow 輸入定義編輯 |
| `frontend/src/lib/components/flows/content/DynamicInputHelpBox.svelte` | 動態輸入 help UI |

### 日誌與結果檢視

| 源碼路徑 | 說明 |
|----------|------|
| `frontend/src/lib/components/LogViewer.svelte` | 通用日誌檢視器 |
| `frontend/src/lib/components/FlowLogViewer.svelte` | Flow 執行日誌（含串流） |
| `frontend/src/lib/components/FlowLogViewerWrapper.svelte` | 日誌檢視器生命週期管理 |
| `frontend/src/lib/components/scriptEditor/LogPanel.svelte` | Script 執行日誌面板 |
| `frontend/src/lib/components/FlowJobResult.svelte` | Job 結果顯示 |
| `frontend/src/lib/components/ResultStreamDisplay.svelte` | 串流結果顯示 |
| `frontend/src/lib/components/FlowLogRow.svelte` | 單行日誌顯示 |

### Context / Stores

| 源碼路徑 | 說明 |
|----------|------|
| `frontend/src/lib/stores.ts` | 全域 stores（user、workspace、theme 等） |
| `frontend/src/lib/storeUtils.ts` | Store 工具函式 |
| `frontend/src/lib/components/flows/flowState.ts` | Flow 編輯器 context（setContext/getContext） |
| `frontend/src/lib/components/apps/store.ts` | App 編輯器 state |

### App Builder

| 源碼路徑 | 說明 |
|----------|------|
| `frontend/src/lib/components/apps/editor/AppEditor.svelte` | 主要 App Builder |
| `frontend/src/lib/components/apps/editor/GridEditor.svelte` | 拖拽式 Grid layout |
| `frontend/src/lib/components/apps/editor/AppPreview.svelte` | App 即時預覽 |
| `frontend/src/lib/components/apps/editor/PublicApp.svelte` | 公開 App 檢視器 |
| `frontend/src/lib/components/raw_apps/RawAppEditor.svelte` | YAML-based App 編輯 |
| `frontend/src/lib/components/apps/components/` | App 元件庫（button、input、table、chart 等） |

---

## Chapter 07 — 超越 Windmill / 事件處理

### Trigger 系統（後端）

| 源碼路徑 | 說明 |
|----------|------|
| `backend/windmill-api/src/triggers/mod.rs` | Trigger 主模組 |
| `backend/windmill-api/src/triggers/handler.rs` | Trigger HTTP handlers/routing |
| `backend/windmill-api/src/triggers/listener.rs` | Trigger listener 管理 |
| `backend/windmill-api/src/triggers/http/` | HTTP/Webhook trigger |
| `backend/windmill-api/src/triggers/websocket/` | WebSocket trigger |
| `backend/windmill-api/src/triggers/kafka/` | Kafka trigger |
| `backend/windmill-api/src/triggers/postgres/` | PostgreSQL LISTEN/NOTIFY |
| `backend/windmill-api/src/triggers/nats/` | NATS trigger |
| `backend/windmill-api/src/triggers/mqtt/` | MQTT trigger |
| `backend/windmill-api/src/triggers/sqs/` | AWS SQS trigger |
| `backend/windmill-api/src/triggers/gcp/` | GCP Pub/Sub trigger |
| `backend/windmill-api/src/triggers/email/` | Email trigger |
| `backend/windmill-api/src/webhook_util.rs` | Webhook 工具 |
| `backend/windmill-common/src/triggers.rs` | 共用 trigger 型別 |

### Trigger 系統（前端）

| 源碼路徑 | 說明 |
|----------|------|
| `frontend/src/routes/(root)/(logged)/websocket_triggers/+page.svelte` | WebSocket trigger 管理頁 |
| `frontend/src/routes/(root)/(logged)/kafka_triggers/+page.svelte` | Kafka trigger 頁 |
| `frontend/src/routes/(root)/(logged)/nats_triggers/+page.svelte` | NATS trigger 頁 |
| `frontend/src/routes/(root)/(logged)/mqtt_triggers/+page.svelte` | MQTT trigger 頁 |
| `frontend/src/routes/(root)/(logged)/sqs_triggers/+page.svelte` | SQS trigger 頁 |
| `frontend/src/routes/(root)/(logged)/gcp_triggers/+page.svelte` | GCP trigger 頁 |
| `frontend/src/routes/(root)/(logged)/postgres_triggers/+page.svelte` | PostgreSQL trigger 頁 |
| `frontend/src/routes/(root)/(logged)/email_triggers/+page.svelte` | Email trigger 頁 |
| `frontend/src/lib/components/triggers/TriggersEditor.svelte` | Trigger 設定編輯器 |
| `frontend/src/lib/components/triggers/TriggerEditorToolbar.svelte` | Trigger 編輯工具列 |

### WebSocket Trigger（長連線實作）

| 源碼路徑 | 說明 |
|----------|------|
| `backend/windmill-trigger-websocket/src/lib.rs` | WebSocket trigger worker library |
| `backend/windmill-trigger-websocket/src/handler.rs` | WebSocket 事件處理（filter + trigger flow） |
| `backend/windmill-trigger-websocket/src/listener.rs` | WebSocket 長連線監聽（`tokio::select!` 多工） |

### Native Trigger（第三方整合）

| 源碼路徑 | 說明 |
|----------|------|
| `backend/windmill-native-triggers/src/lib.rs` | Native trigger 框架 |
| `backend/windmill-native-triggers/src/handler.rs` | Native trigger 請求處理 |
| `backend/windmill-native-triggers/src/github/` | GitHub webhook 整合 |
| `backend/windmill-native-triggers/src/google/` | Google Workspace 整合 |
| `backend/windmill-native-triggers/src/nextcloud/` | Nextcloud 同步 trigger |
| `frontend/src/routes/(root)/(logged)/native_triggers/[service_name]/+page.svelte` | Native trigger 設定 UI |

### 即時串流

| 源碼路徑 | 說明 |
|----------|------|
| `backend/windmill-worker/src/ai/sse.rs` | AI SSE 串流 |
| `backend/windmill-common/src/stream.rs` | 串流處理工具 |
| `backend/windmill-common/src/result_stream.rs` | Job 結果串流協議 |
| `frontend/src/lib/components/ResultStreamDisplay.svelte` | 即時結果顯示 |

### WASM 相關（已有的用法）

| 源碼路徑 | 說明 |
|----------|------|
| `backend/parsers/windmill-parser-wasm/src/lib.rs` | WASM parser 入口 |
| `backend/parsers/windmill-parser-py/src/lib.rs` | Python parser（編譯為 WASM） |
| `backend/parsers/windmill-parser-ts/src/lib.rs` | TypeScript parser（編譯為 WASM） |
| `backend/parsers/windmill-parser-go/src/lib.rs` | Go parser（編譯為 WASM） |
| `frontend/src/lib/infer.ts` | 前端使用 WASM parser 推斷 script 參數 |

### 效能優化

| 源碼路徑 | 說明 |
|----------|------|
| `backend/windmill-worker/src/global_cache.rs` | 全域 runtime 快取 |
| `backend/windmill-store/src/var_resource_cache.rs` | 變數/資源快取層 |
| `backend/windmill-api/src/s3_log_batching.rs` | 批次日誌上傳至 S3 |
| `backend/windmill-api/src/job_metrics.rs` | Job 效能指標 |
| `backend/windmill-queue/tests/debounce_test.rs` | Debounce/批次測試 |
| `frontend/src/lib/components/flows/content/FlowModuleCache.svelte` | Flow 快取設定 UI |
| `frontend/src/lib/components/flows/content/FlowModuleDebounce.svelte` | Flow debounce 設定 UI |
