# 從零打造工作流平台：Windmill 架構全解析

## 本系列文件結構

| 章節 | 主題 | 你會學到 |
|------|------|----------|
| [00-overview](./00-overview.md) | 架構總覽 | 系統全貌、核心設計決策 |
| [01-database](./01-database.md) | 資料庫設計 | Schema、RBAC、Job 生命週期 |
| [02-backend-api](./02-backend-api.md) | Rust API Server | Axum 路由、認證、中間件 |
| [03-job-queue](./03-job-queue.md) | 任務佇列系統 | PostgreSQL-based Queue、排程 |
| [04-worker-executor](./04-worker-executor.md) | 多語言 Worker | 沙箱隔離、語言執行器、快取 |
| [05-flow-engine](./05-flow-engine.md) | 工作流引擎 | 狀態機、分支、迭代、暫停/恢復 |
| [06-frontend](./06-frontend.md) | Svelte 前端 | Flow Editor、Script Editor、App Builder |
| [07-beyond-windmill](./07-beyond-windmill.md) | 超越 Windmill | 差異化功能、iggy、高效能資料源、事件處理分析 |
| [08-source-map](./08-source-map.md) | 源碼地圖 | 每個篇章對應的原始碼檔案路徑 |
| [09-implementation-plan](./09-implementation-plan.md) | 自建平台實作計畫 | Phase 1-4 完整計畫、Sandbox 雙模式、關鍵程式碼 |

---

## Windmill 是什麼？

Windmill 是一個開源的內部工具平台，讓你可以：

1. **透過 UI 設計工作流（Flows）** — 拖拉式的 DAG 編輯器
2. **用多種語言寫腳本** — Python、TypeScript、Go、Bash、Rust、PHP、C#、Java 等 25+ 種
3. **建立內部工具 UI（Apps）** — 低代碼的 App Builder
4. **排程與觸發** — Cron、Webhook、Kafka、NATS、MQTT、PostgreSQL CDC 等

## 高層架構圖

```
┌─────────────────────────────────────────────────────┐
│                   Frontend (Svelte 5)                │
│  ┌──────────┐  ┌──────────┐  ┌──────────────────┐   │
│  │Flow Editor│  │Script    │  │App Builder       │   │
│  │(DAG UI)  │  │Editor    │  │(Low-code UI)     │   │
│  └────┬─────┘  └────┬─────┘  └────────┬─────────┘   │
│       └──────────────┼────────────────┘              │
│                      │ REST API + SSE                │
└──────────────────────┼───────────────────────────────┘
                       │
┌──────────────────────┼───────────────────────────────┐
│              API Server (Rust/Axum)                   │
│  ┌───────────┐ ┌───────────┐ ┌───────────────────┐   │
│  │Auth/RBAC  │ │Job API    │ │Flow/Script CRUD   │   │
│  │Middleware │ │(push/poll)│ │(versioning)        │   │
│  └───────────┘ └─────┬─────┘ └───────────────────┘   │
│                      │                                │
│  ┌───────────────────┼───────────────────────────┐   │
│  │          PostgreSQL (核心資料庫)                │   │
│  │  queue │ completed_job │ script │ flow │ ...   │   │
│  │  ─ Row Level Security (RLS) 多租戶隔離 ─      │   │
│  └───────────────────┬───────────────────────────┘   │
└──────────────────────┼───────────────────────────────┘
                       │
┌──────────────────────┼───────────────────────────────┐
│              Worker Pool (Rust)                       │
│  ┌────────┐  ┌────────┐  ┌────────┐  ┌────────┐     │
│  │Worker 1│  │Worker 2│  │Worker 3│  │Worker N│     │
│  │Python  │  │TS/Deno │  │Go     │  │多語言   │     │
│  └────┬───┘  └────┬───┘  └───┬────┘  └───┬────┘     │
│       │  nsjail 沙箱隔離  │          │              │
│       └───────────┼───────────┘          │              │
│  ┌────────────────┼──────────────────────┘              │
│  │         Flow Engine (狀態機)                       │
│  │  Sequential → Parallel → Branch → Loop → Done     │
│  └───────────────────────────────────────────────┘   │
└──────────────────────────────────────────────────────┘
```

## 核心設計決策

### 1. PostgreSQL 即是一切

Windmill **不用 Redis、不用 RabbitMQ**。PostgreSQL 同時擔任：
- **資料庫**：存 scripts、flows、variables、resources
- **工作佇列**：`queue` 表就是 job queue，worker 用 `SELECT ... FOR UPDATE SKIP LOCKED` 搶 job
- **權限系統**：Row-Level Security (RLS) 實現多租戶隔離
- **鎖機制**：Advisory Locks 用於 concurrency control

**為什麼？** 減少運維複雜度。一個 PostgreSQL 就搞定，不需要維護 Redis + RabbitMQ + 其他。

### 2. Hash-based Script Versioning

Scripts 用**內容 hash 作為版本 ID**，類似 Git：
- 每次修改產生新 hash → 新的 row（immutable）
- `parent_hashes` 欄位記錄版本鏈
- 永遠不會修改已有的 script row

### 3. Flow = JSON DAG + 狀態機

工作流定義存在 `flow.value` (JSONB)，執行時用 `FlowStatus` 狀態機追蹤進度：
```
WaitingForPriorSteps → WaitingForExecutor → InProgress → Success/Failure
```

每個 flow step 都會變成一個獨立的 job 進入 queue。

### 4. 多語言執行 = 子程序 + 沙箱

Worker 不是直接執行程式碼，而是：
1. 為每個 job 建一個臨時目錄
2. 寫入程式碼 + wrapper（處理 JSON 輸入/輸出）
3. 安裝依賴（有快取機制）
4. 用 `nsjail` 沙箱執行子程序
5. 從 `result.json` 讀取結果

### 5. Workspace 多租戶

所有資料都以 `workspace_id` 隔離，包含：
- 獨立的 scripts、flows、variables、resources
- 獨立的使用者和權限
- 路徑格式：`u/username/xxx`、`g/group/xxx`、`f/folder/xxx`

## Rust Crate 結構

```
backend/
├── windmill-api/          # Axum HTTP API server（75+ API 模組）
├── windmill-worker/       # Worker 主程式 + 語言執行器
├── windmill-queue/        # Job queue 操作（push/pull/complete）
├── windmill-common/       # 共用型別、DB 連接、工具函式
├── windmill-types/        # 核心資料型別（Job、Flow、Script）
├── windmill-trigger/      # 觸發器框架（Kafka、NATS、MQTT 等）
├── windmill-alerting/     # 告警系統
├── windmill-audit/        # 審計日誌
├── windmill-indexer/      # 搜尋索引
├── windmill-git-sync/     # Git 同步
├── windmill-parser-*/     # 各語言的解析器（提取函式簽名）
│   ├── windmill-parser-py # Python 解析器
│   ├── windmill-parser-ts # TypeScript 解析器
│   └── windmill-parser-go # Go 解析器
├── migrations/            # 544 個 SQLx 遷移檔案
└── Cargo.toml             # workspace-level Cargo.toml
```

## 前端結構

```
frontend/
├── src/
│   ├── routes/            # SvelteKit 檔案路由
│   │   ├── (root)/        # 主要應用頁面
│   │   │   ├── flows/     # Flow 編輯器頁面
│   │   │   ├── scripts/   # Script 編輯器頁面
│   │   │   ├── apps/      # App Builder 頁面
│   │   │   ├── runs/      # Job 執行歷史
│   │   │   └── schedules/ # 排程管理
│   │   └── user/          # 認證頁面（login/signup）
│   ├── lib/
│   │   ├── components/    # 1436 個 Svelte 組件
│   │   │   ├── flows/     # Flow 相關組件
│   │   │   ├── apps/      # App Builder 組件
│   │   │   ├── scripts/   # Script 相關組件
│   │   │   └── common/    # 共用組件
│   │   ├── gen/           # OpenAPI 自動生成的 API client
│   │   ├── stores/        # Svelte stores（全域狀態）
│   │   └── utils.ts       # 工具函式
│   └── app.html           # HTML 入口
└── package.json
```

## 如果你要從零建一個，建議的開發順序

1. **Phase 1：基礎框架**（2-3 週）
   - PostgreSQL schema + migrations
   - Rust API server (Axum)
   - 基本認證 (JWT)
   - Svelte frontend scaffold

2. **Phase 2：Script 系統**（2-3 週）
   - Script CRUD + versioning
   - 單語言 Worker（先做 Python 或 TypeScript）
   - Job queue (push/pull/complete)
   - 前端 Script Editor (Monaco)

3. **Phase 3：Flow 系統**（3-4 週）
   - Flow 資料模型 (JSON DAG)
   - Flow 狀態機（sequential → parallel → branch）
   - 前端 Flow Editor (拖拉 UI)
   - Step 間資料傳遞

4. **Phase 4：進階功能**（持續）
   - 多語言支援
   - nsjail 沙箱
   - Webhook/Schedule 觸發
   - App Builder
   - 多租戶 + RBAC

接下來的章節將詳細說明每個部分的實作細節。
