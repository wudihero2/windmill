# Run Operations — Cancel / Rerun / Mark Success / Mark Fail

> 已實作。對應程式碼見 `crates/queue/src/cancel.rs`、`rerun.rs`、`mark.rs` 及 `crates/worker/src/worker.rs`。

---

## 1. 問題陳述

CoveFlow 目前缺少 Run 生命週期管理操作。使用者無法取消正在執行的 run、重跑失敗的 run、或手動標記 run 狀態。這是所有工作流引擎的基本功能（Airflow/Windmill/Kestra/Prefect/Dagster 均支援）。

研究了 6 個引擎的原始碼後，CoveFlow 採用最接近自身架構（PG queue + 子程序 sandbox）的 **Windmill 方案**為基礎，並從 Airflow（mark success/fail）和 Kestra（NOTIFY push）補充。

---

## 2. 業界比較

| 操作 | Windmill | Airflow | Kestra | Prefect | Dagster | Temporal |
|------|----------|---------|--------|---------|---------|----------|
| Cancel 偵測 | DB 輪詢 500ms | Scheduler 掃描 RESTARTING | Kill Queue push | Agent 輪詢 CANCELLING | 同步 RunCoordinator | Event（不殺程序）|
| Cancel 殺法 | SIGINT→SIGTERM→SIGKILL | 依 executor（signal/celery/k8s） | process.destroyForcibly / docker stop / k8s delete | infrastructure.kill() | 依 launcher | Workflow 自行處理 |
| Rerun | batch_rerun_jobs | Clear（重設 task 狀態） | restart（可指定 revision） | set_state(Scheduled) | 3 策略（ALL/FROM_FAILURE/FROM_ASSET） | Reset（建新 run） |
| Mark Success | 不支援 | PATCH taskInstance → 自動 clear 下游 | change-status | set_state(Completed, force=true) | 不支援 | 不支援 |
| Mark Fail | 不支援 | PATCH taskInstance → pending 設 SKIPPED | change-status | set_state(Failed, force=true) | report_run_failed | Terminate |

**關鍵結論：**

1. **所有引擎都用 DB 或外部 queue 偵測取消** — 沒有引擎直接 kill 程序（都經過一層間接）
2. **Windmill 的信號鏈（SIGINT→SIGTERM→SIGKILL）是最完整的** — 給予程序 graceful shutdown 機會
3. **只有 Airflow 和 Kestra 支援 Mark Success/Fail** — 其他引擎認為這不是核心功能
4. **Rerun 策略差異大** — CoveFlow 採用 Windmill 的「複製參數建新 run」，最簡潔

---

## 3. CoveFlow 設計 — 雙軌取消偵測

CoveFlow 改進了 Windmill 的純輪詢方式，採用**雙軌**取消偵測機制：

```
軌道 1: LISTEN 'run_cancel'     — NOTIFY 推送，接近即時偵測
軌道 2: 每 500ms 輪詢            — 兜底（NOTIFY 在重連時可能遺漏）

偵測到 → cancel_token.cancel() → sandbox select! 分支觸發 → 信號鏈
```

信號鏈（學 Windmill）：
```
SIGINT → 等 3s → SIGTERM → 等 5s → SIGKILL
```

**相較 Windmill 的改進：**
- Windmill 僅用 500ms DB 輪詢偵測取消，延遲 0-500ms
- CoveFlow 加入 NOTIFY push，正常情況下延遲接近 0ms
- 保留輪詢作為兜底，確保 NOTIFY 在 PgListener 重連時不遺漏

---

## 4. Schema 變更

Migration: `20250507000001_run_operations`

```sql
-- run_queue: 取消支援
ALTER TABLE run_queue ADD COLUMN canceled_by         VARCHAR(255);
ALTER TABLE run_queue ADD COLUMN canceled_reason      TEXT;
ALTER TABLE run_queue ADD COLUMN cancel_requested_at  TIMESTAMPTZ;

-- 部分索引：快速找到已標記取消但仍在執行的 run
CREATE INDEX idx_run_queue_cancel ON run_queue(id)
  WHERE canceled_by IS NOT NULL AND running = TRUE;

-- run_completed: 取消/標記追蹤
ALTER TABLE run_completed ADD COLUMN canceled_by      VARCHAR(255);
ALTER TABLE run_completed ADD COLUMN canceled_reason   TEXT;
ALTER TABLE run_completed ADD COLUMN marked_by         VARCHAR(255);
ALTER TABLE run_completed ADD COLUMN mark_reason       TEXT;

-- run: 重跑追蹤
ALTER TABLE run ADD COLUMN rerun_of UUID REFERENCES run(id);
```

**設計決策：** 不加 `canceled BOOLEAN`，用 `canceled_by IS NOT NULL` 判斷即可（少一欄位）。`claim_run()` 的 WHERE 子句新增 `AND rq.canceled_by IS NULL` 以跳過已取消的 run。

---

## 5. Queue 層函式

### 5.1 cancel.rs

| 函式 | 用途 |
|------|------|
| `cancel_run(db, run_id, canceled_by, reason)` | 軟取消：排隊中 → 直接完成為失敗；執行中 → 設旗標 + NOTIFY `run_cancel` |
| `force_cancel_run(db, run_id, canceled_by, reason)` | 強制取消：不管 running 狀態，直接 INSERT run_completed + DELETE run_queue（用於 zombie/卡住的 run） |
| `cancel_run_tree(db, run_id, canceled_by, reason)` | 遞迴取消：用 `WITH RECURSIVE` CTE 找出所有子孫 run，逐一呼叫 `cancel_run()` |
| `check_cancel(db, run_id)` | Worker 輪詢用：`SELECT canceled_by, canceled_reason FROM run_queue WHERE id = $1`，回傳 `Option<(String, Option<String>)>` |

`cancel_run()` 回傳 `CancelOutcome` 枚舉：

```rust
pub enum CancelOutcome {
    CompletedImmediately,  // 排隊中，已直接完成
    FlagSet,               // 執行中，已設旗標
    AlreadyCompleted,      // 已在 run_completed 中
    NotFound,              // run_id 不存在
}
```

### 5.2 rerun.rs

```
rerun(db, original_run_id, created_by, use_latest_version) → RerunResult { new_run_id, original_run_id }
```

邏輯：
1. 從 `run` 表讀取原始 run 的所有參數
2. 若 `use_latest_version=true` 且原始 run 有 `script_path`，從 `script` 表查最新 hash
3. 呼叫 `submit_run()` 建立新 run（立即排程）
4. `UPDATE run SET rerun_of = original_run_id WHERE id = new_run_id`

### 5.3 mark.rs

| 函式 | 用途 |
|------|------|
| `mark_success(db, run_id, marked_by, reason, result)` | 強制標記成功：INSERT run_completed(success=true) + DELETE run_queue + NOTIFY |
| `mark_fail(db, run_id, marked_by, reason, result)` | 強制標記失敗：INSERT run_completed(success=false) + DELETE run_queue + NOTIFY |

使用 `ON CONFLICT (id) DO UPDATE` 確保冪等性。

---

## 6. Worker 層整合

### 6.1 Sandbox trait 新增

```rust
async fn execute_cancellable(
    &self, ctx: &SandboxContext, cancel: CancellationToken,
) -> SandboxResult<SandboxOutput>;
```

預設實作使用 `tokio::select!` 將 `execute()` 與 `cancel.cancelled()` 競爭。

### 6.2 NoneSandbox 覆寫

`execute_cancellable()` 使用三路 `select!`：

```rust
tokio::select! {
    output = child.wait_with_output() => { /* 正常完成 */ }
    () = tokio::time::sleep(timeout)  => { kill_process_tree(pid); Err(Timeout) }
    () = cancel.cancelled()           => { signal_chain(pid).await; Err(Canceled) }
}
```

新增 `SandboxError::Canceled` variant。

### 6.3 信號鏈

使用 `nix::sys::signal::kill(Pid, Signal)` 發送 OS 信號：

```
signal_chain(pid):
  1. SIGINT → 等 3s → 檢查是否存活
  2. SIGTERM → 等 5s → 檢查是否存活
  3. SIGKILL（最終手段）
```

### 6.4 取消偵測迴圈

每個 running run 啟動一個 `cancel_detection_loop` task：

```
軌道 1: LISTEN 'run_cancel' — 收到 payload == run_id 時觸發
軌道 2: 每 500ms SELECT canceled_by — 兜底
偵測到 → cancel_token.cancel() → sandbox 的 select! 分支觸發
```

若 PgListener 建立失敗，自動降級為純輪詢模式。

### 6.5 execute_run() 整合

```rust
async fn execute_run(...) {
    let cancel_token = CancellationToken::new();

    // 啟動取消偵測（雙軌）
    let detector = tokio::spawn(cancel_detection_loop(db, run_id, cancel_token.clone()));

    // 執行（帶取消支援）
    let result = dispatch_run_cancellable(run, job_dir, resources, cancel_token).await;

    // 停止偵測
    detector.abort();

    match result {
        Err(SandboxError::Canceled(reason)) => finish_run_canceled(db, run_id, reason).await,
        Ok(value) => finish_run(db, run_id, true, value, ...).await,
        Err(e) => finish_run(db, run_id, false, error_json, ...).await,
    }
}
```

---

## 7. API 端點（Phase 2 實作）

```
POST /api/w/{workspace_id}/runs/{id}/cancel        — 軟取消
POST /api/w/{workspace_id}/runs/{id}/force-cancel   — 強制取消（管理員）
POST /api/w/{workspace_id}/runs/{id}/rerun          — 重跑
POST /api/w/{workspace_id}/runs/{id}/mark-success   — 手動標記成功（管理員）
POST /api/w/{workspace_id}/runs/{id}/mark-fail      — 手動標記失敗（管理員）
```

Request/Response：

```rust
// Cancel
POST { reason: Option<String> }
→ 200 { run_id, outcome: "completed_immediately" | "flag_set" | "already_completed" }

// Rerun
POST { use_latest_version: Option<bool> }
→ 200 { new_run_id, original_run_id }

// Mark
POST { reason: Option<String>, result: Option<Value> }
→ 200 { run_id }
```

權限模型：

| 操作 | 所需權限 |
|------|---------|
| Cancel | Run 建立者或 workspace 管理員 |
| Force Cancel | Workspace 管理員 |
| Rerun | Run 建立者或 workspace 管理員 |
| Mark Success/Fail | Workspace 管理員 |

---

## 8. 前端（Phase 2 實作）

Run Detail 頁面新增 Actions dropdown：

| 按鈕 | 顯示條件 | 確認對話框 |
|------|---------|-----------|
| Cancel | run 在 queue 中 | 「確定要取消此 run？」 |
| Force Cancel | running + admin | 「強制取消會直接標記失敗，不等 worker 停止。繼續？」 |
| Rerun | run 已完成 | 無（一鍵操作） |
| Mark Success | run 在 queue 中 + admin | 「將此 run 標記為成功？」 |
| Mark Fail | run 在 queue 中 + admin | 「將此 run 標記為失敗？」 |

Cancel 後顯示「Cancelling...」spinner，30 秒未完成則顯示「Force Cancel?」按鈕。

---

## 9. 驗證方式

```bash
# Cancel
1. 建立 sleep(60) script → 執行 → Cancel → 確認 worker 收到 SIGINT → run 標記 canceled
2. 建立排隊中 run（scheduled_for = 未來）→ Cancel → 確認直接完成
3. 建立 Flow（3 步驟）→ 第 2 步執行中 Cancel → 確認所有子 run 被取消

# Force Cancel
4. 停掉 worker → 有 running run 卡住 → Force Cancel → 確認直接標記失敗

# Rerun
5. 已完成的 run → Rerun → 確認新 run 使用相同參數
6. Rerun with use_latest_version=true → 確認使用最新 script hash
7. 確認 new run 的 rerun_of 指向 original run

# Mark
8. Running run → Mark Success → 確認 run_completed.success=true，marked_by 有值
9. Running run → Mark Fail → 確認 run_completed.success=false，marked_by 有值

# 邊界情境
10. 已完成的 run → Cancel → 回傳 AlreadyCompleted
11. 不存在的 run → Cancel → 回傳 NotFound
12. 同時 Cancel 同一個 run 兩次 → 冪等（第二次回傳 FlagSet）
```

---

## 10. 實作優先序

| 階段 | 範圍 | 狀態 |
|------|------|------|
| Phase 1 | Schema migration + Queue 層函式 + Worker 取消偵測/信號鏈 | **已完成** |
| Phase 2 | API 端點（5 個 route + handler） | 待實作 |
| Phase 2 | 前端 Actions dropdown + Cancel spinner | 待實作 |
| Phase 2+ | 整合測試（上方驗證方式 1-12） | 待實作 |
