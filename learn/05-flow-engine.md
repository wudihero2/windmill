# 第五章：工作流引擎（Flow Engine）

## 核心思想

Windmill 的 Flow Engine 把**工作流執行問題**轉化成**遞迴的 job 問題**：

1. 一個 Flow 本身就是一個 job（`JobKind::Flow`）
2. Flow 的每個 step 會變成一個子 job
3. 每當子 job 完成，Flow Engine 決定下一步
4. Flow job 的 `flow_status` (JSONB) 追蹤整個狀態

```
Flow Job (parent)
├── Step A job (child) → 完成 → 觸發 Step B
├── Step B job (child) → 完成 → 觸發 Step C
├── Step C: BranchOne
│   ├── 條件 1 → Step C-1 job
│   └── 條件 2 → Step C-2 job
└── Step D: ForLoop
    ├── Iteration 0 → job
    ├── Iteration 1 → job
    └── Iteration 2 → job
```

## Flow 資料模型

### FlowValue（Flow 的定義）

`backend/windmill-common/src/flows.rs`：

```rust
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct FlowValue {
    pub modules: Vec<FlowModule>,             // 步驟列表
    pub failure_module: Option<Box<FlowModule>>, // 全域錯誤處理
    #[serde(default)]
    pub same_worker: bool,                    // 所有步驟在同一個 worker
    pub preprocessor_module: Option<FlowModule>, // 前處理模組
    // ...
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct FlowModule {
    pub id: String,                           // 步驟 ID（如 "a", "b", "c"）
    pub value: FlowModuleValue,               // 步驟類型和內容
    pub summary: Option<String>,
    pub retry: Option<Retry>,                 // 重試設定
    pub suspend: Option<Suspend>,             // 暫停/審批設定
    pub sleep: Option<InputTransform>,        // 延遲執行
    pub mock: Option<Mock>,                   // 測試 mock
    pub skip_if: Option<SkipIf>,             // 條件跳過
    pub delete_after_use: Option<bool>,       // 完成後刪除
}
```

### FlowModuleValue（步驟類型）

```rust
#[derive(Serialize, Deserialize, Debug, Clone)]
#[serde(tag = "type")]
pub enum FlowModuleValue {
    // 1. 引用已有腳本
    Script {
        path: String,                          // 腳本路徑
        hash: Option<ScriptHash>,              // 指定版本
        input_transforms: HashMap<String, InputTransform>,  // 輸入映射
        tag_override: Option<String>,
    },

    // 2. 內嵌程式碼
    RawScript {
        content: String,                       // 程式碼內容
        language: ScriptLang,
        input_transforms: HashMap<String, InputTransform>,
        lock: Option<String>,                  // 依賴鎖檔
        tag: Option<String>,
    },

    // 3. 引用另一個 Flow
    Flow {
        path: String,
        input_transforms: HashMap<String, InputTransform>,
    },

    // 4. For 迴圈
    ForloopFlow {
        iterator: InputTransform,              // 迭代來源
        modules: Vec<FlowModule>,              // 迴圈體
        skip_failures: bool,
        parallel: bool,                        // 並行執行
        parallelism: Option<u16>,              // 最大並行數
    },

    // 5. 條件分支（擇一）
    BranchOne {
        branches: Vec<Branch>,                 // 條件分支列表
        default: Vec<FlowModule>,              // 預設分支
    },

    // 6. 並行分支（全部執行）
    BranchAll {
        branches: Vec<BranchAllItem>,
        parallel: bool,
    },

    // 7. 直接傳遞（identity）
    Identity,

    // 8. AI Agent
    AIAgent {
        // ...
    },
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Branch {
    pub expr: String,                          // JavaScript 條件表達式
    pub modules: Vec<FlowModule>,
    pub summary: Option<String>,
}
```

### InputTransform（步驟間的資料傳遞）

```rust
#[derive(Serialize, Deserialize, Debug, Clone)]
#[serde(tag = "type")]
pub enum InputTransform {
    // 靜態值
    Static {
        value: serde_json::Value,
    },
    // JavaScript 表達式（可引用上一步結果）
    Javascript {
        expr: String,
        // 可用的變數：
        // - results.a          → 步驟 a 的結果
        // - results.a.value    → 步驟 a 結果的某個欄位
        // - flow_input.x       → Flow 的輸入參數 x
        // - previous_result    → 上一步的結果
        // - resume             → 審批恢復的值
        // - approvers          → 審批者列表
    },
}
```

## FlowStatus（流程狀態追蹤）

`backend/windmill-types/src/flow_status.rs`：

```rust
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct FlowStatus {
    pub step: i32,                             // 當前步驟索引
    pub modules: Vec<FlowStatusModule>,        // 每個模組的狀態
    pub failure_module: Box<FlowStatusModuleWParent>,  // 錯誤處理模組狀態
    pub retry: RetryStatus,                    // 重試狀態
    pub approval_conditions: Option<ApprovalConditions>,
    pub restarted_from: Option<RestartedFrom>, // 從哪裡重啟的
}

#[derive(Serialize, Deserialize, Debug, Clone)]
#[serde(tag = "type")]
pub enum FlowStatusModule {
    // 等待前置步驟完成
    WaitingForPriorSteps { id: String },

    // 等待外部事件（審批）
    WaitingForEvents {
        id: String,
        count: u16,               // 需要幾個審批
        job: Uuid,                // 對應的 job
    },

    // 等待 executor 執行
    WaitingForExecutor {
        id: String,
        job: Uuid,
    },

    // 執行中
    InProgress {
        id: String,
        job: Uuid,
        iterator: Option<Iterator>,         // For 迴圈狀態
        flow_jobs: Option<Vec<Uuid>>,       // 並行子 job
        branchall: Option<BranchAllStatus>,
        parallel: Option<bool>,
    },

    // 成功完成
    Success {
        id: String,
        job: Uuid,
        flow_jobs: Option<Vec<Uuid>>,
        branch_chosen: Option<BranchChosen>,
        approvers: Option<Vec<Approval>>,
    },

    // 失敗
    Failure {
        id: String,
        job: Uuid,
        flow_jobs: Option<Vec<Uuid>>,
    },
}
```

## 狀態機：Flow 執行邏輯

`backend/windmill-worker/src/worker_flow.rs`（5000+ 行的核心檔案）：

### 整體流程

```
                      ┌──────────────────┐
                      │  Flow Job 開始    │
                      │  step = 0        │
                      └────────┬─────────┘
                               │
                      ┌────────▼─────────┐
                      │  取得當前 module  │
                      │  modules[step]   │
                      └────────┬─────────┘
                               │
                 ┌─────────────┼─────────────┐
                 │             │             │
        ┌────────▼──────┐  ┌──▼──────┐  ┌──▼──────────┐
        │ Script/Raw    │  │ ForLoop │  │ BranchOne   │
        │ → 建立子 job  │  │ → 迭代  │  │ → 評估條件  │
        └────────┬──────┘  └──┬──────┘  └──┬──────────┘
                 │            │            │
                 └────────────┼────────────┘
                              │
                     ┌────────▼─────────┐
                     │  子 job 完成      │
                     │  (callback)      │
                     └────────┬─────────┘
                              │
                     ┌────────▼─────────┐
                     │ update_flow_     │
                     │ status_after_    │
                     │ job_completion() │
                     └────────┬─────────┘
                              │
                 ┌────────────┼────────────┐
                 │            │            │
        ┌────────▼──────┐  ┌─▼─────────┐  ┌▼────────────┐
        │ 還有下一步     │  │ 全部完成   │  │ 失敗        │
        │ step++        │  │ Flow 成功  │  │ → retry?    │
        │ → 繼續        │  │           │  │ → failure   │
        │               │  │           │  │   module?   │
        └───────────────┘  └───────────┘  └─────────────┘
```

### 核心函式

```rust
// 當子 job 完成時觸發
pub async fn update_flow_status_after_job_completion(
    db: &Pool<Postgres>,
    job: &QueuedJob,           // 完成的子 job
    success: bool,
    result: &serde_json::Value,
    worker_name: &str,
) -> Result<()> {
    // 1. 找到父 Flow job
    let flow_job = get_queued_job(db, job.parent_job.unwrap()).await?;
    let mut flow_status: FlowStatus = serde_json::from_value(
        flow_job.flow_status.unwrap()
    )?;

    // 2. 更新當前模組的狀態
    let current_module = &mut flow_status.modules[flow_status.step as usize];
    if success {
        *current_module = FlowStatusModule::Success {
            id: current_module.id().to_string(),
            job: job.id,
            flow_jobs: None,
            branch_chosen: None,
            approvers: None,
        };
    } else {
        // 檢查是否需要重試
        if should_retry(&flow_status, &flow_job, current_module) {
            flow_status.retry.fail_count += 1;
            // 重新推入同一個步驟
            push_retry_job(db, &flow_job, &flow_status).await?;
            return Ok(());
        }

        *current_module = FlowStatusModule::Failure {
            id: current_module.id().to_string(),
            job: job.id,
            flow_jobs: None,
        };
    }

    // 3. 決定下一步
    let next = compute_next_flow_transform(
        &flow_job,
        &flow_status,
        &result,
        db,
    ).await?;

    match next {
        NextFlowTransform::Continue(payload, next_status) => {
            // 推入下一個子 job
            flow_status.step += 1;
            let next_job_id = push_next_flow_job(db, &flow_job, payload).await?;

            // 更新 flow_status
            update_flow_status(db, flow_job.id, &flow_status).await?;
        }
        NextFlowTransform::EmptyInnerFlows { .. } => {
            // 空迴圈/分支，跳到下一步
            flow_status.step += 1;
            // 遞迴處理
        }
    }

    // 4. 檢查是否所有步驟都完成
    if flow_status.step as usize >= flow_status.modules.len() {
        // Flow 完成！
        let final_result = get_step_result(db, &flow_status).await?;
        complete_job(db, &flow_job, true, final_result).await?;
    }

    Ok(())
}
```

### NextFlowTransform 和 ContinuePayload

```rust
enum NextFlowTransform {
    // 空的迴圈/分支（跳過）
    EmptyInnerFlows { branch_chosen: Option<BranchChosen> },
    // 繼續執行
    Continue(ContinuePayload, NextStatus),
}

enum ContinuePayload {
    // 單個 job
    SingleJob(JobPayloadWithTag),
    // 並行多個 job（ForLoop parallel 或 BranchAll）
    ParallelJobs(Vec<JobPayloadWithTag>),
}

enum NextStatus {
    // 進入下一步
    NextStep,
    // 並行 jobs
    AllFlowJobs {
        branchall: Option<BranchAllStatus>,
        iterator: Option<FlowIterator>,
    },
    // 繼續迭代
    NextLoopIteration {
        next: FlowIterator,
    },
}
```

### compute_next_flow_transform

```rust
async fn compute_next_flow_transform(
    flow_job: &QueuedJob,
    flow: &FlowValue,
    module: &FlowModule,
    status: &FlowStatus,
    last_result: &serde_json::Value,
    db: &Pool<Postgres>,
) -> Result<NextFlowTransform> {
    match module.get_value()? {
        // Script → 直接建立 job
        FlowModuleValue::Script { path, input_transforms, .. } => {
            let args = evaluate_input_transforms(
                &input_transforms,
                flow_job,
                status,
                last_result,
            ).await?;

            let payload = JobPayload::ScriptHash {
                hash: get_latest_hash(db, &flow_job.workspace_id, &path).await?,
                path,
            };

            Ok(NextFlowTransform::Continue(
                ContinuePayload::SingleJob(JobPayloadWithTag { payload, tag: None, .. }),
                NextStatus::NextStep,
            ))
        }

        // ForLoop → 迭代或並行
        FlowModuleValue::ForloopFlow { iterator, modules, parallel, parallelism, .. } => {
            let items = evaluate_iterator(&iterator, last_result).await?;

            if parallel {
                // 並行：一次推入所有 iteration 的 job
                let payloads = items.iter().enumerate().map(|(i, item)| {
                    create_loop_iteration_payload(&modules, item, i)
                }).collect();

                Ok(NextFlowTransform::Continue(
                    ContinuePayload::ParallelJobs(payloads),
                    NextStatus::AllFlowJobs {
                        iterator: Some(FlowIterator { index: 0, itered: items }),
                        branchall: None,
                    },
                ))
            } else {
                // 序列：先推第一個
                let payload = create_loop_iteration_payload(&modules, &items[0], 0);
                Ok(NextFlowTransform::Continue(
                    ContinuePayload::SingleJob(payload),
                    NextStatus::NextLoopIteration {
                        next: FlowIterator { index: 1, itered: items },
                    },
                ))
            }
        }

        // BranchOne → 評估條件，選擇分支
        FlowModuleValue::BranchOne { branches, default, .. } => {
            let mut chosen = BranchChosen::Default;

            for (i, branch) in branches.iter().enumerate() {
                let result = evaluate_bool_expr(
                    &branch.expr,
                    flow_job,
                    status,
                    last_result,
                ).await?;

                if result {
                    chosen = BranchChosen::Branch { branch: i };
                    break;
                }
            }

            let modules = match chosen {
                BranchChosen::Default => default,
                BranchChosen::Branch { branch } => branches[branch].modules.clone(),
            };

            let payload = create_inner_flow_payload(&modules);
            Ok(NextFlowTransform::Continue(
                ContinuePayload::SingleJob(payload),
                NextStatus::NextStep,
            ))
        }

        // BranchAll → 全部並行執行
        FlowModuleValue::BranchAll { branches, .. } => {
            let payloads = branches.iter().map(|b| {
                create_inner_flow_payload(&b.modules)
            }).collect();

            Ok(NextFlowTransform::Continue(
                ContinuePayload::ParallelJobs(payloads),
                NextStatus::AllFlowJobs {
                    branchall: Some(BranchAllStatus { branch: 0, len: branches.len() }),
                    iterator: None,
                },
            ))
        }

        // Identity → 直接傳遞
        FlowModuleValue::Identity => {
            Ok(NextFlowTransform::Continue(
                ContinuePayload::SingleJob(JobPayloadWithTag {
                    payload: JobPayload::Identity,
                    ..
                }),
                NextStatus::NextStep,
            ))
        }
    }
}
```

## 暫停/恢復（Approval Steps）

```rust
// 暫停 flow（等待人工審批）
if let Some(suspend) = &module.suspend {
    // 設定 suspend 計數（需要幾個人審批）
    sqlx::query!(
        "UPDATE queue SET suspend = $1 WHERE id = $2",
        suspend.required_events,
        flow_job.id,
    )
    .execute(db)
    .await?;

    // Flow 停在這裡，直到收到足夠的 resume 呼叫
    return Ok(());
}

// 恢復 flow（API endpoint）
pub async fn resume_suspended_flow(
    db: &Pool<Postgres>,
    job_id: Uuid,
    approver: String,
    resume_value: serde_json::Value,
) -> Result<()> {
    // 1. 減少 suspend 計數
    let remaining = sqlx::query_scalar!(
        r#"UPDATE queue
           SET suspend = suspend - 1
           WHERE id = $1
           RETURNING suspend"#,
        job_id,
    )
    .fetch_one(db)
    .await?;

    // 2. 記錄審批者和值
    // ...

    // 3. 如果 suspend 降到 0，繼續執行
    if remaining <= 0 {
        // 推入下一個步驟的 job
        continue_flow_after_resume(db, job_id, resume_value).await?;
    }

    Ok(())
}
```

## 表達式評估

Flow 中的 `input_transforms` 和 `branch.expr` 使用 JavaScript 表達式：

```rust
use deno_core::JsRuntime;

async fn evaluate_input_transforms(
    transforms: &HashMap<String, InputTransform>,
    flow_job: &QueuedJob,
    status: &FlowStatus,
    last_result: &serde_json::Value,
) -> Result<HashMap<String, serde_json::Value>> {
    let mut result = HashMap::new();

    for (key, transform) in transforms {
        let value = match transform {
            InputTransform::Static { value } => value.clone(),
            InputTransform::Javascript { expr } => {
                // 建立 JS 執行環境，注入可用變數
                let context = serde_json::json!({
                    "results": collect_step_results(status),
                    "flow_input": flow_job.args,
                    "previous_result": last_result,
                });

                // 用 Deno runtime 評估 JS 表達式
                evaluate_js_expr(expr, &context).await?
            }
        };
        result.insert(key.clone(), value);
    }

    Ok(result)
}
```

## 重試機制

```rust
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Retry {
    pub constant: Option<ConstantRetry>,
    pub exponential: Option<ExponentialRetry>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct ConstantRetry {
    pub attempts: u32,    // 最大重試次數
    pub seconds: u32,     // 每次間隔（秒）
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct ExponentialRetry {
    pub attempts: u32,
    pub multiplier: u32,  // 指數基數
    pub seconds: u32,     // 初始間隔
    pub random_factor: Option<f64>,  // 隨機抖動
}
```

## 你的實作順序

1. **最簡 Flow** — 只有 Sequential steps（A → B → C）
2. **Input Transforms** — 步驟間用 JS 表達式傳遞資料
3. **FlowStatus** — 追蹤每個步驟的狀態
4. **BranchOne** — 條件分支
5. **ForLoop** — 迴圈（先做 sequential）
6. **Parallel** — 並行執行（ForLoop parallel + BranchAll）
7. **Retry** — 重試機制
8. **Suspend/Resume** — 人工審批
9. **Error handling** — failure_module

### 簡化建議

Windmill 的 `worker_flow.rs` 有 5000+ 行，因為處理了太多 edge case。如果你從零開始，可以：

1. **用 Rust enum 表示狀態**，而不是 JSON
2. **先不支援 parallel**，純 sequential 就夠 80% 場景
3. **用 Lua/Rhai 取代 JavaScript 做表達式評估**，避免依賴 Deno runtime
4. **先不做 suspend/resume**，後面再加
