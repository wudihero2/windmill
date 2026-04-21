# 第三章：任務佇列系統

## 核心概念

Windmill 的 Job Queue 是整個系統的心臟。一切都是 Job：

- 執行腳本 → `JobKind::Script`
- 執行 Flow → `JobKind::Flow`（父 job）
- Flow 的每個步驟 → `JobKind::FlowScript`（子 job）
- 解析依賴 → `JobKind::Dependencies`
- 預覽程式碼 → `JobKind::Preview`

## PostgreSQL-based Queue 的實作

### 為什麼不用 Redis/RabbitMQ？

**PostgreSQL `FOR UPDATE SKIP LOCKED` 就是一個 job queue：**

```sql
-- Worker 搶 job（原子操作）
WITH next_job AS (
    SELECT id
    FROM queue
    WHERE running = FALSE
      AND scheduled_for <= now()
      AND canceled = FALSE
      AND tag = ANY($1)           -- Worker 只處理特定 tag 的 job
    ORDER BY priority DESC,       -- 高優先級先
            scheduled_for ASC     -- 先到先處理
    LIMIT 1
    FOR UPDATE SKIP LOCKED        -- 關鍵！跳過被鎖定的行
)
UPDATE queue
SET running = TRUE,
    started_at = now(),
    last_ping = now()
FROM next_job
WHERE queue.id = next_job.id
RETURNING queue.*;
```

**`SKIP LOCKED` 的魔力：**
- 多個 Worker 同時查詢不會衝突
- 已被其他 Worker 鎖定的行自動跳過
- 不需要外部協調機制
- 效能足夠好（每秒數千 jobs）

### Windmill 的實際 Queue 拉取

`backend/windmill-queue/src/pull.rs` 中的核心邏輯：

```rust
// 簡化版的 pull_single_job
pub async fn pull_single_job(
    db: &Pool<Postgres>,
    worker_name: &str,
    worker_tags: &[String],     // Worker 可以處理的 tag
) -> Result<Option<QueuedJob>> {
    let job = sqlx::query_as::<_, QueuedJob>(
        r#"
        WITH candidate AS (
            SELECT id FROM queue
            WHERE running = FALSE
              AND scheduled_for <= now()
              AND canceled = FALSE
              AND tag = ANY($1)
            ORDER BY
                priority DESC NULLS LAST,
                scheduled_for ASC
            LIMIT 1
            FOR UPDATE SKIP LOCKED
        )
        UPDATE queue q
        SET running = TRUE,
            started_at = now(),
            last_ping = now()
        FROM candidate c
        WHERE q.id = c.id
        RETURNING q.*
        "#,
    )
    .bind(worker_tags)
    .fetch_optional(db)
    .await?;

    if let Some(ref job) = job {
        tracing::info!(
            worker = worker_name,
            job_id = %job.id,
            kind = ?job.job_kind,
            "Pulled job"
        );
    }

    Ok(job)
}
```

## Job 推入 Queue

### 核心 push 函式

`backend/windmill-queue/src/push.rs`：

```rust
pub enum JobPayload {
    ScriptHash {
        hash: ScriptHash,
        path: String,
    },
    Flow {
        path: String,
    },
    RawFlow {
        value: FlowValue,        // 內嵌 Flow 定義
        path: Option<String>,
    },
    RawScript {
        content: String,
        language: ScriptLang,
        lock: Option<String>,
    },
    Identity,                     // 直接傳遞結果
    Noop,                        // 空操作
}

pub async fn push(
    db: &Pool<Postgres>,
    workspace_id: &str,
    payload: JobPayload,
    args: HashMap<String, serde_json::Value>,
    user: &str,
    email: &str,
    // 很多可選參數...
    parent_job: Option<Uuid>,
    root_job: Option<Uuid>,
    scheduled_for: Option<DateTime<Utc>>,
    tag: Option<String>,
    timeout: Option<i32>,
    priority: Option<i16>,
) -> Result<(Uuid, QueueTransaction)> {
    let job_id = Uuid::new_v4();

    // 1. 根據 payload 決定 job_kind、language、script_hash 等
    let (job_kind, script_hash, script_path, language, raw_code, raw_lock) =
        match &payload {
            JobPayload::ScriptHash { hash, path } => {
                (JobKind::Script, Some(*hash), Some(path.clone()), None, None, None)
            }
            JobPayload::Flow { path } => {
                (JobKind::Flow, None, Some(path.clone()), None, None, None)
            }
            JobPayload::RawScript { content, language, lock } => {
                (JobKind::Preview, None, None, Some(*language), Some(content.clone()), lock.clone())
            }
            // ...
        };

    // 2. 決定 tag（用來路由到特定 worker）
    let tag = tag.unwrap_or_else(|| {
        match language {
            Some(ScriptLang::Python3) => "python".to_string(),
            Some(ScriptLang::Deno) | Some(ScriptLang::Bun) => "deno".to_string(),
            Some(ScriptLang::Go) => "go".to_string(),
            _ => "default".to_string(),
        }
    });

    // 3. Flow job 需要初始化 flow_status
    let flow_status = if job_kind == JobKind::Flow {
        let flow = get_flow_by_path(db, workspace_id, script_path.as_ref().unwrap()).await?;
        Some(serde_json::to_value(FlowStatus::new(&flow.value))?)
    } else {
        None
    };

    // 4. 插入 queue
    sqlx::query!(
        r#"INSERT INTO queue
           (id, workspace_id, parent_job, root_job,
            script_hash, script_path, args, job_kind, language,
            created_by, permissioned_as, email,
            scheduled_for, tag, flow_status,
            is_flow_step, priority, timeout)
           VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9,
                   $10, $11, $12, $13, $14, $15, $16, $17, $18)"#,
        job_id,
        workspace_id,
        parent_job,
        root_job,
        script_hash.map(|h| h.0),
        script_path,
        serde_json::to_value(&args)?,
        job_kind as _,
        language as _,
        user,
        format!("u/{}", user),
        email,
        scheduled_for.unwrap_or_else(Utc::now),
        tag,
        flow_status,
        parent_job.is_some(),
        priority,
        timeout,
    )
    .execute(db)
    .await?;

    Ok((job_id, tx))
}
```

## Job 完成

```rust
pub async fn complete_job(
    db: &Pool<Postgres>,
    job: &QueuedJob,
    success: bool,
    result: serde_json::Value,
) -> Result<()> {
    let duration_ms = job.started_at
        .map(|s| (Utc::now() - s).num_milliseconds() as i32);

    // 1. 插入 completed_job
    sqlx::query!(
        r#"INSERT INTO completed_job
           (id, workspace_id, parent_job, created_by, created_at,
            started_at, duration_ms, success, result,
            job_kind, script_hash, script_path, args, logs, language)
           SELECT id, workspace_id, parent_job, created_by, created_at,
                  started_at, $2, $3, $4,
                  job_kind, script_hash, script_path, args, logs, language
           FROM queue WHERE id = $1"#,
        job.id,
        duration_ms,
        success,
        serde_json::to_value(&result)?,
    )
    .execute(db)
    .await?;

    // 2. 從 queue 刪除
    sqlx::query!("DELETE FROM queue WHERE id = $1", job.id)
        .execute(db)
        .await?;

    // 3. 如果是 flow step，通知 parent flow
    if let Some(parent_id) = job.parent_job {
        notify_parent_flow(db, parent_id, job.id, success, &result).await?;
    }

    Ok(())
}
```

## 排程系統

### Cron Scheduler

Windmill 用一個專門的背景任務掃描 `schedule` 表：

```rust
pub async fn run_scheduler(db: Pool<Postgres>) {
    loop {
        // 1. 找到所有需要觸發的排程
        let schedules = sqlx::query_as!(
            Schedule,
            r#"SELECT * FROM schedule
               WHERE enabled = TRUE
               AND next_at <= now()
               FOR UPDATE SKIP LOCKED"#,
        )
        .fetch_all(&db)
        .await
        .unwrap_or_default();

        for schedule in schedules {
            // 2. 推入 job
            let _ = push(
                &db,
                &schedule.workspace_id,
                if schedule.is_flow {
                    JobPayload::Flow { path: schedule.script_path.clone() }
                } else {
                    // 找到最新的 script hash
                    let script = get_latest_script(&db, &schedule.workspace_id, &schedule.script_path).await.unwrap();
                    JobPayload::ScriptHash { hash: script.hash, path: schedule.script_path.clone() }
                },
                schedule.args.clone(),
                &schedule.created_by,
                &schedule.email,
                None, None,
                Some(schedule.next_at),
                None, None, None,
            ).await;

            // 3. 計算下次執行時間
            let next = cron::Schedule::from_str(&schedule.schedule)
                .unwrap()
                .upcoming(chrono::Utc)
                .next()
                .unwrap();

            sqlx::query!(
                "UPDATE schedule SET next_at = $1 WHERE workspace_id = $2 AND path = $3",
                next,
                schedule.workspace_id,
                schedule.path,
            )
            .execute(&db)
            .await
            .ok();
        }

        tokio::time::sleep(std::time::Duration::from_secs(1)).await;
    }
}
```

## 併發控制

### Concurrency Limit

Windmill 支援限制同一個 script 的並發執行數：

```rust
// 在 worker 拉取 job 時檢查
async fn check_concurrency(
    db: &Pool<Postgres>,
    job: &QueuedJob,
) -> Result<bool> {
    if let Some(limit) = job.concurrent_limit {
        let running_count: i64 = sqlx::query_scalar!(
            r#"SELECT COUNT(*) FROM queue
               WHERE script_path = $1
               AND workspace_id = $2
               AND running = TRUE
               AND id != $3"#,
            job.script_path,
            job.workspace_id,
            job.id,
        )
        .fetch_one(db)
        .await?
        .unwrap_or(0);

        if running_count >= limit as i64 {
            // 超過限制，放回 queue
            sqlx::query!(
                "UPDATE queue SET running = FALSE, started_at = NULL WHERE id = $1",
                job.id,
            )
            .execute(db)
            .await?;
            return Ok(false);
        }
    }
    Ok(true)
}
```

## Worker 心跳

```rust
// Worker 定期更新心跳
pub async fn worker_heartbeat(
    db: &Pool<Postgres>,
    worker_name: &str,
    job_id: Option<Uuid>,
) {
    loop {
        // 1. 更新 worker_ping 表
        sqlx::query!(
            r#"INSERT INTO worker_ping (worker, ping_at, jobs_executed)
               VALUES ($1, now(), 0)
               ON CONFLICT (worker) DO UPDATE
               SET ping_at = now()"#,
            worker_name,
        )
        .execute(db)
        .await
        .ok();

        // 2. 如果正在執行 job，更新 last_ping
        if let Some(id) = job_id {
            sqlx::query!(
                "UPDATE queue SET last_ping = now() WHERE id = $1",
                id,
            )
            .execute(db)
            .await
            .ok();
        }

        tokio::time::sleep(std::time::Duration::from_secs(15)).await;
    }
}

// Zombie job 偵測（long-running job 沒有心跳）
pub async fn detect_zombie_jobs(db: &Pool<Postgres>) {
    let zombies = sqlx::query_as!(
        QueuedJob,
        r#"SELECT * FROM queue
           WHERE running = TRUE
           AND last_ping < now() - interval '5 minutes'"#,
    )
    .fetch_all(db)
    .await
    .unwrap_or_default();

    for zombie in zombies {
        tracing::warn!(job_id = %zombie.id, "Detected zombie job, marking as failed");
        complete_job(db, &zombie, false, serde_json::json!({
            "error": "Job timed out (no heartbeat from worker)"
        })).await.ok();
    }
}
```

## 優先級和 Tag 路由

### Worker Tags

```
Worker A: tags = ["python", "default"]    → 處理 Python job + 預設 job
Worker B: tags = ["deno", "bun"]          → 處理 TypeScript job
Worker C: tags = ["go"]                    → 處理 Go job
Worker D: tags = ["gpu"]                   → 處理需要 GPU 的 job
```

### 優先級

```rust
// priority: SMALLINT，數字越大越優先
// 0 = 預設
// 正數 = 高優先級
// 負數 = 低優先級

// Worker 拉取時按 priority DESC 排序
ORDER BY priority DESC NULLS LAST, scheduled_for ASC
```

## 你的實作順序

1. **最簡 Queue** — 只有 `INSERT` 和 `SELECT FOR UPDATE SKIP LOCKED`
2. **Job 完成** — 移到 `completed_job` 表
3. **Worker 心跳** — `worker_ping` 表
4. **Zombie 偵測** — 定期掃描無心跳的 job
5. **排程** — `schedule` 表 + cron 解析
6. **優先級** — `priority` 欄位
7. **Tag 路由** — `tag` 欄位 + Worker 設定
8. **併發控制** — `concurrent_limit`
