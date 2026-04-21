# 第二章：Rust API Server

## 技術選型

| 層次 | Windmill 用的 | 你可以用 |
|------|--------------|----------|
| HTTP 框架 | Axum | Axum（推薦） |
| 序列化 | serde + serde_json | 同上 |
| 資料庫 | sqlx (PostgreSQL) | 同上 |
| 認證 | JWT + Cookie | 同上 |
| OpenAPI | utoipa | utoipa 或 aide |
| 非同步 | tokio | tokio |

## 專案起步

### Cargo.toml (workspace)

```toml
[workspace]
members = [
    "windmill-api",
    "windmill-worker",
    "windmill-queue",
    "windmill-common",
]

[workspace.dependencies]
axum = "0.8"
tokio = { version = "1", features = ["full"] }
sqlx = { version = "0.8", features = ["runtime-tokio", "postgres", "json", "uuid", "chrono"] }
serde = { version = "1", features = ["derive"] }
serde_json = "1"
uuid = { version = "1", features = ["v4", "serde"] }
chrono = { version = "0.4", features = ["serde"] }
tower = "0.5"
tower-http = { version = "0.6", features = ["cors", "compression-gzip"] }
tracing = "0.1"
tracing-subscriber = "0.3"
jsonwebtoken = "9"
```

## Server 啟動流程

Windmill 的啟動流程在 `backend/windmill-api/src/lib.rs`：

```rust
use axum::{Router, middleware};
use sqlx::postgres::PgPoolOptions;
use tower_http::cors::CorsLayer;

pub async fn run_server() -> anyhow::Result<()> {
    // 1. 初始化 DB 連線池
    let db = PgPoolOptions::new()
        .max_connections(50)
        .connect(&std::env::var("DATABASE_URL")?)
        .await?;

    // 2. 執行 migrations
    sqlx::migrate!("./migrations").run(&db).await?;

    // 3. 建立路由
    let app = Router::new()
        .nest("/api", api_routes(&db))
        .layer(CorsLayer::permissive())
        .layer(middleware::from_fn(logging_middleware));

    // 4. 啟動 HTTP server
    let listener = tokio::net::TcpListener::bind("0.0.0.0:8000").await?;
    axum::serve(listener, app).await?;

    Ok(())
}
```

## 路由組織

Windmill 有 75+ 個 API 模組，組織方式值得學習：

```rust
// Windmill 的路由架構（簡化版）
fn api_routes(db: &Pool<Postgres>) -> Router {
    Router::new()
        // 工作區層級的路由（需要 workspace_id）
        .nest("/w/:workspace_id", workspace_routes())
        // 全域路由
        .nest("/auth", auth_routes())
        .nest("/users", user_routes())
        .with_state(AppState { db: db.clone() })
}

fn workspace_routes() -> Router<AppState> {
    Router::new()
        .nest("/scripts", script_routes())
        .nest("/flows", flow_routes())
        .nest("/jobs", job_routes())
        .nest("/variables", variable_routes())
        .nest("/resources", resource_routes())
        .nest("/schedules", schedule_routes())
        .nest("/apps", app_routes())
}
```

### Windmill 的實際路由檔案結構

```
backend/windmill-api/src/
├── lib.rs                  # 主入口，組裝所有路由
├── auth.rs                 # 認證相關
├── users.rs                # 使用者 CRUD
├── workspaces.rs           # Workspace 管理
├── scripts.rs              # Script CRUD + 版本管理
├── flows.rs                # Flow CRUD
├── jobs.rs                 # Job 執行 + 查詢（最大的檔案）
├── variables.rs            # 變數/秘密管理
├── resources.rs            # 外部資源管理
├── schedules.rs            # 排程管理
├── apps.rs                 # App Builder
├── triggers/               # 觸發器模組
│   ├── mod.rs
│   ├── http/               # HTTP trigger
│   ├── kafka/              # Kafka trigger
│   ├── websocket/          # WebSocket trigger
│   └── ...
├── oauth2.rs               # OAuth2 整合
├── settings.rs             # 系統設定
├── capture.rs              # Webhook capture
├── concurrency.rs          # 併發控制
└── sse.rs                  # Server-Sent Events
```

## 認證系統

### JWT + Cookie 雙模式

```rust
use axum::{
    extract::{FromRequestParts, State},
    http::{request::Parts, StatusCode},
};
use jsonwebtoken::{decode, encode, DecodingKey, EncodingKey, Header, Validation};

#[derive(Debug, Serialize, Deserialize)]
struct Claims {
    sub: String,          // email
    exp: usize,           // 過期時間
    workspace_id: String,
    username: String,
    is_admin: bool,
    groups: Vec<String>,
}

// Axum Extractor：從 request 提取已認證的使用者
struct AuthedUser {
    email: String,
    username: String,
    workspace_id: String,
    is_admin: bool,
    groups: Vec<String>,
}

#[axum::async_trait]
impl<S: Send + Sync> FromRequestParts<S> for AuthedUser {
    type Rejection = StatusCode;

    async fn from_request_parts(parts: &mut Parts, _state: &S) -> Result<Self, Self::Rejection> {
        // 1. 嘗試從 Authorization header 取 Bearer token
        let token = parts.headers
            .get("Authorization")
            .and_then(|v| v.to_str().ok())
            .and_then(|v| v.strip_prefix("Bearer "))
            // 2. 或從 Cookie 取
            .or_else(|| {
                parts.headers.get("Cookie")
                    .and_then(|v| v.to_str().ok())
                    .and_then(|v| extract_cookie(v, "token"))
            })
            .ok_or(StatusCode::UNAUTHORIZED)?;

        // 3. 驗證 JWT
        let claims = decode::<Claims>(
            token,
            &DecodingKey::from_secret(JWT_SECRET.as_bytes()),
            &Validation::default(),
        )
        .map_err(|_| StatusCode::UNAUTHORIZED)?
        .claims;

        Ok(AuthedUser {
            email: claims.sub,
            username: claims.username,
            workspace_id: claims.workspace_id,
            is_admin: claims.is_admin,
            groups: claims.groups,
        })
    }
}
```

### Windmill 的認證層

Windmill 實際上更複雜，支援：

1. **JWT Token** — 前端用
2. **API Token** — 存在 `token` 表，hash 後比對
3. **OAuth2** — GitHub、Google、Microsoft 等
4. **Magic Link** — Email 登入連結
5. **Webhook Token** — Flow 觸發用

關鍵的 Authed 結構（`backend/windmill-common/src/auth.rs`）：

```rust
// Windmill 的 AuthedClient 結構
pub struct AuthedClient {
    pub base_internal_url: String,
    pub workspace: String,
    pub token: String,
    pub force_client: Option<reqwest::Client>,
}

// 用於 DB 操作時設定 RLS session
pub async fn set_session_variables(
    pool: &Pool<Postgres>,
    username: &str,
    groups: &[String],
    is_admin: bool,
) -> sqlx::Result<()> {
    let mut tx = pool.begin().await?;
    sqlx::query("SELECT set_config('session.user', $1, true)")
        .bind(username)
        .execute(&mut *tx)
        .await?;
    sqlx::query("SELECT set_config('session.groups', $1, true)")
        .bind(&groups.join(","))
        .execute(&mut *tx)
        .await?;
    sqlx::query("SELECT set_config('session.is_admin', $1, true)")
        .bind(&is_admin.to_string())
        .execute(&mut *tx)
        .await?;
    tx.commit().await?;
    Ok(())
}
```

## API Handler 範例

### Script CRUD

```rust
use axum::{
    extract::{Path, State, Json},
    http::StatusCode,
};

// 建立腳本
async fn create_script(
    authed: AuthedUser,
    State(db): State<Pool<Postgres>>,
    Path(workspace_id): Path<String>,
    Json(new_script): Json<NewScript>,
) -> Result<Json<ScriptHash>, ApiError> {
    // 1. 計算內容 hash
    let hash = calculate_hash(&new_script.content);

    // 2. 設定 RLS session
    set_session_variables(&db, &authed.username, &authed.groups, authed.is_admin).await?;

    // 3. 插入 script
    sqlx::query!(
        r#"INSERT INTO script
           (workspace_id, hash, path, content, language, schema, created_by, parent_hashes, summary)
           VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)"#,
        workspace_id,
        hash,
        new_script.path,
        new_script.content,
        new_script.language as _,
        new_script.schema,
        authed.username,
        &new_script.parent_hashes,
        new_script.summary,
    )
    .execute(&db)
    .await?;

    // 4. 解析腳本簽名（用 parser）
    let schema = parse_script_signature(&new_script.content, &new_script.language)?;

    Ok(Json(ScriptHash { hash }))
}

// 取得最新版本
async fn get_script_by_path(
    authed: AuthedUser,
    State(db): State<Pool<Postgres>>,
    Path((workspace_id, path)): Path<(String, String)>,
) -> Result<Json<Script>, ApiError> {
    let script = sqlx::query_as!(
        Script,
        r#"SELECT * FROM script
           WHERE workspace_id = $1 AND path = $2 AND archived = false
           ORDER BY created_at DESC
           LIMIT 1"#,
        workspace_id,
        path,
    )
    .fetch_optional(&db)
    .await?
    .ok_or(ApiError::NotFound)?;

    Ok(Json(script))
}
```

### 執行 Job

```rust
// 按路徑執行腳本
async fn run_script_by_path(
    authed: AuthedUser,
    State(db): State<Pool<Postgres>>,
    Path((workspace_id, script_path)): Path<(String, String)>,
    Json(args): Json<serde_json::Value>,
) -> Result<Json<Uuid>, ApiError> {
    // 1. 找到最新版本的 script
    let script = get_latest_script(&db, &workspace_id, &script_path).await?;

    // 2. 建立 job 並放入 queue
    let job_id = push_job(
        &db,
        &workspace_id,
        JobPayload::ScriptHash {
            hash: script.hash,
            path: script_path,
        },
        args,
        &authed.username,
        &authed.email,
    )
    .await?;

    Ok(Json(job_id))
}

// 放入 queue
async fn push_job(
    db: &Pool<Postgres>,
    workspace_id: &str,
    payload: JobPayload,
    args: serde_json::Value,
    created_by: &str,
    email: &str,
) -> Result<Uuid, ApiError> {
    let job_id = Uuid::new_v4();

    let (script_hash, script_path, job_kind, language) = match payload {
        JobPayload::ScriptHash { hash, path } => {
            let script = get_script_by_hash(db, workspace_id, hash).await?;
            (Some(hash), Some(path), JobKind::Script, Some(script.language))
        }
        JobPayload::Flow { path } => {
            (None, Some(path), JobKind::Flow, None)
        }
        _ => todo!(),
    };

    sqlx::query!(
        r#"INSERT INTO queue
           (id, workspace_id, script_hash, script_path, args, job_kind, language,
            created_by, permissioned_as, email, scheduled_for, tag)
           VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, now(), $11)"#,
        job_id,
        workspace_id,
        script_hash,
        script_path,
        serde_json::to_value(&args)?,
        job_kind as _,
        language as _,
        created_by,
        format!("u/{}", created_by),
        email,
        "default",  // worker tag
    )
    .execute(db)
    .await?;

    Ok(job_id)
}
```

## 即時更新：SSE (Server-Sent Events)

Windmill 用 SSE 來即時推送 job 狀態和日誌：

```rust
use axum::response::sse::{Event, KeepAlive, Sse};
use tokio_stream::StreamExt;

async fn stream_job_logs(
    authed: AuthedUser,
    State(db): State<Pool<Postgres>>,
    Path((workspace_id, job_id)): Path<(String, Uuid)>,
) -> Sse<impl futures::Stream<Item = Result<Event, anyhow::Error>>> {
    let stream = async_stream::stream! {
        let mut last_log_offset = 0;

        loop {
            // 1. 查詢 job 狀態
            let job = sqlx::query!(
                "SELECT logs, running FROM queue WHERE id = $1 AND workspace_id = $2",
                job_id, workspace_id
            )
            .fetch_optional(&db)
            .await?;

            match job {
                Some(job) => {
                    // 2. 推送新增的日誌
                    if let Some(logs) = &job.logs {
                        if logs.len() > last_log_offset {
                            let new_logs = &logs[last_log_offset..];
                            last_log_offset = logs.len();
                            yield Ok(Event::default()
                                .event("log")
                                .data(new_logs));
                        }
                    }

                    if !job.running {
                        // Job 還在等待
                        yield Ok(Event::default()
                            .event("status")
                            .data("waiting"));
                    }
                }
                None => {
                    // 3. Job 不在 queue 了，檢查 completed_job
                    let completed = sqlx::query!(
                        "SELECT success, result FROM completed_job WHERE id = $1",
                        job_id
                    )
                    .fetch_optional(&db)
                    .await?;

                    if let Some(c) = completed {
                        yield Ok(Event::default()
                            .event("result")
                            .data(serde_json::to_string(&c.result)?));
                        break;
                    }
                }
            }

            tokio::time::sleep(std::time::Duration::from_millis(500)).await;
        }
    };

    Sse::new(stream).keep_alive(KeepAlive::default())
}
```

## 錯誤處理

```rust
use axum::response::{IntoResponse, Response};

#[derive(Debug)]
enum ApiError {
    NotFound,
    Unauthorized,
    BadRequest(String),
    InternalError(anyhow::Error),
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        let (status, message) = match self {
            ApiError::NotFound => (StatusCode::NOT_FOUND, "Not found".to_string()),
            ApiError::Unauthorized => (StatusCode::UNAUTHORIZED, "Unauthorized".to_string()),
            ApiError::BadRequest(msg) => (StatusCode::BAD_REQUEST, msg),
            ApiError::InternalError(e) => {
                tracing::error!("Internal error: {:?}", e);
                (StatusCode::INTERNAL_SERVER_ERROR, "Internal error".to_string())
            }
        };

        (status, Json(serde_json::json!({ "error": message }))).into_response()
    }
}

// 自動轉換 sqlx::Error
impl From<sqlx::Error> for ApiError {
    fn from(e: sqlx::Error) -> Self {
        match e {
            sqlx::Error::RowNotFound => ApiError::NotFound,
            _ => ApiError::InternalError(e.into()),
        }
    }
}
```

## OpenAPI 文件生成

Windmill 用 `utoipa` 自動生成 OpenAPI spec：

```rust
use utoipa::OpenApi;

#[derive(OpenApi)]
#[openapi(
    paths(
        create_script,
        get_script_by_path,
        run_script_by_path,
    ),
    components(schemas(Script, NewScript, ScriptHash))
)]
struct ApiDoc;

// 從 OpenAPI spec 自動生成前端 client
// Windmill 用 openapi-typescript-codegen 生成 frontend/src/lib/gen/
```

## 你的實作順序

1. **空的 Axum server** — 能回 "Hello World"
2. **DB 連線** — PgPool + 第一個 migration
3. **認證** — JWT encode/decode + login endpoint
4. **Script CRUD** — 建立/讀取/列表/刪除
5. **Job Push** — 把 job 放進 queue
6. **SSE** — 即時推送 job 狀態
7. **Flow CRUD** — 工作流的 CRUD
