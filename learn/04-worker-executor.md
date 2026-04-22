# 第四章：多語言 Worker 和執行器

## Worker 架構

Worker 是 Windmill 中實際執行程式碼的元件。每個 Worker 是一個獨立的 Rust 程序（或同一個程序的不同 thread），不斷 poll queue 拿 job 來執行。

```
Worker 主迴圈：
┌─────────────────────────────────────┐
│ loop {                              │
│   1. pull_job() from queue          │
│   2. match job.language {           │
│        Python  → python_executor    │
│        TS/Deno → deno_executor      │
│        Go      → go_executor        │
│        Bash    → bash_executor      │
│        ...                          │
│      }                              │
│   3. complete_job() or fail_job()   │
│   4. sleep if no job                │
│ }                                   │
└─────────────────────────────────────┘
```

### Windmill Worker 的主迴圈

`backend/windmill-worker/src/worker.rs`：

```rust
pub async fn run_worker(
    db: Pool<Postgres>,
    worker_name: String,
    worker_tags: Vec<String>,
    base_internal_url: String,
) {
    // 建立臨時工作目錄
    let worker_dir = format!("/tmp/windmill/{}", worker_name);
    tokio::fs::create_dir_all(&worker_dir).await.unwrap();

    loop {
        // 1. 從 queue 拉取 job
        let job = pull_single_job(&db, &worker_name, &worker_tags).await;

        match job {
            Ok(Some(job)) => {
                // 2. 建立 job 專用目錄
                let job_dir = format!("{}/{}", worker_dir, job.id);
                tokio::fs::create_dir_all(&job_dir).await.unwrap();

                // 3. 執行 job
                let result = handle_job(
                    &job,
                    &db,
                    &job_dir,
                    &base_internal_url,
                    &worker_name,
                ).await;

                // 4. 完成 job
                match result {
                    Ok(value) => {
                        complete_job(&db, &job, true, value).await.ok();
                    }
                    Err(e) => {
                        let error = serde_json::json!({
                            "error": { "message": e.to_string() }
                        });
                        complete_job(&db, &job, false, error).await.ok();
                    }
                }

                // 5. 清理 job 目錄
                tokio::fs::remove_dir_all(&job_dir).await.ok();
            }
            Ok(None) => {
                // 沒有 job，休息一下
                tokio::time::sleep(std::time::Duration::from_millis(500)).await;
            }
            Err(e) => {
                tracing::error!("Error pulling job: {:?}", e);
                tokio::time::sleep(std::time::Duration::from_secs(5)).await;
            }
        }
    }
}
```

### Job 分發

```rust
async fn handle_job(
    job: &QueuedJob,
    db: &Pool<Postgres>,
    job_dir: &str,
    base_internal_url: &str,
    worker_name: &str,
) -> Result<serde_json::Value> {
    match job.job_kind {
        // Flow job → 交給 flow engine
        JobKind::Flow | JobKind::FlowPreview => {
            handle_flow_job(job, db, job_dir, base_internal_url, worker_name).await
        }
        // 一般 script job → 根據語言分發
        _ => {
            let (content, language) = get_job_content(job, db).await?;

            match language {
                ScriptLang::Python3 => {
                    handle_python_job(job, db, &content, job_dir, base_internal_url, worker_name).await
                }
                ScriptLang::Deno | ScriptLang::Bun | ScriptLang::Nativets => {
                    handle_deno_job(job, db, &content, job_dir, base_internal_url, worker_name).await
                }
                ScriptLang::Go => {
                    handle_go_job(job, db, &content, job_dir, base_internal_url, worker_name).await
                }
                ScriptLang::Bash => {
                    handle_bash_job(job, db, &content, job_dir, base_internal_url, worker_name).await
                }
                ScriptLang::Postgresql => {
                    handle_sql_job(job, db, &content, base_internal_url).await
                }
                ScriptLang::Rust => {
                    handle_rust_job(job, db, &content, job_dir, base_internal_url, worker_name).await
                }
                // ... 更多語言
            }
        }
    }
}
```

## 語言執行器的通用模式

**每個語言執行器都遵循相同的 pattern：**

```
1. 寫入程式碼到 job_dir/
2. 生成 wrapper 程式碼（處理 JSON I/O）
3. 安裝依賴（有快取）
4. 建立子程序（可選沙箱）
5. 注入環境變數（secrets、reserved variables）
6. 執行並監控（日誌串流、記憶體追蹤、超時）
7. 讀取 result.json 作為輸出
```

### 共用的輸入/輸出機制

```rust
// 所有語言都用 JSON 做 I/O
async fn create_args_and_out_file(
    client: &AuthedClient,
    job: &QueuedJob,
    job_dir: &str,
) -> Result<()> {
    // 寫入 args.json
    let args = job.args.clone().unwrap_or_default();
    tokio::fs::write(
        format!("{}/args.json", job_dir),
        serde_json::to_string_pretty(&args)?,
    ).await?;

    // 建立空的 result.json
    tokio::fs::write(format!("{}/result.json", job_dir), "").await?;

    Ok(())
}

// 讀取結果
async fn read_result(job_dir: &str) -> Result<serde_json::Value> {
    let result_str = tokio::fs::read_to_string(format!("{}/result.json", job_dir)).await?;
    let result: serde_json::Value = serde_json::from_str(&result_str)?;
    Ok(result)
}
```

## Python 執行器

`backend/windmill-worker/src/python_executor.rs`：

```rust
pub async fn handle_python_job(
    job: &QueuedJob,
    db: &Pool<Postgres>,
    content: &str,           // 使用者的 Python 程式碼
    job_dir: &str,
    base_internal_url: &str,
    worker_name: &str,
) -> Result<serde_json::Value> {

    // 1. 解析依賴（從 import 語句 + requirements 註解）
    let requirements = parse_python_imports(content);

    // 2. 安裝依賴（使用 pip + 快取）
    if !requirements.is_empty() {
        install_python_dependencies(
            &requirements,
            job_dir,
            job,
            db,
            worker_name,
        ).await?;
    }

    // 3. 寫入使用者程式碼
    write_file(job_dir, "inner.py", content)?;

    // 4. 生成 wrapper（處理 JSON I/O）
    let wrapper = generate_python_wrapper(content);
    write_file(job_dir, "wrapper.py", &wrapper)?;

    // 5. 寫入 args.json
    create_args_and_out_file(client, job, job_dir).await?;

    // 6. 建立並執行子程序
    let mut cmd = Command::new("python3");
    cmd.current_dir(job_dir)
       .arg("wrapper.py")
       .env_clear()
       .env("PATH", &*PATH_ENV)
       .env("HOME", &*HOME_ENV)
       .env("BASE_INTERNAL_URL", base_internal_url)
       .env("JOB_ID", job.id.to_string())
       .env("WM_TOKEN", &client.token)        // Windmill API token
       .env("WM_WORKSPACE", &job.workspace_id)
       .envs(reserved_variables)               // 保留變數
       .envs(user_envs)                        // 使用者自訂變數
       .stdout(Stdio::piped())
       .stderr(Stdio::piped());

    let child = cmd.spawn()?;

    // 7. 監控子程序（日誌串流 + 超時 + 記憶體追蹤）
    handle_child(job, db, &mut mem_peak, child, worker_name).await?;

    // 8. 讀取結果
    read_result(job_dir).await
}
```

### Python Wrapper 程式碼

```python
# wrapper.py（Windmill 自動生成）
import json
import sys

# 載入使用者的 main 函式
from inner import main

# 讀取輸入參數
with open("args.json") as f:
    args = json.load(f)

# 執行使用者函式
try:
    result = main(**args)
except Exception as e:
    result = {"error": {"message": str(e), "name": type(e).__name__}}
    with open("result.json", "w") as f:
        json.dump(result, f)
    sys.exit(1)

# 寫入結果
with open("result.json", "w") as f:
    json.dump(result, f)
```

### Python 依賴管理

```rust
async fn install_python_dependencies(
    requirements: &[String],
    job_dir: &str,
    job: &QueuedJob,
    db: &Pool<Postgres>,
) -> Result<()> {
    // 1. 計算 requirements 的 hash（用來快取）
    let req_hash = calculate_hash(&requirements.join("\n"));
    let cache_dir = format!("{}/{}", PIP_CACHE_DIR, req_hash);

    // 2. 檢查快取
    if Path::new(&cache_dir).exists() {
        // 建立 symlink 到 job_dir
        symlink(&cache_dir, &format!("{}/dependencies", job_dir))?;
        return Ok(());
    }

    // 3. 寫入 requirements.txt
    let req_content = requirements.join("\n");
    write_file(job_dir, "requirements.txt", &req_content)?;

    // 4. pip install
    let mut cmd = Command::new("pip");
    cmd.args(&[
        "install", "-r", "requirements.txt",
        "--target", &format!("{}/dependencies", job_dir),
        "--no-color", "--isolated", "--no-warn-conflicts",
        "--disable-pip-version-check",
    ])
    .current_dir(job_dir)
    .stdout(Stdio::piped())
    .stderr(Stdio::piped());

    let child = cmd.spawn()?;
    handle_child(job, db, &mut 0, child, "pip install").await?;

    // 5. 儲存到快取
    copy_dir(&format!("{}/dependencies", job_dir), &cache_dir)?;

    Ok(())
}
```

## Go 執行器

`backend/windmill-worker/src/go_executor.rs`：

Go 的特殊之處在於需要**編譯**：

```rust
pub async fn handle_go_job(
    job: &QueuedJob,
    db: &Pool<Postgres>,
    content: &str,
    job_dir: &str,
    // ...
) -> Result<serde_json::Value> {
    let job_dir = &format!("{}/go", job_dir);

    // 1. 計算 hash（用來快取編譯好的 binary）
    let hash = calculate_hash(&format!("{}{:?}", content, lock));
    let bin_path = format!("{}/{}", GO_BIN_CACHE_DIR, hash);

    // 2. 檢查是否有快取的 binary
    if Path::new(&bin_path).exists() {
        // 直接用快取的 binary
        symlink(&bin_path, &format!("{}/main", job_dir))?;
    } else {
        // 3. 寫入使用者程式碼到 inner/main.go
        write_file(&format!("{}/inner", job_dir), "main.go", content)?;

        // 4. 生成 Go wrapper（處理 JSON I/O）
        let wrapper = r#"
package main

import (
    "encoding/json"
    "os"
    "fmt"
    "mymod/inner"
)

func main() {
    dat, err := os.ReadFile("args.json")
    if err != nil { fmt.Println(err); os.Exit(1) }

    var req inner.Req
    if err := json.Unmarshal(dat, &req); err != nil {
        fmt.Println(err); os.Exit(1)
    }

    res, err := inner.Run(req)
    if err != nil { fmt.Println(err); os.Exit(1) }

    res_json, err := json.Marshal(res)
    if err != nil { fmt.Println(err); os.Exit(1) }

    f, _ := os.OpenFile("result.json", os.O_APPEND|os.O_WRONLY, os.ModeAppend)
    f.WriteString(string(res_json))
}
"#;
        write_file(job_dir, "main.go", wrapper)?;

        // 5. 生成 runner.go（型別轉換）
        let sig = parse_go_sig(content)?;
        let runner = generate_go_runner(&sig);
        write_file(&format!("{}/inner", job_dir), "runner.go", &runner)?;

        // 6. go mod init + go mod tidy
        run_command("go", &["mod", "init", "mymod"], job_dir).await?;
        run_command("go", &["mod", "tidy"], job_dir).await?;

        // 7. 編譯
        run_command("go", &["build", "main.go"], job_dir).await?;

        // 8. 快取 binary
        copy_file(&format!("{}/main", job_dir), &bin_path)?;
    }

    // 9. 準備輸入
    create_args_and_out_file(client, job, job_dir).await?;

    // 10. 執行編譯好的 binary
    let child = Command::new("./main")
        .current_dir(job_dir)
        .env_clear()
        .env("PATH", &*PATH_ENV)
        .envs(reserved_variables)
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()?;

    handle_child(job, db, &mut mem_peak, child, "go run").await?;
    read_result(job_dir).await
}
```

## TypeScript (Deno/Bun) 執行器

```rust
pub async fn handle_deno_job(
    job: &QueuedJob,
    content: &str,
    job_dir: &str,
    // ...
) -> Result<serde_json::Value> {
    // 1. 寫入使用者程式碼
    write_file(job_dir, "main.ts", content)?;

    // 2. 生成 wrapper
    let wrapper = r#"
import { main } from "./main.ts";

const args = JSON.parse(Deno.readTextFileSync("args.json"));
try {
    const result = await main(...Object.values(args));
    Deno.writeTextFileSync("result.json", JSON.stringify(result));
} catch (e) {
    Deno.writeTextFileSync("result.json", JSON.stringify({
        error: { message: e.message, name: e.name }
    }));
    Deno.exit(1);
}
"#;
    write_file(job_dir, "wrapper.ts", wrapper)?;

    // 3. 解析 import map（依賴）
    let lock = resolve_deno_lock(content, job_dir).await?;

    // 4. 執行
    let child = Command::new("deno")
        .args(&["run", "--allow-all", "--lock", "deno.lock", "wrapper.ts"])
        .current_dir(job_dir)
        .env_clear()
        .envs(reserved_variables)
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()?;

    handle_child(job, db, &mut mem_peak, child, "deno run").await?;
    read_result(job_dir).await
}
```

## Bash 執行器

最簡單的語言執行器：

```rust
pub async fn handle_bash_job(
    job: &QueuedJob,
    content: &str,
    job_dir: &str,
    // ...
) -> Result<serde_json::Value> {
    // 1. 將 args 展開為環境變數
    let args: HashMap<String, serde_json::Value> =
        serde_json::from_value(job.args.clone().unwrap_or_default())?;

    // 2. 寫入腳本
    let script = format!(
        "set -e\n{}\n",
        content
    );
    write_file(job_dir, "main.sh", &script)?;

    // 3. 執行
    let mut cmd = Command::new("bash");
    cmd.arg("main.sh")
       .current_dir(job_dir)
       .env_clear()
       .env("PATH", &*PATH_ENV);

    // 將 args 注入為環境變數
    for (key, value) in &args {
        let str_val = match value {
            serde_json::Value::String(s) => s.clone(),
            _ => serde_json::to_string(value)?,
        };
        cmd.env(key, str_val);
    }

    cmd.envs(reserved_variables)
       .stdout(Stdio::piped())
       .stderr(Stdio::piped());

    let child = cmd.spawn()?;
    handle_child(job, db, &mut mem_peak, child, "bash").await?;

    // Bash 的結果是 stdout 的最後一行（嘗試 JSON 解析）
    let stdout = read_stdout(job_dir).await?;
    match serde_json::from_str(&stdout) {
        Ok(v) => Ok(v),
        Err(_) => Ok(serde_json::Value::String(stdout)),
    }
}
```

## 沙箱隔離 (nsjail)

Windmill 用 [nsjail](https://github.com/google/nsjail) 做沙箱隔離：

```rust
// 當啟用沙箱時
if is_sandboxing_enabled() {
    let nsjail_config = NSJAIL_CONFIG_TEMPLATE
        .replace("{JOB_DIR}", job_dir)
        .replace("{SHARED_MOUNT}", shared_mount)
        .replace("{TIMEOUT}", &timeout.to_string());

    write_file(job_dir, "run.config.proto", &nsjail_config)?;

    // 用 nsjail 包裝執行
    let child = Command::new("nsjail")
        .args(&["--config", "run.config.proto", "--", "/usr/bin/python3", "wrapper.py"])
        .current_dir(job_dir)
        .env_clear()
        .envs(reserved_variables)
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()?;
}
```

**nsjail 的 protobuf 設定：**

```protobuf
name: "windmill-job"
mode: ONCE
time_limit: {TIMEOUT}

# 檔案系統隔離
mount {
    src: "{JOB_DIR}"
    dst: "/tmp"
    is_bind: true
    rw: true
}
mount {
    src: "/usr"
    dst: "/usr"
    is_bind: true
}

# 網路隔離（可選）
clone_newnet: false

# 資源限制
rlimit_as_type: SOFT
rlimit_cpu_type: SOFT
cgroup_mem_max: 1073741824  # 1GB
cgroup_pids_max: 100
```

## 子程序監控 (handle_child)

`backend/windmill-worker/src/handle_child.rs`：

```rust
pub async fn handle_child(
    job_id: &Uuid,
    db: &Pool<Postgres>,
    mem_peak: &mut i32,
    canceled_by: &mut Option<CanceledBy>,
    mut child: tokio::process::Child,
    worker_name: &str,
    workspace_id: &str,
    label: &str,           // "python run", "go build", etc.
    timeout: Option<i32>,
) -> Result<()> {
    let stdout = child.stdout.take().unwrap();
    let stderr = child.stderr.take().unwrap();

    // 1. 非同步讀取 stdout/stderr
    let log_task = tokio::spawn(async move {
        let mut stdout_reader = BufReader::new(stdout);
        let mut stderr_reader = BufReader::new(stderr);
        let mut combined_logs = String::new();

        loop {
            tokio::select! {
                line = stdout_reader.read_line() => {
                    match line {
                        Ok(0) => break, // EOF
                        Ok(_) => {
                            combined_logs.push_str(&line);
                            // 即時寫入 DB（讓前端可以 SSE 串流）
                            append_logs(job_id, workspace_id, &line, db).await;
                        }
                        Err(_) => break,
                    }
                }
                line = stderr_reader.read_line() => {
                    // 同上
                }
            }
        }
        combined_logs
    });

    // 2. 記憶體監控
    let mem_task = tokio::spawn(async move {
        loop {
            if let Ok(mem) = get_process_memory(child.id().unwrap()) {
                if mem > *mem_peak { *mem_peak = mem; }
            }
            tokio::time::sleep(Duration::from_secs(1)).await;
        }
    });

    // 3. 等待子程序完成（帶超時）
    let timeout_duration = timeout
        .map(|t| Duration::from_secs(t as u64))
        .unwrap_or(Duration::from_secs(3600)); // 預設 1 小時

    let status = tokio::time::timeout(timeout_duration, child.wait()).await;

    match status {
        Ok(Ok(exit_status)) => {
            if !exit_status.success() {
                return Err(Error::ExecutionErr(format!(
                    "{} exited with code {}",
                    label,
                    exit_status.code().unwrap_or(-1)
                )));
            }
        }
        Ok(Err(e)) => {
            return Err(Error::ExecutionErr(format!("{} failed: {}", label, e)));
        }
        Err(_) => {
            // 超時，殺掉子程序
            child.kill().await.ok();
            return Err(Error::ExecutionErr(format!(
                "{} timed out after {}s", label, timeout.unwrap_or(3600)
            )));
        }
    }

    Ok(())
}
```

## Reserved Variables（保留變數）

每個 job 都會注入一組環境變數：

```rust
fn get_reserved_variables(job: &QueuedJob, token: &str) -> HashMap<String, String> {
    let mut vars = HashMap::new();

    // Windmill 系統變數
    vars.insert("WM_TOKEN".into(), token.to_string());
    vars.insert("WM_WORKSPACE".into(), job.workspace_id.clone());
    vars.insert("WM_JOB_ID".into(), job.id.to_string());
    vars.insert("WM_JOB_PATH".into(), job.script_path().to_string());
    vars.insert("BASE_INTERNAL_URL".into(), base_internal_url.to_string());

    // Flow 相關
    if let Some(parent) = &job.parent_job {
        vars.insert("WM_FLOW_JOB_ID".into(), parent.to_string());
    }
    if let Some(root) = &job.root_job {
        vars.insert("WM_ROOT_FLOW_JOB_ID".into(), root.to_string());
    }
    if let Some(step_id) = &job.flow_step_id {
        vars.insert("WM_FLOW_STEP_ID".into(), step_id.clone());
    }

    vars
}
```

## 快取策略

### 三層快取

```
Layer 1: 本地檔案快取
├── /tmp/windmill/cache/pip/{hash}/     # Python 依賴
├── /tmp/windmill/cache/deno/{hash}/    # Deno 依賴
├── /tmp/windmill/cache/go_bin/{hash}   # Go 編譯結果
└── /tmp/windmill/cache/bun/{hash}/     # Bun 依賴

Layer 2: S3/Object Storage 快取（可選）
└── s3://windmill-cache/{target}_{lang}/{hash}

Layer 3: Job 結果快取（cache_ttl）
└── 在 completed_job 表中，如果 cache_ttl > 0 則重用結果
```

## 你的實作順序

1. **最簡 Worker** — poll queue + 執行 Bash（最簡單）
2. **Python 執行器** — wrapper + pip install + 快取
3. **TypeScript 執行器** — Deno 或 Bun
4. **日誌串流** — 即時寫入 DB，前端用 SSE 讀取
5. **子程序監控** — 超時 + 記憶體追蹤
6. **Go 執行器** — 編譯 + binary 快取
7. **沙箱** — nsjail（Linux only）或 Docker
8. **依賴快取** — hash-based 本地快取

## 深入：為什麼用 nsjail 而不是 Kubernetes Pod？

### nsjail 的設定與控制

nsjail **預設是關閉的**，由環境變數和 Instance Settings 控制：

```rust
// backend/windmill-worker/src/worker.rs (line ~340)
pub static ref DISABLE_NSJAIL: bool = std::env::var("DISABLE_NSJAIL")
    .ok()
    .and_then(|x| x.parse::<bool>().ok())
    .unwrap_or(true);  // 預設 TRUE → nsjail 不啟用

pub static ref JOB_ISOLATION: AtomicU8 = AtomicU8::new(JobIsolationLevel::Undefined as u8);
```

四種隔離等級：

| 等級 | 說明 | 啟動延遲 |
|------|------|---------|
| `Undefined` (0) | 由環境變數決定 | - |
| `None` (1) | 不隔離 | 0ms |
| `Unshare` (2) | 只隔離 PID namespace | ~1ms |
| `NsjailSandboxing` (3) | 完整 nsjail 沙箱 | ~10-50ms |

```rust
pub fn is_sandboxing_enabled() -> bool {
    if !*DISABLE_NSJAIL {
        return true;
    }
    match get_job_isolation() {
        JobIsolationLevel::NsjailSandboxing => true,
        _ => false,
    }
}
```

### nsjail 做了什麼隔離

`backend/windmill-worker/nsjail/` 有 19 個語言專屬的設定檔。以 Python 為例：

```protobuf
# run.python3.config.proto

# --- 檔案系統隔離 ---
mount { src: "/bin"   dst: "/bin"   is_bind: true  rw: false }  # 唯讀
mount { src: "/lib"   dst: "/lib"   is_bind: true  rw: false }
mount { src: "/usr"   dst: "/usr"   is_bind: true  rw: false }
mount { src: "{JOB_DIR}"  dst: "/tmp"  is_bind: true  rw: true }  # 只有 job 目錄可寫
mount { dst: "/tmp"  fstype: "tmpfs"  rw: true  options: "size=500m" }

# --- 資源限制 ---
rlimit_as: 4096        # 虛擬記憶體 4GB
rlimit_cpu: 1000       # CPU 時間 1000 秒
rlimit_fsize: 1000     # 檔案大小限制
rlimit_nofile: 10000   # 最大 file descriptor

# --- 網路 ---
clone_newnet: false     # 不隔離網路（job 需要存取外部 API）
iface_no_lo: true       # 但禁止 loopback（防止 job 之間互通）

# --- Namespace ---
clone_newuser: true     # 隔離 user namespace
mode: ONCE              # 執行一次就退出
```

### nsjail 的呼叫方式

```rust
// backend/windmill-worker/src/bash_executor.rs
let nsjail_config = NSJAIL_CONFIG_RUN_BASH_CONTENT
    .replace("{JOB_DIR}", job_dir)
    .replace("{CLONE_NEWUSER}", &(!*DISABLE_NUSER).to_string())
    .replace("{SHARED_MOUNT}", shared_mount)
    .replace("{TIMEOUT}", &nsjail_timeout);

write_file(job_dir, "run.config.proto", &nsjail_config)?;

let mut nsjail_cmd = Command::new(NSJAIL_PATH.as_str());
nsjail_cmd
    .current_dir(job_dir)
    .env_clear()                       // 清空環境變數
    .envs(reserved_variables)          // 只注入 WM_* 變數
    .args(&["--config", "run.config.proto", "--", "/bin/bash", "wrapper.sh"])
    .spawn()?;
```

### 為什麼不用 Kubernetes Pod 跑每個 Job？

這是一個很好的問題。Airflow 的模式是 `KubernetesPodOperator`——每個 task 啟動一個 K8s Pod。Windmill 為什麼不這樣做？

#### 延遲對比

| 方案 | 啟動延遲 | 說明 |
|------|---------|------|
| 不隔離 | ~0ms | 直接 `Command::new("python")` |
| nsjail | ~10-50ms | Linux namespace + bind mount |
| Docker | ~500ms-2s | Container runtime + overlay fs |
| K8s Pod | ~2-10s | 排程 + 容器啟動 + 網路設定 |
| K8s Pod (cold) | ~10-30s | 加上 image pull |

Windmill 的定位是**低延遲的 job 執行**（大量小 job），2-10 秒的 Pod 啟動延遲是不可接受的。

#### 架構差異

```
Airflow 模式（K8s Pod per Task）：
  Scheduler → K8s API → Pod 排程 → 拉 image → 啟動容器 → 執行 → 回報
  延遲：5-30 秒
  適合：長時間 ETL（分鐘到小時級）

Windmill 模式（常駐 Worker + nsjail）：
  Queue → Worker poll → nsjail spawn → 執行 → 完成
  延遲：10-50 毫秒
  適合：大量快速 job（秒到分鐘級）
```

#### 為什麼 Windmill 不採用 K8s Pod

1. **延遲**：nsjail 10ms vs K8s Pod 5s，差 500 倍
2. **依賴快取**：Worker 本地快取 pip/npm/cargo，每次 Pod 重建要重新安裝
3. **簡單性**：不依賴 K8s，bare metal / Docker Compose 也能跑
4. **資源效率**：一個 Worker 程序可以連續跑數千個 job，不用每次建/刪 Pod
5. **K8s API 壓力**：每秒數千 job 會把 K8s API Server 打爆

#### Windmill 的 K8s 支援是什麼？

Windmill 的 K8s 整合是 **Worker 自動擴縮**（EE 功能），不是 Pod-per-Job：

```
Windmill 的 K8s 模式：
  Queue 積壓 → Autoscaler 偵測 → 增加 Worker Pod 數量 → Worker 搶 job
  （Worker Pod 是長期存活的，不是每個 job 一個 Pod）

NOT:
  每個 Job → 建立 Pod → 執行 → 刪除 Pod
```

```rust
// backend/windmill-autoscaling/src/ (Enterprise Edition)
// 管理 Worker Pod 數量，不是管理 Job Pod
apply_kubernetes_autoscaling()  // 根據 queue 深度調整 Worker 副本數
```

### 什麼時候 K8s Pod 模式比較好？

| 場景 | nsjail (Windmill) | K8s Pod (Airflow) |
|------|-------------------|-------------------|
| 大量小 job（< 1 分鐘） | 最佳 | 啟動延遲佔比太高 |
| 長時間 ETL（> 10 分鐘） | 可以 | 啟動延遲可忽略 |
| 需要不同 Docker image | 不支援 | 最佳 |
| 需要 GPU | 要手動掛載 | K8s 原生支援 |
| 多租戶強隔離 | nsjail 夠用 | Pod 隔離更強 |
| 每個 job 需要不同依賴版本 | 靠快取區分 | 每個 image 獨立 |

### 如果你想兩者兼得

```
你的系統可以同時支援：

快速 job → nsjail / WASM 沙箱（毫秒級啟動）
重型 job → K8s Pod（支援自訂 image、GPU）

透過 tag 路由：
  tag = "fast"  → 常駐 Worker + nsjail
  tag = "heavy" → K8s Job Controller → 動態建 Pod
```

這就是你可以超越 Windmill 的方向之一：**混合執行模式**。
