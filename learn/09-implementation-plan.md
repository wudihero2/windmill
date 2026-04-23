# 第九章：自建工作流平台 — 實作計畫

## 目標

從零打造一個工作流 / 資料管線平台（暫名 **FlowForge**），支援：

- 即時寫 Python（未來多語言）並執行
- DAG 工作流（like Windmill Flow）
- 資料管線排程（like Airflow）
- **Day 1 沙箱隔離**（四模式：Rust 原生 / nsjail / WASM / K8s Pod）
- **Day 1 OpenTelemetry**

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
| 沙箱 | Landlock+seccomp / nsjail / WASM / K8s Pod | **不同於 Windmill**（只有 nsjail），Day 1 四模式 |

---

## 專案結構

```
flowforge/
├── backend/
│   ├── Cargo.toml                 # workspace
│   ├── src/main.rs                # Server + Worker 入口
│   ├── migrations/                # SQLx migrations
│   └── crates/
│       ├── api/src/               # Axum 路由
│       │   ├── lib.rs             # Router 組裝
│       │   ├── auth.rs            # JWT middleware
│       │   ├── scripts.rs         # Script CRUD
│       │   ├── flows.rs           # Flow CRUD
│       │   ├── jobs.rs            # Job 執行/查詢
│       │   └── sse.rs             # SSE 日誌串流
│       ├── types/src/             # 領域型別
│       │   ├── scripts.rs         # Script, ScriptLang
│       │   ├── flows.rs           # FlowValue, FlowModule, InputTransform
│       │   ├── flow_status.rs     # FlowStatus 狀態機
│       │   └── jobs.rs            # Job, JobKind
│       ├── queue/src/             # Job Queue
│       │   ├── push.rs            # 推入 job
│       │   ├── pull.rs            # FOR UPDATE SKIP LOCKED
│       │   └── complete.rs        # 完成/失敗
│       ├── worker/src/            # Worker
│       │   ├── worker.rs          # 主迴圈
│       │   ├── sandbox.rs         # Sandbox trait + nsjail/k8s 實作
│       │   ├── python.rs          # Python executor
│       │   ├── typescript.rs      # TS executor（Phase 4）
│       │   ├── duckdb.rs          # DuckDB executor（Phase 4）
│       │   ├── handle_child.rs    # 子程序監控
│       │   └── flow_engine.rs     # Flow 狀態機
│       ├── jseval/src/lib.rs      # boa_engine JS 表達式求值
│       └── object-store/src/      # S3 整合
├── frontend/
│   ├── src/
│   │   ├── routes/
│   │   │   ├── +layout.svelte
│   │   │   ├── login/+page.svelte
│   │   │   ├── scripts/           # Script 頁面
│   │   │   ├── flows/             # Flow 頁面
│   │   │   ├── jobs/              # Job 頁面
│   │   │   └── schedules/         # 排程頁面
│   │   └── lib/
│   │       ├── components/
│   │       │   ├── ScriptEditor.svelte   # Monaco
│   │       │   ├── FlowEditor.svelte     # @xyflow/svelte DAG
│   │       │   ├── LogViewer.svelte      # SSE 即時日誌
│   │       │   └── ArgInput.svelte       # JSON Schema → 表單
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

## 核心設計：Day 1 Sandbox 架構

### 為什麼 Day 1 就要做沙箱？

Windmill 的 nsjail 是**後加的**，導致：
- 每個語言 executor 都有 `if is_sandboxing_enabled()` 的分支邏輯
- 19 個 nsjail config 檔案，每個語言一份
- 沙箱和非沙箱路徑的程式碼重複

我們的做法：**Sandbox 是 trait，所有執行都經過它**。支援四種模式，按需選擇。

### 四種沙箱模式全景比較

#### 方案 1：Rust 原生沙箱 — Landlock + seccomp（推薦預設）

**不需要外部 binary（不需要 nsjail）**，純 Rust 實作，用 Linux 核心原生的安全機制：

| 技術 | Crate | 作用 | Linux 版本需求 |
|------|-------|------|--------------|
| **Landlock** | `landlock` (v0.4) | 檔案系統 + 網路（TCP）隔離，path-based ACL | 5.13+（ABI v4 需 6.7+） |
| **seccomp** | `seccompiler` (v0.5, rust-vmm/AWS) | 系統呼叫白名單，BPF 過濾 | 3.5+ |
| **User Namespace** | `nix` crate | PID/Mount/Network namespace 隔離 | 3.8+ |

**Landlock 範例（檔案系統隔離）：**

```rust
use landlock::{
    Access, AccessFs, PathBeneath, PathFd,
    Ruleset, RulesetAttr, RulesetCreatedAttr, ABI,
};

fn sandbox_filesystem(job_dir: &str) -> Result<()> {
    let abi = ABI::V4;
    Ruleset::default()
        .handle_access(AccessFs::from_all(abi))?
        .create()?
        // 唯讀：系統目錄
        .add_rule(PathBeneath::new(PathFd::new("/usr")?, AccessFs::from_read(abi)))?
        .add_rule(PathBeneath::new(PathFd::new("/lib")?, AccessFs::from_read(abi)))?
        .add_rule(PathBeneath::new(PathFd::new("/bin")?, AccessFs::from_read(abi)))?
        // 讀寫：只有 job 目錄
        .add_rule(PathBeneath::new(PathFd::new(job_dir)?, AccessFs::from_all(abi)))?
        // 其他路徑全部封鎖
        .restrict_self()?;
    Ok(())
}
```

**seccomp 範例（系統呼叫白名單）：**

```rust
use seccompiler::{SeccompAction, SeccompFilter, SeccompRule, BpfProgram};
use std::collections::BTreeMap;

fn sandbox_syscalls() -> Result<()> {
    let filter = SeccompFilter::new(
        BTreeMap::from([
            // 允許基本 I/O
            (libc::SYS_read, vec![SeccompRule::new(vec![])?]),
            (libc::SYS_write, vec![SeccompRule::new(vec![])?]),
            (libc::SYS_openat, vec![SeccompRule::new(vec![])?]),
            (libc::SYS_close, vec![SeccompRule::new(vec![])?]),
            // 允許記憶體操作
            (libc::SYS_mmap, vec![SeccompRule::new(vec![])?]),
            (libc::SYS_munmap, vec![SeccompRule::new(vec![])?]),
            (libc::SYS_brk, vec![SeccompRule::new(vec![])?]),
            // 允許 Python 需要的
            (libc::SYS_futex, vec![SeccompRule::new(vec![])?]),
            (libc::SYS_clone3, vec![SeccompRule::new(vec![])?]),
            // 允許網路（job 需要呼叫外部 API）
            (libc::SYS_socket, vec![SeccompRule::new(vec![])?]),
            (libc::SYS_connect, vec![SeccompRule::new(vec![])?]),
            // 允許退出
            (libc::SYS_exit_group, vec![SeccompRule::new(vec![])?]),
        ]),
        SeccompAction::KillProcess,  // 未列出的 syscall → 殺掉
        SeccompAction::Allow,
        std::env::consts::ARCH.try_into()?,
    )?;

    let bpf: BpfProgram = filter.try_into()?;
    seccompiler::apply_filter(&bpf)?;
    Ok(())
}
```

**整合成 RustNativeSandbox：**

```rust
pub struct RustNativeSandbox {
    config: RustNativeConfig,
}

impl RustNativeSandbox {
    async fn execute(&self, ctx: &SandboxContext) -> Result<SandboxResult, SandboxError> {
        // 1. fork 子程序（用 nix crate）
        // 2. 在子程序中：
        //    a) unshare(CLONE_NEWPID | CLONE_NEWNS | CLONE_NEWUSER)
        //    b) 設定 Landlock（檔案系統隔離）
        //    c) 設定 seccomp（系統呼叫過濾）
        //    d) 設定 rlimit（記憶體/CPU 限制）
        //    e) exec() 執行 python3/bun 等
        // 3. 父程序監控子程序（日誌串流 + 超時 + 記憶體追蹤）

        let child = unsafe {
            nix::unistd::fork()
        };

        match child {
            Ok(nix::unistd::ForkResult::Child) => {
                // 子程序：套用沙箱後執行
                nix::sched::unshare(
                    nix::sched::CloneFlags::CLONE_NEWPID |
                    nix::sched::CloneFlags::CLONE_NEWNS |
                    nix::sched::CloneFlags::CLONE_NEWUSER
                ).ok();

                sandbox_filesystem(&ctx.job_dir).ok();
                sandbox_syscalls().ok();
                set_rlimits(self.config.memory_limit, self.config.cpu_time_limit);

                // exec
                let err = nix::unistd::execvpe(
                    &std::ffi::CString::new(ctx.command.as_str()).unwrap(),
                    &ctx.args.iter()
                        .map(|a| std::ffi::CString::new(a.as_str()).unwrap())
                        .collect::<Vec<_>>(),
                    &ctx.env.iter()
                        .map(|(k, v)| std::ffi::CString::new(format!("{}={}", k, v)).unwrap())
                        .collect::<Vec<_>>(),
                );
                std::process::exit(1);
            }
            Ok(nix::unistd::ForkResult::Parent { child: pid }) => {
                // 父程序：監控
                handle_child_pid(pid, ctx.timeout_secs).await
            }
            Err(e) => Err(SandboxError::ForkFailed(e.to_string())),
        }
    }
}
```

**優勢**：
- 無外部依賴（不需要安裝 nsjail binary）
- 啟動延遲 ~1-5ms（比 nsjail 的 10-50ms 更快）
- 效能開銷 ~0%（Landlock 和 seccomp 都是核心級機制，BPF JIT 編譯）
- 用 Firecracker（AWS Lambda 底層）同款 seccompiler crate，production-proven

**限制**：
- Linux only（macOS 無 Landlock/seccomp）
- seccomp 需要為每種語言 runtime 調整 syscall 白名單
- 沒有 nsjail 那麼 battle-tested 的「打包方案」，需要自己組合各個元件

#### 方案 2：nsjail（傳統方案，Windmill 使用中）

就是 Windmill 現在用的方式。見 [04-worker-executor.md](./04-worker-executor.md) 的詳細分析。

- 啟動延遲 ~10-50ms
- 成熟穩定（Google 內部生產使用）
- 需要安裝 nsjail binary（C++ 編譯）
- Linux only
- 19 個語言專屬 protobuf config 檔案

#### 方案 3：WASM 沙箱（wasmtime / wasmer）

用 WebAssembly 做最強隔離的沙箱。

**兩大 runtime：**

| 特性 | wasmtime (Bytecode Alliance) | wasmer |
|------|---------------------------|--------|
| 執行速度 | ~85-90% native | ~80-85% native |
| 記憶體/instance | ~15 MB | ~12 MB |
| 冷啟動 | **5 微秒 - 1ms**（AOT 預編譯） | 1-10ms |
| WASI 支援 | WASIp2 + WASIp3 snapshot | WASIX（POSIX 超集） |
| Fuel metering | 每個 operator 可配權重 | 有 |
| 生產使用者 | Fastly, Microsoft | Shopify |
| 標準符合度 | 嚴格 | 務實/偏離 |

**WASM 適合什麼？**

| 語言 | WASM 可行性 | 方式 | 限制 |
|------|-----------|------|------|
| **JavaScript/TS** | **可行** | QuickJS/Javy 編譯成 WASM | 無 Node.js API，無 npm 生態 |
| **Python（純運算）** | **可行** | Pyodide (CPython→WASM) | NumPy/pandas OK，`requests` 不行 |
| **Python（任意 pip）** | **不可行** | - | C extensions 需逐個移植，無 raw socket |
| **Bash** | **不可行** | - | Shell 需要完整 OS 環境 |
| **Go** | **部分可行** | TinyGo | 不是官方 Go，功能受限 |

**Cloudflare Workers 的成功案例**：
Pyodide（CPython 編譯成 WASM）在 Cloudflare 310+ 節點生產運行。支援 NumPy、pandas、scipy、Pillow。冷啟動比 AWS Lambda 快 2.4x，透過**記憶體快照**解決大 runtime 的啟動延遲。

**WASM Sandbox 實作：**

```rust
use wasmtime::*;

pub struct WasmSandbox {
    engine: Engine,
    config: WasmConfig,
}

#[derive(Debug, Clone)]
pub struct WasmConfig {
    /// 最大 fuel（CPU 限制）
    pub max_fuel: u64,         // 預設 10_000_000
    /// 最大記憶體 (bytes)
    pub max_memory: usize,     // 預設 1GB
    /// WASI 檔案系統權限
    pub preopens: Vec<(String, String)>,  // (host_path, guest_path)
    /// 是否允許網路
    pub allow_network: bool,
}

impl WasmSandbox {
    pub fn new(config: WasmConfig) -> Self {
        let mut engine_config = Config::new();
        engine_config.consume_fuel(true);      // 啟用 fuel metering
        engine_config.wasm_component_model(true); // 啟用 Component Model
        // AOT 編譯：最快啟動
        engine_config.strategy(Strategy::Cranelift);

        Self {
            engine: Engine::new(&engine_config).unwrap(),
            config,
        }
    }

    async fn execute_js(&self, code: &str, args: &serde_json::Value) -> Result<SandboxResult> {
        // 用預編譯的 QuickJS WASM module 執行 JavaScript
        let module = Module::from_file(&self.engine, "quickjs.wasm")?;
        let mut store = Store::new(&self.engine, WasmState::new());

        // 資源限制
        store.set_fuel(self.config.max_fuel)?;
        store.limiter(|state| &mut state.limiter);

        // WASI 環境
        let wasi = WasiCtxBuilder::new()
            .inherit_stdout()
            .inherit_stderr()
            .preopened_dir(&ctx.job_dir, "/tmp/job", DirPerms::all(), FilePerms::all())?
            .build();

        let instance = Linker::new(&self.engine)
            .instantiate(&mut store, &module)?;

        // 傳入 code + args，取回 result
        let main = instance.get_typed_func::<(i32, i32), i32>(&mut store, "eval")?;
        let result = main.call(&mut store, (code_ptr, args_ptr))?;

        // 檢查剩餘 fuel
        let fuel_consumed = self.config.max_fuel - store.get_fuel()?;

        Ok(SandboxResult { /* ... */ })
    }

    async fn execute_python(&self, code: &str, args: &serde_json::Value) -> Result<SandboxResult> {
        // 用預編譯的 Pyodide WASM module 執行 Python
        // 注意：只支援純 Python + Pyodide 內建的 C extensions（NumPy, pandas 等）
        // 不支援任意 pip install
        let module = Module::from_file(&self.engine, "pyodide.wasm")?;
        // ... 類似 JS 的流程 ...
        todo!()
    }
}
```

**WASM 的殺手級優勢：**
- **跨平台**：macOS、Windows 也能沙箱（nsjail/Landlock 都是 Linux only）
- **啟動 5 微秒**（AOT 預編譯），比 nsjail 快 1000x
- **Fuel metering**：精確的 CPU 計量，不是粗略的 rlimit
- **記憶體隔離是 WASM 原生的**：不需要 namespace，WASM 線性記憶體天然隔離
- **確定性執行**：同樣的 input 永遠產生同樣的 output

**WASM 的致命限制（2026 現狀）：**
- **無法跑任意 pip 包**：需要 C extension 的包（如 `requests` 的底層 urllib3）不能直接用
- **無多執行緒**：WASI 不支援 thread-spawn（標準化中但未穩定）
- **網路 I/O 慢**：WASI 網路堆疊還不成熟，比 Linux 核心慢很多
- **不支援 Bash**：Shell 需要完整 OS 環境
- **生態不穩定**：WASI Preview 1 → 2 → 3 在兩年內三次 breaking change

#### 方案 4：K8s Pod（重型 job 用）

見原文。適合需要自訂 image、GPU、或完整 OS 環境的場景。

### 四種沙箱的完整比較

| 面向 | Rust 原生 (Landlock+seccomp) | nsjail | WASM (wasmtime) | K8s Pod |
|------|---------------------------|--------|----------------|---------|
| **啟動延遲** | ~1-5ms | ~10-50ms | **~5μs - 1ms** | ~2-10s |
| **效能開銷** | ~0% | ~0% | ~10-15% slower | ~0%（容器內） |
| **記憶體/instance** | ~程序本身 | ~程序本身 | ~15 MB (runtime) | ~50-200 MB |
| **檔案系統隔離** | Landlock (path-based) | namespace + bind mount | WASM 線性記憶體 | 完整容器 |
| **網路隔離** | seccomp (syscall 層) | namespace | WASI capability | K8s NetworkPolicy |
| **CPU 限制** | rlimit | cgroups + rlimit | **Fuel metering**（精確） | K8s resource limits |
| **記憶體限制** | rlimit | cgroups | WASM memory limit | K8s resource limits |
| **自訂 image** | 不支援 | 不支援 | 不支援 | **支援** |
| **GPU** | 不支援 | 不支援 | 不支援 | **K8s 原生** |
| **跨平台** | Linux only | Linux only | **全平台** | 任何有 K8s 的環境 |
| **任意 pip 包** | **支援** | **支援** | 僅 Pyodide 內建 | **支援** |
| **Bash 執行** | **支援** | **支援** | 不支援 | **支援** |
| **外部依賴** | 無（純 Rust crate） | nsjail binary (C++) | 無（純 Rust crate） | K8s cluster |
| **成熟度** | 高（Firecracker 同款） | 高（Google 生產） | 中（Cloudflare 生產） | 高 |
| **安全等級** | 高（多層防禦） | 高 | **最高**（WASM 隔離是記憶體安全的） | 最高 |

### 建議策略：按 Job 類型選擇

```
Job 類型                    → 沙箱模式
─────────────────────────────────────────
Python (任意 pip 包)        → Rust 原生 / nsjail
Python (純運算 + pandas)    → WASM（最快啟動）
JavaScript/TypeScript       → WASM（QuickJS，微秒級）
Bash                        → Rust 原生 / nsjail
需要自訂 Docker image       → K8s Pod
需要 GPU                    → K8s Pod
macOS 開發環境              → WASM / None
```

### Sandbox Trait 設計

```rust
// crates/worker/src/sandbox.rs

use async_trait::async_trait;
use tokio::process::Child;

/// 沙箱執行模式
#[derive(Debug, Clone, serde::Deserialize)]
#[serde(tag = "mode")]
pub enum SandboxMode {
    /// 不隔離（開發用，生產不建議）
    None,
    /// Rust 原生 — Landlock + seccomp + namespace（1-5ms 啟動，無外部依賴）
    RustNative(RustNativeConfig),
    /// nsjail — Linux namespace 隔離（10-50ms 啟動，成熟穩定）
    Nsjail(NsjailConfig),
    /// WASM — wasmtime 沙箱（5μs 啟動，跨平台，僅限支援的語言）
    Wasm(WasmConfig),
    /// K8s Pod — 完整容器隔離（2-10s 啟動，支援自訂 image/GPU）
    KubernetesPod(K8sPodConfig),
}

#[derive(Debug, Clone, serde::Deserialize)]
pub struct NsjailConfig {
    /// nsjail binary 路徑
    pub nsjail_path: String,  // 預設 "nsjail"
    /// 記憶體限制 (bytes)
    pub memory_limit: u64,    // 預設 1GB
    /// CPU 時間限制 (秒)
    pub cpu_time_limit: u32,  // 預設 1000
    /// 是否隔離網路
    pub clone_newnet: bool,   // 預設 false（job 需要存取外部 API）
    /// tmpfs 大小 (bytes)
    pub tmpfs_size: u64,      // 預設 500MB
}

#[derive(Debug, Clone, serde::Deserialize)]
pub struct K8sPodConfig {
    /// K8s namespace
    pub namespace: String,    // 預設 "flowforge-jobs"
    /// 預設 image（可被 job 覆蓋）
    pub default_image: String,
    /// CPU request/limit
    pub cpu_request: String,  // 預設 "100m"
    pub cpu_limit: String,    // 預設 "1000m"
    /// Memory request/limit
    pub memory_request: String, // 預設 "128Mi"
    pub memory_limit: String,   // 預設 "1Gi"
    /// Service account
    pub service_account: Option<String>,
    /// Node selector
    pub node_selector: Option<std::collections::HashMap<String, String>>,
    /// Image pull secrets
    pub image_pull_secrets: Vec<String>,
    /// 是否自動清理完成的 Pod
    pub auto_cleanup: bool,
}

#[derive(Debug, Clone, serde::Deserialize)]
pub struct RustNativeConfig {
    /// 記憶體限制 (bytes)
    pub memory_limit: u64,       // 預設 1GB
    /// CPU 時間限制 (秒)
    pub cpu_time_limit: u32,     // 預設 1000
    /// 是否隔離網路（用 CLONE_NEWNET）
    pub isolate_network: bool,   // 預設 false
    /// 是否啟用 Landlock 檔案系統隔離
    pub enable_landlock: bool,   // 預設 true
    /// 是否啟用 seccomp 系統呼叫過濾
    pub enable_seccomp: bool,    // 預設 true
    /// 額外允許存取的唯讀路徑
    pub readonly_paths: Vec<String>,  // 預設 ["/usr", "/lib", "/bin"]
}

#[derive(Debug, Clone, serde::Deserialize)]
pub struct WasmConfig {
    /// 最大 fuel（CPU 限制，wasmtime fuel metering）
    pub max_fuel: u64,           // 預設 10_000_000
    /// 最大記憶體 (bytes)
    pub max_memory: usize,       // 預設 1GB
    /// 預編譯的 QuickJS WASM module 路徑
    pub quickjs_module: Option<String>,
    /// 預編譯的 Pyodide WASM module 路徑
    pub pyodide_module: Option<String>,
    /// 是否允許 WASI 網路存取
    pub allow_network: bool,     // 預設 false
}

/// Sandbox trait — 所有執行器都透過這個介面執行程式碼
#[async_trait]
pub trait Sandbox: Send + Sync {
    /// 在沙箱中執行命令
    async fn execute(
        &self,
        ctx: &SandboxContext,
    ) -> Result<SandboxResult, SandboxError>;

    /// 檢查沙箱是否可用
    async fn health_check(&self) -> Result<(), SandboxError>;

    /// 取得沙箱類型名稱（用於日誌）
    fn name(&self) -> &str;
}

/// 執行上下文
pub struct SandboxContext {
    /// Job ID
    pub job_id: uuid::Uuid,
    /// 工作目錄（包含 main.py, args.json, result.json 等）
    pub job_dir: String,
    /// 要執行的命令
    pub command: String,
    /// 命令參數
    pub args: Vec<String>,
    /// 環境變數
    pub env: std::collections::HashMap<String, String>,
    /// 超時（秒）
    pub timeout_secs: u32,
    /// 語言（決定哪些系統檔案需要掛載）
    pub language: ScriptLang,
    /// 自訂 image（僅 K8s 模式有效）
    pub custom_image: Option<String>,
    /// OpenTelemetry trace context（注入子程序）
    pub trace_context: Option<TraceContext>,
}

pub struct SandboxResult {
    pub exit_code: i32,
    pub stdout: String,
    pub stderr: String,
    pub duration_ms: u64,
    pub memory_peak_bytes: u64,
}
```

### nsjail 實作

```rust
// crates/worker/src/sandbox_nsjail.rs

pub struct NsjailSandbox {
    config: NsjailConfig,
}

#[async_trait]
impl Sandbox for NsjailSandbox {
    async fn execute(&self, ctx: &SandboxContext) -> Result<SandboxResult, SandboxError> {
        // 1. 根據語言選擇 config template
        let template = match ctx.language {
            ScriptLang::Python3 => include_str!("../../nsjail/run.python3.config.proto"),
            ScriptLang::Bash => include_str!("../../nsjail/run.bash.config.proto"),
            ScriptLang::TypeScript => include_str!("../../nsjail/run.typescript.config.proto"),
            _ => return Err(SandboxError::UnsupportedLanguage(ctx.language)),
        };

        // 2. 填入動態值
        let nsjail_timeout = ctx.timeout_secs + 15; // 多 15 秒緩衝
        let config_content = template
            .replace("{JOB_DIR}", &ctx.job_dir)
            .replace("{TIMEOUT}", &nsjail_timeout.to_string())
            .replace("{CLONE_NEWUSER}", "true")
            .replace("{TMPFS_SIZE}", &self.config.tmpfs_size.to_string());

        // 3. 寫入 config 檔
        let config_path = format!("{}/run.config.proto", ctx.job_dir);
        tokio::fs::write(&config_path, &config_content).await?;

        // 4. 組裝 nsjail 命令
        let mut cmd = tokio::process::Command::new(&self.config.nsjail_path);
        cmd.current_dir(&ctx.job_dir)
            .env_clear()
            .envs(&ctx.env)
            .args(&["--config", "run.config.proto", "--"])
            .arg(&ctx.command)
            .args(&ctx.args)
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::piped());

        // 5. 注入 OTel trace context
        if let Some(trace) = &ctx.trace_context {
            cmd.env("TRACEPARENT", &trace.traceparent);
            cmd.env("TRACESTATE", &trace.tracestate);
        }

        // 6. 啟動並監控
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

    async fn health_check(&self) -> Result<(), SandboxError> {
        // 檢查 nsjail binary 是否存在
        let output = tokio::process::Command::new(&self.config.nsjail_path)
            .arg("--help")
            .output()
            .await?;
        if output.status.success() {
            Ok(())
        } else {
            Err(SandboxError::Unavailable("nsjail binary not found".into()))
        }
    }

    fn name(&self) -> &str { "nsjail" }
}
```

### K8s Pod 實作

```rust
// crates/worker/src/sandbox_k8s.rs

use k8s_openapi::api::core::v1::Pod;
use kube::{Api, Client, api::PostParams};

pub struct K8sPodSandbox {
    client: Client,
    config: K8sPodConfig,
}

#[async_trait]
impl Sandbox for K8sPodSandbox {
    async fn execute(&self, ctx: &SandboxContext) -> Result<SandboxResult, SandboxError> {
        let pods: Api<Pod> = Api::namespaced(self.client.clone(), &self.config.namespace);

        // 1. 選擇 image
        let image = ctx.custom_image
            .as_deref()
            .unwrap_or(&self.config.default_image);

        // 2. 建立 Pod spec
        let pod_name = format!("ff-job-{}", ctx.job_id);
        let pod: Pod = serde_json::from_value(serde_json::json!({
            "apiVersion": "v1",
            "kind": "Pod",
            "metadata": {
                "name": pod_name,
                "namespace": self.config.namespace,
                "labels": {
                    "app": "flowforge",
                    "component": "job",
                    "job-id": ctx.job_id.to_string(),
                    "language": format!("{:?}", ctx.language),
                }
            },
            "spec": {
                "restartPolicy": "Never",
                "serviceAccountName": self.config.service_account,
                "imagePullSecrets": self.config.image_pull_secrets.iter()
                    .map(|s| serde_json::json!({"name": s}))
                    .collect::<Vec<_>>(),
                "containers": [{
                    "name": "job",
                    "image": image,
                    "command": [ctx.command.clone()],
                    "args": ctx.args.clone(),
                    "env": ctx.env.iter()
                        .map(|(k, v)| serde_json::json!({"name": k, "value": v}))
                        .collect::<Vec<_>>(),
                    "resources": {
                        "requests": {
                            "cpu": self.config.cpu_request,
                            "memory": self.config.memory_request,
                        },
                        "limits": {
                            "cpu": self.config.cpu_limit,
                            "memory": self.config.memory_limit,
                        }
                    },
                    "volumeMounts": [{
                        "name": "job-data",
                        "mountPath": "/tmp/job",
                    }]
                }],
                "volumes": [{
                    "name": "job-data",
                    "configMap": {
                        // job_dir 的內容透過 ConfigMap 或 PVC 掛載
                        "name": format!("ff-job-{}", ctx.job_id),
                    }
                }],
                // 超時：用 activeDeadlineSeconds
                "activeDeadlineSeconds": ctx.timeout_secs as i64,
                "nodeSelector": self.config.node_selector,
            }
        }))?;

        // 3. 建立 Pod
        let start = std::time::Instant::now();
        pods.create(&PostParams::default(), &pod).await?;

        // 4. 等待 Pod 完成（串流日誌）
        let result = self.wait_for_pod(&pods, &pod_name, ctx).await?;

        // 5. 清理 Pod
        if self.config.auto_cleanup {
            pods.delete(&pod_name, &Default::default()).await.ok();
        }

        Ok(SandboxResult {
            exit_code: result.exit_code,
            stdout: result.stdout,
            stderr: result.stderr,
            duration_ms: start.elapsed().as_millis() as u64,
            memory_peak_bytes: 0, // K8s 不直接暴露 peak memory
        })
    }

    async fn health_check(&self) -> Result<(), SandboxError> {
        // 檢查 K8s 連線 + namespace 是否存在
        let ns: Api<k8s_openapi::api::core::v1::Namespace> = Api::all(self.client.clone());
        ns.get(&self.config.namespace).await
            .map(|_| ())
            .map_err(|e| SandboxError::Unavailable(format!("K8s namespace check failed: {e}")))
    }

    fn name(&self) -> &str { "kubernetes-pod" }
}

impl K8sPodSandbox {
    /// 等待 Pod 完成，同時串流日誌
    async fn wait_for_pod(
        &self,
        pods: &Api<Pod>,
        pod_name: &str,
        ctx: &SandboxContext,
    ) -> Result<PodResult, SandboxError> {
        use futures::StreamExt;
        use kube::runtime::watcher;

        // 監聽 Pod 狀態變化
        let watcher = watcher::watcher(
            pods.clone(),
            watcher::Config::default()
                .labels(&format!("job-id={}", ctx.job_id)),
        );

        tokio::pin!(watcher);

        let timeout = tokio::time::Duration::from_secs(ctx.timeout_secs as u64 + 30);
        let deadline = tokio::time::Instant::now() + timeout;

        loop {
            tokio::select! {
                event = watcher.next() => {
                    if let Some(Ok(event)) = event {
                        if let watcher::Event::Applied(pod) = event {
                            if let Some(status) = &pod.status {
                                if let Some(phase) = &status.phase {
                                    match phase.as_str() {
                                        "Succeeded" => {
                                            let logs = self.get_pod_logs(pods, pod_name).await?;
                                            return Ok(PodResult {
                                                exit_code: 0,
                                                stdout: logs,
                                                stderr: String::new(),
                                            });
                                        }
                                        "Failed" => {
                                            let logs = self.get_pod_logs(pods, pod_name).await?;
                                            return Ok(PodResult {
                                                exit_code: 1,
                                                stdout: String::new(),
                                                stderr: logs,
                                            });
                                        }
                                        _ => {} // Pending, Running → 繼續等待
                                    }
                                }
                            }
                        }
                    }
                }
                _ = tokio::time::sleep_until(deadline) => {
                    // 超時，刪除 Pod
                    pods.delete(pod_name, &Default::default()).await.ok();
                    return Err(SandboxError::Timeout(ctx.timeout_secs));
                }
            }
        }
    }

    async fn get_pod_logs(&self, pods: &Api<Pod>, name: &str) -> Result<String, SandboxError> {
        pods.logs(name, &Default::default())
            .await
            .map_err(|e| SandboxError::K8sError(e.to_string()))
    }
}

struct PodResult {
    exit_code: i32,
    stdout: String,
    stderr: String,
}
```

### Sandbox 路由器（根據 tag 自動分發）

```rust
// crates/worker/src/sandbox_router.rs

/// 根據 job 的 tag + 語言選擇最佳沙箱模式
pub struct SandboxRouter {
    rust_native: Option<RustNativeSandbox>,
    nsjail: Option<NsjailSandbox>,
    wasm: Option<WasmSandbox>,
    k8s: Option<K8sPodSandbox>,
    none: NoneSandbox,
}

impl SandboxRouter {
    pub fn from_config(config: &WorkerConfig) -> Self {
        Self {
            rust_native: config.rust_native.as_ref().map(|c| RustNativeSandbox::new(c.clone())),
            nsjail: config.nsjail.as_ref().map(|c| NsjailSandbox::new(c.clone())),
            wasm: config.wasm.as_ref().map(|c| WasmSandbox::new(c.clone())),
            k8s: config.k8s_pod.as_ref().map(|c| {
                let client = kube::Client::try_default()
                    .expect("K8s client init failed");
                K8sPodSandbox::new(client, c.clone())
            }),
            none: NoneSandbox,
        }
    }

    /// 根據 tag + 語言選擇沙箱
    ///
    /// tag 規則：
    ///   "wasm"       → WASM（微秒級啟動，僅 JS/純 Python）
    ///   "fast"       → Rust 原生 / nsjail（毫秒級，全語言支援）
    ///   "heavy"/"gpu"→ K8s Pod（秒級，自訂 image/GPU）
    ///   "none"/"dev" → 不隔離（開發用）
    ///   預設          → 按優先級自動選擇
    pub fn select(&self, tag: &str, language: Option<ScriptLang>) -> &dyn Sandbox {
        match tag {
            "wasm" => {
                // WASM 只支援 JS/TS 和純 Python
                if matches!(language, Some(ScriptLang::TypeScript) | Some(ScriptLang::Python3)) {
                    if let Some(wasm) = &self.wasm {
                        return wasm as &dyn Sandbox;
                    }
                }
                // fallback
                self.select_default()
            }
            "fast" | "native" => {
                self.rust_native.as_ref()
                    .map(|s| s as &dyn Sandbox)
                    .or_else(|| self.nsjail.as_ref().map(|s| s as &dyn Sandbox))
                    .unwrap_or(&self.none)
            }
            "nsjail" => {
                self.nsjail.as_ref()
                    .map(|s| s as &dyn Sandbox)
                    .unwrap_or(&self.none)
            }
            "heavy" | "k8s" | "gpu" => {
                self.k8s.as_ref()
                    .map(|s| s as &dyn Sandbox)
                    .unwrap_or(&self.none)
            }
            "none" | "dev" => &self.none,
            _ => self.select_default(),
        }
    }

    /// 預設優先級：Rust 原生 → nsjail → WASM → K8s → none
    fn select_default(&self) -> &dyn Sandbox {
        if let Some(rn) = &self.rust_native {
            rn as &dyn Sandbox
        } else if let Some(nsjail) = &self.nsjail {
            nsjail as &dyn Sandbox
        } else if let Some(wasm) = &self.wasm {
            wasm as &dyn Sandbox
        } else if let Some(k8s) = &self.k8s {
            k8s as &dyn Sandbox
        } else {
            &self.none
        }
    }
}

/// 最簡單的「沙箱」：直接 spawn 子程序
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
}
```

### 使用方式：Executor 變得極簡

```rust
// crates/worker/src/python.rs
// 對比 Windmill 的 python_executor.rs（有 nsjail 分支邏輯），這裡完全沒有 sandbox 相關的 if/else

pub async fn handle_python_job(
    job: &QueuedJob,
    db: &PgPool,
    content: &str,
    job_dir: &str,
    sandbox: &dyn Sandbox,  // ← 注入的沙箱，executor 不需要知道用哪種
) -> Result<serde_json::Value> {
    // 1. 解析依賴
    let requirements = parse_python_imports(content);

    // 2. 安裝依賴（快取）
    if !requirements.is_empty() {
        install_python_deps(&requirements, job_dir, db).await?;
    }

    // 3. 寫入使用者程式碼 + wrapper
    write_file(job_dir, "inner.py", content)?;
    write_file(job_dir, "wrapper.py", &generate_python_wrapper())?;

    // 4. 準備 I/O 檔案
    create_args_and_out_file(job, job_dir).await?;

    // 5. 透過 sandbox 執行（不管是 nsjail/K8s/none，呼叫方式一模一樣）
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

    // 6. 讀取結果
    read_result(job_dir).await
}
```

### 四模式對比（已移至上方「四種沙箱的完整比較」表格）

### 設定檔範例

```toml
# flowforge.toml — Worker 設定

[worker]
name = "worker-01"
tags = ["default", "fast"]

# === 模式 1：Rust 原生沙箱（推薦預設，無外部依賴）===
[worker.sandbox.rust_native]
memory_limit = 1073741824    # 1GB
cpu_time_limit = 1000        # 秒
isolate_network = false
enable_landlock = true       # 需要 Linux 5.13+
enable_seccomp = true
readonly_paths = ["/usr", "/lib", "/lib64", "/bin", "/etc"]

# === 模式 2：nsjail（成熟方案，需要 nsjail binary）===
[worker.sandbox.nsjail]
nsjail_path = "nsjail"
memory_limit = 1073741824
cpu_time_limit = 1000
clone_newnet = false
tmpfs_size = 524288000       # 500MB

# === 模式 3：WASM（微秒啟動，跨平台，僅 JS/純 Python）===
[worker.sandbox.wasm]
max_fuel = 10000000          # CPU 限制（fuel metering）
max_memory = 1073741824      # 1GB
quickjs_module = "/opt/flowforge/quickjs.wasm"
pyodide_module = "/opt/flowforge/pyodide.wasm"
allow_network = false

# === 模式 4：K8s Pod（重型 job、自訂 image、GPU）===
[worker.sandbox.k8s_pod]
namespace = "flowforge-jobs"
default_image = "flowforge/python-runner:3.12"
cpu_request = "100m"
cpu_limit = "2000m"
memory_request = "256Mi"
memory_limit = "4Gi"
service_account = "flowforge-job-runner"
auto_cleanup = true
image_pull_secrets = ["registry-secret"]

[worker.sandbox.k8s_pod.node_selector]
"node-type" = "compute"
```

### 各模式適用場景

```
生產環境（Linux server，高吞吐）：
  → rust_native（預設）+ k8s_pod（重型 job）

生產環境（Linux server，已有 nsjail）：
  → nsjail（預設）+ k8s_pod（重型 job）

Edge / Serverless（需要微秒級冷啟動）：
  → wasm（JS/TS job）+ rust_native（Python/Bash）

macOS / Windows 開發環境：
  → wasm（JS/TS）+ none（Python/Bash，開發時不隔離）

多租戶 SaaS（最強隔離需求）：
  → k8s_pod（所有 job 都走 Pod）
  → 或 rust_native + seccomp（成本更低）
```

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
    role VARCHAR(20) NOT NULL DEFAULT 'editor',  -- admin, editor, viewer
    PRIMARY KEY (workspace_id, email)
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
    created_by VARCHAR(255) NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    -- OTel trace context
    trace_id CHAR(32),
    span_id CHAR(16)
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
    last_ping TIMESTAMPTZ
);

CREATE INDEX idx_job_queue_pull ON job_queue(scheduled_for, priority DESC)
    WHERE running = FALSE;

-- job_completed：結果（完成後從 job_queue 刪除，插入這裡）
CREATE TABLE job_completed (
    id UUID PRIMARY KEY REFERENCES job(id),
    success BOOLEAN NOT NULL,
    result JSONB,
    result_s3_key VARCHAR(255),        -- 大結果存 S3
    duration_ms INTEGER NOT NULL,
    memory_peak_bytes BIGINT,
    completed_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- job_log：日誌（獨立表，避免頻繁 UPDATE job_queue）
CREATE TABLE job_log (
    job_id UUID NOT NULL REFERENCES job(id),
    log_offset INTEGER NOT NULL DEFAULT 0,
    logs TEXT NOT NULL DEFAULT '',
    PRIMARY KEY (job_id)
);

-- flow 狀態（獨立表）
CREATE TABLE job_flow_status (
    job_id UUID PRIMARY KEY REFERENCES job(id),
    flow_status JSONB NOT NULL
);

-- === Worker 健康 ===

CREATE TABLE worker_ping (
    worker VARCHAR(100) PRIMARY KEY,
    ping_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    tags TEXT[] NOT NULL DEFAULT '{}',
    ip VARCHAR(45),
    sandbox_mode VARCHAR(20)  -- "nsjail", "k8s", "none"
);
```

**與 Windmill 的差異：**
- Script hash 用 `CHAR(64)` SHA256（Windmill 用 `i64`）
- Job 拆成三表：`job`（不可變）+ `job_queue`（可變狀態）+ `job_completed`（結果）
- `job_log` 獨立表（Windmill v1 的 logs 直接 UPDATE queue 表，v2 才分離）
- Day 1 就有 `trace_id` / `span_id` 欄位
- Worker 記錄 `sandbox_mode`

### 1.2 後端 API

```rust
// crates/api/src/lib.rs

use axum::{Router, middleware};

pub fn create_router(db: PgPool, sandbox: Arc<SandboxRouter>) -> Router {
    let auth_middleware = middleware::from_fn_with_state(
        db.clone(),
        auth::require_auth,
    );

    Router::new()
        // 公開路由
        .route("/api/auth/login", post(auth::login))
        .route("/api/auth/signup", post(auth::signup))
        // 需要認證的路由
        .nest("/api/w/:workspace_id", Router::new()
            // Scripts
            .route("/scripts/create", post(scripts::create_script))
            .route("/scripts/list", get(scripts::list_scripts))
            .route("/scripts/get/p/*path", get(scripts::get_script_by_path))
            .route("/scripts/get/h/:hash", get(scripts::get_script_by_hash))
            // Jobs
            .route("/jobs/run/p/*path", post(jobs::run_script_by_path))
            .route("/jobs/run/preview", post(jobs::run_preview))
            .route("/jobs/:id", get(jobs::get_job))
            .route("/jobs/:id/result", get(jobs::get_job_result))
            .route("/jobs/:id/logs", get(sse::stream_job_logs))
            .route("/jobs/list", get(jobs::list_jobs))
            .layer(auth_middleware)
        )
        .with_state(AppState { db, sandbox })
}
```

#### Auth（JWT）

```rust
// crates/api/src/auth.rs

use argon2::{Argon2, PasswordHash, PasswordVerifier, PasswordHasher};
use jsonwebtoken::{encode, decode, Header, Validation, EncodingKey, DecodingKey};

#[derive(serde::Serialize, serde::Deserialize)]
struct Claims {
    email: String,
    exp: u64,
}

pub async fn login(
    State(db): State<PgPool>,
    Json(req): Json<LoginRequest>,
) -> Result<Json<LoginResponse>, ApiError> {
    // 1. 查帳號
    let account = sqlx::query_as!(Account,
        "SELECT email, password_hash FROM account WHERE email = $1",
        req.email
    )
    .fetch_optional(&db)
    .await?
    .ok_or(ApiError::Unauthorized)?;

    // 2. 驗密碼（argon2）
    let parsed_hash = PasswordHash::new(&account.password_hash)
        .map_err(|_| ApiError::Internal("hash parse error".into()))?;
    Argon2::default()
        .verify_password(req.password.as_bytes(), &parsed_hash)
        .map_err(|_| ApiError::Unauthorized)?;

    // 3. 簽 JWT
    let claims = Claims {
        email: account.email.clone(),
        exp: (chrono::Utc::now() + chrono::Duration::hours(24)).timestamp() as u64,
    };
    let token = encode(
        &Header::default(),
        &claims,
        &EncodingKey::from_secret(JWT_SECRET.as_bytes()),
    )?;

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
    // 1. 計算 SHA256 hash
    let hash = {
        let mut hasher = Sha256::new();
        hasher.update(&req.content);
        hasher.update(&req.path);
        hasher.update(req.language.as_str());
        format!("{:x}", hasher.finalize())
    };

    // 2. 查詢 parent（前一個版本）
    let parent = sqlx::query_scalar!(
        "SELECT hash FROM script WHERE workspace_id = $1 AND path = $2
         ORDER BY created_at DESC LIMIT 1",
        workspace_id, req.path
    )
    .fetch_optional(&state.db)
    .await?;

    let parent_hashes: Vec<String> = parent.into_iter().collect();

    // 3. 解析函式簽名 → JSON Schema
    let schema = match req.language {
        ScriptLang::Python3 => parse_python_signature(&req.content)?,
        ScriptLang::TypeScript => parse_typescript_signature(&req.content)?,
        _ => None,
    };

    // 4. 插入
    sqlx::query!(
        "INSERT INTO script (workspace_id, hash, path, content, language, schema, parent_hashes, created_by)
         VALUES ($1, $2, $3, $4, $5, $6, $7, $8)",
        workspace_id, hash, req.path, req.content,
        req.language.as_str(), schema, &parent_hashes, user.email
    )
    .execute(&state.db)
    .await?;

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
    // 1. 找到最新版本的 script
    let script = sqlx::query_as!(Script,
        "SELECT * FROM script WHERE workspace_id = $1 AND path = $2
         ORDER BY created_at DESC LIMIT 1",
        workspace_id, path
    )
    .fetch_optional(&state.db)
    .await?
    .ok_or(ApiError::NotFound)?;

    // 2. 推入 queue
    let job_id = queue::push_job(&state.db, PushJobArgs {
        workspace_id: &workspace_id,
        kind: JobKind::Script,
        script_hash: Some(&script.hash),
        script_path: Some(&script.path),
        language: Some(script.language),
        args: Some(args),
        tag: "default",
        created_by: &user.email,
        // OTel: 從當前 span 取 trace context
        trace_id: current_trace_id(),
        span_id: current_span_id(),
        ..Default::default()
    }).await?;

    Ok(Json(JobCreated { id: job_id }))
}

/// Preview：直接執行 inline code（不存 script）
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
        created_by: &user.email,
        trace_id: current_trace_id(),
        span_id: current_span_id(),
        ..Default::default()
    }).await?;

    Ok(Json(JobCreated { id: job_id }))
}
```

#### SSE 日誌串流

```rust
// crates/api/src/sse.rs

use axum::response::sse::{Event, Sse};
use futures::stream::Stream;

pub async fn stream_job_logs(
    State(state): State<AppState>,
    Path((workspace_id, job_id)): Path<(String, Uuid)>,
) -> Sse<impl Stream<Item = Result<Event, axum::Error>>> {
    let db = state.db.clone();

    let stream = async_stream::stream! {
        let mut last_offset = 0;

        loop {
            // 1. 讀取新日誌
            let row = sqlx::query!(
                "SELECT logs, log_offset FROM job_log WHERE job_id = $1",
                job_id
            )
            .fetch_optional(&db)
            .await;

            if let Ok(Some(row)) = row {
                let current_len = row.logs.len();
                if current_len > last_offset {
                    let new_logs = &row.logs[last_offset..];
                    yield Ok(Event::default()
                        .event("log")
                        .data(new_logs));
                    last_offset = current_len;
                }
            }

            // 2. 檢查 job 是否完成
            let completed = sqlx::query_scalar!(
                "SELECT EXISTS(SELECT 1 FROM job_completed WHERE id = $1)",
                job_id
            )
            .fetch_one(&db)
            .await;

            if let Ok(Some(true)) = completed {
                // 發送最終結果
                let result = sqlx::query!(
                    "SELECT success, result FROM job_completed WHERE id = $1",
                    job_id
                )
                .fetch_optional(&db)
                .await;

                if let Ok(Some(r)) = result {
                    yield Ok(Event::default()
                        .event("result")
                        .data(serde_json::to_string(&serde_json::json!({
                            "success": r.success,
                            "result": r.result,
                        })).unwrap()));
                }
                break;
            }

            tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
        }
    };

    Sse::new(stream)
}
```

### 1.3 Queue 操作

```rust
// crates/queue/src/push.rs

pub async fn push_job(db: &PgPool, args: PushJobArgs<'_>) -> Result<Uuid> {
    let mut tx = db.begin().await?;

    let job_id = Uuid::new_v4();

    // 1. 插入 job（不可變定義）
    sqlx::query!(
        "INSERT INTO job (id, workspace_id, kind, script_hash, script_path,
         raw_code, language, args, tag, parent_job, root_job, flow_step_id,
         created_by, trace_id, span_id)
         VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15)",
        job_id, args.workspace_id, args.kind.as_str(),
        args.script_hash, args.script_path,
        args.raw_code, args.language.map(|l| l.as_str()),
        args.args, args.tag,
        args.parent_job, args.root_job, args.flow_step_id,
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

    // 3. 建立空日誌
    sqlx::query!(
        "INSERT INTO job_log (job_id) VALUES ($1)", job_id
    )
    .execute(&mut *tx)
    .await?;

    tx.commit().await?;
    Ok(job_id)
}

// crates/queue/src/pull.rs

/// FOR UPDATE SKIP LOCKED — 搶 job
pub async fn pull_job(db: &PgPool, worker_name: &str, tags: &[String]) -> Result<Option<PulledJob>> {
    let row = sqlx::query_as!(PulledJob,
        r#"
        WITH next_job AS (
            SELECT jq.id
            FROM job_queue jq
            WHERE jq.running = FALSE
              AND jq.scheduled_for <= now()
              AND jq.tag = ANY($1)
            ORDER BY jq.priority DESC, jq.scheduled_for ASC
            LIMIT 1
            FOR UPDATE SKIP LOCKED
        )
        UPDATE job_queue
        SET running = TRUE, started_at = now(), worker = $2, last_ping = now()
        FROM next_job
        WHERE job_queue.id = next_job.id
        RETURNING job_queue.id, job_queue.tag
        "#,
        tags, worker_name
    )
    .fetch_optional(db)
    .await?;

    if let Some(row) = row {
        // 讀取完整 job 定義
        let job = sqlx::query_as!(QueuedJob,
            "SELECT * FROM job WHERE id = $1", row.id
        )
        .fetch_one(db)
        .await?;
        Ok(Some(PulledJob { job, tag: row.tag }))
    } else {
        Ok(None)
    }
}

// crates/queue/src/complete.rs

pub async fn complete_job(
    db: &PgPool,
    job_id: Uuid,
    success: bool,
    result: serde_json::Value,
    duration_ms: i32,
    memory_peak: i64,
    s3_key: Option<&str>,
) -> Result<()> {
    let mut tx = db.begin().await?;

    // 1. 寫入結果
    sqlx::query!(
        "INSERT INTO job_completed (id, success, result, result_s3_key, duration_ms, memory_peak_bytes)
         VALUES ($1, $2, $3, $4, $5, $6)",
        job_id, success,
        if s3_key.is_some() { None } else { Some(result) },
        s3_key, duration_ms, memory_peak,
    )
    .execute(&mut *tx)
    .await?;

    // 2. 從 queue 移除
    sqlx::query!("DELETE FROM job_queue WHERE id = $1", job_id)
        .execute(&mut *tx)
        .await?;

    tx.commit().await?;
    Ok(())
}
```

### 1.4 Worker 主迴圈

```rust
// crates/worker/src/worker.rs

use opentelemetry::trace::{Tracer, SpanKind};

pub async fn run_worker(
    db: PgPool,
    worker_name: String,
    tags: Vec<String>,
    sandbox_router: Arc<SandboxRouter>,
    tracer: opentelemetry::global::BoxedTracer,
) {
    let worker_dir = format!("/tmp/flowforge/{}", worker_name);
    tokio::fs::create_dir_all(&worker_dir).await.unwrap();

    // 健康檢查：定期 ping
    let db2 = db.clone();
    let wn = worker_name.clone();
    tokio::spawn(async move {
        loop {
            sqlx::query!(
                "INSERT INTO worker_ping (worker, ping_at, tags)
                 VALUES ($1, now(), $2)
                 ON CONFLICT (worker) DO UPDATE SET ping_at = now()",
                wn, &tags_for_ping
            ).execute(&db2).await.ok();
            tokio::time::sleep(std::time::Duration::from_secs(15)).await;
        }
    });

    loop {
        match queue::pull_job(&db, &worker_name, &tags).await {
            Ok(Some(pulled)) => {
                let job = pulled.job;
                let job_dir = format!("{}/{}", worker_dir, job.id);
                tokio::fs::create_dir_all(&job_dir).await.unwrap();

                // OTel: 建立 span（繼承 parent trace context）
                let span = tracer.span_builder(format!("job.{}", job.kind))
                    .with_kind(SpanKind::Consumer)
                    .with_attributes(vec![
                        KeyValue::new("job.id", job.id.to_string()),
                        KeyValue::new("job.workspace", job.workspace_id.clone()),
                        KeyValue::new("job.tag", pulled.tag.clone()),
                        KeyValue::new("sandbox.mode", sandbox_router.select(&pulled.tag).name().to_string()),
                    ])
                    .start(&tracer);
                let cx = opentelemetry::Context::current_with_span(span);

                // 選擇沙箱
                let sandbox = sandbox_router.select(&pulled.tag);

                // 執行 job
                let start = std::time::Instant::now();
                let result = handle_job(&job, &db, &job_dir, sandbox).await;
                let duration_ms = start.elapsed().as_millis() as i32;

                // 完成
                match result {
                    Ok((value, mem_peak)) => {
                        let s3_key = maybe_upload_to_s3(&value).await;
                        queue::complete_job(
                            &db, job.id, true, value, duration_ms,
                            mem_peak, s3_key.as_deref()
                        ).await.ok();
                        cx.span().set_status(opentelemetry::trace::StatusCode::Ok, "".into());
                    }
                    Err(e) => {
                        let error = serde_json::json!({"error": {"message": e.to_string()}});
                        queue::complete_job(
                            &db, job.id, false, error, duration_ms, 0, None
                        ).await.ok();
                        cx.span().set_status(
                            opentelemetry::trace::StatusCode::Error,
                            e.to_string(),
                        );
                    }
                }

                cx.span().end();

                // 清理
                tokio::fs::remove_dir_all(&job_dir).await.ok();

                // 如果是 flow 的 child job，通知 flow engine
                if let Some(parent_job) = job.parent_job {
                    update_flow_after_job_completion(&db, parent_job, job.id).await.ok();
                }
            }
            Ok(None) => {
                tokio::time::sleep(std::time::Duration::from_millis(500)).await;
            }
            Err(e) => {
                tracing::error!("Error pulling job: {:?}", e);
                tokio::time::sleep(std::time::Duration::from_secs(5)).await;
            }
        }
    }
}

async fn handle_job(
    job: &QueuedJob,
    db: &PgPool,
    job_dir: &str,
    sandbox: &dyn Sandbox,
) -> Result<(serde_json::Value, i64)> {
    match job.kind.as_str() {
        "flow" | "flow_preview" => {
            handle_flow_job(job, db, job_dir, sandbox).await
        }
        _ => {
            let (content, language) = get_job_content(job, db).await?;
            match language {
                ScriptLang::Python3 => handle_python_job(job, db, &content, job_dir, sandbox).await,
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

# 載入使用者的 main 函式
from inner import main

# 讀取輸入
with open("args.json") as f:
    args = json.load(f)

# 執行
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

# 寫入結果
with open("result.json", "w") as f:
    json.dump(result, f, default=str)
```

### 1.6 前端

#### ScriptEditor.svelte（Monaco 編輯器 + Run 按鈕）

```svelte
<!-- src/lib/components/ScriptEditor.svelte -->
<script lang="ts">
  import { onMount } from 'svelte'
  import type { editor } from 'monaco-editor'

  let {
    content = $bindable(),
    language = 'python',
    onRun,
  }: {
    content: string
    language?: string
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

    monacoEditor.onDidChangeModelContent(() => {
      content = monacoEditor.getValue()
    })

    // Ctrl+Enter 執行
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
    <button onclick={() => onRun?.(content)}>
      Run
    </button>
  </div>
  <div bind:this={editorContainer} class="editor-container"></div>
</div>

<style>
  .editor-wrapper {
    display: flex;
    flex-direction: column;
    height: 100%;
  }
  .toolbar {
    display: flex;
    gap: 8px;
    padding: 8px;
    background: #1e1e1e;
    border-bottom: 1px solid #333;
  }
  .editor-container {
    flex: 1;
  }
</style>
```

#### LogViewer.svelte（SSE 即時日誌）

```svelte
<!-- src/lib/components/LogViewer.svelte -->
<script lang="ts">
  let {
    jobId,
    workspaceId,
    onResult,
  }: {
    jobId: string
    workspaceId: string
    onResult?: (result: { success: boolean; result: any }) => void
  } = $props()

  let logs = $state('')
  let status = $state<'running' | 'success' | 'failure'>('running')
  let logContainer: HTMLPreElement

  $effect(() => {
    if (!jobId) return

    const eventSource = new EventSource(
      `/api/w/${workspaceId}/jobs/${jobId}/logs`
    )

    eventSource.addEventListener('log', (e) => {
      logs += e.data
      // 自動捲到底部
      if (logContainer) {
        logContainer.scrollTop = logContainer.scrollHeight
      }
    })

    eventSource.addEventListener('result', (e) => {
      const data = JSON.parse(e.data)
      status = data.success ? 'success' : 'failure'
      onResult?.(data)
      eventSource.close()
    })

    eventSource.onerror = () => {
      status = 'failure'
      eventSource.close()
    }

    return () => eventSource.close()
  })
</script>

<div class="log-viewer">
  <div class="status-bar" class:success={status === 'success'} class:failure={status === 'failure'}>
    {#if status === 'running'}
      Running...
    {:else if status === 'success'}
      Completed
    {:else}
      Failed
    {/if}
  </div>
  <pre bind:this={logContainer} class="logs">{logs}</pre>
</div>

<style>
  .log-viewer {
    display: flex;
    flex-direction: column;
    height: 100%;
    background: #1e1e1e;
    color: #d4d4d4;
    font-family: 'Fira Code', monospace;
  }
  .status-bar {
    padding: 4px 12px;
    background: #333;
    font-size: 12px;
  }
  .status-bar.success { color: #4ec9b0; }
  .status-bar.failure { color: #f44747; }
  .logs {
    flex: 1;
    overflow-y: auto;
    padding: 12px;
    margin: 0;
    white-space: pre-wrap;
    font-size: 13px;
  }
</style>
```

### 1.7 main.rs（Server + Worker 同一程序）

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
        .with_exporter(
            opentelemetry_otlp::new_exporter()
                .tonic()
                .with_endpoint("http://localhost:4317"),
        )
        .install_batch(opentelemetry_sdk::runtime::Tokio)?;

    let telemetry = tracing_opentelemetry::layer().with_tracer(tracer.clone());
    let subscriber = tracing_subscriber::registry()
        .with(tracing_subscriber::fmt::layer())
        .with(telemetry);
    tracing::subscriber::set_global_default(subscriber)?;

    // 2. DB 連線
    let db = sqlx::PgPool::connect(
        &std::env::var("DATABASE_URL")
            .unwrap_or_else(|_| "postgres://postgres:changeme@localhost:5432/flowforge".into())
    ).await?;

    // 3. 跑 migration
    sqlx::migrate!("./migrations").run(&db).await?;

    // 4. 初始化 Sandbox Router
    let config: WorkerConfig = load_config()?;
    let sandbox = Arc::new(SandboxRouter::from_config(&config));

    // 5. 啟動 API Server
    let app = api::create_router(db.clone(), sandbox.clone());
    let server = tokio::spawn(async move {
        let addr = "0.0.0.0:8000";
        tracing::info!("API server listening on {}", addr);
        let listener = tokio::net::TcpListener::bind(addr).await.unwrap();
        axum::serve(listener, app).await.unwrap();
    });

    // 6. 啟動 Worker（可以多個）
    let num_workers = config.num_workers.unwrap_or(4);
    let workers: Vec<_> = (0..num_workers).map(|i| {
        let db = db.clone();
        let sandbox = sandbox.clone();
        let tags = config.tags.clone();
        let tracer = global::tracer("flowforge-worker");
        tokio::spawn(async move {
            worker::run_worker(
                db,
                format!("worker-{}", i),
                tags,
                sandbox,
                tracer,
            ).await;
        })
    }).collect();

    // 7. 等待結束
    tokio::select! {
        _ = server => {},
        _ = futures::future::join_all(workers) => {},
        _ = tokio::signal::ctrl_c() => {
            tracing::info!("Shutting down...");
        }
    }

    // 8. 清理 OTel
    global::shutdown_tracer_provider();

    Ok(())
}
```

### 1.8 docker-compose.yml

```yaml
version: "3.8"

services:
  postgres:
    image: postgres:16-alpine
    environment:
      POSTGRES_DB: flowforge
      POSTGRES_USER: postgres
      POSTGRES_PASSWORD: changeme
    ports:
      - "5432:5432"
    volumes:
      - pgdata:/var/lib/postgresql/data

  minio:
    image: minio/minio
    command: server /data --console-address ":9001"
    environment:
      MINIO_ROOT_USER: minioadmin
      MINIO_ROOT_PASSWORD: minioadmin
    ports:
      - "9000:9000"
      - "9001:9001"  # MinIO Console
    volumes:
      - miniodata:/data

  jaeger:
    image: jaegertracing/all-in-one:latest
    environment:
      COLLECTOR_OTLP_ENABLED: true
    ports:
      - "16686:16686"  # Jaeger UI
      - "4317:4317"    # OTLP gRPC
      - "4318:4318"    # OTLP HTTP

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
    pub same_worker: bool,           // 是否所有步驟用同一 worker
    pub concurrent_limit: Option<u32>,
}

/// 一個步驟
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FlowModule {
    pub id: String,                  // "a", "b", "c"...
    pub value: FlowModuleValue,
    pub retry: Option<Retry>,
    pub sleep: Option<InputTransform>,   // 步驟間延遲
    pub summary: Option<String>,
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
        tag_override: Option<String>,  // 可覆蓋沙箱模式
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
    /// 靜態值
    Static { value: serde_json::Value },
    /// JavaScript 表達式（用 boa_engine 求值）
    /// 例如："results.step_a.count + 1"
    Javascript { expr: String },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Branch {
    pub summary: Option<String>,
    pub expr: String,           // JS 條件表達式
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
pub struct RetryConstant {
    pub attempts: u32,
    pub seconds: u32,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RetryExponential {
    pub attempts: u32,
    pub multiplier: u32,     // 秒
    pub seconds: u32,        // base
    pub random_factor: Option<f64>,  // jitter
}
```

### 2.2 Flow 狀態機

```rust
// crates/types/src/flow_status.rs

/// 整個 Flow 的執行狀態，存在 job_flow_status.flow_status JSONB
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
        flow_jobs: Option<Vec<Uuid>>,  // 並行時的子 job
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
pub struct BranchChosen {
    pub branch: usize,  // 哪個分支
}

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
    flow_job: &QueuedJob,
    db: &PgPool,
    job_dir: &str,
    sandbox: &dyn Sandbox,
) -> Result<(serde_json::Value, i64)> {
    let flow_value: FlowValue = serde_json::from_value(
        flow_job.flow_value.clone().ok_or(Error::MissingFlowValue)?
    )?;

    // 讀取或初始化 FlowStatus
    let mut status = get_or_init_flow_status(db, flow_job.id, &flow_value).await?;

    loop {
        let step = status.step;

        // 所有步驟完成
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
                // 1. 求值 input_transforms
                let args = transform_inputs(input_transforms, &status, db).await?;

                // 2. 推入 child job
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

                // 3. 更新 FlowStatus
                status.modules[step] = FlowStatusModule::InProgress {
                    id: module.id.clone(),
                    job: child_id,
                    iterator: None,
                    branch_chosen: None,
                    parallel: false,
                    flow_jobs: None,
                };
                save_flow_status(db, flow_job.id, &status).await?;

                // 等待 child job 完成（由 worker 主迴圈觸發 update_flow_after_job_completion）
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
                    id: module.id.clone(),
                    job: child_id,
                    iterator: None,
                    branch_chosen: None,
                    parallel: false,
                    flow_jobs: None,
                };
                save_flow_status(db, flow_job.id, &status).await?;
                return Ok((serde_json::Value::Null, 0));
            }
            FlowModuleValue::ForloopFlow { iterator, modules, parallel, skip_failures } => {
                // 求值 iterator → 陣列
                let items = eval_input_transform(iterator, &status)?;
                let items = items.as_array()
                    .ok_or(Error::BadRequest("ForLoop iterator must be an array"))?;

                if *parallel {
                    // 並行：同時推入所有 iteration jobs
                    let mut child_ids = Vec::new();
                    for (i, item) in items.iter().enumerate() {
                        let child_id = push_forloop_iteration(
                            db, flow_job, module, modules, item, i
                        ).await?;
                        child_ids.push(child_id);
                    }
                    status.modules[step] = FlowStatusModule::InProgress {
                        id: module.id.clone(),
                        job: Uuid::nil(),
                        iterator: Some(IteratorStatus {
                            index: items.len(),
                            itered: items.clone(),
                            args: serde_json::Value::Null,
                        }),
                        branch_chosen: None,
                        parallel: true,
                        flow_jobs: Some(child_ids),
                    };
                } else {
                    // 序列：推入第一個
                    if let Some(first) = items.first() {
                        let child_id = push_forloop_iteration(
                            db, flow_job, module, modules, first, 0
                        ).await?;
                        status.modules[step] = FlowStatusModule::InProgress {
                            id: module.id.clone(),
                            job: child_id,
                            iterator: Some(IteratorStatus {
                                index: 0,
                                itered: items.clone(),
                                args: serde_json::Value::Null,
                            }),
                            branch_chosen: None,
                            parallel: false,
                            flow_jobs: None,
                        };
                    }
                }
                save_flow_status(db, flow_job.id, &status).await?;
                return Ok((serde_json::Value::Null, 0));
            }
            FlowModuleValue::BranchOne { branches, default } => {
                // 求值每個 branch 的條件
                let mut chosen = None;
                for (i, branch) in branches.iter().enumerate() {
                    let result = eval_js_expr(&branch.expr, &status)?;
                    if result.as_bool().unwrap_or(false) {
                        chosen = Some((i, &branch.modules));
                        break;
                    }
                }
                let (branch_idx, modules) = chosen
                    .unwrap_or((branches.len(), default));

                // 推入分支的第一個步驟作為 child flow
                let child_id = push_branch_as_flow(
                    db, flow_job, module, modules
                ).await?;

                status.modules[step] = FlowStatusModule::InProgress {
                    id: module.id.clone(),
                    job: child_id,
                    iterator: None,
                    branch_chosen: Some(BranchChosen { branch: branch_idx }),
                    parallel: false,
                    flow_jobs: None,
                };
                save_flow_status(db, flow_job.id, &status).await?;
                return Ok((serde_json::Value::Null, 0));
            }
            FlowModuleValue::BranchAll { branches, parallel } => {
                let mut child_ids = Vec::new();
                for branch in branches {
                    let child_id = push_branch_as_flow(
                        db, flow_job, module, &branch.modules
                    ).await?;
                    child_ids.push(child_id);
                }
                status.modules[step] = FlowStatusModule::InProgress {
                    id: module.id.clone(),
                    job: Uuid::nil(),
                    iterator: None,
                    branch_chosen: None,
                    parallel: *parallel,
                    flow_jobs: Some(child_ids),
                };
                save_flow_status(db, flow_job.id, &status).await?;
                return Ok((serde_json::Value::Null, 0));
            }
            FlowModuleValue::Identity => {
                // 直接傳遞前一步結果
                let prev_result = if step > 0 {
                    match &status.modules[step - 1] {
                        FlowStatusModule::Success { result, .. } => result.clone(),
                        _ => serde_json::Value::Null,
                    }
                } else {
                    serde_json::Value::Null
                };
                status.modules[step] = FlowStatusModule::Success {
                    id: module.id.clone(),
                    job: Uuid::nil(),
                    result: prev_result,
                };
                status.step += 1;
                save_flow_status(db, flow_job.id, &status).await?;
                // 繼續 loop 處理下一步
            }
        }
    }
}

/// Child job 完成後更新 Flow 狀態
pub async fn update_flow_after_job_completion(
    db: &PgPool,
    flow_job_id: Uuid,
    child_job_id: Uuid,
) -> Result<()> {
    let completed = sqlx::query!(
        "SELECT success, result FROM job_completed WHERE id = $1",
        child_job_id
    )
    .fetch_one(db)
    .await?;

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
        // 檢查是否要重試
        let flow_value = get_flow_value(db, flow_job_id).await?;
        let module = &flow_value.modules[step];
        if let Some(retry) = &module.retry {
            if should_retry(&status.retry, retry) {
                status.retry.fail_count += 1;
                // 重新推入同一步驟（不遞增 step）
                // ... 推入 child job ...
                save_flow_status(db, flow_job_id, &status).await?;
                return Ok(());
            }
        }

        // 不重試 → 標記失敗
        status.modules[step] = FlowStatusModule::Failure {
            id: status.modules[step].id().to_string(),
            job: child_job_id,
            error: completed.result.unwrap_or(serde_json::Value::Null),
        };

        // 執行 failure_module
        if let Some(failure_module) = &flow_value.failure_module {
            // ... 推入 failure_module 作為 child job ...
        }

        save_flow_status(db, flow_job_id, &status).await?;
        // 標記 flow job 本身為失敗
        queue::complete_job(db, flow_job_id, false, /* ... */).await?;
        return Ok(());
    }

    save_flow_status(db, flow_job_id, &status).await?;

    // 推入 flow job 回 queue，繼續處理下一步
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

    // 注入 results 物件
    let results_obj = serde_json_to_js_value(
        &serde_json::to_value(results)?,
        &mut context,
    )?;
    context.register_global_property(
        "results",
        results_obj,
        Attribute::READONLY,
    )?;

    // 注入 flow_input
    let flow_input_val = serde_json_to_js_value(flow_input, &mut context)?;
    context.register_global_property(
        "flow_input",
        flow_input_val,
        Attribute::READONLY,
    )?;

    // 注入 previous_result
    let prev_val = serde_json_to_js_value(previous_result, &mut context)?;
    context.register_global_property(
        "previous_result",
        prev_val,
        Attribute::READONLY,
    )?;

    // 求值
    let result = context.eval(Source::from_bytes(expr.as_bytes()))
        .map_err(|e| JsEvalError::EvalError(e.to_string()))?;

    // 轉回 serde_json::Value
    js_value_to_serde_json(&result, &mut context)
}

fn serde_json_to_js_value(
    val: &serde_json::Value,
    ctx: &mut Context,
) -> Result<JsValue, JsEvalError> {
    match val {
        serde_json::Value::Null => Ok(JsValue::null()),
        serde_json::Value::Bool(b) => Ok(JsValue::from(*b)),
        serde_json::Value::Number(n) => {
            Ok(JsValue::from(n.as_f64().unwrap_or(0.0)))
        }
        serde_json::Value::String(s) => {
            Ok(JsValue::from(boa_engine::JsString::from(s.as_str())))
        }
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
                    boa_engine::property::PropertyKey::from(
                        boa_engine::JsString::from(key.as_str())
                    ),
                    js_val,
                    false,
                    ctx,
                )?;
            }
            Ok(js_obj.into())
        }
    }
}
```

### 2.5 前端 Flow Editor（@xyflow/svelte）

```svelte
<!-- src/lib/components/FlowEditor.svelte -->
<script lang="ts">
  import {
    SvelteFlow,
    Controls,
    Background,
    MiniMap,
    type Node,
    type Edge,
  } from '@xyflow/svelte'
  import '@xyflow/svelte/dist/style.css'

  import StepNode from './flow/StepNode.svelte'
  import StepConfigPanel from './flow/StepConfigPanel.svelte'

  let {
    flowValue = $bindable(),
    onSave,
  }: {
    flowValue: FlowValue
    onSave?: (flow: FlowValue) => void
  } = $props()

  // 自訂 node 類型
  const nodeTypes = { step: StepNode }

  // FlowValue → xyflow nodes/edges
  let nodes = $state<Node[]>([])
  let edges = $state<Edge[]>([])
  let selectedNodeId = $state<string | null>(null)

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
        id: mod.id,
        type: 'step',
        position: { x: 300, y: i * 150 },
        data: {
          module: mod,
          index: i,
        },
      })
      if (i > 0) {
        e.push({
          id: `${flow.modules[i - 1].id}-${mod.id}`,
          source: flow.modules[i - 1].id,
          target: mod.id,
          animated: true,
        })
      }
    })

    return { n, e }
  }

  // Node 點選 → 開啟設定面板
  function onNodeClick(event: CustomEvent) {
    selectedNodeId = event.detail.node.id
  }

  // 拖入新步驟
  function addStep(type: string) {
    const id = String.fromCharCode(97 + flowValue.modules.length) // a, b, c...
    const newModule: FlowModule = {
      id,
      value: type === 'script'
        ? { type: 'Script', path: '', input_transforms: {} }
        : { type: 'RawScript', content: '', language: 'python3', input_transforms: {} },
      retry: null,
      sleep: null,
      summary: `Step ${id}`,
    }
    flowValue.modules = [...flowValue.modules, newModule]
  }

  let selectedModule = $derived(
    selectedNodeId
      ? flowValue.modules.find(m => m.id === selectedNodeId)
      : null
  )
</script>

<div class="flow-editor">
  <!-- 左側：步驟庫 -->
  <div class="step-palette">
    <h3>Steps</h3>
    <button onclick={() => addStep('script')}>+ Script</button>
    <button onclick={() => addStep('raw')}>+ Inline Code</button>
    <button onclick={() => addStep('forloop')}>+ For Loop</button>
    <button onclick={() => addStep('branch')}>+ Branch</button>
  </div>

  <!-- 中間：DAG 畫布 -->
  <div class="canvas">
    <SvelteFlow
      {nodes}
      {edges}
      {nodeTypes}
      fitView
      on:nodeclick={onNodeClick}
    >
      <Controls />
      <Background />
      <MiniMap />
    </SvelteFlow>
  </div>

  <!-- 右側：步驟設定面板 -->
  {#if selectedModule}
    <StepConfigPanel
      module={selectedModule}
      onUpdate={(updated) => {
        flowValue.modules = flowValue.modules.map(m =>
          m.id === updated.id ? updated : m
        )
      }}
    />
  {/if}
</div>

<style>
  .flow-editor {
    display: grid;
    grid-template-columns: 200px 1fr 300px;
    height: 100vh;
  }
  .step-palette {
    padding: 16px;
    border-right: 1px solid #ddd;
  }
  .canvas {
    position: relative;
  }
</style>
```

### 2.6 新增 API

```rust
// Flow CRUD
POST   /api/w/{ws}/flows/create       // 建立/更新 flow
GET    /api/w/{ws}/flows/list          // 列出 flows
GET    /api/w/{ws}/flows/get/p/{path}  // 取得 flow 定義
POST   /api/w/{ws}/jobs/run/f/{path}   // 執行 flow
GET    /api/w/{ws}/jobs/{id}/flow_status  // 查詢 flow 執行狀態
```

### 2.7 驗證方式

```
1. 建立 3 步驟 Flow：
   A(return 42) → B(return results.a * 2) → C(return results.b + 10)
   預期結果：94

2. 殺掉 Worker 重啟 → 確認 flow 從中斷點恢復
   （靠 FlowStatus JSONB + re-enqueue）

3. 讓 Step B 失敗 → 確認 failure_module 執行

4. 在 Jaeger 看到完整的 flow trace：
   flow_job → child_a → child_b → child_c（parent-child span 關係）
```

---

## Phase 3：Data Pipeline 功能（Week 7-9）

### 目標

排程、重試、並行、大資料傳遞——像 Airflow 一樣

### 3.1 新功能

| 功能 | 實作方式 |
|------|---------|
| Cron 排程 | `schedule` 表 + `cron` crate + 背景 scheduler loop |
| Retry | `Retry { constant, exponential }` + 指數退避 + random jitter |
| BranchOne | 求值各 branch 的 JS 條件，走第一個 true |
| ForLoop | 求值 iterator → 陣列，每個元素跑一次 modules |
| 並行 ForLoop | `parallel: true` → 同時推入所有 iteration jobs |
| BranchAll | 所有分支同時執行 |
| S3 大結果 | >2MB 結果上傳 S3，DB 只存 reference |

### 3.2 排程系統

```sql
CREATE TABLE schedule (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    workspace_id VARCHAR(50) NOT NULL REFERENCES workspace(id),
    path VARCHAR(255) NOT NULL,          -- schedule 的路徑
    cron_expr VARCHAR(100) NOT NULL,     -- "*/5 * * * *"
    timezone VARCHAR(50) NOT NULL DEFAULT 'UTC',
    enabled BOOLEAN NOT NULL DEFAULT TRUE,
    script_path VARCHAR(255),            -- 要跑的 script
    flow_path VARCHAR(255),              -- 或要跑的 flow
    args JSONB NOT NULL DEFAULT '{}',
    on_failure_path VARCHAR(255),        -- 失敗時執行
    on_recovery_path VARCHAR(255),       -- 恢復時執行
    on_success_path VARCHAR(255),        -- 成功時執行
    last_triggered_at TIMESTAMPTZ,
    next_trigger_at TIMESTAMPTZ,
    created_by VARCHAR(255) NOT NULL,
    UNIQUE (workspace_id, path)
);
```

```rust
// 背景 scheduler loop
async fn schedule_loop(db: PgPool) {
    loop {
        // 找到需要觸發的排程
        let schedules = sqlx::query_as!(Schedule,
            "SELECT * FROM schedule
             WHERE enabled = TRUE
               AND next_trigger_at <= now()
             ORDER BY next_trigger_at ASC
             LIMIT 100
             FOR UPDATE SKIP LOCKED"
        )
        .fetch_all(&db)
        .await
        .unwrap_or_default();

        for schedule in schedules {
            // 推入 job
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
                    // ... flow 相關欄位
                    ..Default::default()
                }).await
            } else {
                continue;
            };

            // 計算下次觸發時間
            let next = cron::Schedule::from_str(&schedule.cron_expr)
                .ok()
                .and_then(|s| s.upcoming(chrono::Utc).next());

            sqlx::query!(
                "UPDATE schedule SET last_triggered_at = now(), next_trigger_at = $1
                 WHERE id = $2",
                next, schedule.id
            )
            .execute(&db)
            .await
            .ok();
        }

        tokio::time::sleep(std::time::Duration::from_secs(1)).await;
    }
}
```

### 3.3 S3 大結果

```rust
// 結果 > 2MB → 上傳 S3
async fn maybe_upload_to_s3(result: &serde_json::Value) -> Option<String> {
    let serialized = serde_json::to_vec(result).ok()?;

    if serialized.len() > 2 * 1024 * 1024 {
        let key = format!("results/{}/{}.json",
            chrono::Utc::now().format("%Y/%m/%d"),
            Uuid::new_v4()
        );

        let client = aws_sdk_s3::Client::new(&aws_config::load_defaults(BehaviorVersion::latest()).await);
        client.put_object()
            .bucket(&S3_BUCKET)
            .key(&key)
            .body(serialized.into())
            .content_type("application/json")
            .send()
            .await
            .ok()?;

        Some(key)
    } else {
        None
    }
}
```

### 3.4 驗證方式

```
1. 建立 `*/1 * * * *` 排程 → 確認每分鐘觸發
2. 建立 retry 3 次的 flow step → 讓它失敗 → 確認重試 3 次 + 指數間隔
3. ForLoop parallel → 迭代 [1,2,3] → 確認 3 個 child job 同時啟動
4. 產生 5MB 結果 → 確認上傳 S3 + DB 只存 s3_key
5. 所有操作在 Jaeger 可追蹤
```

---

## Phase 4：超越 Windmill（Week 10-12）

### 4.1 DataEngine trait — 可插拔的資料處理引擎

**設計原則**：不要把 DuckDB 寫死。用 trait 抽象，Day 1 實作 DuckDB，未來可接 DataFusion、ClickHouse、Polars 等。

```rust
// crates/worker/src/data_engine.rs

use async_trait::async_trait;

/// 可插拔的資料處理引擎 trait
#[async_trait]
pub trait DataEngine: Send + Sync {
    /// 引擎名稱（用於 FlowModuleValue::Query 的路由）
    fn name(&self) -> &str;

    /// 執行 SQL 查詢
    async fn execute_query(
        &self,
        query: &str,
        sources: &[DataSource],
        s3_config: &S3Config,
    ) -> Result<serde_json::Value, DataEngineError>;

    /// 健康檢查
    async fn health_check(&self) -> Result<(), DataEngineError>;
}

/// 資料來源（引擎無關）
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type")]
pub enum DataSource {
    Parquet { url: String },           // s3://bucket/path/*.parquet
    Csv { url: String },
    Json { url: String },
    Postgres { url: String, table: String },
    PreviousStepResult { step_id: String },  // 引用前一步的結果
}
```

**DuckDB 實作：**

```rust
// crates/worker/src/engine_duckdb.rs

pub struct DuckDbEngine;

#[async_trait]
impl DataEngine for DuckDbEngine {
    fn name(&self) -> &str { "duckdb" }

    async fn execute_query(
        &self,
        query: &str,
        sources: &[DataSource],
        s3_config: &S3Config,
    ) -> Result<serde_json::Value, DataEngineError> {
        let conn = duckdb::Connection::open_in_memory()?;

        conn.execute_batch("INSTALL httpfs; LOAD httpfs;")?;
        conn.execute_batch(&format!(
            "SET s3_region='{}'; SET s3_access_key_id='{}'; SET s3_secret_access_key='{}';",
            s3_config.region, s3_config.access_key, s3_config.secret_key
        ))?;

        // 註冊資料來源為 DuckDB 表
        for (i, source) in sources.iter().enumerate() {
            let table_name = format!("source_{}", i);
            match source {
                DataSource::Parquet { url } => {
                    conn.execute(&format!(
                        "CREATE VIEW {} AS SELECT * FROM read_parquet('{}')",
                        table_name, url
                    ), [])?;
                }
                DataSource::Csv { url } => {
                    conn.execute(&format!(
                        "CREATE VIEW {} AS SELECT * FROM read_csv('{}')",
                        table_name, url
                    ), [])?;
                }
                DataSource::Postgres { url, table } => {
                    conn.execute_batch(&format!(
                        "INSTALL postgres; LOAD postgres;
                         ATTACH '{}' AS pg (TYPE POSTGRES);
                         CREATE VIEW {} AS SELECT * FROM pg.{};",
                        url, table_name, table
                    ))?;
                }
                _ => {}
            }
        }

        // 執行使用者 SQL
        let mut stmt = conn.prepare(query)?;
        let column_count = stmt.column_count();
        let column_names: Vec<String> = (0..column_count)
            .map(|i| stmt.column_name(i).unwrap().to_string())
            .collect();

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
        duckdb::Connection::open_in_memory()
            .map(|_| ())
            .map_err(|e| DataEngineError::Unavailable(e.to_string()))
    }
}
```

**DataFusion 實作（未來）：**

```rust
// crates/worker/src/engine_datafusion.rs

pub struct DataFusionEngine;

#[async_trait]
impl DataEngine for DataFusionEngine {
    fn name(&self) -> &str { "datafusion" }

    async fn execute_query(
        &self,
        query: &str,
        sources: &[DataSource],
        s3_config: &S3Config,
    ) -> Result<serde_json::Value, DataEngineError> {
        let ctx = datafusion::prelude::SessionContext::new();

        for (i, source) in sources.iter().enumerate() {
            let table_name = format!("source_{}", i);
            match source {
                DataSource::Parquet { url } => {
                    ctx.register_parquet(&table_name, url, Default::default()).await?;
                }
                DataSource::Csv { url } => {
                    ctx.register_csv(&table_name, url, Default::default()).await?;
                }
                _ => {}
            }
        }

        let df = ctx.sql(query).await?;
        let batches = df.collect().await?;
        // Arrow RecordBatch → JSON
        Ok(arrow_batches_to_json(&batches)?)
    }

    async fn health_check(&self) -> Result<(), DataEngineError> { Ok(()) }
}
```

**DataEngine Router（類似 SandboxRouter）：**

```rust
pub struct DataEngineRouter {
    engines: HashMap<String, Box<dyn DataEngine>>,
}

impl DataEngineRouter {
    pub fn new() -> Self {
        let mut engines: HashMap<String, Box<dyn DataEngine>> = HashMap::new();
        engines.insert("duckdb".into(), Box::new(DuckDbEngine));
        // 未來：
        // engines.insert("datafusion".into(), Box::new(DataFusionEngine));
        // engines.insert("clickhouse".into(), Box::new(ClickHouseEngine));
        // engines.insert("polars".into(), Box::new(PolarsEngine));
        Self { engines }
    }

    pub fn get(&self, name: &str) -> Option<&dyn DataEngine> {
        self.engines.get(name).map(|e| e.as_ref())
    }
}
```

**Flow 中使用（新增 FlowModuleValue::Query）：**

```rust
// 在 FlowModuleValue enum 中新增：
enum FlowModuleValue {
    // ... 原有的 Script, RawScript, ForloopFlow, BranchOne, BranchAll, Identity ...

    /// 資料查詢步驟（引擎可選）
    Query {
        engine: String,         // "duckdb", "datafusion", "clickhouse"
        query: String,          // SQL
        sources: Vec<DataSource>,
        input_transforms: HashMap<String, InputTransform>,
    },
}
```

**為什麼這樣設計？**

| 方案 | 問題 |
|------|------|
| DuckDB 寫死在 ScriptLang 裡 | 加 DataFusion 要改 enum + executor + 前端 |
| DataEngine trait | 加新引擎只需實作 trait + 註冊到 Router |
| FlowModuleValue::Query | 前端只需一個「Query Step」UI，下拉選引擎 |

使用者在 Flow Editor 中看到的是：

```
[Query Step]
├── Engine: [DuckDB ▼]     ← 下拉選擇
├── SQL: SELECT ...         ← Monaco (SQL mode)
└── Sources:                ← 可加多個
    ├── S3 Parquet: s3://bucket/data/*.parquet
    └── Previous Step: results.step_a
```

### 4.2 OpenTelemetry — 完整鏈路

Phase 1 已建好基礎，Phase 4 加強：

```rust
// 每個 flow step 都有 span，parent-child 關係完整
//
// Jaeger 上看到的 trace 結構：
//
// flow_job (root span)
// ├── step_a (child span) ── python execution
// ├── step_b (child span) ── python execution
// │   └── retry_1 (child span)
// └── step_c (child span) ── duckdb query
//     └── s3_upload (child span)
```

### 4.3 TypeScript（Bun runtime）

```rust
// crates/worker/src/typescript.rs

pub async fn handle_ts_job(
    job: &QueuedJob,
    db: &PgPool,
    content: &str,
    job_dir: &str,
    sandbox: &dyn Sandbox,
) -> Result<(serde_json::Value, i64)> {
    // 寫入使用者程式碼
    write_file(job_dir, "main.ts", content)?;

    // Bun wrapper
    let wrapper = r#"
import { main } from "./main.ts";
const args = JSON.parse(await Bun.file("args.json").text());
try {
    const result = await main(...Object.values(args));
    await Bun.write("result.json", JSON.stringify(result));
} catch (e) {
    await Bun.write("result.json", JSON.stringify({
        error: { message: e.message, name: e.name }
    }));
    process.exit(1);
}
"#;
    write_file(job_dir, "wrapper.ts", wrapper)?;
    create_args_and_out_file(job, job_dir).await?;

    let ctx = SandboxContext {
        job_id: job.id,
        job_dir: job_dir.to_string(),
        command: "bun".to_string(),
        args: vec!["run".to_string(), "wrapper.ts".to_string()],
        env: get_reserved_variables(job),
        timeout_secs: job.timeout.unwrap_or(3600) as u32,
        language: ScriptLang::TypeScript,
        custom_image: job.custom_image.clone(),
        trace_context: get_current_trace_context(),
    };

    let result = sandbox.execute(&ctx).await?;
    if result.exit_code != 0 {
        return Err(Error::ExecutionErr(result.stderr));
    }
    read_result(job_dir).await.map(|v| (v, result.memory_peak_bytes as i64))
}
```

### 4.4 Dedicated Worker（消除冷啟動）

每個 job 都 spawn 新的 Python/Bun 子程序，冷啟動佔比高達 75-98%。Dedicated Worker 保持語言 runtime 常駐：

```
正常模式（每個 job）：
  spawn python3 → import → main() → exit     冷啟動 ~60ms + 執行 ~5ms = 65ms

Dedicated Worker：
  python3 常駐 → loop { 收 job → main() }    冷啟動 0ms + 執行 ~5ms = 5ms
                                              → 13x 加速
```

**Windmill 實測：40 個輕量 Python task，Dedicated 2.09s vs Normal 4.38s（2.1x 提升）**

```rust
// crates/worker/src/dedicated.rs

use tokio::sync::mpsc;

/// 常駐語言 runtime — 一個長期存活的子程序，透過 stdin/stdout 收發 job
pub struct DedicatedRunner {
    /// 傳送 job 給常駐子程序
    tx: mpsc::Sender<DedicatedJob>,
    /// 子程序 handle
    handle: tokio::task::JoinHandle<()>,
    /// 語言
    language: ScriptLang,
    /// 綁定的 script path（同一 path 的 job 共用同一個 runner）
    script_path: String,
}

struct DedicatedJob {
    args: serde_json::Value,
    result_tx: tokio::sync::oneshot::Sender<Result<serde_json::Value>>,
}

impl DedicatedRunner {
    pub async fn spawn(
        language: ScriptLang,
        script_path: &str,
        script_content: &str,
        job_dir: &str,
        sandbox: &dyn Sandbox,
    ) -> Result<Self> {
        let (tx, mut rx) = mpsc::channel::<DedicatedJob>(64);

        // 寫入 runner script（常駐迴圈）
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

        // 用 sandbox 啟動常駐子程序
        // ... spawn 邏輯 ...

        let handle = tokio::spawn(async move {
            while let Some(job) = rx.recv().await {
                // 寫 args 到 stdin
                // 讀 result 從 stdout
                // 透過 oneshot channel 回傳
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
```

### 4.5 內建節點類型系統（學習 Kestra）

Windmill 的步驟只有 Script 和 RawScript。Kestra 有豐富的內建節點（Log、HTTP、Switch 等）。
我們用**型別化的 FlowModuleValue enum**，而非 Kestra 的 Java 反射，保持 Rust 的型別安全：

```rust
// 擴展 FlowModuleValue — 加入內建節點類型
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type")]
pub enum FlowModuleValue {
    // === 原有的 ===
    Script { path, hash, input_transforms, tag_override },
    RawScript { content, language, input_transforms, tag_override },
    ForloopFlow { iterator, modules, parallel, skip_failures },
    BranchOne { branches, default },
    BranchAll { branches, parallel },
    Identity,
    Query { engine, query, sources, input_transforms },

    // === 新增：內建節點 ===

    /// 日誌節點 — 打印訊息到 job log（不需要寫程式碼）
    Log {
        message: InputTransform,     // 支援 JS 表達式，例如 "results.a.count = ${results.a.count}"
        level: LogLevel,             // Debug, Info, Warn, Error
    },

    /// HTTP 請求節點 — 發送 HTTP 請求（不需要寫 requests 程式碼）
    HttpRequest {
        url: InputTransform,
        method: HttpMethod,          // GET, POST, PUT, DELETE, PATCH
        headers: HashMap<String, InputTransform>,
        body: Option<InputTransform>,
        timeout_secs: Option<u32>,
        // 結果自動存入 results.{step_id} = { status, headers, body }
    },

    /// 延遲節點 — 等待一段時間
    Sleep {
        duration: InputTransform,    // 秒數，支援 JS 表達式
    },

    /// 條件閘道 — 只檢查條件，不執行任何東西
    /// 條件為 false 時整個 flow 提前結束或跳到 failure_module
    Assert {
        expr: String,                // JS 布林表達式
        error_message: Option<String>,
    },

    /// 變數設定 — 在 results 中設定一個值（方便後續步驟引用）
    SetVariable {
        key: String,
        value: InputTransform,
    },

    /// 自訂節點（使用者定義的 plugin）
    Custom {
        /// 節點類型 ID（對應 custom_node_type 表）
        node_type_id: String,
        /// 參數
        input_transforms: HashMap<String, InputTransform>,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum LogLevel { Debug, Info, Warn, Error }

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum HttpMethod { GET, POST, PUT, DELETE, PATCH, HEAD }
```

**Worker 執行這些內建節點時不需要 spawn 子程序**，直接在 Rust 中處理：

```rust
// crates/worker/src/builtin_nodes.rs

pub async fn handle_builtin_node(
    module: &FlowModuleValue,
    status: &FlowStatus,
    db: &PgPool,
    job_id: Uuid,
) -> Result<serde_json::Value> {
    match module {
        FlowModuleValue::Log { message, level } => {
            let msg = eval_input_transform(message, status)?;
            let msg_str = msg.as_str().unwrap_or(&msg.to_string());
            // 寫入 job_log 表
            append_logs(job_id, &format!("[{:?}] {}", level, msg_str), db).await?;
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
            let secs = eval_input_transform(duration, status)?
                .as_f64().unwrap_or(1.0);
            tokio::time::sleep(std::time::Duration::from_secs_f64(secs)).await;
            Ok(serde_json::Value::Null)
        }
        FlowModuleValue::SetVariable { key, value } => {
            let val = eval_input_transform(value, status)?;
            Ok(val) // 存入 results.{step_id}，後續步驟用 results.{step_id} 引用
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

### 4.6 自訂節點系統（User-defined Node Types）

使用者可以把常用的 Script 包裝成可重用的節點類型：

```sql
-- 自訂節點類型表
CREATE TABLE custom_node_type (
    id VARCHAR(100) NOT NULL,        -- "my_team/slack_notify"
    workspace_id VARCHAR(50) NOT NULL REFERENCES workspace(id),
    name VARCHAR(255) NOT NULL,      -- "Slack 通知"
    description TEXT,
    icon VARCHAR(50),                -- emoji 或 icon name
    color VARCHAR(7),                -- "#FF6B6B"
    -- 底層實作：指向一個 Script
    script_path VARCHAR(255) NOT NULL,
    -- 參數 schema（JSON Schema），定義前端表單
    input_schema JSONB NOT NULL,
    -- 輸出 schema
    output_schema JSONB,
    created_by VARCHAR(255) NOT NULL,
    PRIMARY KEY (workspace_id, id)
);
```

```rust
// 執行自訂節點 = 執行底層 Script + 參數映射
FlowModuleValue::Custom { node_type_id, input_transforms } => {
    // 1. 查詢 custom_node_type 表
    let node_type = get_custom_node_type(db, workspace_id, node_type_id).await?;
    // 2. 求值 input_transforms
    let args = transform_inputs(input_transforms, status, db).await?;
    // 3. 推入 child job（執行底層 script）
    let child_id = queue::push_job(db, PushJobArgs {
        kind: JobKind::Script,
        script_path: Some(&node_type.script_path),
        args: Some(args),
        ..Default::default()
    }).await?;
    // ...
}
```

**前端 Flow Editor 中的自訂節點：**

```
步驟庫（左側面板）：
├── 內建
│   ├── Script (Python/TS/Bash)
│   ├── Log
│   ├── HTTP Request
│   ├── Sleep
│   ├── Assert
│   ├── Set Variable
│   ├── For Loop
│   └── Branch
├── Data Engine
│   ├── DuckDB Query
│   └── DataFusion Query
└── 自訂（使用者建立的）
    ├── 🔔 Slack 通知        ← 底層是 Python script
    ├── 📧 Send Email        ← 底層是 Python script
    └── 🗄️ S3 Upload        ← 底層是 Python script
```

### 4.7 Flow 匯出 / 匯入（YAML + JSON）

讓 Flow 可以被分享、版本控制、複製：

```rust
// crates/api/src/flows.rs

/// 匯出 Flow 為 YAML（人類可讀，適合 Git 版本控制）
pub async fn export_flow_yaml(
    Path((workspace_id, path)): Path<(String, String)>,
) -> Result<String, ApiError> {
    let flow = get_flow(db, &workspace_id, &path).await?;
    let export = FlowExport {
        version: "1.0".to_string(),
        path: flow.path,
        summary: flow.summary,
        description: flow.description,
        value: flow.value,
        schema: flow.schema,
    };
    Ok(serde_yaml::to_string(&export)?)
}

/// 匯入 Flow（從 YAML 或 JSON）
pub async fn import_flow(
    Json(req): Json<ImportFlowRequest>,  // { format: "yaml"|"json", content: "..." }
) -> Result<Json<FlowCreated>, ApiError> {
    let export: FlowExport = match req.format.as_str() {
        "yaml" => serde_yaml::from_str(&req.content)?,
        "json" => serde_json::from_str(&req.content)?,
        _ => return Err(ApiError::BadRequest("format must be yaml or json")),
    };
    // 建立 flow...
    Ok(Json(FlowCreated { path: export.path }))
}
```

**匯出的 YAML 長這樣（學習 Kestra 的清晰格式）：**

```yaml
version: "1.0"
path: "f/data-team/daily_etl"
summary: "Daily ETL Pipeline"

value:
  modules:
    - id: a
      summary: "Extract from API"
      value:
        type: HttpRequest
        url:
          type: Static
          value: "https://api.example.com/data"
        method: GET
        headers:
          Authorization:
            type: Static
            value: "Bearer ${secrets.API_TOKEN}"

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
          data:
            type: Javascript
            expr: "results.a.body"

    - id: c
      summary: "Load to DuckDB"
      value:
        type: Query
        engine: duckdb
        query: |
          INSERT INTO analytics.daily_users
          SELECT * FROM source_0
        sources:
          - type: PreviousStepResult
            step_id: b

    - id: d
      summary: "Notify Slack"
      value:
        type: Custom
        node_type_id: "my_team/slack_notify"
        input_transforms:
          channel:
            type: Static
            value: "#data-alerts"
          message:
            type: Javascript
            expr: "'ETL completed: ' + results.c.length + ' rows processed'"

  failure_module:
    id: error_handler
    value:
      type: Log
      message:
        type: Javascript
        expr: "'ETL failed: ' + JSON.stringify(error)"
      level: Error

schema:
  properties: {}
```

**API：**

```
GET  /api/w/{ws}/flows/export/p/{path}?format=yaml   → YAML 字串
GET  /api/w/{ws}/flows/export/p/{path}?format=json   → JSON
POST /api/w/{ws}/flows/import                         → 匯入
```

### 4.8 Flow Editor 內嵌程式碼編輯器（學習 Kestra）

**需求**：在 Flow Editor 頁面中，使用者可以直接點擊 RawScript 步驟，在旁邊的 tab 中編輯 Python 程式碼，就像 VS Code 的分割畫面。

**學習對象**：Kestra 的 `io.kestra.plugin.scripts.python.Commands` 允許使用者在 YAML 中寫 inline script，前端有完整的編輯體驗。

```
┌──────────────────────────────────────────────────────────────────┐
│ Flow Editor                                                [Save]│
├──────────────┬───────────────────────────────────────────────────┤
│              │ Tabs: [Flow ▾] [step_b.py] [step_d.py]           │
│  步驟庫      │                                                   │
│  ─────       │  ┌─ Monaco Editor ──────────────────────────┐    │
│  + Script    │  │ import requests                           │    │
│  + Log       │  │                                           │    │
│  + HTTP      │  │ def main(url: str, api_key: str):         │    │
│  + Sleep     │  │     resp = requests.get(url,               │    │
│  + Query     │  │         headers={"Authorization": api_key})│    │
│              │  │     return resp.json()                     │    │
│  自訂節點    │  │                                           │    │
│  ─────       │  └───────────────────────────────────────────┘    │
│  🔔 Slack    │                                                   │
│  📧 Email   │  ┌─ Input Transforms ────────────────────────┐    │
│              │  │ url:  results.step_a.endpoint    [JS ▾]   │    │
│              │  │ api_key: secrets.API_KEY          [Static] │    │
│              │  └───────────────────────────────────────────┘    │
├──────────────┤                                                   │
│              │  ┌─ Test Run ───────────────────────────────┐    │
│  DAG 畫布    │  │ [▶ Run This Step]  [▶ Run From Here]     │    │
│  ─────       │  │                                           │    │
│  ┌──┐        │  │ stdout: {"users": [{"id": 1, ...}]}      │    │
│  │ A│→┐      │  └───────────────────────────────────────────┘    │
│  └──┘ │      │                                                   │
│       ▼      │                                                   │
│  ┌──────┐    │                                                   │
│  │B(py) │ ◀──── 目前選中，右側顯示程式碼                         │
│  └──┬───┘    │                                                   │
│     │        │                                                   │
│     ▼        │                                                   │
│  ┌──┐        │                                                   │
│  │ C│        │                                                   │
│  └──┘        │                                                   │
└──────────────┴───────────────────────────────────────────────────┘
```

**關鍵 UX**：
1. 點擊 DAG 上的 RawScript 節點 → 自動在 tab 中開啟該步驟的程式碼
2. 程式碼修改即時同步到 FlowValue
3. 可以單獨測試一個步驟（`Run This Step`），mock 上游步驟的 results
4. 可以從某個步驟開始跑（`Run From Here`），使用上游已完成的結果

```svelte
<!-- src/lib/components/FlowEditorWithCode.svelte -->
<script lang="ts">
  import FlowEditor from './FlowEditor.svelte'
  import ScriptEditor from './ScriptEditor.svelte'

  let { flowValue = $bindable() }: { flowValue: FlowValue } = $props()

  // 目前開啟的程式碼 tab
  let openTabs = $state<{ id: string; name: string }[]>([])
  let activeTabId = $state<string | null>(null)

  // 從 DAG 點選 RawScript 節點
  function onNodeSelect(moduleId: string) {
    const mod = flowValue.modules.find(m => m.id === moduleId)
    if (!mod) return

    if (mod.value.type === 'RawScript') {
      const tabId = moduleId
      if (!openTabs.find(t => t.id === tabId)) {
        const ext = mod.value.language === 'python3' ? '.py' : '.ts'
        openTabs = [...openTabs, { id: tabId, name: `${moduleId}${ext}` }]
      }
      activeTabId = tabId
    }
  }

  // 程式碼修改 → 同步回 FlowValue
  function onCodeChange(moduleId: string, newContent: string) {
    flowValue.modules = flowValue.modules.map(m => {
      if (m.id === moduleId && m.value.type === 'RawScript') {
        return { ...m, value: { ...m.value, content: newContent } }
      }
      return m
    })
  }

  let activeModule = $derived(
    activeTabId
      ? flowValue.modules.find(m => m.id === activeTabId)
      : null
  )
</script>

<div class="flow-editor-with-code">
  <!-- 左側：DAG -->
  <FlowEditor {flowValue} onNodeClick={onNodeSelect} />

  <!-- 右側：Tab 式程式碼編輯器 -->
  <div class="code-panel">
    <div class="tabs">
      <button
        class:active={activeTabId === null}
        onclick={() => activeTabId = null}
      >Flow</button>
      {#each openTabs as tab}
        <button
          class:active={activeTabId === tab.id}
          onclick={() => activeTabId = tab.id}
        >
          {tab.name}
          <span class="close" onclick|stopPropagation={() => {
            openTabs = openTabs.filter(t => t.id !== tab.id)
            if (activeTabId === tab.id) activeTabId = null
          }}>x</span>
        </button>
      {/each}
    </div>

    {#if activeModule?.value.type === 'RawScript'}
      <ScriptEditor
        content={activeModule.value.content}
        language={activeModule.value.language}
        onchange={(c) => onCodeChange(activeTabId!, c)}
      />
    {:else}
      <!-- Flow 設定面板 -->
      <p>Select a code step to edit</p>
    {/if}
  </div>
</div>
```

### 4.9 驗證方式

```
1. DataEngine: DuckDB Query step 查詢 S3 Parquet → 確認結果正確
2. OTel: 啟動 Jaeger → 跑 flow → 確認 trace 完整（parent-child span）
3. TypeScript: Bun 執行成功
4. 內建節點: Log / HTTP Request / Sleep / Assert 正常工作
5. 自訂節點: 建立 "Slack Notify" 自訂節點 → 在 flow 中使用
6. 匯出匯入: Flow → YAML → 匯入另一個 workspace → 執行成功
7. 內嵌編輯器: 點擊 RawScript 節點 → tab 開啟 → 編輯 → Run This Step
8. Dedicated Worker: 連續跑 100 個輕量 Python job → 對比正常模式延遲
9. Sandbox: tag="k8s" → K8s Pod / tag="fast" → nsjail
```

---

## 各家 Queue 機制比較（為什麼選 PostgreSQL）

| 平台 | Queue 技術 | 外部依賴 | 延遲 |
|------|-----------|---------|------|
| **Airflow** | Celery (Redis/RabbitMQ) | 需要 Redis 或 RabbitMQ | 低（push） |
| **Prefect 3** | PostgreSQL + HTTP polling | 無 | ~15s（polling） |
| **Dagster** | PostgreSQL (run queue) | 無 | daemon polling |
| **Temporal** | 內建 Matching Service (Go) | 無（但 4 個內部服務） | ~0ms（sync match） |
| **Kestra** | PostgreSQL (JDBC) 或 Kafka | 可選 Kafka | 依 backend |
| **Windmill** | PostgreSQL `FOR UPDATE SKIP LOCKED` | 無 | ~50ms（polling） |

**我們的選擇：PostgreSQL `FOR UPDATE SKIP LOCKED` + `LISTEN/NOTIFY`**

理由：
1. 已被 Windmill（5000 RPS）和 Dagster 驗證可行
2. 零外部依賴（不需要 Redis/RabbitMQ/Kafka）
3. Job 入隊和元資料在同一個事務中，不會出現「job 已分發但元資料沒寫入」
4. `LISTEN/NOTIFY` 可以把延遲從 50ms 降到個位數毫秒
5. Rust + sqlx + Tokio 天然適配

```rust
// LISTEN/NOTIFY 增強（比純 polling 快 10x）
async fn pull_with_notify(db: &PgPool, worker: &str, tags: &[String]) -> Result<Option<Job>> {
    let mut listener = sqlx::postgres::PgListener::connect_with(&db).await?;
    listener.listen("new_job").await?;

    loop {
        // 先嘗試拉取
        if let Some(job) = pull_job(db, worker, tags).await? {
            return Ok(Some(job));
        }
        // 沒有 job → 等 NOTIFY（最多 5 秒，防止漏通知）
        tokio::select! {
            _ = listener.recv() => { /* 收到通知，立即重試 */ }
            _ = tokio::time::sleep(Duration::from_secs(5)) => { /* 安全兜底 */ }
        }
    }
}

// 推入 job 時發 NOTIFY
async fn push_job_with_notify(db: &PgPool, args: PushJobArgs<'_>) -> Result<Uuid> {
    let mut tx = db.begin().await?;
    let job_id = push_job_inner(&mut tx, args).await?;
    sqlx::query("SELECT pg_notify('new_job', $1)")
        .bind(job_id.to_string())
        .execute(&mut *tx)
        .await?;
    tx.commit().await?;
    Ok(job_id)
}
```

---

## 與 Windmill 的關鍵差異總結

| 面向 | Windmill | FlowForge |
|------|---------|-----------|
| Flow Editor | 自建 SVG（5000+ 行） | `@xyflow/svelte`（現成）+ 內嵌 Monaco tab |
| JS 求值 | QuickJS + Deno（C 依賴） | `boa_engine`（純 Rust） |
| 可觀測性 | 後加 OTel | Day 1 OpenTelemetry |
| 沙箱 | nsjail only（Linux only） | **四模式**：Rust 原生 / nsjail / WASM / K8s Pod |
| Script Hash | `i64`（不易讀） | SHA256 hex（可讀） |
| Crate 數量 | 50+（複雜） | 6 核心（簡潔） |
| Job 表設計 | 單表（queue + 結果混合） | 三表分離 + `LISTEN/NOTIFY` |
| 資料處理 | DuckDB 是附加 | DataEngine trait（DuckDB / DataFusion 可插拔） |
| 內建節點 | 只有 Script | Log / HTTP / Sleep / Assert / SetVariable / Query |
| 自訂節點 | 無 | 使用者可把 Script 封裝成可重用節點 |
| Flow 分享 | 無標準格式 | YAML / JSON 匯出匯入 |
| 程式碼編輯 | Script Editor 獨立頁面 | Flow Editor 內嵌 Monaco tab（like VS Code） |
| K8s 執行 | 只有 Worker 自動擴縮 | **Job-level K8s Pod 執行** |

## 未來方向（Phase 4 之後）

- Iggy 取代 PostgreSQL queue（100K+ msg/sec）
- Event-driven CEP（時間窗口、事件關聯）
- Plugin-based trigger 框架（Webhook、Cron、Kafka、MQTT）
- App Builder（低代碼 UI）
- Dedicated Worker 進階：Runner Groups（多個 script 共用依賴時共享同一 runtime）
- Firecracker microVM 作為第五種沙箱模式（~125ms 啟動，AWS Lambda 底層技術）
- Flow 版本控制（Git-like diff + merge）
- Marketplace（分享自訂節點和 Flow 範本）
