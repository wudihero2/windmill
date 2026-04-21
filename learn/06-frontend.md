# 第六章：Svelte 前端

## 技術選型

| 層次 | Windmill 用的 | 說明 |
|------|--------------|------|
| 框架 | SvelteKit + Svelte 5 | 檔案路由 + Runes |
| 程式碼編輯 | Monaco Editor | VS Code 同款 |
| 流程圖 | 自建 (SVG + Drag) | 非 react-flow |
| UI 組件 | 自建 + Lucide icons | 非 UI library |
| 狀態管理 | Svelte stores + Context | 無 Redux/Zustand |
| API 層 | OpenAPI 自動生成 | openapi-typescript-codegen |
| 樣式 | Tailwind CSS | utility-first |
| Toast | @zerodevx/svelte-toast | 輕量級 |

## 專案結構

```
frontend/
├── src/
│   ├── routes/                    # SvelteKit 檔案路由
│   │   ├── (root)/                # 主應用（需要登入）
│   │   │   ├── +layout.svelte     # 主佈局（sidebar + header）
│   │   │   ├── flows/
│   │   │   │   ├── add/+page.svelte
│   │   │   │   ├── edit/[...path]/+page.svelte
│   │   │   │   └── get/[...path]/+page.svelte
│   │   │   ├── scripts/
│   │   │   │   ├── add/+page.svelte
│   │   │   │   └── edit/[...path]/+page.svelte
│   │   │   ├── apps/
│   │   │   ├── runs/              # Job 執行紀錄
│   │   │   ├── schedules/         # 排程管理
│   │   │   ├── resources/         # 外部資源
│   │   │   └── variables/         # 變數管理
│   │   └── user/                  # 認證頁面
│   │       ├── login/+page.svelte
│   │       └── signup/+page.svelte
│   ├── lib/
│   │   ├── gen/                   # OpenAPI 自動生成的 API client
│   │   │   ├── services/
│   │   │   │   ├── ScriptService.ts
│   │   │   │   ├── FlowService.ts
│   │   │   │   ├── JobService.ts
│   │   │   │   └── ...
│   │   │   └── models/
│   │   ├── components/            # 1436 個 Svelte 組件
│   │   │   ├── flows/             # Flow 編輯器
│   │   │   ├── apps/              # App Builder
│   │   │   ├── scripts/           # Script 編輯器
│   │   │   ├── common/            # 共用組件
│   │   │   └── runs/              # Job 執行 UI
│   │   ├── stores/                # 全域 stores
│   │   ├── utils.ts
│   │   └── toast.ts
│   └── app.html
├── static/
├── svelte.config.js
├── vite.config.ts
└── package.json
```

## API Client 自動生成

Windmill 從後端的 OpenAPI spec 自動生成前端 API client：

```bash
# 後端生成 OpenAPI spec
cd backend && cargo run --bin openapi

# 前端用 openapi-typescript-codegen 生成 client
npx openapi-typescript-codegen \
  --input ../backend/windmill-api/openapi.yaml \
  --output src/lib/gen \
  --client fetch
```

生成的 client 使用起來：

```typescript
import { ScriptService, FlowService, JobService } from '$lib/gen'

// 取得腳本
const script = await ScriptService.getScriptByPath({
    workspace: 'my-workspace',
    path: 'u/admin/hello'
})

// 執行腳本
const jobId = await JobService.runScriptByPath({
    workspace: 'my-workspace',
    path: 'u/admin/hello',
    requestBody: { name: 'World' }
})

// 查詢 job 結果
const result = await JobService.getCompletedJob({
    workspace: 'my-workspace',
    id: jobId
})
```

## 認證狀態管理

```typescript
// src/lib/stores/user.ts
import { writable } from 'svelte/store'

export interface UserInfo {
    email: string
    username: string
    is_admin: boolean
    workspace_id: string
}

export const userStore = writable<UserInfo | null>(null)
export const workspaceStore = writable<string>('')

// 登入
export async function login(email: string, password: string) {
    const response = await fetch('/api/auth/login', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ email, password }),
    })

    if (response.ok) {
        const { token } = await response.json()
        // 儲存 token 到 cookie
        document.cookie = `token=${token}; path=/; max-age=86400`
        // 取得使用者資訊
        await loadUser()
    }
}
```

## Script Editor

### Monaco Editor 整合

```svelte
<!-- src/lib/components/scripts/ScriptEditor.svelte -->
<script lang="ts">
    import { onMount } from 'svelte'
    import * as monaco from 'monaco-editor'

    let { content = $bindable(), language }: {
        content: string
        language: string
    } = $props()

    let editorContainer: HTMLDivElement
    let editor: monaco.editor.IStandaloneCodeEditor

    // 語言對應
    const LANG_MAP: Record<string, string> = {
        python3: 'python',
        deno: 'typescript',
        bun: 'typescript',
        nativets: 'typescript',
        go: 'go',
        bash: 'shell',
        postgresql: 'sql',
        rust: 'rust',
    }

    onMount(() => {
        editor = monaco.editor.create(editorContainer, {
            value: content,
            language: LANG_MAP[language] ?? 'plaintext',
            theme: 'vs-dark',
            minimap: { enabled: false },
            automaticLayout: true,
            fontSize: 14,
            tabSize: 4,
        })

        // 同步到 parent
        editor.onDidChangeModelContent(() => {
            content = editor.getValue()
        })

        return () => editor.dispose()
    })
</script>

<div bind:this={editorContainer} class="h-full w-full" />
```

### Script 頁面

```svelte
<!-- src/routes/(root)/scripts/edit/[...path]/+page.svelte -->
<script lang="ts">
    import { page } from '$app/stores'
    import { ScriptService, JobService } from '$lib/gen'
    import ScriptEditor from '$lib/components/scripts/ScriptEditor.svelte'
    import { workspaceStore } from '$lib/stores/user'
    import { sendUserToast } from '$lib/toast'

    let script = $state<any>(null)
    let content = $state('')
    let language = $state('python3')
    let testResult = $state<any>(null)
    let running = $state(false)

    // 載入腳本
    $effect(() => {
        const path = $page.params.path
        ScriptService.getScriptByPath({
            workspace: $workspaceStore,
            path,
        }).then(s => {
            script = s
            content = s.content
            language = s.language
        })
    })

    // 儲存
    async function save() {
        await ScriptService.createScript({
            workspace: $workspaceStore,
            requestBody: {
                path: script.path,
                content,
                language,
                parent_hash: script.hash,
                summary: script.summary,
            }
        })
        sendUserToast('Script saved!')
    }

    // 測試執行
    async function testRun() {
        running = true
        try {
            // 用 RawScript 方式預覽執行
            const jobId = await JobService.runScriptPreview({
                workspace: $workspaceStore,
                requestBody: {
                    content,
                    language,
                    args: {},  // 測試參數
                }
            })

            // 等待結果
            await pollJobResult(jobId)
        } finally {
            running = false
        }
    }

    async function pollJobResult(jobId: string) {
        while (true) {
            try {
                const job = await JobService.getCompletedJob({
                    workspace: $workspaceStore,
                    id: jobId,
                })
                testResult = job.result
                return
            } catch {
                // Job 還沒完成，等一下再查
                await new Promise(r => setTimeout(r, 500))
            }
        }
    }
</script>

<div class="flex h-screen">
    <!-- 左側：程式碼編輯器 -->
    <div class="flex-1">
        <ScriptEditor bind:content {language} />
    </div>

    <!-- 右側：控制面板 -->
    <div class="w-96 border-l p-4">
        <div class="flex gap-2 mb-4">
            <button onclick={save} class="btn btn-primary">Save</button>
            <button onclick={testRun} disabled={running} class="btn">
                {running ? 'Running...' : 'Test'}
            </button>
        </div>

        {#if testResult}
            <pre class="bg-gray-100 p-4 rounded overflow-auto">
                {JSON.stringify(testResult, null, 2)}
            </pre>
        {/if}
    </div>
</div>
```

## Flow Editor（重點！）

Flow Editor 是最複雜的前端組件。Windmill 用自建的 SVG + Drag 系統：

### 架構

```
FlowEditor.svelte
├── FlowGraph.svelte          # SVG 流程圖（拖拉、連線）
│   ├── FlowNode.svelte       # 單個節點
│   ├── FlowEdge.svelte       # 連線
│   └── FlowControls.svelte   # 縮放/平移控制
├── FlowModuleEditor.svelte   # 右側面板：編輯步驟
│   ├── ScriptPicker.svelte   # 選擇腳本
│   ├── InputTransformEditor  # 輸入映射編輯器
│   ├── RetryEditor.svelte    # 重試設定
│   └── BranchEditor.svelte   # 分支條件編輯
└── FlowPreview.svelte        # 預覽/執行面板
```

### Flow Graph 的簡化實作

```svelte
<!-- FlowGraph.svelte（概念版） -->
<script lang="ts">
    import type { FlowValue, FlowModule } from '$lib/gen'

    let { flow = $bindable(), selectedModule = $bindable() }: {
        flow: FlowValue
        selectedModule: string | null
    } = $props()

    // 計算節點位置
    let nodes = $derived(
        flow.modules.map((module, i) => ({
            id: module.id,
            x: 200,
            y: i * 120 + 50,
            width: 200,
            height: 60,
            module,
        }))
    )

    // 計算連線
    let edges = $derived(
        nodes.slice(0, -1).map((node, i) => ({
            from: node.id,
            to: nodes[i + 1].id,
            fromY: node.y + node.height,
            toY: nodes[i + 1].y,
            x: node.x + node.width / 2,
        }))
    )

    // 拖拉
    let dragging = $state<string | null>(null)
    let dragOffset = $state({ x: 0, y: 0 })

    function onMouseDown(nodeId: string, e: MouseEvent) {
        dragging = nodeId
        const node = nodes.find(n => n.id === nodeId)!
        dragOffset = { x: e.clientX - node.x, y: e.clientY - node.y }
    }

    function onMouseMove(e: MouseEvent) {
        if (!dragging) return
        // 更新節點位置...
    }

    // 新增步驟
    function addModule(afterId: string, type: string) {
        const newId = generateId()
        const idx = flow.modules.findIndex(m => m.id === afterId)
        flow.modules.splice(idx + 1, 0, {
            id: newId,
            value: createDefaultModuleValue(type),
            summary: '',
        })
        flow = flow  // 觸發 reactivity
        selectedModule = newId
    }
</script>

<svg class="w-full h-full" onmousemove={onMouseMove} onmouseup={() => dragging = null}>
    <!-- 連線 -->
    {#each edges as edge}
        <line
            x1={edge.x} y1={edge.fromY}
            x2={edge.x} y2={edge.toY}
            stroke="#94a3b8" stroke-width="2"
            marker-end="url(#arrow)"
        />
    {/each}

    <!-- 節點 -->
    {#each nodes as node}
        <g
            transform="translate({node.x}, {node.y})"
            onmousedown={(e) => onMouseDown(node.id, e)}
            onclick={() => selectedModule = node.id}
            class="cursor-pointer"
        >
            <rect
                width={node.width} height={node.height}
                rx="8" ry="8"
                fill={selectedModule === node.id ? '#3b82f6' : '#f1f5f9'}
                stroke={selectedModule === node.id ? '#2563eb' : '#cbd5e1'}
                stroke-width="2"
            />
            <text x="100" y="25" text-anchor="middle" font-size="12" fill="#1e293b">
                {node.module.summary || node.module.id}
            </text>
            <text x="100" y="45" text-anchor="middle" font-size="10" fill="#64748b">
                {getModuleType(node.module)}
            </text>
        </g>
    {/each}

    <!-- 新增按鈕（節點之間） -->
    {#each edges as edge, i}
        <g
            transform="translate({edge.x - 12}, {(edge.fromY + edge.toY) / 2 - 12})"
            onclick={() => addModule(nodes[i].id, 'rawscript')}
            class="cursor-pointer opacity-0 hover:opacity-100 transition-opacity"
        >
            <circle cx="12" cy="12" r="12" fill="#3b82f6" />
            <text x="12" y="16" text-anchor="middle" fill="white" font-size="16">+</text>
        </g>
    {/each}
</svg>
```

### Input Transform Editor

```svelte
<!-- InputTransformEditor.svelte -->
<script lang="ts">
    import type { InputTransform } from '$lib/gen'

    let { transforms = $bindable(), previousStepIds }: {
        transforms: Record<string, InputTransform>
        previousStepIds: string[]
    } = $props()
</script>

<div class="space-y-4">
    {#each Object.entries(transforms) as [key, transform]}
        <div class="border rounded p-3">
            <div class="flex items-center justify-between mb-2">
                <label class="font-medium text-sm">{key}</label>
                <select
                    value={transform.type}
                    onchange={(e) => {
                        transforms[key] = e.target.value === 'static'
                            ? { type: 'static', value: '' }
                            : { type: 'javascript', expr: '' }
                        transforms = transforms
                    }}
                >
                    <option value="static">Static</option>
                    <option value="javascript">Expression</option>
                </select>
            </div>

            {#if transform.type === 'static'}
                <input
                    type="text"
                    value={JSON.stringify(transform.value)}
                    onchange={(e) => {
                        transforms[key] = {
                            type: 'static',
                            value: JSON.parse(e.target.value)
                        }
                        transforms = transforms
                    }}
                    class="w-full border rounded px-2 py-1"
                />
            {:else}
                <textarea
                    value={transform.expr}
                    oninput={(e) => {
                        transforms[key] = {
                            type: 'javascript',
                            expr: e.target.value
                        }
                        transforms = transforms
                    }}
                    placeholder="results.a.value"
                    class="w-full border rounded px-2 py-1 font-mono text-sm"
                    rows="2"
                />
                <p class="text-xs text-gray-500 mt-1">
                    Available: {previousStepIds.map(id => `results.${id}`).join(', ')}, flow_input, previous_result
                </p>
            {/if}
        </div>
    {/each}
</div>
```

## 即時 Job 監控（SSE）

```typescript
// src/lib/services/jobStream.ts
export function streamJobLogs(
    workspace: string,
    jobId: string,
    onLog: (log: string) => void,
    onResult: (result: any) => void,
    onError: (error: string) => void,
) {
    const eventSource = new EventSource(
        `/api/w/${workspace}/jobs/stream/${jobId}`
    )

    eventSource.addEventListener('log', (e) => {
        onLog(e.data)
    })

    eventSource.addEventListener('result', (e) => {
        const result = JSON.parse(e.data)
        onResult(result)
        eventSource.close()
    })

    eventSource.addEventListener('error', (e) => {
        onError('Connection lost')
        eventSource.close()
    })

    return () => eventSource.close()
}
```

### Job Run Viewer

```svelte
<!-- JobViewer.svelte -->
<script lang="ts">
    import { streamJobLogs } from '$lib/services/jobStream'
    import { workspaceStore } from '$lib/stores/user'

    let { jobId }: { jobId: string } = $props()

    let logs = $state('')
    let result = $state<any>(null)
    let status = $state<'running' | 'success' | 'failure'>('running')

    $effect(() => {
        const cleanup = streamJobLogs(
            $workspaceStore,
            jobId,
            (log) => { logs += log },
            (r) => {
                result = r
                status = r.error ? 'failure' : 'success'
            },
            (err) => { status = 'failure' },
        )
        return cleanup
    })
</script>

<div class="flex flex-col h-full">
    <!-- 狀態指示 -->
    <div class="flex items-center gap-2 p-3 border-b">
        {#if status === 'running'}
            <div class="animate-spin h-4 w-4 border-2 border-blue-500 border-t-transparent rounded-full" />
            <span>Running...</span>
        {:else if status === 'success'}
            <span class="text-green-600">Success</span>
        {:else}
            <span class="text-red-600">Failed</span>
        {/if}
    </div>

    <!-- 日誌 -->
    <div class="flex-1 overflow-auto bg-gray-900 text-gray-100 p-4 font-mono text-sm">
        <pre>{logs}</pre>
    </div>

    <!-- 結果 -->
    {#if result}
        <div class="border-t p-4">
            <h3 class="font-medium mb-2">Result</h3>
            <pre class="bg-gray-100 p-3 rounded overflow-auto text-sm">
                {JSON.stringify(result, null, 2)}
            </pre>
        </div>
    {/if}
</div>
```

## Context API 使用模式

Windmill 大量使用 Svelte Context（723 次 setContext/getContext）：

```svelte
<!-- 父組件設定 context -->
<script lang="ts">
    import { setContext } from 'svelte'

    // Flow Editor Context
    setContext('FlowEditorContext', {
        flow: $state(flowValue),
        selectedModule: $state(null),
        addModule: (afterId, type) => { ... },
        removeModule: (id) => { ... },
        updateModule: (id, updates) => { ... },
    })
</script>

<!-- 子組件取用 context -->
<script lang="ts">
    import { getContext } from 'svelte'

    const { flow, selectedModule, updateModule } = getContext('FlowEditorContext')
</script>
```

## 你的前端實作順序

1. **SvelteKit scaffold** — 路由 + 登入頁
2. **API client** — 先手寫，後面再用 OpenAPI 生成
3. **Script 列表 + 編輯器** — Monaco Editor 整合
4. **Job 執行 + 日誌** — SSE 即時串流
5. **Flow 列表** — CRUD
6. **Flow Editor** — SVG 流程圖（最難的部分）
7. **Input Transform Editor** — 步驟間資料映射
8. **Schedule UI** — Cron 表達式 + 排程管理
9. **App Builder** — 如果有需要
