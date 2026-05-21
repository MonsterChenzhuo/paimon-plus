# Native IO 诊断 UI 与 API 重构设计（AI 友好）

## 文档关系

本文档对 `2026-05-21-native-io-spark-ui-diagnostics-design.md`（以下简称 "v1 spec"）做 v2 改造：

- **保留**：v1 spec 的 `NativeIOEvent` 字段定义（version = 1）、Spark History Server 回放通道、`NativeRejectReason` 类型枚举。
- **supersede**：v1 spec 的 UI 章节（8 page → 3 page）、Spark Listener 章节（增加 SQL/Stage 生命周期与延迟补齐）、配置章节（v2 新增 29 项参数，详见配置节）。
- **新增**：诊断 REST API、瓶颈判定规则引擎、采样与 batching 性能策略、AI-friendly 字段集。

## 目标

线上 SQL 命中 Native IO 时，运维和 AI 都能在不抓 thread dump、不读 driver log 的前提下回答以下 5 个问题：

1. 这条 SQL 是否真的走了 native fast path？没走的原因是什么？
2. 当前的瓶颈是哪个 phase？证据是什么？怎么调？
3. 当前的并发度是多少？是 CPU 不饱和还是已经堆满？
4. 当前在做什么（哪个文件、哪个 OBS request、哪个 phase）？
5. 上一次同类 SQL 跑了多久，瓶颈是什么？这次和上次比变快还是变慢？

所有这些问题，AI 必须能通过单一 REST 接口拿到结构化答案，不需要解析 HTML、不需要 grep 日志。

## 非目标

- 不替换 Spark 自身的 SQL、stage、executor 页面。
- 不引入外部 metrics 服务或独立 HTTP server，复用 SparkUI 内嵌 Jetty。
- 不在 Spark 3.4.4 之外的版本上验证。
- 不实现可视化图表前端组件库，stacked bar 等用 ASCII 字符渲染。
- 不在 Java fallback 路径上做完整诊断；fallback 只需要透出 reject reason 与可操作建议。

## 总体架构

数据从三路 producer 汇聚到 driver 端 `NativeIOStore`，UI 和 API 共享同一份数据模型：

```
Producers (events)
  ① Rust native (JNR callback)
  ② Java boundary (PaimonNativeExporter / ExportParquetProcedure)
  ③ Spark Listener (SQL / Stage / Task lifecycle)
       │
       ▼
NativeIOStore (driver, in-memory)
  sqls: LinkedHashMap[sqlId, SqlExecutionState]
  ├── operations / phases / objects / tasks / files / events
  └── BottleneckRuleEngine (11 rules)
       │
       ├──────────────┬──────────────┐
       ▼              ▼              ▼
   NativeIOTab    NativeIOApi   SparkListenerNativeIOEvent
   (3 pages)     (4 endpoints)  → spark.eventLog → History Server replay
```

executor 上的 producer 先经 `NativeIODiagnosticsBatcher`（v2 新增，ring buffer + flusher）做本地批处理，再通过现有 `NativeIORpcSupport` 把 batch 发到 driver endpoint `paimon-native-io-diagnostics`。详见性能节。

## 数据模型

### Store 顶层结构

`NativeIOStore` 由 operation 平铺改为以 `sqlId` 为顶层 key，与 Spark SQL execution id 对齐：

```
sqls: LinkedHashMap[Long, SqlExecutionState]  // sql_id 用 Spark SQL execution id (Long)

SqlExecutionState:
  sql_id, description, submitter, mode, status, start_time
  operations: List[OperationState]
  phases: Map[PhaseId, PhaseAggregate]
  objects: ObjectStorageCounters
  tasks: Map[TaskAttemptId, TaskState]
  files: Map[FilePath, FileState]
  events: BoundedRingBuffer[NativeIOEvent]
  bottleneck: BottleneckVerdict
  diagnostics_health: DiagnosticsHealth
```

`completed` 队列上限由 `spark.paimon.native-io.ui.max-completed-sqls` 控制（默认 200）。

### REST API

所有 endpoint 挂在 SparkUI 的 Jetty 上，路径前缀 `/native-io/api/`，content-type `application/json`，沿用 Spark UI ACL。

#### `GET /native-io/api/session`

会话级元信息，AI 用来确认采集链路正常：

```json
{
  "app_id": "application_...",
  "app_name": "...",
  "start_time": "ISO8601",
  "native_io": {
    "enabled": true,
    "jar_version": "1.4-SNAPSHOT",
    "jar_commit": "abc1234",
    "native_lib_loaded": true,
    "native_lib_version": "0.3.1",
    "callbacks_active": true
  },
  "config": { "ui.enabled": true, "ui.max-completed-sqls": 200, ... },
  "counters": {
    "sqls_total": 142,
    "sqls_native": 98, "sqls_fallback": 42, "sqls_failed": 2,
    "sqls_stuck_now": 0
  },
  "diagnostics": {
    "rpc_errors": 0, "emit_errors": 0,
    "driver_buffer_usage": 0.12,
    "throttle_events_last_5m": 0
  }
}
```

`callbacks_active` 用于解决"装的 jar 没带最新 callback 入口"的现场问题（commit `d9f62f9` 暗示过的故障模式）。

#### `GET /native-io/api/summary`

list endpoint，AI 第一跳。支持 query param：`status=running|stuck|completed|failed`、`mode=native|fallback`、`limit=N`（默认 50）。

每个元素字段：

- `sql_id`, `description`, `submitter`, `mode`, `status`, `start_time`, `elapsed_ms`, `last_event_age_ms`
- `progress`：`files_done/total`, `rows_read/output`, `bytes_read/written`
- `concurrency`：`tasks_active/total`, `native_threads_active/peak/max`, `multipart_queue_depth`
- `resource`：`peak_buffered_bytes`, `writer_rolls`
- `bottleneck`：`phase`（11 规则 enum + `none|warming_up|unknown|reject`）, `confidence`（0..1）, `evidence`, `hint`（中文）, `actionable[]`
- `reject`：仅当 `mode=fallback` 时存在，包含 `reason_code`, `reason_text`, `actionable`
- `stuck_at`：仅当 `status=stuck` 时存在，包含 `phase`, `file`, `task`, `hint`
- `error`：仅当 `status=failed` 时存在
- `diagnostics_health`：`sampled`, `sample_rate`, `events_emitted`, `events_dropped`, `capped`, `kill_switch_active`
- `links`：`detail`, `timeline`, `spark_sql_ui`

#### `GET /native-io/api/detail/{sqlId}`

drill endpoint，AI 用来验证 summary 的 `bottleneck.evidence`。返回：

- `summary`：同 list 中的单条
- `phase_breakdown`：每个 phase 的 `total_ms`, `share`, `count`，OBS 类 phase 额外暴露 `retries`, `throttle_429`, `p50_ms`, `p95_ms`；`java_overhead` 额外暴露 `breakdown.plan/commit`
- `task_breakdown`：`total/active/completed/failed`, `elapsed_p50/max_ms`, `skew_ratio`, `slowest[]`
- `file_breakdown`：`total/completed`, `slowest[]`（含 `row_groups_read/pruned`, `current_phase`）
- `obs_counters`：`read/write_requests`, `bytes_read/written`, `retries`, `4xx`, `5xx`
- `memory_samples[]`：来自 `MEMORY_SNAPSHOT` 的时间序列
- `bottleneck_candidates[]`：11 项规则全部输出，按 confidence 降序

#### `GET /native-io/api/timeline/{sqlId}`

原始事件流。支持 `since=<ts>`、`limit=N`（默认 5000，上限 20000）、`types=PHASE_END,OBJECT_REQUEST_END,...` 过滤。带 `next_cursor` 分页。

事件字段沿用 v1 spec 定义，新增的事件类型见下节。

## 事件模型扩展

在 v1 spec 的事件类型基础上新增以下事件：

| Event | Source | 触发点 | 关键字段 |
|---|---|---|---|
| `FFI_ENTERED` | Rust | `paimon_exporter_export_parquet` 入口，先于 JSON 解析 | `op_id` |
| `OBJECT_REQUEST_START` | Rust | OBS client 调用前 | `req_id`, `op`, `file` |
| `OBJECT_REQUEST_END` | Rust | OBS client 返回后 | `req_id`, `duration_ms`, `bytes`, `retries`, `status_code` |
| `MEMORY_SNAPSHOT` | Rust | 每 5s 节流 | `buffered_bytes`, `threads` |
| `PLAN_REJECT` | Java | `ExportParquetProcedure` 决定走 Java fallback 时 | `sql_id`, `reason_code`, `reason_text`, `reason_detail`, `actionable` |
| `JAVA_FALLBACK_START / END` | Java | Java fallback 路径开始/结束 | `sql_id`, `duration_ms` |
| `COMMIT_START / END` | Java | Paimon commit 前后 | `sql_id`, `duration_ms`, `bytes_committed` |
| `PLAN_START / END` | Java | Spark plan 阶段计时 | `sql_id`, `duration_ms` |

`PLAN_REJECT` 取代当前的 `LOG.warn(rejectReason)`，把 30 种 `NativeRejectReason` 上升为结构化事件。

`read_ms` 从 `decode_ms` 中拆分（v1 spec 已要求，当前实现恒 0）；`parquet_row_groups_pruned` 实际计数（当前实现恒 0）。

## UI 改造

8 page 缩减为 3 page。每 page 在底部提供 `[ JSON ▾ /api/... ]` 链接，让 AI 用 API、人用 UI。

### Summary（默认页 `/native-io`）

- **Session bar**：`enabled / jar_version / lib_version / callbacks ✓`，SQL 总数分类计数。
- **Active & Stuck 区**：sticky 表，列 `# / SQL / Mode / Status / Elapsed / Progress / Concurrency / Verdict`。Stuck 标红、Fallback 标黄，Verdict 列显示 `bottleneck.phase + share + confidence`。
- **Recent completed**：最近 50 条完成的 SQL，支持按 mode 过滤。
- **`Auto-refresh 5s` checkbox** 与 **`Copy AI prompt for selected row`** 按钮（拼一段 markdown 让人手工丢给 AI，内含 detail + timeline 关键片段）。

### Detail（`/native-io/detail/{sqlId}`）

- **Bottleneck 横幅**：phase + confidence + evidence + hint + actionable[]
- **Phase breakdown**：ASCII 横向 stacked bar + 详细表（含 OBS phase 的 p50/p95/retries、filter_dv 的 pruned/filtered、java_overhead 的 plan/commit 拆分）
- **Tasks 表**：`skew_ratio`, `slowest[]`（含 current_phase）
- **Slowest files**：top 5（含 row groups read/pruned）
- **OBS counters / Memory**：请求计数、字节、retry、4xx/5xx；peak_buffered_bytes、writer_rolls
- 底栏：`[ View Timeline ▶ ]`, `[ Copy AI prompt ]`, `[ JSON ▾ /api/detail/{sqlId} ]`

### Timeline（`/native-io/timeline/{sqlId}`）

类 dmesg 的事件流。支持 type / task / file 过滤。429 / retry / error 类事件高亮。cursor 分页，每页 5000 条，超 20000 时强制翻页。

## 瓶颈判定规则引擎

`BottleneckRuleEngine` 在 driver 端运行，触发模型：

- 周期：每 5s（`spark.paimon.native-io.bottleneck.interval-ms`）
- 增量：事件 commit 时局部更新（避免每次全量扫）
- 跳过：`elapsed_ms < 5000` 时输出 `phase=warming_up`

置信度公式 `confidence = min(1, signal_strength × coverage)`：

- `signal_strength`：主信号超阈值的程度（如 `share=0.71` 对阈值 `0.50` → 1.0；`share=0.55` → 0.5）。
- `coverage`：依赖指标的完整度（如缺 `MEMORY_SNAPSHOT` 时 `memory_pressure` 的 coverage = 0.5）。
- 采样模式（`diagnostics_health.sampled=true`）下 confidence 额外乘 0.85。

### 11 项规则

| # | Phase | 主信号 | 阈值 | 次信号 |
|---|---|---|---|---|
| 1 | `obs_read` | `phase_share(obs_read)` | `> 0.50` | `retries >= 5` 或 `4xx >= 3` |
| 2 | `parquet_decode` | `phase_share(parquet_decode)` | `> 0.40` | `threads_active = max` |
| 3 | `filter_dv` | `phase_share(filter_dv) > 0.30` 或 `pruned_ratio < 0.05 且 dv_filter > 0.5` | — | `rows_filtered/rows_read >= 0.8` |
| 4 | `parquet_encode` | `phase_share(parquet_encode)` | `> 0.40` | `threads_active = max` |
| 5 | `obs_write` | `phase_share(obs_write)` | `> 0.40` | `write_retries >= 3` |
| 6 | `multipart_finish` | `multipart_finish_ms`（单次） | `>= 5000` | `queue_depth >= 8` 持续 30s |
| 7 | `cpu_idle` | `threads_active < 0.5 × threads_max` 持续 20s | — | `elapsed > 30s` |
| 8 | `memory_pressure` | `peak_buffered / configured_limit` | `> 0.85` | `writer_rolls/files > 1.5` |
| 9 | `jni_wait` | `(jni_end − jni_start) − native_ms` | `> 30%` 总耗时 | `FFI_ENTERED ↔ REQUEST_PARSED > 1s` |
| 10 | `java_overhead` | `sql_elapsed − sum(native_op_elapsed)` | `> 30%` 总耗时 | `plan_ms + commit_ms > 5s` |
| 11 | `stage_skew` | `max_task_ms / median_task_ms` | `>= 3` | `sample >= 4 tasks` |

### 输出

- `summary.bottleneck`：最高 confidence 的一条
- `detail.bottleneck_candidates[]`：11 项全部，按 confidence 降序
- `healthy`：所有规则 confidence < 0.3 时输出 `phase=none, hint="healthy"`
- `unknown`：coverage=0 时输出 `phase=unknown` 并提示去查 `/api/session.native_io.callbacks_active`
- `fallback`：`mode=fallback` 时跳过 1–11 判定，直接用 `reject_reason` 生成 hint/actionable，`bottleneck.phase=reject`
- `mixed`：`mode=mixed`（一条 SQL 同时存在 native 和 fallback 路径）时按 native 部分跑 1–11 判定，并在 `bottleneck.hint` 中追加"部分操作走 Java fallback"提示
- `stuck` 优先：`status=stuck` 时强制 `bottleneck.phase = stuck_at.phase`，confidence=1.0

## 性能与隐患消除

性能验收门槛：**开 diagnostics 与关 diagnostics 的 export 端到端耗时差异 ≤ 1%**（默认采样下）。压测脚本纳入 plan 阶段，覆盖 100-file / 1000-file / 10000-file 三档 SQL。

### 事件分级与默认采样

| 事件 | 优先级 | 默认采样 |
|---|---|---|
| `ERROR` | critical | 1.0（强制） |
| `FFI_ENTERED`, `REQUEST_PARSED`, `PLAN_REJECT`, `JAVA_FALLBACK_*`, `COMMIT_*`, `FILE_START/END`, `PHASE_START/END`, `JNI_CALL_*` | high | 1.0 |
| `MEMORY_SNAPSHOT`, `RUNTIME_SNAPSHOT` | medium | 节流 5s |
| `OBJECT_REQUEST_START/END` | medium | **0.05（5% 采样）** |
| `PHASE_PROGRESS` | low | **0（默认关闭）** |
| `DEBUG` | low | 0 |

效果：100-file SQL 的事件总量从 ~2500 降到 ~900（约 36% 原方案）。

### 强制 batching + 异步 RPC

Executor 端 `NativeIODiagnosticsBatcher`：

- ring buffer 容量 10k，按优先级丢弃 low → medium → high
- flusher daemon thread，单线程，trigger = 100 events 或 200ms 或 emergency-flush
- batch RPC 失败 retry 3 次指数退避，3 次失败 → inc `rpc_send_errors` counter + 丢弃

效果：2000 个 OBJECT_REQUEST 事件 → 默认采样后 100 个 → batch 100 = ~1 次 RPC / SQL；即使采样率拉到 1.0，也只是 ~20 次 RPC / SQL。

### 修复 silent failure

- `NativeIODiagnosticsEmitter.java:31-43`：移除整段 try/catch 静默，catch 时 inc `emit_errors` counter + 首次 WARN log + throttle 后续日志。
- `NativeIORpcSupport.scala:48-53`：`sendToDriver` 失败 retry 3 次 + inc `rpc_send_errors`。
- driver 端接收 queue 满时回 throttle 信号给 executor，executor 临时降低 `OBJECT_REQUEST` 采样率到 0.01，60s 无 throttle 后恢复。

### 硬上限 + kill-switch

- `max-events-per-sql=50000`：超过后该 sql 停止采集，inc `capped` flag。
- `driver.buffer.capacity=200000`：满则丢 low/medium 优先级事件。
- 连续 60s drop_rate > 50% → `OBJECT_REQUEST` 采样率自动降到 0.01，发 WARN。
- 连续 5min 仍严重 → 自动 disable diagnostics（保留 `ERROR/PLAN_REJECT/COMMIT/FALLBACK`），发 ERROR log。

### API 暴露诊断自身健康度

`/api/summary.sqls[*].diagnostics_health` 与 `/api/session.diagnostics`（见数据模型节）让 AI 看到：

- `sampled=true` → 知道 OBS counter 是 ×20 反推估算
- `kill_switch_active=true` → 知道指标不全
- `capped=true` → 知道事件被截断
- 任何 `*_errors > 0` → 知道采集链路有问题

## Spark History Server 支持

v2 的所有能力（3-page UI、4 个 REST endpoint、瓶颈判定、采样元数据）在 Spark History Server replay 时同样可用。事后排查不需要回到 driver、不需要重新跑 SQL。

### 注册与生命周期

- 沿用现有 `NativeIOHistoryServerPlugin`（commit `05c1505` 引入），它在 Spark History Server 启动时按 `org.apache.spark.status.AppHistoryServerPlugin` SPI 注册。
- Plugin 在每个 app 加载阶段构造一份独立的 `NativeIOStore` 实例与 `NativeIOTab`、`NativeIOApi`，attach 到 history server 的 Jetty。
- replay 期间由 `NativeIOHistoryListener` 解析 `SparkListenerNativeIOEvent` 重建 store；replay 完成后由 plugin 调用 `BottleneckRuleEngine.recompute(store)` 一次性回算所有 SQL 的 verdict。

### 事件持久化

driver 端所有事件（包括 v2 新增的 `FFI_ENTERED / OBJECT_REQUEST_* / MEMORY_SNAPSHOT / PLAN_REJECT / JAVA_FALLBACK_* / COMMIT_* / PLAN_*`）都包成 `SparkListenerNativeIOEvent` 写入 Spark event log。事件 schema 带 `version` 字段；future field 用 `Optional` 兼容旧 event log。

为了让 history mode 下也能还原诊断元数据，以下计数器写入 event log：

- 每条 SQL 完成时 emit 一条 `DIAGNOSTICS_HEALTH_FINAL` 事件，记录该 SQL 的 `sample_rate / events_emitted / events_dropped / capped / kill_switch_active`。
- 每次 driver kill-switch 状态变化（active/inactive）emit 一条 `DIAGNOSTICS_KILLSWITCH` 事件。
- session 级 `rpc_errors / emit_errors / throttle_events` 在 driver 退出前 emit 一条 `DIAGNOSTICS_SESSION_FINAL` 快照。

### history mode 下的差异点

- **"now" 语义**：live mode 下 `elapsed_ms / last_event_age_ms` 基于 `System.currentTimeMillis()`；history mode 下基于 `app_end_time`。stuck 判定在 history mode 下重新解释为"app 结束前持续 X 毫秒无事件"。
- **`/api/session`**：history mode 下 `native_io.callbacks_active` 返回 `"history"`，并增加 `mode: "history"` 字段；前端 session bar 显示标识。
- **auto-refresh**：history mode 下 UI 关闭自动刷新（数据是静态的）。
- **diagnostics_health.sampled**：history mode 下从 `DIAGNOSTICS_HEALTH_FINAL` 还原；若该 event 缺失（v1 event log），默认 `sampled=unknown` 并 confidence 不再乘 0.85（无法判断）。

### 跨 driver 重启 / 跨 session 比较

v2 不引入跨 session 持久化层。"和上次同类 SQL 比"这一类问题通过两步：

1. AI 从当前 session 拿 `app_id` 和 `description`。
2. AI 调用 Spark History Server 已有的 `GET /api/v1/applications` 列出历史 app，对每个感兴趣的 app 调用 `/api/v1/applications/{app_id}/native-io/api/summary?description=...` 拿到该 app 的同名 SQL 摘要。

在 Spark History Server 上每个 app 的诊断路径为 `/history/{app_id}[/{attempt_id}]/native-io/api/*`（沿用 Spark History Server 既有的 per-app 路由约定，由 `AppHistoryServerPlugin.setupUI(SparkUI)` 时 attach 到该 app 自身的 SparkUI handler）。

## 实施范围

### Module 1 · Rust（paimon-native-io）

- `paimon-native-io-c/src/lib.rs`：
  - `NativeDiagnosticEvent` 新增 `FfiEntered / ObjectRequestStart / ObjectRequestEnd / MemorySnapshot`
  - `read_ms` 与 `decode_ms` 拆分
  - `parquet_row_groups_pruned` 实际计数
  - OBS client 调用包 `OBJECT_REQUEST_START/END`，含 status_code / retries / bytes
  - `MEMORY_SNAPSHOT` 周期节流（每 5s）
  - `RuntimeSnapshot` 增加 `threads_max`

### Module 2 · Java boundary（paimon-core / paimon-spark-common）

- `NativeIODiagnosticsEmitter.java`：移除 silent swallow，emit 错误计数
- `NativeRejectReason.java`：30 种 reason 加 `reasonCode` 与 `actionableHint(SparkContext)`
- `ExportParquetProcedure.java`：`LOG.warn(rejectReason)` → `emit PLAN_REJECT`；包 `JAVA_FALLBACK_START/END`、`COMMIT_START/END`、`PLAN_START/END`
- `PaimonNativeExporter.java`：JNI 调用前 emit `FFI_ENTERED`

### Module 3 · Spark UI / API（paimon-spark-3.4）

- `NativeIOStore.scala`：重组为 `sqlId` 顶层 + `SqlExecutionState`，加 completed 队列上限
- `NativeIOLifecycleListener.scala`：扩展 `onSQLExecutionStart/End`、`onStageSubmitted/Completed`，实现延迟补齐
- `NativeIODiagnostics.scala`：record() 走 `BottleneckRuleEngine.update(event)`；`NativeIODiagnosticsConfig` 暴露 v2 新增的 29 个 conf 读取
- `BottleneckRuleEngine.scala`（新增）：11 个 Rule trait 实现、5s timer + 增量
- `NativeIOApi.scala`（新增）：4 个 handler，application/json，attach 到 SparkUI Jetty，使用 jackson
- `NativeIOTab.scala`：缩减为 3 page
- `NativeIOPage.scala` 删除，拆为 `NativeIOSummaryPage / NativeIODetailPage / NativeIOTimelinePage`
- `NativeIODiagnosticsBatcher.scala`（新增）：executor 端 ring buffer + flusher
- `NativeIORpcSupport.scala`：失败 retry + 错误计数 + 背压
- `NativeIOHistoryServerPlugin.scala`：history server 加载 app 时构造独立 `NativeIOStore` + `NativeIOTab` + `NativeIOApi`，attach 到 history Jetty
- `NativeIOHistoryListener.scala`：解析所有 `SparkListenerNativeIOEvent` 子类型（含 v2 新增事件 + `DIAGNOSTICS_HEALTH_FINAL` / `DIAGNOSTICS_KILLSWITCH` / `DIAGNOSTICS_SESSION_FINAL`）重建 store；replay 完成后触发 `BottleneckRuleEngine.recompute(store)`
- live driver 路径与 history 路径共用 `BottleneckRuleEngine`、`NativeIOApi`、`NativeIOSummaryPage / NativeIODetailPage / NativeIOTimelinePage`；唯一差异是"now"语义和 auto-refresh 由调用方注入

### Module 4 · 测试策略

- `BottleneckRuleEngineTest`：每条规则 4 个用例（happy / threshold-just-below / threshold-just-above / coverage-missing），共 44 用例
- `NativeIOStoreTest`：sqlId 顶层组织、completed 队列上限、并发写入
- `NativeIOLifecycleListenerTest`：SQL/Stage 生命周期配对、延迟补齐
- `NativeIOApiTest`：4 endpoint 的 schema、status filter、cursor 分页、404；用 jackson schema validator
- `NativeIOSummaryPageTest` 等：HTML 渲染后用 jsoup 断言关键字段
- 集成测试：复用 `ExportParquetSuite`，跑一次 export 后 `GET /api/summary` 断言 bottleneck.phase 符合预期
- Rust 单元测试：`paimon-native-io-c/tests/` 加事件 emit 覆盖
- History server replay 测试：mock event log → 启 history server → 验证 (a) 3 page UI 渲染、(b) 4 个 REST endpoint 返回 (c) `BottleneckRuleEngine.recompute` 在 replay 完成后回填 verdict、(d) `DIAGNOSTICS_HEALTH_FINAL` 还原 `diagnostics_health.sampled`、(e) v1 旧 event log 在缺失元数据事件时按 `sampled=unknown` 兜底
- 性能基准测试：100/1000/10000-file 三档，开 vs 关 diagnostics 端到端耗时差 ≤ 1%
- 验证镜像：`monster830/paimon-plus:spark344-java8`（CLAUDE.md 规定）

### 实施顺序（plan 阶段细化）

1. Rust 事件补全 + read_ms 拆分 + pruned 计数 + OBJECT_REQUEST 包装
2. Java 侧 NativeRejectReason / ExportParquetProcedure emit 改造
3. `NativeIODiagnosticsBatcher` + RPC 修复（先消除隐患再加新功能）
4. `NativeIOStore` 重组 + Listener 扩展（含延迟补齐）
5. `BottleneckRuleEngine` + 瓶颈相关 15 conf（13 个阈值 + interval + min-elapsed）
6. `NativeIOApi` 4 endpoint
7. UI 砍 8 → 3
8. History server adapter
9. 端到端集成 + 性能基准 + 文档

## 配置

### v1 spec 已有（保留默认值，部分需要落地）

| Key | 默认 |
|---|---|
| `spark.paimon.native-io.ui.enabled` | `true`（已落地） |
| `spark.paimon.native-io.ui.event-log.enabled` | `true`（需落地） |
| `spark.paimon.native-io.ui.max-events` | `100000`（需落地） |
| `spark.paimon.native-io.ui.max-slow-operations` | `1000`（需落地） |
| `spark.paimon.native-io.ui.slow-operation-threshold` | `5 s`（需落地） |

### v2 新增

性能与采样：

| Key | 默认 |
|---|---|
| `spark.paimon.native-io.diagnostics.object-request.sample-rate` | `0.05` |
| `spark.paimon.native-io.diagnostics.phase-progress.enabled` | `false` |
| `spark.paimon.native-io.diagnostics.memory-snapshot.interval-ms` | `5000` |
| `spark.paimon.native-io.diagnostics.runtime-snapshot.interval-ms` | `5000` |
| `spark.paimon.native-io.diagnostics.batch.max-events` | `100` |
| `spark.paimon.native-io.diagnostics.batch.max-interval-ms` | `200` |
| `spark.paimon.native-io.diagnostics.buffer.capacity` | `10000` |
| `spark.paimon.native-io.diagnostics.driver.buffer.capacity` | `200000` |
| `spark.paimon.native-io.diagnostics.max-events-per-sql` | `50000` |
| `spark.paimon.native-io.diagnostics.kill-switch.drop-rate` | `0.50` |
| `spark.paimon.native-io.diagnostics.kill-switch.duration-ms` | `60000` |
| `spark.paimon.native-io.diagnostics.rpc-retry.max-attempts` | `3` |

UI 与 store：

| Key | 默认 |
|---|---|
| `spark.paimon.native-io.ui.max-active-operations` | `10000` |
| `spark.paimon.native-io.ui.max-completed-sqls` | `200` |
| `spark.paimon.native-io.ui.stuck-threshold-ms` | `30000` |
| `spark.paimon.native-io.ui.timeline-retention` | `20000` |

瓶颈判定阈值：

| Key | 默认 |
|---|---|
| `spark.paimon.native-io.bottleneck.interval-ms` | `5000` |
| `spark.paimon.native-io.bottleneck.min-elapsed-ms` | `5000` |
| `spark.paimon.native-io.bottleneck.obs-read.share-threshold` | `0.50` |
| `spark.paimon.native-io.bottleneck.decode.share-threshold` | `0.40` |
| `spark.paimon.native-io.bottleneck.encode.share-threshold` | `0.40` |
| `spark.paimon.native-io.bottleneck.write.share-threshold` | `0.40` |
| `spark.paimon.native-io.bottleneck.filter.share-threshold` | `0.30` |
| `spark.paimon.native-io.bottleneck.multipart.finish-ms` | `5000` |
| `spark.paimon.native-io.bottleneck.cpu-idle.utilization` | `0.50` |
| `spark.paimon.native-io.bottleneck.memory.headroom` | `0.85` |
| `spark.paimon.native-io.bottleneck.jni-wait.share` | `0.30` |
| `spark.paimon.native-io.bottleneck.java-overhead.share` | `0.30` |
| `spark.paimon.native-io.bottleneck.skew.ratio` | `3.0` |

## 风险

- **History Server 兼容**：v1 spec 已假定 Spark 3.4.4 可保留 custom `SparkListenerEvent`，replay 时直接复用 parser。新事件字段加入后必须保证 `version=1` 的旧 event log 仍可回放，缺字段以默认值填充。新增的 `DIAGNOSTICS_HEALTH_FINAL` 等元数据事件在旧 event log 中不存在，replay 时按 `sampled=unknown` 兜底，confidence 公式跳过采样修正。
- **History Server Jetty 注册时机**：v2 把 `NativeIOApi` 也 attach 到 history server 的 Jetty。Spark 3.4.4 的 `AppHistoryServerPlugin` 在 app 加载完成后给 plugin 一次 attach 机会，必须确认 attach 的是该 app 自己的 SparkUI 实例（每 app 隔离），避免不同 app 的 API 互相干扰。
- **OBJECT_REQUEST 采样的代表性**：5% 采样足够估算 p50/p95 但不能精确算 ratio。`detail.phase_breakdown.obs_read.p50/p95` 必须用采样修正（HyperLogLog 类近似可不上，简单 quantile 估算够用）。如果 AI 反复表达需要全量，再考虑临时上调或落 trace 文件。
- **Driver 内存膨胀**：完成队列上限 200 SQL，每条 ~10 KB，共 2 MB；timeline ring buffer 20k × 200 bytes = 4 MB；总驻留 ~10 MB 量级，可接受。但单条 SQL 的事件本身硬上限 `max-events-per-sql=50000`，若被 cap 必须显式 flag。
- **Rust 侧 OBJECT_REQUEST 埋点开销**：包 OBS client 调用前/后两次 emit + JNR callback。worst case 单 OBS call 增加 ~10 µs。采样模式下可忽略；全量模式下若 OBS call 极密集需另测。
- **API 鉴权**：依赖 SparkUI ACL。Spark 默认 ACL 关闭时 API 完全开放——本设计不再次实现独立认证，部署侧需保证 SparkUI ACL 已正确配置。
- **配置项数量大（共 34 个，v2 新增 29 个）**：以默认值合理为前提；CLAUDE.md 已要求文档 / plan 同步维护。运维 90% 场景应不需要调整任何 conf。
