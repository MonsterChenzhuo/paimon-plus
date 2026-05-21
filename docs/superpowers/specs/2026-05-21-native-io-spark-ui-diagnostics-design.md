# Native IO Spark UI 诊断设计

## 目标

在 Spark UI 和 Spark History Server 中暴露 Native IO 执行细节，用于线上问题诊断和性能瓶颈定位。第一版只要求 Spark 3.4.4 可用，并聚焦 `sys.export_parquet` native fast path。

## 非目标

- 第一版不支持 Spark 3.2、3.3、3.5、4.0。
- 不建设外部 metrics 服务。
- 不替换 Spark 现有 SQL、stage、executor 页面。
- 不要求 Java export path 在第一版具备完整诊断能力。

## 用户体验

Spark UI 增加一个顶层 tab：`Native IO`。

该 tab 包含这些页面：

- `Summary`：应用级 Native IO 状态、活跃操作、总行数和字节数、最慢操作、最近错误。
- `Per SQL`：按 Spark SQL execution id 和 SQL 描述聚合 Native IO 指标。
- `Per Stage`：按 Spark stage id 和 attempt id 聚合 Native IO 指标。
- `Per Task`：展示 Native IO task attempt、executor host、JNI 耗时、行数、字节数、阶段耗时、错误信息。
- `Per File`：展示源文件和输出文件级别的读写统计。
- `Timeline`：按时间顺序展示 native operation event，用于观察运行中的任务。
- `Slow Operations`：展示最慢的 JNI 调用、文件读、对象存储请求、写入、multipart finish、native runtime 等待。
- `Stuck Operations`：展示当前阶段耗时超过阈值的活跃操作。该页面面向线上 incident 排查，必须展示当前阶段、已持续时间、executor、host、task id、文件路径、输出路径、最后事件时间。

当 export 卡在 JNI 内部时，UI 必须仍然能展示进入阻塞点之前的最后一个已知事件。这是本次线上问题暴露出的核心诊断需求。

UI 必须能在不抓 thread dump 的情况下回答这个问题：这个 native task 不返回，它最后进入了哪个 native 阶段，已经停留多久，当时正在处理哪个文件或对象存储请求？如果 `Native IO` tab 无法回答这个问题，第一版就不能算完成。

## 架构

使用进程内事件管道：

1. Native export Java 和 Rust 代码发出结构化 `NativeIOEvent`，覆盖生命周期、阶段、文件、对象存储请求、runtime、内存和错误事件。
2. executor 侧代码通过 Spark 3.4.4 可用的机制把事件传回 driver。
3. driver 侧 `NativeIOSparkListener` 关联 Spark SQL/job/stage/task 生命周期事件，并把 native event 写入 `NativeIOStore`。
4. `NativeIOTab` 从 `NativeIOStore` 渲染页面。
5. Native IO event 同时写入 Spark event log，使 Spark History Server 可以回放并重建同样的 store。

设计上保持 native 事件采集和 UI 渲染解耦。Native 代码只负责发事件；Spark 维度的聚合和展示留在 Spark 模块。

## 事件模型

`NativeIOEvent` 是 live UI 和 event-log replay 共用的稳定 wire model。

必填字段：

- `eventId`：事件源生成的唯一 id。
- `eventTime`：epoch milliseconds。
- `eventType`：事件类型，例如 `OPERATION_START`、`OPERATION_END`、`PHASE_START`、`PHASE_END`、`FILE_START`、`FILE_END`、`OBJECT_REQUEST_START`、`OBJECT_REQUEST_END`、`RUNTIME_SNAPSHOT`、`MEMORY_SNAPSHOT`、`ERROR`。
- `operationId`：一个 native task 内所有事件共享的 native operation id。
- `operationName`：第一版固定为 `export_parquet`。
- `sqlExecutionId`：可获取时填 Spark SQL execution id。
- `stageId`、`stageAttemptId`、`taskAttemptId`、`taskIndex`、`attemptNumber`。
- `executorId`、`host`。
- `threadId`。

可选字段：

- `phase`：`jni`、`parse_request`、`plan`、`open_reader`、`read`、`decode`、`filter`、`encode`、`write`、`multipart_finish`、`close`。
- `filePath`、`outputPath`、`objectRequestId`、`objectOperation`。
- `durationMs`、`rows`、`bytes`、`queueDepth`、`runtimeThreads`、`nativeMemoryBytes`、`peakBufferedBytes`。
- `metricsJson`：task 完成时已有的 native export metrics JSON。
- `errorClass`、`errorMessage`、`stackTrace`。

事件必须带 `version = 1`，方便后续扩展字段而不破坏 History Server 回放。

## Native 埋点

Java 侧在这些位置发事件：

- `PaimonNativeExporter.exportParquet` 进入 JNR native call 前后。
- `NativeExportProviderImpl.executeTask` 收到 task payload 时。
- task 返回 `NativeExportTaskResult` 或抛异常时。

Rust 侧在这些位置发事件：

- FFI 入口，且必须在 request JSON 解析前。
- request 解析成功或失败。
- export 开始和结束。
- 每个源文件开始和结束。
- reader 创建完成。
- batch read/decode/filter/write 阶段摘要。
- 可获取时的 OBS object request 开始和结束。
- multipart writer create、upload part、complete、abort。
- runtime snapshot，包含 queue depth 和 worker count。
- memory snapshot，包含当前和峰值 native buffered bytes。

Rust 必须在任何可能很慢的解析或对象存储操作之前发事件，这样 native call 卡住时 Spark UI 仍然能看到进展。

第一版为了定位卡住任务，以下事件是强制要求：

- Java `JNI_CALL_START`：调用 `paimon_exporter_export_parquet` 之前。
- Rust `FFI_ENTERED`：进入 `paimon_exporter_export_parquet` 后第一时间发出，必须早于 JSON 解析。
- Rust `REQUEST_PARSED`：`parse_export_task` 之后。
- Rust `FILE_START`：打开每个源文件之前。
- Rust `READER_READY`：reader 创建完成之后。
- Rust `PHASE_START` 和 `PHASE_END`：覆盖 `read`、`decode`、`filter`、`encode`、`write`、`multipart_finish`。
- Rust `OBJECT_REQUEST_START` 和 `OBJECT_REQUEST_END`：在 OBS SDK 边界可获取时覆盖 OBS read、upload part、complete multipart、abort multipart。
- Java `JNI_CALL_END`：native 返回或抛异常之后。

active operation view 通过“最新 start 且没有对应 end 的事件”计算 `currentPhase`。页面同时展示 `phaseElapsedMs = now - eventTime`，以及可用的 `filePath`、`outputPath`、`objectOperation`、`objectRequestId`。

## Spark Listener 和 Store

`NativeIOSparkListener` 监听 Spark 3.4.4 生命周期事件：

- SQL execution start/end。
- Job start/end。
- Stage submitted/completed。
- Task start/end。

listener 同时消费 Native IO event，并在可能时补充 Spark 上下文。对于暂时缺少完整上下文的事件，store 先按 `operationId` 保存，等 Spark 生命周期事件到达后回填 SQL/stage/task 字段。

`NativeIOStore` 是 driver 内存中的有界 store：

- 保留最近的 operation。
- 保留按耗时排序的 top slow operation。
- 保留 Per SQL、Per Stage、Per Task、Per File 聚合。
- 单独保留 active operation，使 live stuck task 始终可见。
- 通过配置限制 timeline event 数量，避免 driver 内存被打爆。

## Spark UI

实现 `NativeIOTab`，并为每个视图实现独立页面。页面使用 Spark 3.4.4 UI helper 服务端渲染 HTML。

每个页面的展示原则：

- 活跃操作优先。
- 完成态聚合其次。
- 错误和慢操作显著展示。
- 能拿到 id 时链接回 Spark SQL、stage、executor 页面。

`Summary` 页面必须包含 `Active Native Operations` 表，字段包括：

- SQL execution id。
- Stage id 和 task attempt id。
- Executor id 和 host。
- Operation name。
- Current phase。
- Phase elapsed time。
- Last event time。
- File 或 object path。
- Object operation。
- 已观测到的 rows 和 bytes。

这个表是解决本次生产故障模式的主入口：Spark 只显示所有 task running，但 task metrics 还没有完成时，用户仍然能看到 native task 最后卡在哪个阶段。

第一版可以只使用普通表格和 query 参数排序，不要求图表。

## Event Log 和 History Server

Native IO event 必须支持 Spark History Server 回放。

第一版优先把 native event 写成 Spark listener event，使用稳定事件名和 JSON payload。live driver 和 History Server replay 使用同一个 parser 重建 `NativeIOStore`。

如果 Spark 3.4.4 在目标环境中无法干净保留 custom event，则 fallback 到 Spark listener 可见的 accumulator update event batch。fallback 也必须保留 Summary、Per Task、Per File、Timeline、Slow Operations 所需字段。

## 配置

增加 Spark/Paimon 配置：

- `spark.paimon.native-io.ui.enabled`：默认在 native export 开启时为 `true`。
- `spark.paimon.native-io.ui.event-log.enabled`：默认 `true`。
- `spark.paimon.native-io.ui.max-events`：默认 `100000`。
- `spark.paimon.native-io.ui.max-slow-operations`：默认 `1000`。
- `spark.paimon.native-io.ui.slow-operation-threshold`：默认 `5 s`。

现有 native export 配置不变。

## 测试

第一版只验证 Spark 3.4.4。

必须覆盖：

- event JSON 序列化和兼容解析单测。
- store 聚合和有界保留单测。
- Spark 3.4 集成测试：运行小规模 native export 并验证 Native IO tab 注册成功。
- Spark listener 测试：验证 native event 与 SQL、stage、task id 的关联。
- event-log replay 测试：验证回放后可重建同样的 Native IO summary。
- failure-path 测试：native export 抛异常时 UI/store 保留 error event。
- stuck-operation 测试：只发送 `JNI_CALL_START`、`FFI_ENTERED`、`REQUEST_PARSED`、`FILE_START`，不发送完成事件，验证 Summary 和 Stuck Operations 页面能展示 active current phase 和 elapsed time。

## 风险

- Spark UI 和 History Server 内部 API 并不完全稳定。第一版只限定 Spark 3.4.4，可以减少兼容面。
- executor 到 driver 的 live event 传输如果太细，会带来开销。默认发摘要事件；per-request 事件优先用于慢操作或 debug 模式。
- native runtime 未必一开始就能暴露所有内部指标。事件模型先预留字段，Rust 埋点可以逐步增强。
- event-log 体积可能快速增长。第一版必须实现有界保留和慢操作过滤。
