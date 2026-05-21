# Native IO Spark UI 诊断实现计划

> **给 agentic worker 的要求：** 必须使用 `superpowers:subagent-driven-development`（推荐）或 `superpowers:executing-plans` 按任务执行。本计划使用 checkbox（`- [ ]`）追踪进度。

**目标：** 构建只要求 Spark 3.4.4 可用的 Native IO 诊断 tab，能展示 live active/stuck native export 阶段，并能从 event log 重建完成态诊断信息。

**架构：** 在 `paimon-spark-3.4` 下实现 Spark 3.4 专用诊断 UI 和 listener，在 `paimon-core` 中定义通用事件模型。Native export Java 侧围绕 task/JNI 边界发事件，Rust 侧围绕 parse、file、phase、object request、runtime、memory 发事件，Spark UI tab 从 driver 侧有界 store 渲染页面。第一优先级是解决生产卡住场景：任务未完成时，UI 也必须显示最后进入的 native 阶段。

**技术栈：** Java 8、Scala 2.12、Spark 3.4.4 UI/listener API、Paimon shaded Jackson 工具、现有 Paimon native export Java/Rust 代码。

---

## 文件结构

- 新增 `paimon-core/src/main/java/org/apache/paimon/operation/nativeio/diagnostics/NativeIOEvent.java`：可序列化的版本化 native event 模型。
- 新增 `paimon-core/src/main/java/org/apache/paimon/operation/nativeio/diagnostics/NativeIOEventType.java`：event type 枚举。
- 新增 `paimon-core/src/main/java/org/apache/paimon/operation/nativeio/diagnostics/NativeIOPhase.java`：native phase 枚举。
- 新增 `paimon-core/src/main/java/org/apache/paimon/operation/nativeio/diagnostics/NativeIOEventJson.java`：event JSON 序列化工具。
- 新增 `paimon-core/src/test/java/org/apache/paimon/operation/nativeio/diagnostics/NativeIOEventJsonTest.java`：模型 round-trip 测试。
- 新增 `paimon-spark/paimon-spark-3.4/src/main/scala/org/apache/paimon/spark/nativeio/diagnostics/NativeIOOperationState.scala`：active/completed operation 状态。
- 新增 `paimon-spark/paimon-spark-3.4/src/main/scala/org/apache/paimon/spark/nativeio/diagnostics/NativeIOStore.scala`：有界内存 store 和聚合逻辑。
- 新增 `paimon-spark/paimon-spark-3.4/src/main/scala/org/apache/paimon/spark/nativeio/diagnostics/NativeIOSparkListener.scala`：Spark listener 和事件关联逻辑。
- 新增 `paimon-spark/paimon-spark-3.4/src/main/scala/org/apache/paimon/spark/nativeio/diagnostics/NativeIOTab.scala`：Spark UI tab 注册。
- 新增 `paimon-spark/paimon-spark-3.4/src/main/scala/org/apache/paimon/spark/nativeio/diagnostics/NativeIOPage.scala`：页面渲染公共 helper。
- 新增页面类：`NativeIOSummaryPage.scala`、`NativeIOStuckOperationsPage.scala`、`NativeIOTimelinePage.scala`、`NativeIOSlowOperationsPage.scala`、`NativeIOTaskPage.scala`、`NativeIOFilePage.scala`、`NativeIOStagePage.scala`、`NativeIOSqlPage.scala`。
- 新增 `paimon-spark/paimon-spark-3.4/src/main/scala/org/apache/paimon/spark/nativeio/diagnostics/NativeIODiagnostics.scala`：供 `ExportParquetProcedure` 调用的安装入口。
- 修改 `paimon-spark/paimon-spark-common/src/main/java/org/apache/paimon/spark/procedure/ExportParquetProcedure.java`：native export 前安装 diagnostics，并发送 driver/task 边界事件。
- 修改 `paimon-native-io/src/main/java/org/apache/paimon/nativeio/export/NativeExportProviderImpl.java`：发送 task/JNI 边界事件。
- 修改 `paimon-native-io/src/main/java/org/apache/paimon/nativeio/export/PaimonNativeExporter.java`：发送 `JNI_CALL_START` 和 `JNI_CALL_END`。
- 修改 `paimon-native-io/rust/paimon-native-io-c/src/lib.rs`：发送 Rust 侧强制 progress event，包括 parse 前、parse 后、per file、per phase，以及可用的 object request 边界。
- 新增测试目录 `paimon-spark/paimon-spark-3.4/src/test/scala/org/apache/paimon/spark/nativeio/diagnostics`。

---

### Task 1: 事件模型

**文件：**
- 新增：`paimon-core/src/main/java/org/apache/paimon/operation/nativeio/diagnostics/NativeIOEventType.java`
- 新增：`paimon-core/src/main/java/org/apache/paimon/operation/nativeio/diagnostics/NativeIOPhase.java`
- 新增：`paimon-core/src/main/java/org/apache/paimon/operation/nativeio/diagnostics/NativeIOEvent.java`
- 新增：`paimon-core/src/main/java/org/apache/paimon/operation/nativeio/diagnostics/NativeIOEventJson.java`
- 测试：`paimon-core/src/test/java/org/apache/paimon/operation/nativeio/diagnostics/NativeIOEventJsonTest.java`

- [ ] **Step 1: 编写序列化测试**

测试 active file event 和 completed object request event 的 round-trip。断言 `version`、`eventType`、`operationId`、`phase`、`filePath`、`objectOperation`、`durationMs`、`rows`、`bytes`。

- [ ] **Step 2: 运行失败测试**

运行：`mvn -pl paimon-core -Pfast-build -Dtest=NativeIOEventJsonTest test`

预期：编译失败，因为 diagnostics 类还不存在。

- [ ] **Step 3: 实现事件模型**

实现 Java 8 immutable serializable 类，使用 Jackson 注解和 builder-style `withX` 方法。除 `version`、`eventId`、`eventTime`、`eventType`、`operationId`、`operationName` 外，其余字段允许为空。

- [ ] **Step 4: 运行测试**

运行：`mvn -pl paimon-core -Pfast-build -Dtest=NativeIOEventJsonTest test`

预期：PASS。

---

### Task 2: Store 和卡住任务语义

**文件：**
- 新增：`paimon-spark/paimon-spark-3.4/src/main/scala/org/apache/paimon/spark/nativeio/diagnostics/NativeIOOperationState.scala`
- 新增：`paimon-spark/paimon-spark-3.4/src/main/scala/org/apache/paimon/spark/nativeio/diagnostics/NativeIOStore.scala`
- 测试：`paimon-spark/paimon-spark-3.4/src/test/scala/org/apache/paimon/spark/nativeio/diagnostics/NativeIOStoreSuite.scala`

- [ ] **Step 1: 编写 store 测试**

构造事件序列：`JNI_CALL_START`、`FFI_ENTERED`、`REQUEST_PARSED`、`FILE_START`，不发送 end event。断言 store 返回一个 active operation，包含 `currentPhase`、`phaseElapsedMs > 0`、executor id、host、task attempt id、file path。

- [ ] **Step 2: 运行失败测试**

运行：`mvn -pl paimon-spark/paimon-spark-3.4 -am -Pfast-build -DfailIfNoTests=false -DwildcardSuites=org.apache.paimon.spark.nativeio.diagnostics.NativeIOStoreSuite -Dtest=none test`

预期：编译失败，因为 store 类还不存在。

- [ ] **Step 3: 实现 `NativeIOOperationState` 和 `NativeIOStore`**

按 `operationId` 跟踪 active operation。start event 更新 `currentPhase` 和 `lastEventTime`；匹配的 end event 设置 `durationMs`；`OPERATION_END` 将 operation 移入 completed 状态。按 `maxEvents` 限制 timeline event。

- [ ] **Step 4: 运行 store 测试**

运行同一个 Maven 命令。

预期：PASS。

---

### Task 3: Spark Listener 关联

**文件：**
- 新增：`paimon-spark/paimon-spark-3.4/src/main/scala/org/apache/paimon/spark/nativeio/diagnostics/NativeIOSparkListener.scala`
- 测试：`paimon-spark/paimon-spark-3.4/src/test/scala/org/apache/paimon/spark/nativeio/diagnostics/NativeIOSparkListenerSuite.scala`

- [ ] **Step 1: 编写 listener 测试**

创建 listener 和内存 store。依次喂入 SQL execution start、stage submitted、task start、native event。断言 native event 在缺少完整上下文时，能通过 task attempt id 或 operation id 回填 SQL/stage/task 字段。

- [ ] **Step 2: 运行失败测试**

运行：`mvn -pl paimon-spark/paimon-spark-3.4 -am -Pfast-build -DfailIfNoTests=false -DwildcardSuites=org.apache.paimon.spark.nativeio.diagnostics.NativeIOSparkListenerSuite -Dtest=none test`

预期：编译失败，因为 listener 类还不存在。

- [ ] **Step 3: 实现 listener**

继承 `org.apache.spark.scheduler.SparkListener`。处理 `onJobStart`、`onStageSubmitted`、`onTaskStart`、`onTaskEnd`，以及 `org.apache.spark.sql.execution.ui` 中的 SQL execution event。按 job id、stage id、task attempt id 维护映射。

- [ ] **Step 4: 运行 listener 测试**

运行同一个 Maven 命令。

预期：PASS。

---

### Task 4: Spark UI Tab 和 Stuck 页面

**文件：**
- 新增：`paimon-spark/paimon-spark-3.4/src/main/scala/org/apache/paimon/spark/nativeio/diagnostics/NativeIOTab.scala`
- 新增：`paimon-spark/paimon-spark-3.4/src/main/scala/org/apache/paimon/spark/nativeio/diagnostics/NativeIOPage.scala`
- 新增：`paimon-spark/paimon-spark-3.4/src/main/scala/org/apache/paimon/spark/nativeio/diagnostics/NativeIOSummaryPage.scala`
- 新增：`paimon-spark/paimon-spark-3.4/src/main/scala/org/apache/paimon/spark/nativeio/diagnostics/NativeIOStuckOperationsPage.scala`
- 新增：Per SQL、Per Stage、Per Task、Per File、Timeline、Slow Operations 的 placeholder 页面。
- 测试：`paimon-spark/paimon-spark-3.4/src/test/scala/org/apache/paimon/spark/nativeio/diagnostics/NativeIOTabSuite.scala`

- [ ] **Step 1: 编写 UI 注册和渲染测试**

创建 local SparkSession，安装 diagnostics，断言 Spark UI 包含 `nativeio` tab。向 store 写入 stuck event chain，渲染 Summary 和 Stuck Operations 页面，断言 HTML 包含 `Active Native Operations`、`Current Phase`、executor id、host、task id、file path。

- [ ] **Step 2: 运行失败测试**

运行：`mvn -pl paimon-spark/paimon-spark-3.4 -am -Pfast-build -DfailIfNoTests=false -DwildcardSuites=org.apache.paimon.spark.nativeio.diagnostics.NativeIOTabSuite -Dtest=none test`

预期：编译失败，因为 UI 类还不存在。

- [ ] **Step 3: 实现 UI 类**

使用 Spark 3.4 的 `SparkUITab`、`WebUIPage`、`UIUtils.headerSparkPage`。第一版先渲染普通表格。Summary 必须展示 spec 要求的 active operation 表。Stuck Operations 过滤当前阶段耗时超过配置阈值的 active operation。

- [ ] **Step 4: 运行 UI 测试**

运行同一个 Maven 命令。

预期：PASS。

---

### Task 5: Diagnostics 安装入口

**文件：**
- 新增：`paimon-spark/paimon-spark-3.4/src/main/scala/org/apache/paimon/spark/nativeio/diagnostics/NativeIODiagnostics.scala`
- 修改：`paimon-spark/paimon-spark-common/src/main/java/org/apache/paimon/spark/procedure/ExportParquetProcedure.java`
- 测试：`paimon-spark/paimon-spark-3.4/src/test/scala/org/apache/paimon/spark/nativeio/diagnostics/NativeIODiagnosticsInstallSuite.scala`

- [ ] **Step 1: 编写安装测试**

断言重复安装 diagnostics 只注册一个 listener 和一个 tab。断言关闭配置时不注册 tab。

- [ ] **Step 2: 运行失败测试**

运行：`mvn -pl paimon-spark/paimon-spark-3.4 -am -Pfast-build -DfailIfNoTests=false -DwildcardSuites=org.apache.paimon.spark.nativeio.diagnostics.NativeIODiagnosticsInstallSuite -Dtest=none test`

预期：编译失败或测试失败，因为 installer 缺失。

- [ ] **Step 3: 实现安装入口**

按 SparkContext 安装一次，使用稳定 shared marker。向 SparkContext 注册 `NativeIOSparkListener`，当 `sparkContext.ui` 存在时 attach `NativeIOTab`。在 `ExportParquetProcedure.nativeExport` 提交 native task 前调用 installer。

- [ ] **Step 4: 运行安装测试**

运行同一个 Maven 命令。

预期：PASS。

---

### Task 6: Java Native Export 事件发送

**文件：**
- 修改：`paimon-native-io/src/main/java/org/apache/paimon/nativeio/export/NativeExportProviderImpl.java`
- 修改：`paimon-native-io/src/main/java/org/apache/paimon/nativeio/export/PaimonNativeExporter.java`
- 修改：`paimon-spark/paimon-spark-common/src/main/java/org/apache/paimon/spark/procedure/ExportParquetProcedure.java`
- 测试：`paimon-native-io/src/test/java/org/apache/paimon/nativeio/export/PaimonNativeExporterDiagnosticsTest.java`

- [ ] **Step 1: 编写 Java 事件发送测试**

使用 fake `LibPaimonNativeIO`，断言 `JNI_CALL_START` 在 native call 前发送，`JNI_CALL_END` 在成功或失败后发送。

- [ ] **Step 2: 运行失败测试**

运行：`mvn -pl paimon-native-io -Pfast-build -Dtest=PaimonNativeExporterDiagnosticsTest test`

预期：FAIL，因为 event emitter 还不存在。

- [ ] **Step 3: 实现 Java 事件发送**

在 core 中增加最小 diagnostics emitter interface，默认 no-op。通过 `NativeExportTask` 或 task-local context 传递 operation id 和 task context。发送 task received、JNI start、JNI end、error event。

- [ ] **Step 4: 运行 Java 事件发送测试**

运行同一个 Maven 命令。

预期：PASS。

---

### Task 7: Rust Native Progress Events

**文件：**
- 修改：`paimon-native-io/rust/paimon-native-io-c/src/lib.rs`
- 测试：同文件中的 Rust unit test。

- [ ] **Step 1: 编写 Rust event 测试**

使用小本地文件调用 `export_parquet`，捕获 emitted event JSON。断言第一个 native event 是 `FFI_ENTERED`，且早于 request parsing；事件序列包含 `REQUEST_PARSED`、`FILE_START`、`READER_READY`、phase start/end pair、operation end。

- [ ] **Step 2: 运行失败 Rust 测试**

运行：`cd paimon-native-io/rust && cargo test -p paimon-native-io-c native_export_emits_progress_events`

预期：FAIL，因为还没有发送事件。

- [ ] **Step 3: 实现 Rust event callback 或 stderr bridge**

优先使用 JNR 可用的 Java callback bridge。如果 callback 集成侵入性过高，先用 `[paimon-native-io-event]` 前缀输出结构化 JSON 行作为中间步骤，并在 Java 测试中解析。生产路径最终必须能把事件传回 Spark driver。

- [ ] **Step 4: 运行 Rust 测试**

运行同一个 cargo 命令。

预期：PASS。

---

### Task 8: History Replay

**文件：**
- 修改或新增 `paimon-spark/paimon-spark-3.4/src/main/scala/org/apache/paimon/spark/nativeio/diagnostics` 下的 Spark 3.4 diagnostics 类。
- 测试：`paimon-spark/paimon-spark-3.4/src/test/scala/org/apache/paimon/spark/nativeio/diagnostics/NativeIOReplaySuite.scala`

- [ ] **Step 1: 编写 replay 测试**

通过选定的 Spark 3.4 event-log 机制写入 native event JSON payload。回放到新 store，断言 Summary、Per Task、Per File、Timeline、Slow Operations 数据与 live store 一致。

- [ ] **Step 2: 运行失败测试**

运行：`mvn -pl paimon-spark/paimon-spark-3.4 -am -Pfast-build -DfailIfNoTests=false -DwildcardSuites=org.apache.paimon.spark.nativeio.diagnostics.NativeIOReplaySuite -Dtest=none test`

预期：FAIL，因为 replay 未实现。

- [ ] **Step 3: 实现 Spark 3.4 replay 路径**

先做 Spark 3.4.4 API spike，选择最干净的可用机制。只有确认 Spark 3.4 `JsonProtocol` 能保留 custom listener event 时才使用 custom event。否则使用 accumulator-backed event batch，确保 task end 中可见并可被 History Server 读取。

- [ ] **Step 4: 运行 replay 测试**

运行同一个 Maven 命令。

预期：PASS。

---

### Task 9: Spark 3.4.4 端到端验证

**文件：**
- 按需修改 `paimon-spark/paimon-spark-3.4/src/test/scala/org/apache/paimon/spark/nativeio/diagnostics` 下的测试。

- [ ] **Step 1: 运行聚焦单测**

运行：

```bash
mvn -pl paimon-core,paimon-native-io,paimon-spark/paimon-spark-3.4 -am -Pfast-build -DfailIfNoTests=false \
  -Dtest=NativeIOEventJsonTest,PaimonNativeExporterDiagnosticsTest test
```

预期：PASS。

- [ ] **Step 2: 运行 Spark 3.4 diagnostics suites**

运行：

```bash
mvn -pl paimon-spark/paimon-spark-3.4 -am -Pfast-build -DfailIfNoTests=false \
  -DwildcardSuites=org.apache.paimon.spark.nativeio.diagnostics.NativeIOStoreSuite,org.apache.paimon.spark.nativeio.diagnostics.NativeIOSparkListenerSuite,org.apache.paimon.spark.nativeio.diagnostics.NativeIOTabSuite,org.apache.paimon.spark.nativeio.diagnostics.NativeIODiagnosticsInstallSuite,org.apache.paimon.spark.nativeio.diagnostics.NativeIOReplaySuite \
  -Dtest=none test
```

预期：PASS。

- [ ] **Step 3: 不使用 fast profile 编译 Spark 3.4 模块**

运行：`mvn -pl paimon-spark/paimon-spark-3.4 -am -DskipTests compile`

预期：PASS。

---

## 自检

- 计划通过 mandatory early events、active operation state、Summary 表、Stuck Operations 页面覆盖生产卡住任务诊断需求。
- 计划将 Spark 兼容范围限制为 Spark 3.4.x，符合“其他版本不做要求”。
- History replay 已纳入计划；具体机制需要先对 Spark 3.4.4 做 API spike，因为 custom event logging 行为必须实测确认。
