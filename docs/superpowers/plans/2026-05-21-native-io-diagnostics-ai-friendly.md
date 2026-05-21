# Native IO Diagnostics AI-Friendly UI 实施计划

> Spec: [`docs/superpowers/specs/2026-05-21-native-io-diagnostics-ai-friendly-design.md`](../specs/2026-05-21-native-io-diagnostics-ai-friendly-design.md)
> Status: ready-for-execution
> Date: 2026-05-21

## 目标

将现有 v1 native-io UI（8 页 SparkUITab + History Server SPI）演进为 AI-friendly v2 设计，使任何 LLM/工程师只看 UI 与 REST API 就能回答 5 个核心问题：

1. 当前 SQL 是否走 native？为什么？
2. 当前瓶颈是什么？
3. native 占多少并发？
4. 此刻 native 在做什么？
5. 慢，怎么慢的？

## 架构变更（从 v1 到 v2）

| 维度 | v1 当前 | v2 目标 |
|------|---------|---------|
| UI 页面 | 8 页 (Active/Stuck/Recent/Completed/Rejected/Aggregates/Timeline/About) | 3 页 (Summary/Detail/Timeline) |
| 数据组织 | 以 NativeIOOperationKey 为主键 | 以 SQL Execution Id 为主键，挂载所有 native 操作 |
| 事件接入 | JNI callback → NativeIODiagnostics.record() 同步写 store | Batcher 异步聚合 → RPC → driver store/listenerBus → event log |
| 瓶颈识别 | 无自动诊断 | 11 条 BottleneckRule，输出 reason / hint / confidence |
| REST API | 无 | 4 个端点（session/summary/detail/timeline） |
| 性能 | 全采样 | OBJECT_REQUEST 5% / PHASE_PROGRESS off / 内存 5s 节流，kill-switch |
| Reject Reason | 30 个 String 常量 | 加 reasonCode + actionableHint |

## 技术栈

- Scala 2.12 / Java 8 / Spark 3.4.4
- Jackson（已在 paimon-core 中可用，需在 paimon-spark-3.4 显式声明）
- jsoup（仅测试，验证 UI HTML 结构）
- 不引入 JS 图表库；时间线使用 ASCII 堆叠条
- Rust JNR 通过现有 `PaimonJnrLoader`，新增 event 变体不破坏 ABI

## 给 agentic worker 的提示

- 每个 Task 都遵循 TDD 顺序：**写失败测试 → 跑测试确认红 → 实现 → 跑测试确认绿 → commit**。
- 所有验证必须在容器内：`docker run --rm -v "$PWD":/workspace -w /workspace monster830/paimon-plus:spark344-java8 ...`。
- 单 Task 通过后立即 commit；不要批量 commit。
- 修改 Spec 字段时，同步更新 [Spec 文档](../specs/2026-05-21-native-io-diagnostics-ai-friendly-design.md)（per CLAUDE.md）。
- 工作区可能含他人改动，只动本任务相关文件。

## 文件结构总览

### 新建

```
paimon-core/src/main/java/org/apache/paimon/operation/nativeio/diagnostics/
  ├── NativeIODiagnosticsConfig.java
  ├── NativeIODiagnosticsBatcher.java
  ├── BottleneckRule.java
  ├── BottleneckRuleEngine.java
  └── rules/
      ├── ObsReadRule.java
      ├── ParquetDecodeRule.java
      ├── FilterDvRule.java
      ├── ParquetEncodeRule.java
      ├── ObsWriteRule.java
      ├── MultipartFinishRule.java
      ├── CpuIdleRule.java
      ├── MemoryPressureRule.java
      ├── JniWaitRule.java
      ├── JavaOverheadRule.java
      └── StageSkewRule.java

paimon-spark/paimon-spark-3.4/src/main/scala/org/apache/paimon/spark/nativeio/diagnostics/
  ├── SqlExecutionState.scala
  ├── NativeIOApi.scala                    (REST handler)
  ├── pages/
  │   ├── SummaryPage.scala
  │   ├── DetailPage.scala
  │   └── TimelinePage.scala
  └── BottleneckRenderer.scala
```

### 修改

```
paimon-native-io/rust/paimon-native-io-c/src/lib.rs
  - 新增 NativeDiagnosticEvent 字段
  - read_ms / decode_ms 拆分
  - parquet_row_groups_pruned 真实计数
  - OBS_REQUEST_START / END / FAIL 上报
  - MEMORY_SNAPSHOT 5s 周期
  - PHASE_PROGRESS 默认 off

paimon-core/src/main/java/org/apache/paimon/operation/nativeio/
  ├── NativeRejectReason.java              (加 reasonCode / actionableHint)
  ├── NativeIOOptions.java                 (新增 v2 配置项)
  └── diagnostics/
      ├── NativeIOEvent.java               (字段：reasonCode, hint, bottlenecks…)
      ├── NativeIOEventType.java           (新增 14 个变体)
      ├── NativeIOEventJson.java           (jackson 适配)
      └── NativeIODiagnosticsEmitter.java  (修复静默吞错)

paimon-native-io/src/main/java/org/apache/paimon/nativeio/export/
  └── PaimonNativeExporter.java            (FFI_ENTERED, callback 错误计数)

paimon-spark/paimon-spark-common/src/main/java/org/apache/paimon/spark/procedure/
  └── ExportParquetProcedure.java          (PLAN_REJECT / JAVA_FALLBACK_* / COMMIT_*)

paimon-spark/paimon-spark-3.4/src/main/scala/org/apache/paimon/spark/nativeio/diagnostics/
  ├── NativeIOStore.scala                  (sqlId top-level)
  ├── NativeIODiagnostics.scala            (Batcher 集成)
  ├── NativeIORpcSupport.scala             (backpressure + retry)
  ├── NativeIOLifecycleListener.scala      (SQL/Stage/Task 钩子)
  ├── NativeIOHistoryListener.scala        (v2 event 解析)
  ├── NativeIOHistoryServerPlugin.scala    (v2 SetupUI)
  ├── NativeIOTab.scala                    (cut to 3 pages)
  ├── NativeIOTabSupport.scala             (REST 注册)
  └── (删除 v1 8 个 NativeIO*Page.scala)

paimon-spark/paimon-spark-common/src/main/scala/org/apache/paimon/spark/extensions/
  └── PaimonSparkSessionExtensions.scala   (适配新 install API)

paimon-spark/paimon-spark-3.4/pom.xml      (jackson-databind, jsoup test scope)
docs/superpowers/specs/2026-05-21-native-io-diagnostics-ai-friendly-design.md
docs/superpowers/agent-guide.md            (新增 native-io 诊断引导)
```

### 删除（v1 SparkUITab 老页面）

```
paimon-spark/paimon-spark-3.4/src/main/scala/org/apache/spark/ui/
  ├── NativeIOActivePage.scala
  ├── NativeIOStuckPage.scala
  ├── NativeIORecentPage.scala
  ├── NativeIOCompletedPage.scala
  ├── NativeIORejectedPage.scala
  ├── NativeIOAggregatesPage.scala
  ├── NativeIOTimelinePage.scala (v1 实现)
  └── NativeIOAboutPage.scala
```

（v1 的 `NativeIOPage.scala` 保留为入口跳转壳，渲染交给 v2 三页。）

## 实施阶段总览

| 阶段 | 范围 | Task 数 | 依赖 |
|------|------|---------|------|
| Phase 1 | Rust 事件扩展 | 7 | 无 |
| Phase 2 | Java 边界与拒绝原因 | 4 | Phase 1（事件枚举共享） |
| Phase 3 | 配置 + Batcher + RPC 增强 | 5 | Phase 2 |
| Phase 4 | Store 重构 + 生命周期监听 | 4 | Phase 3 |
| Phase 5 | BottleneckRuleEngine | 4 | Phase 4 |
| Phase 6 | REST API | 4 | Phase 5 |
| Phase 7 | UI 三页重写 | 4 | Phase 6 |
| Phase 8 | History Server v2 | 3 | Phase 7 |
| Phase 9 | 集成 / 性能 / 文档 | 3 | Phase 1–8 |

---

## Phase 1 — Rust 事件扩展

> 目的：让 native 侧产出诊断所需的全部原子事件与字段，为后续 Java/Spark 层提供数据基础。

### Task 1 — 扩展 `NativeDiagnosticEvent` 字段

**Files**

- Modify: `paimon-native-io/rust/paimon-native-io-c/src/lib.rs` 的 `NativeDiagnosticEvent`（92-105 行）
- Test (inline): `lib.rs` 既有 `#[cfg(test)] mod` 中加 `diagnostic_event_serializes_v2_fields`

**Steps**

1. 在 `NativeDiagnosticEvent` 中追加 `Option<u64>` 字段：`object_request_id`、`object_size_bytes`、`object_range_offset`、`object_range_length`、`error_code`、`error_message`、`reason_code`、`actionable_hint`。
2. 写 failing test：构造一个事件，序列化为 JSON，断言含上述字段（snake_case）；先确认未实现时编译失败或缺字段。
3. 实现 `Serialize`（手写或 derive）。
4. `cargo test -p paimon-native-io-c diagnostic_event_serializes_v2_fields`（容器内 `cargo` 已就绪）。
5. Commit: `feat(native-io): extend NativeDiagnosticEvent with v2 fields`.

### Task 2 — 拆分 `read_ms` 与 `decode_ms`

**Files**

- Modify: `lib.rs` 2359-2363 计时器结构 + 2444-2481 row group / record batch 计时 + 2686-2697 metrics JSON 写回。
- Test (inline): `read_ms_separated_from_decode_ms`

**Steps**

1. 在 `NativeExportDiagnostics` 中拆 `read_ms`（仅 OBS / object_store 等 IO 等待）与 `decode_ms`（parquet record_batch 解码 CPU）。
2. failing test：构造一个 mock OBS reader 阻塞 50ms、解码 10ms 的场景，断言 read_ms≈50、decode_ms≈10。
3. 实现：在 `fetch_range_uncached` / `fetch_range_uncached_async` 周围用 `Instant::now()` 累计 read；保留原 decode timer 仅覆盖 record batch loop。
4. `cargo test read_ms_separated_from_decode_ms`。
5. Commit: `feat(native-io): separate read_ms from decode_ms timing`.

### Task 3 — 真实统计 `parquet_row_groups_pruned`

**Files**

- Modify: `lib.rs` 2684 行（当前写 0）+ row group 过滤逻辑（参考 2444 附近）。
- Test (inline): `parquet_pruned_counter_increments_when_predicate_eliminates_group`

**Steps**

1. 在 row group 过滤循环里增 `pruned_row_groups: u64`，每跳过一个递增。
2. failing test：构造两个 row group，predicate 排除其一，断言 metrics_json 里 `parquet_row_groups_pruned == 1`。
3. 实现：用 arrow `Statistics` predicate；累加进 diagnostics。
4. `cargo test parquet_pruned_counter_increments_when_predicate_eliminates_group`。
5. Commit: `feat(native-io): report real row group pruned count`.

### Task 4 — 包装 OBS 读取：`OBJECT_REQUEST_START` / `END` / `FAIL`

**Files**

- Modify: `lib.rs` `ObsObjectReader::fetch_range_uncached`（616-655）和 `_async`（723-762）+ 新事件枚举。
- Test (inline): `obs_read_emits_start_end_events`、`obs_read_fail_emits_error_code`

**Steps**

1. 在 `NativeDiagnosticEvent::event_type` 枚举中加 `OBJECT_REQUEST_START` / `OBJECT_REQUEST_END` / `OBJECT_REQUEST_FAIL`（与 Java `NativeIOEventType` 名字一致）。
2. failing test：mock 一个 obs reader 返回 ok 和 err 两种路径，断言事件序列。
3. 实现：包装 `get_object` 调用，前后发事件；FAIL 时填 `error_code` / `error_message`，按事件采样率（默认 5%）控制 START；END/FAIL 始终发。
4. `cargo test obs_read_emits_start_end_events obs_read_fail_emits_error_code`。
5. Commit: `feat(native-io): emit OBJECT_REQUEST_START/END/FAIL for OBS reads`.

### Task 5 — 包装 OBS 写：`put_object` / `submit_part` / `complete`

**Files**

- Modify: `lib.rs` `MultipartUploadState::put_object`（1944-1972）、`submit_part`（2010-2068）、`complete`（1893-1942）、`ensure_multipart_upload`（1974-2008）。
- Test (inline): `obs_write_emits_phase_events_for_all_three_paths`

**Steps**

1. 同 Task 4 的事件结构，但区分 `phase`: `OBJECT_PUT` / `OBJECT_PART_UPLOAD` / `OBJECT_COMPLETE_MULTIPART`。
2. failing test：mock 三条写路径，断言每条都发 START/END。
3. 实现：包装三个函数。
4. `cargo test obs_write_emits_phase_events_for_all_three_paths`。
5. Commit: `feat(native-io): emit OBJECT_REQUEST events for OBS writes`.

### Task 6 — 周期 `MEMORY_SNAPSHOT`（5s 节流）

**Files**

- Modify: `lib.rs` `export_parquet_with_diagnostics`（2321-2711）主循环。
- Test (inline): `memory_snapshot_emits_every_5s`（用 mock clock）

**Steps**

1. 在主循环中维持 `last_memory_snapshot_at: Instant`；每次 row group flush 后检查间隔 ≥ 5s 则发 `MEMORY_SNAPSHOT`，字段：`native_memory_bytes`、`peak_buffered_bytes`、`queue_depth`。
2. failing test：用 `mock_instant` crate 或注入 clock，advance 6s，断言至少 1 条快照。
3. 实现。
4. `cargo test memory_snapshot_emits_every_5s`。
5. Commit: `feat(native-io): emit periodic MEMORY_SNAPSHOT events`.

### Task 7 — `PHASE_PROGRESS` 改为默认 off，加 `threads_max`

**Files**

- Modify: `lib.rs` 现有 PHASE_PROGRESS 调用点 + diagnostics 配置。
- Test (inline): `phase_progress_default_off`、`threads_max_reported`

**Steps**

1. 加 `diagnostics_config.phase_progress_enabled: bool` 默认 false；调用点 gate。
2. 在每个 `NativeDiagnosticEvent` 中带 `threads_max: u32`（来自 rayon thread_pool 大小或 tokio handle）。
3. failing test：默认配置下断言无 PHASE_PROGRESS 事件；显式开启后有。
4. 实现。
5. `cargo test phase_progress_default_off threads_max_reported`。
6. Commit: `feat(native-io): gate PHASE_PROGRESS, add threads_max metric`.

---

## Phase 2 — Java 边界与拒绝原因

> 目的：把 Rust 新事件转成 `NativeIOEvent`，并在 Spark 端把 reject 与 fallback 流程的可观测性补齐。

### Task 8 — `NativeRejectReason` 增 `reasonCode` 与 `actionableHint`

**Files**

- Modify: `paimon-core/src/main/java/org/apache/paimon/operation/nativeio/NativeRejectReason.java` (22-54)
- Test (new): `paimon-core/src/test/java/org/apache/paimon/operation/nativeio/NativeRejectReasonTest.java`

**Steps**

1. 把原 30 个 String 常量改成 enum，每项含 `reasonCode`（snake_case）+ `actionableHint`（一句话给运维或 AI）。
2. failing test：断言 enum 数量、code 唯一、hint 非空。
3. 实现 enum；同步更新使用方（`ExportParquetProcedure` 等的 `LOG.warn` 处保持二进制兼容，传 `reason.message()`）。
4. `mvn -pl paimon-core -Pfast-build -Dtest=NativeRejectReasonTest test`
5. Commit: `feat(native-io): convert NativeRejectReason to enum with code+hint`.

### Task 9 — `NativeIOEventType` 新增 14 个变体 + `NativeIOPhase` 扩展

**Files**

- Modify: `paimon-core/src/main/java/org/apache/paimon/operation/nativeio/diagnostics/NativeIOEventType.java`
- Modify: `paimon-core/src/main/java/org/apache/paimon/operation/nativeio/diagnostics/NativeIOPhase.java`
- Test (new): `NativeIOEventTypeTest.java`

**Steps**

1. 在 `NativeIOEventType` 中加：`OBJECT_REQUEST_START`、`OBJECT_REQUEST_END`、`OBJECT_REQUEST_FAIL`、`MEMORY_SNAPSHOT`、`RUNTIME_SNAPSHOT`、`PLAN_REJECT`、`PLAN_REJECT_DETAIL`、`JAVA_FALLBACK_START`、`JAVA_FALLBACK_END`、`COMMIT_START`、`COMMIT_END`、`COMMIT_FAIL`、`DIAGNOSTICS_HEALTH_FINAL`、`KILLSWITCH_TRIGGERED`、`SESSION_FINAL`。
2. 在 `NativeIOPhase` 中加：`OBJECT_PUT`、`OBJECT_PART_UPLOAD`、`OBJECT_COMPLETE_MULTIPART`、`COMMIT`、`PLANNING`、`FALLBACK`。
3. failing test：断言枚举完整、`fromString` 大小写不敏感。
4. 实现。
5. `mvn -pl paimon-core -Pfast-build -Dtest=NativeIOEventTypeTest test`
6. Commit: `feat(native-io): add v2 event types and phases`.

### Task 10 — `NativeIODiagnosticsEmitter` 修复静默吞错 + `errorCount` 计数

**Files**

- Modify: `paimon-core/src/main/java/org/apache/paimon/operation/nativeio/diagnostics/NativeIODiagnosticsEmitter.java` (40)
- Modify: `paimon-native-io/src/main/java/org/apache/paimon/nativeio/export/PaimonNativeExporter.java` (71-78)
- Test (new): `paimon-core/src/test/java/.../NativeIODiagnosticsEmitterErrorCountTest.java`

**Steps**

1. failing test：注入一个会抛 `JsonParseException` 的 listener，调用 N 次后断言 `emitter.parseErrorCount() == N`，且首条错误进入 debug 日志（用 logback test appender）。
2. 实现：去掉 `} catch (Throwable ignored) {`，改为 `LOG.debug("native diagnostic event parse failed", e)`，并维护 `AtomicLong parseErrorCount`；对外暴露 `getParseErrorCount()`。在 `PaimonNativeExporter` callback parse 失败处同样累计。
3. 跑测试通过。
4. `mvn -pl paimon-core -Pfast-build -Dtest=NativeIODiagnosticsEmitterErrorCountTest test`
5. Commit: `fix(native-io): surface diagnostic emitter parse errors via counter+debug log`.

### Task 11 — `ExportParquetProcedure` 发 `PLAN_REJECT` / `JAVA_FALLBACK_*` / `COMMIT_*`

**Files**

- Modify: `paimon-spark/paimon-spark-common/src/main/java/org/apache/paimon/spark/procedure/ExportParquetProcedure.java` (321, 324-337, 572-607, 696, 749)
- Test (new): `paimon-spark/paimon-spark-ut/src/test/scala/.../ExportParquetEventEmissionTest.scala`

**Steps**

1. failing test：在 IT 中收集 `SparkListenerNativeIOEvent`（注册 listener），跑一次会触发 reject 的 export，断言含 `PLAN_REJECT` + `JAVA_FALLBACK_START` + `JAVA_FALLBACK_END`；正常 native 路径断言含 `COMMIT_START` + `COMMIT_END`。
2. 实现：
   - reject 后调用 `NativeIODiagnostics.recordPlanReject(sqlId, reason, hint)`。
   - fallback 前后调用 `recordJavaFallbackStart/End`。
   - `_SUCCESS` 提交前后调用 `recordCommitStart/End`，失败时 `recordCommitFail(throwable)`。
   - 三个 helper 都在 `NativeIODiagnostics` 中新增，签名稳定。
3. `mvn -pl paimon-spark/paimon-spark-ut -am -Pfast-build,native-io,spark3 -DwildcardSuites=org.apache.paimon.spark.procedure.ExportParquetEventEmissionTest -Dtest=none test`
4. Commit: `feat(native-io): emit plan/fallback/commit lifecycle events from ExportParquetProcedure`.

---

## Phase 3 — 配置 + Batcher + RPC 增强

### Task 12 — `NativeIODiagnosticsConfig`（采样 / 节流 / kill-switch 阈值）

**Files**

- New: `paimon-core/src/main/java/org/apache/paimon/operation/nativeio/diagnostics/NativeIODiagnosticsConfig.java`
- Modify: `paimon-core/src/main/java/org/apache/paimon/operation/nativeio/NativeIOOptions.java`
- Test (new): `NativeIODiagnosticsConfigTest.java`

**Steps**

1. failing test：从 `Configuration` 解析，断言默认值（per spec：obj 5%、phase 0%、memory 5s、batch=100、flush=200ms、buffer=10000、drop_threshold=0.5、drop_window=60s）。
2. 实现：在 `NativeIOOptions` 中新增 ConfigOption 常量（29 个 v2 项），Config 类从中读取，提供 immutable getter。
3. `mvn -pl paimon-core -Pfast-build -Dtest=NativeIODiagnosticsConfigTest test`
4. Commit: `feat(native-io): introduce NativeIODiagnosticsConfig with v2 options`.

### Task 13 — `NativeIODiagnosticsBatcher` 环形缓冲 + 定时 flush

**Files**

- New: `paimon-core/src/main/java/org/apache/paimon/operation/nativeio/diagnostics/NativeIODiagnosticsBatcher.java`
- Test (new): `NativeIODiagnosticsBatcherTest.java`

**Steps**

1. failing test：
   - 容量 10、阈值 5、flush 100ms。
   - push 7 条后 100ms 内观察到 1 次 flush（含 5 条），剩 2 条在 100ms 后 flush。
   - push 20 条立即触发，超出容量的最旧 10 条进入 `droppedCount`。
2. 实现：单生产单 flush 线程（`ScheduledExecutorService`），尾部丢弃旧条，flush 调用注入的 `Consumer<List<NativeIOEvent>>`。
3. `mvn -pl paimon-core -Pfast-build -Dtest=NativeIODiagnosticsBatcherTest test`
4. Commit: `feat(native-io): add ring-buffer batcher for diagnostic events`.

### Task 14 — Batcher 接入 `NativeIODiagnostics.record`

**Files**

- Modify: `paimon-spark/paimon-spark-3.4/src/main/scala/.../NativeIODiagnostics.scala` (52, 73)
- Test (modify): `NativeIODiagnosticsInstallSuite.scala`

**Steps**

1. failing test：注入 mock RPC，连发 200 条事件，断言 batcher 聚合为 ≤4 次 RPC（默认 100/批）。
2. 实现：`record()` 改为 `batcher.offer(event)`；batcher 的 flush callback 调 `sendToDriver(batch)`。Driver 直接消费 batch（List）。
3. `mvn -pl paimon-spark/paimon-spark-3.4 -am -Pfast-build,native-io,spark3 -DwildcardSuites=...NativeIODiagnosticsInstallSuite -Dtest=none test`
4. Commit: `feat(native-io): route events through batcher before RPC`.

### Task 15 — RPC 重试 + driver 端 backpressure

**Files**

- Modify: `paimon-spark/paimon-spark-3.4/src/main/scala/.../NativeIORpcSupport.scala` (47, 55, 85)
- Test (new): `NativeIORpcSupportBackpressureSuite.scala`

**Steps**

1. failing test：
   - mock driver endpoint，500ms 不返回；executor 端 batcher 不应阻塞 callback 主线程。
   - 连续 RPC 失败 3 次后，executor 端开关切到 local-only（仍写 event log，不再 send to driver）。
2. 实现：`sendToDriver` 改为 `ask` 带 timeout 2s 与 3 次指数退避；失败计数过阈值后置位 `executorDisabled.set(true)`，记录 `KILLSWITCH_TRIGGERED` 事件（reason=`rpc_unhealthy`）。
3. 跑测试。
4. Commit: `feat(native-io): rpc retry with backpressure and kill-switch`.

### Task 16 — 全局 kill-switch（连续 60s drop_rate>50%）

**Files**

- Modify: `NativeIODiagnostics.scala`、`NativeIODiagnosticsBatcher.java`
- Test (new): `NativeIOKillSwitchSuite.scala`

**Steps**

1. failing test：构造 batcher 在 60s 滚动窗口 drop_rate=0.6，断言 1) 触发 `KILLSWITCH_TRIGGERED` 事件、2) `record()` 后续直接 short-circuit。
2. 实现：Batcher 暴露 `dropRate(windowMs)`；Diagnostics 周期监控（同 lifecycle 的 5s 定时器复用），超阈值置位全局 disabled，并发 KILLSWITCH 事件（带 hint：`reduce paimon.native.diagnostics.batch.flush.events.max`）。
3. 跑测试。
4. Commit: `feat(native-io): global kill-switch on sustained drop_rate`.

---

## Phase 4 — Store 重构 + 生命周期监听

### Task 17 — `SqlExecutionState` 数据模型

**Files**

- New: `paimon-spark/paimon-spark-3.4/src/main/scala/.../SqlExecutionState.scala`
- Test (new): `SqlExecutionStateSuite.scala`

**Steps**

1. failing test：构造一个 case-class 状态机，断言：
   - `addStage(stageId, attemptId)` / `addTask(stageId, taskId, executorId)` / `addNativeOp(opKey)` / `complete(endTime)` 顺序正确。
   - `snapshot()` 返回 immutable view，含 nativeOps、stages、tasks、bottlenecks。
2. 实现：scala case class + companion with `Builder`。
3. `mvn ... -DwildcardSuites=...SqlExecutionStateSuite test`
4. Commit: `feat(native-io): introduce SqlExecutionState model`.

### Task 18 — `NativeIOStore` 改为 sqlId 顶层索引 + LRU

**Files**

- Modify: `paimon-spark/paimon-spark-3.4/src/main/scala/.../NativeIOStore.scala` (36, 56, 60, 69, 80)
- Test (modify): `NativeIOStoreSuite.scala`

**Steps**

1. failing test：
   - record 一条 native op（绑定 sqlId=42）和一条无 sqlId 的事件，断言 store.byExecution(42) 含该 op。
   - 写 300 条 completed execution，断言只保留最近 200（LRU）。
2. 实现：
   - 用 `ConcurrentHashMap[Long, SqlExecutionState]` 作主表。
   - 无 sqlId 事件先入 `pendingByOpKey`，待 `lateBind(opKey, sqlId)` 触发后挪入主表。
   - completed 上限来自配置；超限按 endTime 最旧者淘汰。
3. `mvn ... -DwildcardSuites=...NativeIOStoreSuite test`
4. Commit: `refactor(native-io): re-key store by sql execution id with LRU eviction`.

### Task 19 — `NativeIOLifecycleListener` 接 SparkListener SQL/Stage/Task 钩子

**Files**

- Modify: `paimon-spark/paimon-spark-3.4/src/main/scala/org/apache/spark/scheduler/NativeIOLifecycleListener.scala`
- Test (new): `NativeIOLifecycleListenerSuite.scala`

**Steps**

1. failing test：
   - 注入 listener，post `SparkListenerSQLExecutionStart`、`SparkListenerStageSubmitted`、`SparkListenerTaskStart/End`、`SparkListenerSQLExecutionEnd` 序列。
   - 断言 store 中 sqlId 对应状态正确扩展。
2. 实现：override 上述方法，转交 `NativeIOStore`；SQL_END 触发 ruleEngine 一次终局诊断（详见 Phase 5）。
3. `mvn ... -DwildcardSuites=...NativeIOLifecycleListenerSuite test`
4. Commit: `feat(native-io): bind SQL/Stage/Task lifecycle into diagnostics store`.

### Task 20 — Late binding：opKey 后到事件归并到 sqlId

**Files**

- Modify: `NativeIOStore.scala` + `NativeIOLifecycleListener.scala`
- Test (new): `NativeIOLateBindingSuite.scala`

**Steps**

1. failing test：先 record OBJECT_REQUEST_START（仅有 opKey），再 record NATIVE_OPERATION_START（含 sqlId 与 opKey）。断言前者最终被挂到 sqlId 下。
2. 实现：维护 `pendingByOpKey: Map[OpKey, Vector[NativeIOEvent]]`；NATIVE_OPERATION_START 时 drain 到 SqlExecutionState；超过 5 分钟未绑定则进入 `orphan` 桶（供 REST 报告）。
3. `mvn ... -DwildcardSuites=...NativeIOLateBindingSuite test`
4. Commit: `feat(native-io): late-bind orphan events to sql execution id`.

---

## Phase 5 — Bottleneck Rule Engine

### Task 21 — `BottleneckRule` 接口与 `BottleneckRuleEngine` 框架

**Files**

- New: `paimon-core/src/main/java/.../diagnostics/BottleneckRule.java`
- New: `paimon-core/src/main/java/.../diagnostics/BottleneckRuleEngine.java`
- New: `paimon-core/src/main/java/.../diagnostics/BottleneckDiagnosis.java`
- Test (new): `BottleneckRuleEngineTest.java`

**Steps**

1. failing test：注入两条 no-op rule，断言 engine.evaluate(state) 返回有序列表 + confidence 计算公式（`min(1, signal × coverage) × (sampled ? 0.85 : 1.0)`）。
2. 实现：
   - `BottleneckRule { String code(); BottleneckDiagnosis evaluate(SqlExecutionStateSnapshot); }`
   - `BottleneckDiagnosis { code, severity (low/medium/high), confidence, hint }`
   - Engine 顺序执行，按 severity desc + confidence desc 排序。
3. `mvn -pl paimon-core -Pfast-build -Dtest=BottleneckRuleEngineTest test`
4. Commit: `feat(native-io): bottleneck rule engine scaffold`.

### Task 22 — 规则 1–6：obs_read / parquet_decode / filter_dv / parquet_encode / obs_write / multipart_finish

**Files**

- New 6 个 rule 类位于 `paimon-core/.../diagnostics/rules/`
- Test (new): `BottleneckRulesIOTest.java`（覆盖六条）

**Steps**

1. failing test：每条 rule 给一个达到阈值的合成 state（per spec 阈值定义），断言触发；另一个未达阈值的，断言不触发。
2. 实现：从 SqlExecutionStateSnapshot 聚合 metric（如 `read_ms_sum / wallclock_ms`），按 spec §8.1 表给的阈值与公式判定。
3. `mvn ... -Dtest=BottleneckRulesIOTest test`
4. Commit: `feat(native-io): add bottleneck rules for IO/CPU paths`.

### Task 23 — 规则 7–11：cpu_idle / memory_pressure / jni_wait / java_overhead / stage_skew

**Files**

- New 5 个 rule 类
- Test (new): `BottleneckRulesRuntimeTest.java`

**Steps**

1. failing test：同 Task 22 模式。`stage_skew` 用 stage 内 task 时长分布（p95/p50 > 阈值）触发。
2. 实现。
3. `mvn ... -Dtest=BottleneckRulesRuntimeTest test`
4. Commit: `feat(native-io): add runtime bottleneck rules`.

### Task 24 — Engine 定时整合（5s 周期 + SQL_END 终局）

**Files**

- Modify: `NativeIODiagnostics.scala`、`NativeIOLifecycleListener.scala`
- Test (new): `BottleneckEngineIntegrationSuite.scala`

**Steps**

1. failing test：跑一个 mock SQL 全流程，期间 5s 定时触发 ≥1 次 evaluate（写入 SqlExecutionState.snapshot.bottlenecks）；SQL_END 触发最终 evaluate 且发 `DIAGNOSTICS_HEALTH_FINAL` 事件。
2. 实现：driver 端单线程 `ScheduledExecutorService`，对 active SqlExecutionState 周期 evaluate。
3. 跑测试。
4. Commit: `feat(native-io): periodic + final bottleneck evaluation`.

---

## Phase 6 — REST API

> 全部走 Spark 的 `JettyUtils.createServletHandler` 在 `SparkUI` 注册 `/native-io/api/*`。

### Task 25 — `NativeIOApi` 框架与 jackson writer

**Files**

- New: `paimon-spark/paimon-spark-3.4/src/main/scala/.../NativeIOApi.scala`
- Modify: `paimon-spark-3.4/pom.xml` 加 `jackson-databind` 显式依赖（compile）+ `jsoup` test scope
- Test (new): `NativeIOApiFrameworkSuite.scala`

**Steps**

1. failing test：起一个内嵌 jetty，GET 任意未注册路径返回 404 + JSON `{error}`。
2. 实现：handler trait `Endpoint { path; handle(req): JsonNode }`，注册到 `SparkUI.attachHandler`。
3. `mvn ... -DwildcardSuites=...NativeIOApiFrameworkSuite test`
4. Commit: `feat(native-io): rest api scaffold under /native-io/api`.

### Task 26 — `/native-io/api/session`

**Files**

- New: `NativeIOApi.scala` 加 SessionEndpoint
- Test (new): `NativeIOSessionApiSuite.scala`

**Steps**

1. failing test：返回字段 `appId`、`startedAt`、`config`（v2 字段子集）、`diagnosticsHealth` (drop_rate / killSwitchActive / errorCount)、`installed: true/false`。
2. 实现：从 `SparkContext.getConf` + `NativeIODiagnostics.healthSnapshot()` 拼装。
3. 跑测试。
4. Commit: `feat(native-io): rest api /session`.

### Task 27 — `/native-io/api/summary`

**Files**

- New endpoint
- Test (new): `NativeIOSummaryApiSuite.scala`

**Steps**

1. failing test：在 store 中放 3 个 SqlExecutionState（含 native / rejected / failed 三类），断言 summary 含 counts、活跃 native ops、top bottlenecks、最近 N 个 sqlId 概览。
2. 实现：聚合 store.byExecution(*)，限制 size（默认 50）。
3. 跑测试。
4. Commit: `feat(native-io): rest api /summary`.

### Task 28 — `/native-io/api/detail/{sqlId}` + `/timeline/{sqlId}`

**Files**

- New endpoints
- Test (new): `NativeIODetailApiSuite.scala`

**Steps**

1. failing test：
   - detail 返回 plan / native ops / bottlenecks / commit info / reject reason / hint。
   - timeline 返回 events 按 phase 聚合（read / decode / filter / encode / write / commit / idle / jni / fallback），每 phase 给 startMs/endMs/durationMs/sampleEvents（≤10）。
   - 不存在的 sqlId 返回 404。
2. 实现。
3. 跑测试。
4. Commit: `feat(native-io): rest api /detail and /timeline`.

---

## Phase 7 — UI 三页重写

> 删除 v1 八页；保留 `NativeIOPage.scala` 作为 `/native-io` 根入口跳转壳。

### Task 29 — `SummaryPage`

**Files**

- New: `.../diagnostics/pages/SummaryPage.scala`
- Test (new): `SummaryPageRenderSuite.scala`（用 jsoup 校验 HTML 节点）

**Steps**

1. failing test：构造 store 数据 → 渲染 → jsoup parse，断言含 5 个核心问答区块 ID（`q1-is-native` 等）、`Top Bottlenecks` 表、`Active Native Ops` 表、`Native vs Java` 计数。
2. 实现：纯 scala XML，无 JS；表格用 spark-ui 既有 css class。
3. `mvn ... -DwildcardSuites=...SummaryPageRenderSuite test`
4. Commit: `feat(native-io-ui): summary page answering 5 core questions`.

### Task 30 — `DetailPage`

**Files**

- New: `pages/DetailPage.scala`
- Test (new): `DetailPageRenderSuite.scala`

**Steps**

1. failing test：渲染一个含 native / rejected / fallback 的 sqlId，断言：
   - reject reason 块（reasonCode + hint）。
   - bottleneck 块列出 code、severity、confidence、hint。
   - native ops 表。
   - 链接到 timeline 页。
2. 实现。
3. 跑测试。
4. Commit: `feat(native-io-ui): detail page per sql execution`.

### Task 31 — `TimelinePage`（ASCII 堆叠条 + 文本块）

**Files**

- New: `pages/TimelinePage.scala`
- New: `BottleneckRenderer.scala`
- Test (new): `TimelinePageRenderSuite.scala`

**Steps**

1. failing test：给定 timeline JSON（phase 比例 read=40% decode=30% encode=20% other=10%），断言渲染出 40 个 `#`、30 个 `=`、20 个 `+`、10 个 `.`（或固定符号映射），并在 details `<pre>` 中包含每 phase 的 ms 与 sample 事件。
2. 实现：phase → 字符固定映射；总长 80 字符；ASCII 表格分别列出每个 native op 的子条形。
3. 跑测试。
4. Commit: `feat(native-io-ui): ascii timeline page`.

### Task 32 — `NativeIOTab` 裁为三页 + 删除 v1 老页

**Files**

- Modify: `paimon-spark-3.4/src/main/scala/org/apache/spark/ui/NativeIOTab.scala`
- Delete: `NativeIOActivePage` / `NativeIOStuckPage` / `NativeIORecentPage` / `NativeIOCompletedPage` / `NativeIORejectedPage` / `NativeIOAggregatesPage` / `NativeIOTimelinePage`(v1) / `NativeIOAboutPage`.scala
- Modify: `NativeIOPage.scala` 仅做跳转
- Test (modify): `NativeIOTabSupportSuite.scala`

**Steps**

1. failing test：附加 Tab 后断言 `tab.pages.map(_.prefix)` == `Seq("", "detail", "timeline")`（summary 落 root path）。
2. 实现：attach 三个 page，prefix 见上。`NativeIOTabSupport.attach` 同时注册 REST handler。
3. 跑测试。
4. Commit: `refactor(native-io-ui): collapse to summary/detail/timeline tabs`.

---

## Phase 8 — History Server v2

### Task 33 — 新增 final 事件（HEALTH / KILLSWITCH / SESSION_FINAL）写入 event log

**Files**

- Modify: `NativeIODiagnostics.scala`（SQL_END 或 SparkContext shutdown 时落盘）
- Modify: `NativeIORpcSupport.scala`（postToEventLog）
- Test (new): `NativeIOEventLogV2Suite.scala`

**Steps**

1. failing test：跑一个完整 SQL，断言 event log 中出现 `DIAGNOSTICS_HEALTH_FINAL`、`SESSION_FINAL`；触发 kill-switch 场景出现 `KILLSWITCH_TRIGGERED`。
2. 实现：lifecycle 在 SQL_END 时 emit；driver shutdown hook emit SESSION_FINAL。
3. 跑测试。
4. Commit: `feat(native-io): emit health/kill-switch/session-final events to log`.

### Task 34 — `NativeIOHistoryListener` 解析 v2 事件并重建 store

**Files**

- Modify: `paimon-spark-3.4/src/main/scala/org/apache/spark/scheduler/NativeIOHistoryListener.scala`
- Test (new): `NativeIOHistoryReplaySuite.scala`

**Steps**

1. failing test：从夹具 event log 文件（含 v1 + v2 事件混合）replay，断言 store 与 live 模式一致（用 snapshot equality）。
2. 实现：跟随 Task 9 新增的 EventType 列表，分发到 store；保留对未知 type 的宽容（debug log + 跳过）。
3. 跑测试。
4. Commit: `feat(native-io): history replay supports v2 events`.

### Task 35 — `NativeIOHistoryServerPlugin` SetupUI 改用三页

**Files**

- Modify: `paimon-spark-3.4/src/main/scala/org/apache/spark/status/NativeIOHistoryServerPlugin.scala` (32, 36)
- Test (new): `NativeIOHistoryServerPluginV2Suite.scala`

**Steps**

1. failing test：mock `SparkUI`，调用 `setupUI`，断言 attach 的 tab 为 v2 三页结构（与 live mode 同）。
2. 实现：复用 `NativeIOTabSupport.attach`。
3. 跑测试。
4. Commit: `feat(native-io): history server plugin uses v2 ui`.

---

## Phase 9 — 集成 / 性能 / 文档

### Task 36 — `ExportParquetSuite` E2E 增加 v2 断言

**Files**

- Modify: `paimon-spark/paimon-spark-ut/src/test/scala/.../ExportParquetProcedureTest.scala`（226 行；4 个测试）
- 可能 New: `ExportParquetV2DiagnosticsTest.scala`

**Steps**

1. failing test：跑现有 native + reject + fallback 三种场景，断言 REST `/native-io/api/detail/{sqlId}` 返回字段完整、`/timeline` 内 phase 总和 ≈ wallclock。
2. 实现：测试中通过 `SparkSession.sparkContext.statusStore` + JettyUtils 模拟请求。
3. `mvn -pl paimon-spark/paimon-spark-ut -am -Pfast-build,native-io,spark3 -DwildcardSuites=...ExportParquetV2DiagnosticsTest -Dtest=none test`
4. Commit: `test(native-io): e2e v2 diagnostics assertions`.

### Task 37 — 性能基准 harness（开关诊断对 export 影响 ≤3%）

**Files**

- Modify: `paimon-spark/paimon-spark-common/src/main/java/.../procedure/NativeIOBenchmarkProcedure.java`（工作区已有）
- Test (new): `NativeIOBenchmarkOverheadTest.scala`

**Steps**

1. failing test：在 200MB 合成数据集上跑两次 export（一次诊断关，一次默认采样），断言 `(withDiag - withoutDiag) / withoutDiag <= 0.03`。
2. 实现：让 benchmark procedure 输出 JSON metrics，测试解析比较。
3. `mvn ... -DwildcardSuites=...NativeIOBenchmarkOverheadTest test`
4. Commit: `test(native-io): benchmark overhead under default sampling`.

### Task 38 — 文档同步与 agent-guide

**Files**

- Modify: `docs/superpowers/specs/2026-05-21-native-io-diagnostics-ai-friendly-design.md`（如实施中字段微调需回填）
- Modify or New: `docs/superpowers/agent-guide.md` 加 native-io 诊断小节
- New: `docs/superpowers/runbooks/native-io-diagnostics.md`（运维 SOP）

**Steps**

1. 同步 spec（若实现中改动字段名或阈值）。
2. agent-guide 加：
   - 入口路径 `/native-io/`、REST endpoints 列表
   - 5 个核心问题对应 UI 区块的位置
   - kill-switch 触发后的恢复步骤
3. 无代码改动，无测试。Commit: `docs(native-io): v2 diagnostics guide and runbook`.

---

## 验证总览

| 阶段 | 最小验证命令（容器内） |
|------|------------------------|
| Phase 1 | `cargo test -p paimon-native-io-c` |
| Phase 2 | `mvn -pl paimon-core,paimon-native-io -Pfast-build test` |
| Phase 3 | `mvn -pl paimon-core,paimon-spark/paimon-spark-3.4 -am -Pfast-build,native-io,spark3 test` |
| Phase 4–5 | 同上 + 加 ut 模块 |
| Phase 6–7 | `mvn -pl paimon-spark/paimon-spark-3.4 -am -Pfast-build,native-io,spark3 test` |
| Phase 8 | 同上 |
| Phase 9 | `mvn -pl paimon-spark/paimon-spark-ut -am -Pfast-build,native-io,spark3 test` |
| Final | `mvn clean install -DskipTests -Pfast-build,native-io,spark3 -pl paimon-spark/paimon-spark-3.4 -am` |

## 风险与缓解

| 风险 | 缓解 |
|------|------|
| Rust 新事件 ABI 变化破坏老 Java | NativeDiagnosticEvent 新字段全部 `Option<T>`，JSON 反序列化容忍未知字段 |
| Batcher 丢事件导致诊断失真 | confidence 公式带 coverage 因子；drop_rate 高时 UI 显式标注 `sampled` |
| kill-switch 误触发 | 60s 窗口 + 50% 阈值（spec 可调）；触发同时发 hint 指导调优 |
| History replay 顺序错乱 | listener 按 event time 排序后再喂 store |
| 5s 定时器额外线程开销 | 单 daemon ScheduledExecutorService，全 driver 共用 |

## 完成定义

- 所有 38 个 Task 的测试在 `monster830/paimon-plus:spark344-java8` 镜像内 `mvn ... test` 全绿。
- `paimon-spark-3.4_2.12-1.4-SNAPSHOT.jar` 打包成功。
- UI 三页可在 Spark live + history server 模式下渲染。
- REST 4 个端点返回符合 spec 字段。
- agent-guide / runbook 更新并 push 到 `main`。


