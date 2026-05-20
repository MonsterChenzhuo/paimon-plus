# Paimon export_parquet Native Fast Path 设计

**日期**：2026-05-19  
**状态**：待评审  
**目标读者**：Paimon native IO / Spark SQL 维护者  

> 当前实现状态：native export SPI、配置校验、JNR FFI、JSON request/result 合约、Spark split 到 raw Parquet source file 的规划、DV position 下沉、基础 predicate JSON 转换，以及 Rust 本地/OBS Parquet 读写 pipeline 已落到代码中。当前实现仍是第一阶段 fast path：OBS 写出使用临时本地文件后单 PUT，暂未实现 multipart upload；`target_file_size` 已在 native writer 内按 batch 内存估算做文件滚动，尚不是基于最终压缩后对象大小的精确滚动。Rust export 读 Parquet 时会裁剪到输出列和 predicate 列，并在 OBS range reader 中汇总 read request/bytes 指标。

---

## 1. 背景

当前 `CALL sys.export_parquet` 的执行路径是：

1. Driver 侧用 Paimon scan 规划 split。
2. Spark executor 侧按 split 创建 `TableRead`。
3. `TableRead` 读取 `InternalRow`。
4. Java 侧逐行执行 projected predicate。
5. Java `ParquetWriter` 逐行写出目标 parquet。

`spark.paimon.native-io.enabled=true` 后，读侧可以命中 native reader，但 `export_parquet` 的导出循环仍是 row-by-row：

- `ExportParquetProcedure.exportSplits` 中每行调用 `iterator.next()`。
- 每行调用 `predicate.test(row)`。
- 每行调用 `RollingParquetWriter.addElement(...)`。
- native 输出的 Arrow batch 还会经过 `Arrow -> Paimon ColumnVector -> InternalRow -> ParquetWriter`。

一次线上诊断中，`application_1772605260987_22534` 的主要 stage 是 `collect at ExportParquetProcedure.java:252`，11 个 task 运行约 151 秒，无 shuffle、无 spill、GC ratio 约 1.8%，说明瓶颈不在 Spark shuffle/GC，而在 export procedure 自身的读写路径。`target_file_size => '512 MB'` 还把 `parallelism => 32` 压到了 11 个实际 task。

因此，仅修 `NativeFileRecordReader` 或调大 batch 无法达到极致性能。要让 `export_parquet` 变快，需要为该 procedure 新增一个专用 native fast path：Spark 只做规划和调度，executor task 直接调用 Rust 完成读、过滤、DV 应用、Parquet 编码和 OBS 写出。

---

## 2. 目标

### 2.1 性能目标

在 OBS 上导出 DV PK Parquet 表时：

- 相对当前 `spark.paimon.native-io.enabled=true` 但仍走 Java `export_parquet` 循环的路径，wall time 至少提升 2x。
- 相对纯 Java Paimon reader 路径，wall time 至少提升 1.5x。
- 对宽表 `columns => '*'` 场景，不能因为 Arrow/JVM row materialization 抵消 native 读侧收益。
- 对 `target_file_size` 场景，实际 task 并发不应被过度压低；文件滚动应在 native task 内完成。

### 2.2 正确性目标

native fast path 输出必须与现有 Java 路径语义一致：

- 导出列顺序、字段名、字段类型与 `columns` 参数一致。
- `where` 过滤结果一致。
- DV 删除行必须被过滤。
- 分区列、schema evolution、cast mapping、system fields 的可支持范围必须明确；第一阶段不支持需要 JVM cast mapping 的文件，遇到这类 split 整次回退 Java。
- 输出目录 overwrite 行为、`_SUCCESS`、返回 row count 与现有 procedure 保持一致。
- 任一不支持的条件必须整次回退 Java 路径，不能部分 native、部分 Java 混跑后产生难以解释的性能和语义差异。

### 2.3 可诊断目标

native fast path 必须提供足够指标解释性能：

- 每个 Spark task 输出 native metrics。
- 能区分 OBS 读、Parquet decode、predicate/DV filter、Parquet encode、OBS write 的耗时。
- 能看到 OBS range request 次数和字节数，避免再次出现“Spark input bytes 为 0，看不出实际 IO”的问题。
- 回退 Java 路径时必须给出稳定 reason，不靠解析日志文本。

---

## 3. 非目标

本设计不做以下事情：

- 不改普通 Spark SQL scan 的列式接口，不引入 `SupportsColumnarReads`。
- 不改 Spark DataSource V2 写路径。
- 不支持 Flink。
- 不支持 ORC、Avro、CSV。
- 不支持非 OBS 文件系统的 native export。
- 不支持非 OBS 输出路径；本地文件输出只用于 Rust/Java 测试，不作为 Spark 生产路径。
- 不支持非 DV PK 表的 native fast path。
- 不把复杂表达式、UDF、子查询等 Spark 表达式下推到 Rust。
- 不做 Paimon table commit；`export_parquet` 是外部导出，只写 output path 和 `_SUCCESS`。

---

## 4. 总体设计

新增 `export_parquet` native fast path：

```text
Spark driver
  ExportParquetProcedure
    parse arguments
    load table
    parse columns / where
    plan Paimon splits
    check native export applicability
    partition NativeExportTask list
      |
      v
Spark executor task
  NativeExportRunner
    deserialize NativeExportTask
    call PaimonNativeExporter via JNR
      |
      v
Rust libpaimon_native_io
  export task
    read OBS Parquet with buffered range reader
    project columns
    apply supported predicate
    apply deletion vector
    write OBS Parquet with zstd
    roll files by target_file_size
    return row count + metrics
```

Java 路径仍保留为 fallback。默认行为建议分两阶段：

- 开发阶段：`spark.paimon.native-io.export.enabled=false` 默认关闭，显式开启。
- 稳定后：可在 `spark.paimon.native-io.enabled=true` 且 `spark.paimon.native-io.export.enabled=true` 时自动尝试 native fast path。

### 4.1 模块与 classpath 边界

`ExportParquetProcedure` 位于 `paimon-spark-common`，而 native 实现位于 `paimon-native-io`。为了不让 Spark connector 在编译期强依赖 JNR、Arrow C Data 和 native resource，第一阶段采用 **ServiceLoader SPI** 而不是 Spark module 直接 import native export 类。

新增 core 级 SPI：

```text
paimon-core/src/main/java/org/apache/paimon/operation/nativeio/export/
  NativeExportProviderFactory.java
  NativeExportProvider.java
  NativeExportContext.java
  NativeExportPreflightResult.java
  NativeExportTaskResult.java
```

职责边界：

- `paimon-spark-common` 只依赖 `paimon-core` 中的 SPI 和轻量 DTO。
- `paimon-native-io` 通过 `META-INF/services/...NativeExportProviderFactory` 提供实现。
- native jar 不在 classpath、JNR 缺失、`.so` 不可用时，ServiceLoader 阶段不能破坏 Java export；这些情况必须表现为 `NO_PROVIDER` 或 `EXPORT_NATIVE_LIBRARY_UNAVAILABLE`。
- provider factory 构造阶段不能加载 native lib；真正的 `.so` 加载只允许发生在 executor task 调用 `PaimonNativeExporter` 时。

如果后续决定让 Spark artifact 显式依赖 `paimon-native-io`，必须先更新打包策略，确保 `paimon-spark-3.4_2.12` 的普通用户不因 native 依赖缺失而失败。本 spec 默认不采用直接依赖。

### 4.2 LakeSoul native-io 可借鉴点

LakeSoul 的 native-io 实现给 Paimon export fast path 提供了几个可以直接吸收的设计经验：

- **Java/Rust 边界**：LakeSoul 用 JNR + opaque pointer + `CStatus` / bytes result 管理错误与资源释放，Java 侧只负责构造 config、传 Arrow C Data 或读取结果。Paimon export 不需要 Arrow 批次跨 JNI 往返，但应借鉴这种“单一入口、显式 free、错误字符串稳定返回”的 C ABI 风格。
- **native lib 加载**：LakeSoul 的 `JnrLoader` 是单例加载；Paimon 当前已有 classloader-local `PaimonJnrLoader`、resource path、tmpdir 和 load failure 记录。export fast path 应复用现有 `PaimonJnrLoader`，不要另写 loader，避免同一 executor 内重复解压/加载 `.so`。
- **Hadoop conf 到 native options**：LakeSoul `NativeIOUtils` 从 Hadoop `Configuration` / `S3AFileSystem` 提取 bucket、AK/SK、endpoint、region、path-style、signer、defaultFS 和其他写入选项。Paimon export 必须有同等的配置收集层，从 Spark Hadoop conf、catalog options 和 table filesystem options 中生成 native object_store options。
- **配置集中化**：LakeSoul 的 `LakeSoulIOConfig` 同时承载 files、schema、partition schema、filter、object store options、batch size、row group size、multipart 参数和 memory/spill 参数。Paimon 应避免把 native export 参数散落在多个 Java 类和 Rust 函数参数里，统一成 versioned request/config。
- **object_store 注册**：LakeSoul 通过 `object_store` crate 和 DataFusion `RuntimeEnv` 注册 `s3/s3a/hdfs/file`，并统一做 endpoint、bucket、path-style、retry、timeout、默认 FS 归一化。Paimon 第一阶段只要求 OBS，但 Rust 侧也应采用同类 object-store abstraction，而不是在 export fast path 里继续手写阻塞 range reader。
- **DataFusion Parquet pipeline**：LakeSoul 用 `FileScanConfigBuilder`、`ParquetFormat`、`ParquetSource`、projection/filter optimizer 和 `SendableRecordBatchStream` 组织读取，并尝试把 filter pushdown 到 scan config。Paimon 第一阶段可以更薄，但 reader 设计必须保留切换到 DataFusion scan/stream 的空间，不能把逻辑绑死在 arrow-rs `Read` 的逐次小读模型上；同时要显式设计 row group pruning 指标。
- **runtime 和 cache 复用**：LakeSoul 使用全局 Tokio runtime 和 metadata cache，避免每个 reader/writer 重建线程池和重复拉 parquet metadata。Paimon export FFI 也应使用 executor process 级 lazy runtime；每个 task 只创建轻量 export context。
- **Substrait filter 通道**：LakeSoul 已有 Java 侧传 Substrait protobuf、Rust 侧用 `datafusion_substrait` 转 DataFusion `Expr` 的路径。Paimon 第一阶段可先用保守 JSON predicate AST，但 request 必须带 `predicate_format` / `request_version`，并把 Substrait 作为后续兼容格式，而不是让自定义 JSON 成为唯一长期协议。
- **类型/literal 转换**：LakeSoul 的 Substrait 工具对 Date、Timestamp、TimestampTZ、Decimal、Binary 等 literal 有专门转换。Paimon 第一阶段即使用 JSON AST，也必须把 DATE、TIMESTAMP、DECIMAL 的编码写死并测试；不能把 Java `toString()` 作为跨语言协议。
- **multipart 写出**：LakeSoul 的 `MultiPartAsyncWriter` 用 object_store multipart upload + arrow-rs `ArrowWriter`，row group flush 后把内存 buffer 作为 part 异步上传，并在 close 时 `finish()` / 失败时 `abort()`。Paimon export 写 OBS 时应借鉴该结构，避免先写本地临时文件或单 PUT 大对象。
- **内存上限和 backpressure**：LakeSoul writer 维护 `buffered_size`，达到 `mem_limit` 会切 batch 并 flush/roll writer；partitioning writer 也用 bounded receiver capacity。Paimon export 必须限制单 task read/write pipeline 的 in-flight bytes，不能只靠 Spark executor memory 扛住宽表输出。
- **分区列处理**：LakeSoul writer 在写 parquet 时会从 writer schema 中排除 range partition 列，并用 partition metadata 表达分区。Paimon `export_parquet` 是外部平铺导出，分区列必须作为普通输出列物化；这点不能照搬 LakeSoul，但应借鉴其显式 partition schema/config 建模。
- **metrics 和缓存**：LakeSoul 在 writer/plan 中接入 DataFusion metrics 和 metadata cache。Paimon 第一阶段不需要完整 DataFusion metrics 栈，但必须返回稳定的 read/write request、bytes、decode、filter、encode、multipart flush 指标。

因此，本设计对 LakeSoul 的结论是：**借鉴 native IO 的架构边界与 object_store/multipart/DataFusion 组织方式，不照搬 LakeSoul 表语义、merge/sort writer 或动态分区 commit 逻辑**。

### 4.3 LakeSoul 代码对照清单

后续实现时应优先对照以下 LakeSoul 文件，避免只停留在概念借鉴：

- `LakeSoul/native-io/lakesoul-io-java/src/main/java/com/dmetasoul/lakesoul/lakesoul/io/NativeIOBase.java`：参考 config builder、object store option setter、Arrow schema 传递和 callback 引用管理；Paimon export 只借鉴 builder/option/error 风格，不跨边界传 Arrow batch。
- `LakeSoul/native-io/lakesoul-io-java/src/main/java/com/dmetasoul/lakesoul/lakesoul/io/NativeIOWriter.java`：参考 opaque writer pointer、`write_record_batch_blocked`、`flush_and_close_writer`、`abort_and_close_writer` 和 native result 释放模式；Paimon 的 C ABI 应保持同样的显式 free/abort 纪律。
- `LakeSoul/native-io/lakesoul-io-java/src/main/java/com/dmetasoul/lakesoul/lakesoul/local/LakeSoulLocalJavaWriter.java`：参考 Hadoop/S3A 配置到 native object store option 的归一化；Paimon 需要补 OBS/S3A alias、bucket 校验、credential 脱敏和 credential-provider-only 回退。
- `LakeSoul/rust/lakesoul-io/src/session.rs`：参考 process-wide Tokio runtime、metadata cache、DataFusion session/runtime env 和 object store 注册；Paimon export 不应按 Spark task 重建 runtime。
- `LakeSoul/rust/lakesoul-io/src/writer/async_writer/multipart_writer.rs`：参考 `ArrowWriter<InMemBuf>` + `object_store::WriteMultipart`、row group flush 后上传 part、`finish()` 后 `head()` 获取大小、失败 `abort()`；这是 Paimon OBS 写侧的主要模板。
- `LakeSoul/rust/lakesoul-io/src/writer/async_writer/partitioning_writer.rs`：只参考 bounded receiver、async sink、flush join 和错误 abort 模式；不要引入 LakeSoul 的 range partition、PK repartition、sort/merge、commit 语义。
- `LakeSoul/rust/lakesoul-io/src/physical_plan/datasource/parquet.rs` 与 `LakeSoul/rust/lakesoul-io/src/session.rs` 的 `FileScanConfigBuilder` / `ParquetFormat` 使用：作为第二阶段 row group pruning 和 DataFusion parquet scan 的落点；第一阶段 reader 要保留可替换接口。
- `LakeSoul/rust/lakesoul-io/src/config/options.rs`：参考 memory/spill/max file size 等 option key 集中化；Paimon export option 不应散落在 Java/Rust 多处硬编码。

这些文件不是复制源。Paimon export 的边界仍是 Paimon split、DV、projection、predicate、外部 output path；LakeSoul 中 CDC、动态分区写表、primary key merge、hash bucket、LSH 和 metadata commit 逻辑均排除。

---

## 5. 适用条件

native fast path 只有在以下条件全部满足时才启用：

1. `spark.paimon.native-io.enabled=true`。
2. `spark.paimon.native-io.export.enabled=true`。
3. 执行引擎是 Spark。
4. 表是 DV PK 表。
5. 数据文件是 Parquet。
6. 数据文件路径都是 `obs://...`。
7. 输出路径是 `obs://...`。
8. OBS 配置完整，包括 endpoint、access key、secret key，session token 可选。
9. `columns` 能转换为确定的 Paimon field projection。
10. `where` 能拆成：
   - 可由 Paimon scan 使用的 partition/file pruning predicate；
   - 可由 native export 执行的 row predicate；
   - 或空 predicate。
11. 输出类型能被 Rust Parquet writer 支持。
12. split 中的 deletion files 能转换为 native 可消费的 DV representation。
13. 每个数据文件的实际 Parquet schema 能直接映射到输出 schema；如果需要 Paimon `castMapping`、字段补默认值、复杂字段重命名、类型提升、row tracking system fields，第一阶段回退 Java。

任一条件不满足，整次 procedure 回退现有 Java export。

---

## 6. Predicate 支持范围

第一阶段支持 Paimon `Predicate` 的安全子集：

- `AND`
- `OR`
- `NOT`，仅当子表达式可转换且 Rust evaluator 支持时启用
- `=`
- `!=`
- `<`
- `<=`
- `>`
- `>=`
- `IN`
- `IS NULL`
- `IS NOT NULL`

支持类型：

- BOOLEAN
- TINYINT / SMALLINT / INT / BIGINT
- FLOAT / DOUBLE
- DECIMAL，literal 编码为 unscaled integer string + precision/scale，precision/scale 必须和 Arrow/Rust decimal writer 支持一致
- CHAR / VARCHAR / STRING
- DATE，literal 编码为 epoch day integer
- TIMESTAMP，literal 编码为 microseconds since epoch；先限制到已在 native reader 中支持的 precision

不支持：

- LIKE / startsWith / contains
- UDF
- Spark Catalyst expression 原文
- nested field predicate
- MAP / ARRAY / ROW 类型比较
- timezone 语义无法稳定证明一致的 timestamp expression

转换失败时回退 Java。转换成功后，Java correctness test 必须覆盖 native result 与 Java result 的逐行一致性。

JSON predicate literal 不能使用 Java `Object.toString()` 作为协议。每个 literal 必须带 type：

```json
{"op":"eq","field":"dt","literal":{"type":"DATE","epoch_day":20579}}
```

DECIMAL 示例：

```json
{"op":"gte","field":"amount","literal":{"type":"DECIMAL","precision":20,"scale":4,"unscaled":"123456"}}
```

---

## 7. DV 处理

现有 native reader 通过附加 `__paimon_native_row_index` 后在 Java 侧套 `ApplyDeletionVectorReader`。native export fast path 不再返回行给 Java，因此 DV 必须下沉到 Rust。

Java executor task 在构造 native request 前负责：

1. 用现有 `DeletionVector.Factory` 为每个 `DataFileMeta` 找到对应 DV。
2. 将 DV 转换成 native request 可表达的 row-position filter。
3. 如果 DV 无法序列化为支持格式，则整次回退 Java。

第一阶段 DV request 格式：

```text
file_path -> deleted row positions
```

实现选择：

- 小 DV：直接传 sorted deleted row positions。
- 大 DV：传 Roaring bitmap 的二进制序列化格式，Rust 侧解码。

Rust 侧读取每个 RecordBatch 后按 file-local row index 过滤 deleted rows。过滤必须发生在写出前，且 metrics 记录 `dv_filtered_rows`。

---

## 8. 分区列与输出 schema

`columns => '*'` 会按 Paimon table row type 输出字段。Paimon 数据文件通常不包含分区列，现有 Java 路径通过 `DataFileRecordReader` 的 partition mapping 物化分区值。因此 native fast path 必须显式处理分区列，否则真实导出场景会因为 `dt` 等分区字段回退。

第一阶段设计：

- Driver/task 构造 request 时，为每个 split 附带分区字段常量值。
- Rust 读取 data file 后，按 output schema 顺序组装 RecordBatch。
- 对数据文件中存在的业务列，直接使用 Parquet column。
- 对分区列，生成与 batch row count 相同的 Arrow constant array。
- 对 row tracking system fields、sequence number 等系统字段，第一阶段不物化，遇到输出 projection 包含这类字段时回退 Java。

Request 中每个文件增加：

```json
{
  "path": "obs://bucket/table/data/file.parquet",
  "partition": {
    "dt": {"type": "STRING", "value": "2026-05-06"}
  }
}
```

输出 schema 必须使用 Paimon field order，而不是 Parquet physical schema order。Rust writer 的最终 RecordBatch 字段顺序必须与 `columns` 参数一致。

---

## 9. Schema Evolution 边界

第一阶段只支持“直接映射”的 schema evolution：

- Parquet 文件字段名能匹配 Paimon 输出字段名。
- Parquet physical/logical type 与 Paimon 输出类型完全兼容。
- 字段重排可以支持，因为 request 以输出字段顺序驱动 projection。
- 分区字段由 split partition 常量物化。

第一阶段不支持：

- 需要 Paimon `castMapping` 的类型变化。
- 历史文件缺列后用 default value 补齐。
- 字段重命名后只能靠 field id 对齐的场景。
- nested schema evolution。
- row tracking/system fields。

遇到不支持的文件时，driver preflight 必须整次回退 Java。不能让某些 split native、某些 split Java。

长期增强方向是把 Paimon schema evolution mapping 显式编码进 request，包括 field id、default value、cast expression 和 nullability policy；不在第一阶段实现。

---

## 10. 文件规划与并发

当前 `numPartitions(parallelism, splits, targetFileSize)` 会在设置 `target_file_size` 时用 `totalSplitSize / targetFileSize` 压低 task 数。这对 Java row writer 是为了控制小文件，但对 native fast path 会限制并发。

native fast path 改成两层控制：

- Spark task 并发由 `parallelism` 和 split 数决定，默认不再因 `target_file_size` 降低。
- 每个 native task 内部按 `target_file_size` 滚动写多个 parquet 文件。

新规则：

```text
native_task_count = min(parallelism, split_count)
```

如果 split 过少但单 split 很大，第一阶段不做 split 内二次切分；后续可用 Parquet row group 级任务拆分增强并发。

输出文件命名：

```text
part-native-${taskAttemptId}-${ordinal}-${uuid}.parquet
```

要求：

- task retry 时不会覆盖已写文件。
- job 成功后写 `_SUCCESS`。
- 如果 task 失败留下孤儿文件，行为与现有 Spark 外部导出类似；后续可加 attempt temp directory + commit rename，但 OBS rename 成本高，第一阶段不做。

### 10.1 输出目录生命周期

执行顺序必须是：

1. 解析参数、加载 table、规划 splits。
2. 执行 native export preflight，决定 native 或 Java。
3. 如果 preflight 失败且 fallback enabled，选择 Java。
4. 只有在策略选定后才执行 `prepareOutputDirectory`。
5. Spark job 成功后由 driver 写 `_SUCCESS`。

这样可以避免 native preflight 失败时先删除 output path 再回退或报错。native task 只写 part 文件，不负责删除目录、不写 `_SUCCESS`。

---

## 11. Java 侧改动

### 11.1 新增配置

在 `CoreOptions` 或 Spark option merge 可见范围新增：

- `native-io.export.enabled`，默认 `false`
- `native-io.export.fallback.enabled`，默认 `true`，仅表示 driver preflight 不适用时回退 Java；native task 已开始执行后失败仍 fail fast
- `native-io.export.metrics.enabled`，默认 `true`
- `native-io.export.obs.read-buffer-size`，默认 `8 MB`
- `native-io.export.obs.read-concurrency`，默认 `4`
- `native-io.export.writer.batch-size`，默认 `8192`
- `native-io.export.writer.row-group-size`，默认 `250000`
- `native-io.export.writer.multipart-part-size`，默认 `64 MB`
- `native-io.export.memory-limit`，默认 `512 MB`，单 native task 读写 pipeline 的软内存上限
- `native-io.export.runtime-threads`，默认 `4`，executor process 级 native Tokio runtime worker 数
- `native-io.export.metadata-cache.enabled`，默认 `true`
- `native-io.export.target-task-size`，可选，用于后续 split grouping，不影响第一阶段行为
- `native-io.export.fail-on-fallback`，默认 `false`；设为 `true` 时 preflight 不适用直接失败，便于压测确认一定命中 native

Spark SQL 使用方式：

```bash
--conf spark.paimon.native-io.enabled=true
--conf spark.paimon.native-io.export.enabled=true
```

### 11.2 新增 Java 类

```text
paimon-core/src/main/java/org/apache/paimon/operation/nativeio/export/
  NativeExportProviderFactory.java
  NativeExportProvider.java
  NativeExportContext.java
  NativeExportPreflightResult.java
  NativeExportTaskResult.java

paimon-native-io/src/main/java/org/apache/paimon/nativeio/export/
  NativeExportApplicability.java
  NativeExportPlan.java
  NativeExportTask.java
  NativeExportFile.java
  NativeExportObjectStoreOptions.java
  NativeExportPredicateJson.java
  NativeExportResult.java
  NativeExportMetrics.java
  NativeExportRunner.java
  PaimonNativeExporter.java
```

职责：

- `NativeExportProviderFactory`：core SPI，供 Spark procedure 发现 native export provider。
- `NativeExportProvider`：暴露 preflight、task planning、task execution 三个轻量方法。
- `NativeExportContext`：Spark procedure 传入 table、splits、projection、predicate、output path、options 的上下文。
- `NativeExportApplicability`：判断能否走 native export，包含 stable reject reason。
- `NativeExportPlan`：driver 侧可序列化计划，包含 output schema、task list、compression、target size。
- `NativeExportTask`：executor task 输入，包含 data files、DV、projection、predicate、OBS options、output path。
- `NativeExportObjectStoreOptions`：从 Spark Hadoop conf、catalog options、table filesystem options 中收集 OBS/S3A 兼容参数，做 bucket/path-style/endpoint/session token 归一化和脱敏。
- `NativeExportPredicateJson`：把支持的 Paimon predicate 转成 Rust 可解析 JSON AST。
- `NativeExportRunner`：Spark `mapPartitions` 中调用的纯 Java runner。
- `PaimonNativeExporter`：JNR wrapper，负责调用 Rust FFI。

### 11.3 修改 ExportParquetProcedure

`ExportParquetProcedure.export(...)` 增加策略：

```text
if native export applicable:
    return nativeExport(...)
else:
    return javaExport(...)
```

`nativeExport(...)` 仍使用 `JavaSparkContext.parallelize(tasks, nativeTaskCount).map(...).collect()`，但 map 内不创建 `TableRead`，只调用 native exporter。

`prepareOutputDirectory(...)` 和 `_SUCCESS` 继续由 Java driver 负责，避免 native 侧处理全局目录语义。

### 11.4 Native library loading

native export 不新增独立 loader，统一复用现有 `org.apache.paimon.nativeio.jnr.PaimonJnrLoader`：

- provider preflight 只能检查 `PaimonJnrLoader.current().hasNativeResource()` / `available()`，并把 `loadFailure()` 转成稳定 reject reason。
- executor task 内 `PaimonNativeExporter` 通过同一个 loader 拿 `LibPaimonNativeIO`。
- 不允许在 ServiceLoader factory 构造阶段加载 `.so`。
- 不允许修改 LakeSoul 那种 Arrow 全局 unsafe property；export fast path 不通过 Arrow C Data 跨 Java/Rust 传批次。

---

## 12. Rust 侧改动

### 12.1 FFI API

新增 C ABI：

```c
void* paimon_exporter_new();
void paimon_exporter_free(void* exporter);
int paimon_exporter_export_parquet(
    void* exporter,
    const char* request_json,
    char** result_json,
    char** error_message);
void paimon_string_free(char* value);
```

FFI 约定借鉴 LakeSoul `CStatus` / bytes result 的资源管理方式：

- Rust 分配的 `result_json` 和 `error_message` 必须由 Java 调用 `paimon_string_free` 释放。
- `paimon_exporter_export_parquet` 返回 0 表示成功，非 0 表示失败；失败时 `error_message` 必须是 UTF-8 字符串。
- Java wrapper 必须在 `finally` 中释放 exporter 和 Rust 字符串，不能依赖 JVM finalizer。
- 错误消息必须包含 stable error code，例如 `UNSUPPORTED_PREDICATE`, `OBS_AUTH_FAILED`, `PARQUET_SCHEMA_MISMATCH`，方便 driver fallback/preflight 和压测定位。

request 和 result 第一阶段使用 versioned JSON，理由：

- 结构比现有 reader config 复杂，逐字段 C setter 容易膨胀。
- Java/Rust 都容易测试。
- 借鉴 LakeSoul config builder 的集中化思路，但避免为 export 引入大量 C setter。
- 稳定后可换成 protobuf/flatbuffers；JSON request 必须从第一版就带 `request_version`，避免协议无版本演进。

### 12.2 Request JSON

核心结构：

```json
{
  "request_version": 1,
  "output_path": "obs://bucket/path/export",
  "compression": "zstd",
  "target_file_size_bytes": 536870912,
  "read_buffer_size_bytes": 8388608,
  "read_concurrency": 4,
  "writer_batch_size": 8192,
  "writer_row_group_size": 250000,
  "multipart_part_size_bytes": 67108864,
  "memory_limit_bytes": 536870912,
  "schema": {
    "fields": [
      {"name": "id", "type": "BIGINT", "nullable": true, "kind": "data"},
      {"name": "dt", "type": "STRING", "nullable": false, "kind": "partition"}
    ]
  },
  "projection": ["id", "score"],
  "predicate_format": "paimon-json-v1",
  "predicate": {"op": "and", "children": []},
  "object_store": {
    "fs.obs.endpoint": "...",
    "fs.obs.access.key": "...",
    "fs.obs.secret.key": "...",
    "fs.obs.session.token": "...",
    "fs.obs.path.style.access": "true",
    "fs.s3a.endpoint": "...",
    "fs.s3a.path.style.access": "true"
  },
  "files": [
    {
      "path": "obs://bucket/table/data/file.parquet",
      "row_count": 12345,
      "file_size": 104857600,
      "schema_id": 8,
      "first_row_id": 0,
      "max_sequence_number": 12,
      "partition": {
        "dt": {"type": "STRING", "value": "2026-05-06"}
      },
      "dv": {
        "format": "positions",
        "positions": [1, 7, 9]
      }
    }
  ]
}
```

Request 不能包含明文 credential 的 debug dump；Java 侧日志只允许输出脱敏 JSON。

object_store options 约束：

- Java 侧需要同时收集 Paimon filesystem options、Spark Hadoop conf 和 catalog options。
- Rust 侧第一阶段接受 `fs.obs.*` 和 `fs.s3a.*` 两类 key；OBS key 优先级高于 S3A alias。
- bucket 可以从 `obs://bucket/path` 解析；如果用户显式提供 bucket，必须与 path bucket 一致，否则 preflight 失败。
- 支持 access key / secret key / session token；不支持 Hadoop credential provider、STS assumed role provider 时必须 preflight 失败，而不是运行时才 OBS_AUTH_FAILED。
- path-style / virtual-host-style 必须显式进入 request，不能依赖 object_store 默认值。
- timeout、retry limit、multipart part size、connection pool 参数必须能从 conf 透传；第一阶段至少支持 connect timeout、request timeout、retry limit。

Predicate 协议约束：

- 第一阶段 `predicate_format=paimon-json-v1`，只承载第 6 节的安全子集。
- 后续允许新增 `predicate_format=substrait-v1`，Java 侧把表达式转成 Substrait protobuf 后 base64 放入 request，Rust 侧用 `datafusion_substrait` 转 DataFusion `Expr`。
- Rust 必须拒绝未知 `predicate_format`，不能静默忽略 predicate。
- 即使第一阶段不用 Substrait，request/result test 也要覆盖未知格式的错误路径。

### 12.3 Result JSON

```json
{
  "rows_output": 1000000,
  "files_written": [
    {
      "path": "obs://bucket/path/export/part-native-...parquet",
      "rows": 500000,
      "bytes": 268435456
    }
  ],
  "metrics": {
    "rows_read": 1200000,
    "rows_output": 1000000,
    "predicate_filtered_rows": 150000,
    "dv_filtered_rows": 50000,
    "parquet_row_groups_read": 128,
    "parquet_row_groups_pruned": 64,
    "peak_buffered_bytes": 134217728,
    "writer_rolls": 2,
    "obs_read_requests": 320,
    "obs_read_bytes": 1073741824,
    "obs_write_requests": 12,
    "obs_write_bytes": 536870912,
    "obs_read_retries": 0,
    "read_ms": 12000,
    "decode_ms": 18000,
    "filter_ms": 3000,
    "encode_ms": 22000,
    "obs_write_ms": 15000,
    "multipart_finish_ms": 1200
  }
}
```

### 12.4 Rust crate 组织

Rust 侧新增 export 模块时，应按 LakeSoul 的 config/session 分层组织，而不是把所有逻辑塞进 FFI 函数：

```text
paimon-native-io/rust/src/export/
  mod.rs
  request.rs          // request/result/version/error code
  object_store.rs     // OBS object_store registration and credential mapping
  reader.rs           // Parquet RecordBatch stream, projection, metrics
  predicate.rs        // paimon-json-v1 evaluator; future substrait-v1 adapter
  deletion_vector.rs  // positions / roaring bitmap filtering
  partition.rs        // partition constant Arrow arrays
  writer.rs           // multipart parquet writer, rolling, abort
  metrics.rs          // per-task counters and timers
  runtime.rs          // executor-process global runtime and optional metadata cache
```

关键约束：

- `object_store.rs` 负责把 Paimon/Hadoop OBS 参数映射成 object_store/S3-compatible client 参数，包括 endpoint、bucket、path-style、retry、timeout、连接池。
- `reader.rs` 可以第一阶段直接使用 arrow-rs parquet reader，但 IO 层必须通过 object_store abstraction；后续可切换到 DataFusion `ParquetSource` / `FileScanConfigBuilder`。
- `writer.rs` 必须支持 multipart upload、`finish()` 和 `abort()`；task 失败或 drop 前未 finish 时必须尽力 abort。
- `runtime.rs` 使用 `LazyLock`/`OnceCell` 初始化 executor process 级 Tokio runtime，worker 数由 `native-io.export.runtime-threads` 控制；不能每个 task 新建 runtime。
- 所有模块返回统一 error code；FFI 层只负责 JSON/string 转换，不吞异常、不拼业务逻辑。

---

## 13. OBS Reader 设计

当前 Rust `ObsObjectRead::read` 每次 `Read::read` 都会同步调用 `fetch_range`，容易产生大量小 range GET。

native export 不应继续沿用这个阻塞小 range 模型。第一阶段 reader 设计为 object_store-backed buffered reader：

- OBS 通过 object_store/S3-compatible client 注册，endpoint、bucket、credential、path-style、retry、timeout 由 request 中的 object_store options 提供。
- 每个 file reader 持有可复用 buffer。
- 顺序读时按 `read_buffer_size` 预取，例如 8 MB。
- `get_bytes(start, length)` 仍支持 Parquet footer / page index 这类随机读取。
- 统计 range request 次数、字节数、重试次数和耗时。
- 对连续小读合并成一个 OBS range request。
- 支持 per task read concurrency 限制，默认 4，避免 executor 内过度并发。

后续可增强：

- row group 粒度预取。
- 多 column chunk 并发读取。
- DataFusion `ParquetSource` / `FileScanConfig`，复用其 projection、统计信息、filter pruning 和 metadata cache。

第一阶段只要求 buffered sequential read 解决小 range 放大。

### 13.1 Runtime、metadata cache 与 backpressure

native export 在 executor 进程内必须复用全局 runtime：

- Rust 使用 process-wide lazy Tokio runtime，默认 4 worker threads。
- 如果启用 DataFusion 或 object_store metadata cache，cache 也应是 process-wide，并有容量上限。
- 每个 native task 通过 semaphore 限制 read concurrency 和 multipart upload concurrency。
- reader -> filter -> writer 不做无限队列；第一阶段可以是同步 batch loop，后续如改成 async pipeline，channel 必须 bounded。

内存控制：

- `memory_limit_bytes` 是单 native task 的软上限，统计 read buffer、Arrow batch、writer buffer、multipart in-flight part 的估算值。
- 达到上限时优先 flush/roll writer，必要时降低 read prefetch，不允许继续累积 batch。
- metrics 返回 `peak_buffered_bytes` 和 `writer_rolls`。

---

## 14. Rust Pipeline

每个 native export task 的 pipeline：

1. 解析 request JSON。
2. 注册 object_store-backed OBS client。
3. 对每个 data file：
   - 创建 object_store buffered parquet reader。
   - 用 projection 构造 `ProjectionMask`。
   - 构造 Arrow `RecordBatchReader`。
   - 对每个 batch 生成 file-local row index。
   - 应用 DV filter。
   - 应用 native predicate。
   - 按 output schema 注入分区常量列并重排列顺序。
   - 追加到 parquet writer。
4. writer 根据 `target_file_size` 滚动文件。
5. 返回 result JSON。

写侧使用 arrow-rs parquet writer + object_store multipart upload，结构参考 LakeSoul `MultiPartAsyncWriter`：

- `ArrowWriter` 写入内存 buffer。
- row group flush 后把 buffer 作为 multipart part 上传。
- 文件 close 时先 close `ArrowWriter`，再 flush 剩余 buffer，最后 multipart `finish()`。
- task 失败时调用 multipart `abort()`。
- 每个 task 可以写多个 part parquet 文件，rolling 由 `target_file_size` 控制。
- 每个输出文件 close 后执行 head/stat 获取最终 byte size，写入 result JSON。

压缩第一阶段只保证 `zstd`，其他 compression 参数不支持时回退 Java。

写出的 Parquet schema 必须与 Java output schema 一致。字段顺序以 `columns` projection 为准。

Rust pipeline 不做 LakeSoul 的 merge/sort/dynamic partition commit 逻辑。Paimon export 是只读导出，不更新表 metadata；所有 LakeSoul writer 里与 primary key merge、range partition path commit、LSH、CDC 相关的逻辑都不进入本 fast path。

### 14.1 Predicate pruning 层级

为达到“极致性能”，predicate 不能只在 batch 解码后过滤。native export 至少要明确三层 predicate 处理：

1. **Driver/Paimon scan pruning**：沿用 Paimon scan 的 partition/file pruning，减少进入 native task 的 files。
2. **Rust Parquet row group pruning**：对能从 Parquet statistics 判断的 predicate，在读取 column pages 前跳过 row group；metrics 记录 `parquet_row_groups_read` 和 `parquet_row_groups_pruned`。
3. **Rust batch filter**：对剩余 batch 做精确过滤，保证语义正确。

第一阶段如果 arrow-rs reader 难以稳定做 row group pruning，可以先不启用该优化，但必须保留 request/predicate 表达与 metrics 字段，并在 plan 中把 DataFusion `ParquetSource` / `FileScanConfig` 作为第二阶段优化点。线上 benchmark 如果 where 选择性高而 native 仍慢，优先补 row group pruning，而不是继续调大 batch。

---

## 15. 回退策略

默认 `native-io.export.fallback.enabled=true`：

- driver 侧 applicability 失败：直接走 Java export。
- executor 侧 native 初始化失败：如果已经写出任何 native 文件，则 task fail，不在 task 内回退，避免重复输出。
- executor 侧 native 未开始写出前发现 request unsupported：task fail，并由 driver 侧在后续 retry 仍失败；这种错误应尽量前移到 driver applicability。

为了避免半 native 半 Java：

- 是否 native 由 driver 在 task 提交前一次性决定。
- native task 中不允许按文件 fallback Java。
- 任何 native task 失败，整个 Spark job 失败，由用户重跑 Java 或修 bug。

如果需要生产兜底，可以在 driver 侧做 two-phase：

1. native applicability/preflight 全量通过后才提交 native job。
2. native job 运行期失败则 fail fast，不自动重跑 Java。

`native-io.export.fail-on-fallback=true` 时，driver preflight 失败必须抛出包含 reject reason 和 detail 的异常，不能回退 Java。该开关用于 benchmark 和验收，防止用户误以为跑到了 native fast path。

---

## 16. Metrics 与日志

Java `NativeExportResult` 聚合所有 task metrics，driver 侧记录一条结构化 summary：

```text
native export summary:
  tasks=32
  rows_read=...
  rows_output=...
  files_written=...
  obs_read_requests=...
  obs_read_bytes=...
  obs_read_retries=...
  obs_read_ms=...
  obs_write_requests=...
  obs_write_bytes=...
  obs_write_ms=...
  read_ms=...
  decode_ms=...
  filter_ms=...
  encode_ms=...
  multipart_finish_ms=...
```

Spark EventLog 仍只能看到 `collect` stage，但 stage name 应改得更可识别：

```text
nativeExportParquet at ExportParquetProcedure
```

后续可把 metrics 写入 Spark accumulator，但第一阶段日志和 result 聚合即可。

日志和 result 中不能输出 OBS access key、secret key、security token。request JSON 只允许在 debug dump 中输出脱敏版本。

metrics 命名必须和 Rust result JSON 保持稳定，不能只依赖 tracing 文本。每个 task 至少返回：

- `files_read`
- `files_written`
- `rows_read`
- `rows_output`
- `predicate_filtered_rows`
- `dv_filtered_rows`
- `parquet_row_groups_read`
- `parquet_row_groups_pruned`
- `peak_buffered_bytes`
- `writer_rolls`
- `obs_read_requests`
- `obs_read_retries`
- `obs_read_bytes`
- `obs_write_requests`
- `obs_write_bytes`
- `decode_ms`
- `filter_ms`
- `encode_ms`
- `multipart_finish_ms`

---

## 17. 测试设计

### 17.1 Java 单元测试

覆盖：

- native export applicability reject reason。
- predicate JSON 转换。
- DATE / TIMESTAMP / DECIMAL literal 编码。
- 未知 `predicate_format` 拒绝。
- projection / schema 转换。
- task grouping 不受 `target_file_size` 压低并发。
- fallback 到 Java export 的条件。
- 分区列物化 request 构造。
- native jar 不在 classpath 时透明回退。
- FFI 成功/失败时 Rust string 释放路径。
- Spark Hadoop conf / catalog options / filesystem options 到 `NativeExportObjectStoreOptions` 的优先级和脱敏。

### 17.2 Rust 单元测试

覆盖：

- request JSON 解析。
- `request_version` 和未知 `predicate_format` 拒绝。
- object_store OBS option 映射，至少覆盖 endpoint、bucket、path-style、credential 脱敏。
- object_store buffered reader 的 range 合并行为，可用 fake object reader。
- predicate row group pruning 的 metrics 聚合；即使第一阶段未启用 pruning，也要验证字段存在且为 0。
- predicate evaluator。
- DV positions / roaring bitmap filter。
- parquet 写出 schema 和 row count。
- target file size rolling。
- multipart writer finish/abort 行为。
- 分区 constant array 注入。

### 17.3 本地集成测试

用本地 file reader 或 fake object store：

- Java export 与 native export 输出 parquet 内容一致。
- `columns => '*'` 和显式列列表一致。
- 空结果仍生成 `_SUCCESS`，返回 0。
- predicate 过滤一致。
- DV 删除过滤一致。
- `columns => '*'` 包含分区列时结果一致。

### 17.4 OBS E2E benchmark

必须包含三组：

1. Java reader + Java export。
2. native reader + Java export。
3. native export fast path。

每组固定：

- 同一 table snapshot。
- 同一 where。
- 同一 output compression。
- 独立 output path。
- 清理或区分缓存影响。
- 记录 Spark app id、wall time、native metrics、输出 row count、输出文件数和总 bytes。

验收标准：

- row count 一致。
- parquet schema 一致。
- 抽样 checksum 一致。
- native export fast path 达到性能目标。
- `native-io.export.fail-on-fallback=true` 模式能证明 benchmark 确实命中 native fast path。

---

## 18. Rollout

阶段 1：功能关闭默认值

- 实现完整 fast path。
- 只通过显式 conf 开启。
- 出错 fail fast。

阶段 2：生产压测

- 在目标宽表上跑 A/B。
- 压测必须打开 `native-io.export.fail-on-fallback=true`，确保结果不是 Java fallback。
- 用 metrics 判断瓶颈是否从 read 转到 write / encode / OBS。
- 调整 buffer size、writer batch size、read concurrency。

阶段 3：灰度

- 对指定库表或指定 Spark session 开启。
- 所有回退 reason 可观测。
- 输出路径使用独立目录，避免和 Java export 混写。

---

## 19. 风险

### 19.1 OBS 写入失败留下孤儿文件

第一阶段不做 temp commit 协议。task 失败后可能留下 part 文件。由于 `export_parquet` 是外部目录导出，用户可通过 overwrite 重跑清理。后续如需要强一致输出，可引入 attempt temp directory，但 OBS rename/list 成本需要单独评估。

### 19.2 Predicate 语义不一致

只支持明确可证明一致的表达式。timestamp、decimal、string collation 需要专门测试。不支持时回退 Java。

### 19.3 DV 编码成本过高

如果 Java 侧把大 DV 展开成 positions，可能增加 driver/executor 内存和 task 序列化成本。大 DV 必须使用 bitmap 二进制格式。

### 19.4 小文件数量

提高 task 并发可能增加输出文件数。由 native writer 的 `target_file_size` 滚动控制文件大小；如果 split 本身很小，仍可能产生小文件。后续可做 task grouping。

### 19.5 Rust writer 与 Java writer 文件属性不完全一致

必须以读取语义和 schema/row count/checksum 为准验收。Parquet writer metadata 不要求字节级一致。

### 19.6 SPI 与 classloader 问题

Spark executor 用户 jar、Paimon bundle、native-io jar 可能由不同 classloader 承载。Native export provider discovery 必须优先使用线程上下文 classloader，并按 classloader 缓存结果；不能用一个全局 static 缓存把某次 `NO_PROVIDER` 永久污染后续 session。

### 19.7 分区值类型转换

分区值从 Paimon `BinaryRow` / partition spec 转成 JSON literal，再由 Rust 生成 Arrow constant array。DATE、DECIMAL、TIMESTAMP 分区值必须有明确编码。第一阶段如果无法证明一致，必须回退 Java。

### 19.8 object_store 与 OBS 兼容性

LakeSoul 的 object_store 路径主要按 S3/S3A 建模，OBS 需要验证 endpoint、bucket、path-style、签名、region、session token、HTTP keepalive 和 retry 行为。第一阶段必须在目标 OBS 环境做 E2E；如果 object_store 不能满足 OBS 鉴权或性能，再封装 OBS-specific store，但上层 reader/writer 仍保持 object_store-style 接口。

### 19.9 Substrait 演进风险

第一阶段自定义 JSON predicate 简单可控，但长期表达能力有限。request 必须保留 `predicate_format`，并在测试里固定未知格式失败行为，避免后续引入 Substrait 时出现协议兼容问题。

### 19.10 Row group pruning 缺失导致选择性查询仍慢

如果 `where` 选择性很高，只做 batch 级过滤会浪费 Parquet decode 和 OBS read。第一阶段可以先交付可工作的 native fast path，但 metrics 必须暴露 row group read/pruned 数；压测发现选择性过滤仍慢时，下一步应接入 DataFusion `ParquetSource` 或 arrow-rs row group selector。

### 19.11 Hadoop credential provider 不可透传

LakeSoul 只处理显式 AK/SK/session token 和部分 S3A 参数。Paimon 生产环境可能通过 Hadoop credential provider、临时 token 或 Yarn delegation token 注入凭证。native export preflight 必须识别无法落到 request 的凭证来源并回退 Java，避免 Rust 运行时才鉴权失败。

### 19.12 executor 线程和内存放大

如果每个 Spark task 都创建 runtime、client、metadata cache 或无界 multipart buffer，开启高并发后会把瓶颈变成 executor OOM 或线程数爆炸。native export 必须复用 process-wide runtime/cache，并通过 `memory_limit_bytes`、read/upload semaphore 和 bounded pipeline 控制 in-flight bytes。

---

## 20. 验收清单

- `spark.paimon.native-io.export.enabled=false` 时行为与现有代码一致。
- native export applicability 能解释每次是否启用和为何回退。
- `native-io.export.fail-on-fallback=true` 时不允许静默回退 Java。
- native provider 不在 classpath 时不会影响原 Java export。
- `target_file_size` 不再降低 Spark task 并发。
- native OBS reader 通过 object_store-backed buffered range read，metrics 能证明 request 次数受控。
- native writer 通过 multipart upload 写 OBS，失败时执行 abort。
- native pipeline 不产生 `InternalRow`。
- `columns => '*'` 包含分区列时输出与 Java export 一致。
- DV 过滤在 Rust 侧完成。
- 简单 predicate 在 Rust 侧完成。
- DATE / TIMESTAMP / DECIMAL predicate literal 编码跨 Java/Rust 一致。
- row group pruning metrics 可见；第一阶段未启用时必须稳定输出 0。
- `peak_buffered_bytes` 和 `writer_rolls` metrics 可见，内存上限测试能触发 writer rolling。
- native runtime 是 executor process 级复用，不按 Spark task 重建线程池。
- OBS/S3A object_store option 优先级、bucket 校验、credential 脱敏可测试。
- 未知 `request_version` / `predicate_format` fail fast。
- 输出 `_SUCCESS` 和 row count 正确。
- Java/native 三路 benchmark 可复现。
- 目标业务表上 native export fast path 达到性能目标，或者 metrics 明确指出剩余瓶颈。
