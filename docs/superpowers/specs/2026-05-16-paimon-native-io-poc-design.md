# Paimon Native IO PoC 设计（基于 LakeSoul 移植）

**日期**：2026-05-16
**作者**：zhuoyuchen
**目标读者**：内部团队
**状态**：草案，待评审

---

## 1. 背景与目标

Paimon 当前 Spark 读路径全程 JVM 行式（`PaimonPartitionReader` → `RawFileSplitRead` → `DataFileRecordReader` → `VectorizedParquetRecordReader` 内部列式但对外 `InternalRow`）。Parquet 解码 + 类型转换是热点，没有利用 SIMD / 异步 IO / 直接内存。

LakeSoul 已经在生产中使用基于 Rust + DataFusion + arrow-rs 的 native IO（`lakesoul-io` crate + C ABI + JNR-FFI 桥接），技术栈成熟。本 PoC 评估**把 LakeSoul 的 native IO 移植/裁剪后接入 Paimon**，专门加速 **DV 模式 PK 表**的读取。

### 1.1 为什么选 DV 模式 PK 表作为 PoC 起点

- DV 模式下 split 被标记为 `rawConvertible`，读取走 `RawFileSplitRead` 单文件直读路径（`RawFileSplitRead.java:74`），**不经过 sort-merge**
- DV 通过 `ApplyDeletionVectorReader` 在 Parquet decode 之后做行级过滤（`RawFileSplitRead.java:308-309`），是一个干净的可剥离层
- 跳过了 PK 表最难的部分（多 sorted run 的 `MergeFileSplitRead`），PoC 风险最低，价值最直接

### 1.2 为什么选 LakeSoul 而不是 datafusion-comet

| 维度 | LakeSoul 移植 | Comet |
|---|---|---|
| JDK 8 兼容 | ✅ Rust 不依赖 JDK 版本，Java 侧用 JNR-FFI 工作在 JDK 8 | ❌ Comet ≥0.9 要求 JDK 11+，0.8.x 已无升级路径 |
| 社区/合规 | 内部使用，无需考虑社区接受度 | 倾向 Apache 项目，但 Paimon 引入 native 组件本身在社区可能有阻力 |
| 控制力 | 完全自主，可定制 DV / Paimon 元数据 | 跟随 Comet 接口，对扩展点有约束 |
| 维护成本 | 高（fork + rebase） | 低（依赖 release） |

既然定位是**内部 PoC**，控制力 + JDK 8 兼容性是决定性因素，选 LakeSoul 路径。

### 1.3 总目标

分两阶段：

- **阶段 1（正确性）**：DV 模式 PK 表查询走 native IO 链路，**结果集与 JVM 路径完全一致**（默认 synthetic PK+DV 数据集 + 我们自有业务数据双重校验；TPC-DS/SSB 只作为可选派生数据集）
- **阶段 2（性能）**：在控制变量条件下，**scan 阶段 wall-time 提升 ≥1.5x**（参考 LakeSoul 自己公布的数字，对 Parquet 顺序大列 scan 通常能到 2-3x）

---

## 2. 范围

### 2.1 In Scope

- Spark 3.4.4 + JDK 8 runtime
- Paimon **PK 表 + DV 模式**（`'deletion-vectors.enabled' = 'true'`）
- **Parquet 格式**（Paimon DV 模式下 ORC 也支持，但 PoC 先 Parquet）
- **华为云 OBS 对象存储**（唯一支持的文件系统；scheme 固定为 `obs://`，配置前缀固定为 `fs.obs.*`）
- **Linux x86_64** 目标平台（生产唯一目标；macOS 仅本地开发）
- 端到端：Spark SQL 查询 → 命中 native IO → 输出 `InternalRow` → 与原路径一致的结果

### 2.2 Out of Scope（PoC 阶段不做）

- 非 DV 模式 PK 表（涉及 `MergeFileSplitRead` 的 sort-merge，工作量翻倍）
- Append-only 表（虽然技术上更简单，但和 PoC 论证目标重复，先聚焦 PK 表证明价值）
- Write 路径（`NativeIOWriter` 不移植）
- ORC、CSV、Avro 等其他格式
- S3 / OSS / HDFS / 本地文件系统等所有非 OBS 文件系统（native IO 文件系统层只实现 OBS）
- `SupportsColumnarReads`（Spark 列式接口）—— 阶段 1 还是输出 `InternalRow`，避开下游 Spark operator 改造
- Substrait filter pushdown 协议（LakeSoul 用 Substrait 传 filter；PoC 阶段 1 不下推 SQL filter；带 data filter/topN/limit 的查询不进 native）
- LakeSoul 的 `MergeParquetExec` / `sorted_stream_merger`（用不上）
- LakeSoul 的 metadata service / catalog（完全不需要）

### 2.3 Non-Goals（明确不追求）

- 替换 Paimon 现有 Java Parquet reader（保留作为 fallback）
- 多 Spark 版本兼容（仅 Spark 3.4.4）
- 完整 ANSI / 复杂类型支持（PoC 仅覆盖典型 OLAP 数值/字符串/日期类型）

---

## 3. 总体架构

```
┌──────────────────────────────────────────────────────────────────┐
│ Spark SQL Driver                                                 │
│   PaimonScan → PaimonInputPartition (含 DataSplit)               │
└──────────────────────────────────────────────────────────────────┘
                              │
                              ▼ Executor
┌──────────────────────────────────────────────────────────────────┐
│ PaimonPartitionReader (保持现有入口，executor 侧 newRead)       │
│   │                                                              │
│   ├─ KeyValueTableRead/SplitReadProvider 选择读实现              │
│   │                                                              │
│   ├─ 现有路径：RawFileSplitRead → DataFileRecordReader (保留)    │
│   │                                                              │
│   └─ Native 路径：NativeRawFileSplitRead (新增)                  │
│        │                                                         │
│        └─ NativeFileRecordReader (新增)                          │
│             │                                                    │
│             ├─ PaimonNativeReader (Java, JNR-FFI 包装)           │
│             │     │                                              │
│             │     └─ libpaimon_native_io.so (Rust cdylib)        │
│             │           │                                        │
│             │           ├─ paimon-io crate (从 lakesoul-io 裁剪) │
│             │           │     ├─ FileScanConfigBuilder           │
│             │           │     │   + ParquetSource (直接构建,     │
│             │           │     │   不走 ListingTable)              │
│             │           │     ├─ Projection (target_schema)      │
│             │           │     ├─ row_index 虚拟列 (DV 对齐用)    │
│             │           │     └─ object_store OBS 注册层        │
│             │           │        (仅接受 obs:// + fs.obs.*)       │
│             │           │                                        │
│             │           └─ Arrow C Data Interface (输出 RB +     │
│             │              row_index 列)                          │
│             │                                                    │
│             ├─ Arrow → Paimon InternalRow 转换 (JVM, 复用现有)   │
│             │  + returnedPosition() 从 row_index 列读取          │
│             └─ ApplyDeletionFileRecordIterator (复用，按真实       │
│                row position 查 DV)                                │
└──────────────────────────────────────────────────────────────────┘
```

### 3.1 关键架构决策

| 决策 | 选择 | 理由 |
|---|---|---|
| C ABI vs JNI | **C ABI + JNR-FFI**（沿用 LakeSoul） | JNR-FFI 不需要 javah 步骤，绑定动态生成；LakeSoul 已经验证 |
| Native scan 路径 | **直接 `FileScanConfigBuilder + ParquetSource → create_physical_plan`**（不走 `ListingTable`） | LakeSoul 的 `LakeSoulIOSession::build_physical_plan` (session.rs:720-803) 就是这条路径，最短最直接；`ListingTable` 是给 SessionContext 注册用的，PoC 用不上 |
| Projection 表达 | **传 target Schema**（不是 indices） | LakeSoul `with_schema(SchemaRef)` (config/mod.rs:495)，native 内部 `compute_projection_indices` 算 indices |
| DV 应用层位置 | **JVM 侧**（复用 `ApplyDeletionFileRecordIterator`） | 阶段 1 最小改动；阶段 2 性能仍有瓶颈再下沉到 Rust |
| **DV rowId 对齐机制** | **Parquet row_index 虚拟列**（native 输出，JVM 用作 `returnedPosition()`） | Paimon Java 用 `PageReadStore.getRowIndexes()`；native 镜像同样机制，与所有 pushdown 兼容 |
| Native 输出格式 | **Arrow RecordBatch + row_index 列**（via C Data Interface） | LakeSoul 现成接口，arrow-java 自带 `Data.importIntoVectorSchemaRoot` |
| JVM 端消费 | **Arrow VSR → Paimon ColumnVector → InternalRow**（复用 paimon-arrow 转换） | 阶段 1 最小改动；阶段 2 可考虑接 Spark `ColumnarBatch` |
| Tokio Runtime | **全局静态 LazyLock<Runtime>**，env var 调线程数 | LakeSoul `session.rs:63-77` `GLOBAL_RUNTIME` 模式，executor 共享，默认 4 worker |
| Error 类型 | **`anyhow::Error`**（PoC 用，比 LakeSoul 用的 `rootcause::Report` 更主流） | 跨 FFI 时 `e.to_string()` 写入 `CString`，行为一致 |
| Switch 方式 | **多层 override**：session conf > table property > cluster conf > default(false) | Paimon `CoreOptions` 现成模式，覆盖灰度需求 |
| Auto-fallback | **PoC 不做**，native 失败直接抛异常 | 暴露 bug；用户改 conf 显式回退 |
| Metric | **PoC 不做** | 等 PoC 验证通过后再加 |
| Native lib 分发 | **打入 paimon-native-io jar 的 resources** | 沿用 LakeSoul `JnrLoader` 模式，从 classpath 解压到 tmpdir |

---

## 4. 代码结构

### 4.1 新增 Maven 模块

```
paimon-native-io/                         # 新增顶层模块
├── pom.xml                               # 编译 Rust + 打 native lib 进 jar
├── rust/                                 # Rust 子目录
│   ├── Cargo.toml                        # workspace
│   ├── paimon-io/                        # 核心 crate（从 lakesoul-io 裁剪）
│   │   ├── Cargo.toml
│   │   └── src/
│   │       ├── lib.rs                    # 入口 + Result 别名
│   │       ├── config.rs                 # PaimonIOConfig + Builder
│   │       │                             #   对应 lakesoul-io/src/config/mod.rs
│   │       ├── session.rs                # PaimonIOSession + GLOBAL_RUNTIME
│   │       │                             #   + build_physical_plan
│   │       │                             #   对应 lakesoul-io/src/session.rs
│   │       ├── reader.rs                 # PaimonReader +
│   │       │                             #   SyncSendableMutablePaimonReader
│   │       │                             #   对应 lakesoul-io/src/reader.rs
│   │       ├── helpers.rs                # compute_projection_indices /
│   │       │                             #   infer_schema 等辅助
│   │       │                             #   对应 lakesoul-io/src/helpers/mod.rs
│   │       ├── object_store.rs           # 仅 OBS 注册与 DataFusion ObjectStore 适配
│   │       └── obs/                      # 内嵌 OBS client（从 obs-rust-sdk 迁入/裁剪）
│   │           ├── mod.rs
│   │           ├── client.rs             # head/get/range/stream/list 基础能力
│   │           ├── config.rs             # endpoint/ak/sk/session token/region
│   │           ├── signer.rs             # OBS 签名逻辑
│   │           ├── error.rs              # OBS 错误解析与分类
│   │           └── xml.rs                # OBS XML 响应解析
│   └── paimon-io-c/                      # C ABI crate (cdylib)
│       ├── Cargo.toml                    # crate-type = ["cdylib"]
│       └── src/
│           └── lib.rs                    # 暴露 #[no_mangle] extern "C" fn
├── src/main/java/org/apache/paimon/nativeio/
│   ├── jnr/
│   │   ├── JnrLoader.java                # 从 LakeSoul JnrLoader 改名移植
│   │   └── LibPaimonNativeIO.java        # JNR 接口（对应 LibLakeSoulIO）
│   ├── PaimonNativeReader.java           # 从 NativeIOReader 改造
│   └── PaimonNativeReaderBase.java       # 从 NativeIOBase 改造
└── src/test/                             # 单元测试
    └── java/.../PaimonNativeReaderTest.java
```

`paimon-native-io/pom.xml` 依赖边界：

- 依赖 `paimon-core`（拿 `SplitRead` / `DataFileRecordReader` / `FormatReaderFactory` 等 read path API）
- 依赖 `paimon-format`（`NativeParquetSchemaValidator` 需要读取/解释 Parquet footer；不要依赖 transitive dependency 偷用 Parquet 类）
- 依赖 `paimon-arrow`（复用 Arrow ↔ Paimon vector 转换）
- 依赖 `com.github.jnr:jnr-ffi`，版本先参考 LakeSoul 当前 `2.2.19`，最终以 Paimon dependency management 锁定
- 不依赖 Spark / Flink；Spark 端只需要把 jar 放到 driver/executor classpath
- 不新增 `arrow-memory-netty`；沿用 Paimon `paimon-arrow` 的 `arrow-memory-unsafe`
- Paimon/Spark 现有 classpath 不包含 `jnr-ffi`；PoC 线上压测不能只分发瘦 jar。推荐产出 `paimon-native-io-*-with-dependencies.jar`，包含 `jnr-ffi` 及其运行时依赖和 native lib resource，但不重新打包 Paimon/Spark/Arrow 主依赖；如果不做 shaded/with-dependencies jar，benchmark procedure 必须显式校验并列出所有额外 `--jars` 依赖
- 构建 with-dependencies jar 时必须保留/合并 `META-INF/services/org.apache.paimon.operation.nativeio.NativeSplitReadProviderFactory`（例如 shade `ServicesResourceTransformer`），并原样保留 `/linux/x86_64/libpaimon_native_io.so` 等 native resource；不能被 resource filtering 改名或压缩到不可被 `PaimonJnrLoader` 查找的位置。还要保留 `jnr-ffi` / `jffi` 自身的 service 与 native resource（例如 `com/kenai/jffi/**`），否则 JNR runtime 会在加载 Paimon native lib 前先失败
- `paimon-format` 在 package 阶段会把 `org.apache.parquet` relocate 到 `org.apache.paimon.shade.org.apache.parquet`。`NativeParquetSchemaValidator` 不要在 native jar 里直接保留对 unshaded `org.apache.parquet.*` 的运行时引用，除非 native jar 同步 shade/relocate Parquet；推荐在 `paimon-format` 暴露一个小的 footer/schema inspection helper，返回 Paimon 自己的中立结果对象，native module 只调用该 helper，避免 Spark/Paimon classpath 上 Parquet 包冲突。该 helper 的公开返回类型不能包含 `ParquetMetadata`、`MessageType`、`ColumnDescriptor` 等 Parquet 类；只返回字段名/id、primitive/logical type、repetition、precision/scale、timestamp unit、每个 row group/column chunk codec、encryption flag、row group/row count 等 POJO/enum

顶层 `paimon/pom.xml` 接入方式：

- PoC 可以把 `<module>paimon-native-io</module>` 放在默认 reactor 中，但 `paimon-native-io` 自身默认不执行 cargo build；没有 Rust 环境时仍能跑普通 Maven 编译
- `-Pnative-io` profile 才执行 cargo build、复制 `.so/.dylib` 到 resources，并启用 native smoke tests；CI 可先只在 Linux x86_64 job 开启
- 默认 profile 下 `paimon-native-io` 只编译 Java wrapper/provider 代码和 service 文件，不要求 resources 中存在 `.so/.dylib`。所有会实际调用 `PaimonJnrLoader.load()`、JNR symbol 或 Rust smoke 的测试都必须挂到 `native-io` profile；普通单元测试只能验证 provider factory 轻量加载、option 解析、guard 逻辑等不依赖 native lib 的部分
- `-Pnative-io` profile 的 Maven lifecycle 要明确三步：cargo build 产物进入 `target/native/<os>/<arch>/`；process-resources 复制到 `target/classes/<os>/<arch>/libpaimon_native_io.*`；package/shade 保留该 resource。不能把 cargo 输出直接写进 `src/main/resources`，避免本地构建污染 source tree
- 如果社区评审要求默认 source build 完全不触发 Rust 工具链，也可以把 `<module>paimon-native-io</module>` 放进 `native-io` profile；但要保证 core 的 ServiceLoader SPI 在默认构建里仍可编译和测试
- `paimon-bundle` PoC 阶段不依赖该模块；线上压测通过 `--jars` / executor classpath 显式分发 `paimon-native-io` jar

### 4.2 Paimon Java/Core 接入改动

新增文件：

```
paimon-core/src/main/java/org/apache/paimon/operation/nativeio/
├── NativeSplitReadProviderFactory.java   # SPI 接口，core 只依赖接口，不依赖 Arrow/JNR/native 实现
├── NativeSplitReadContext.java           # 传递 RawFileSplitRead 所需的 core 上下文
├── NativeSplitReadProviderLoader.java    # ServiceLoader 可选加载；未找到 provider 时 native 不生效
├── NativeApplicability.java              # applicable + reason + detail，供 guard 与 benchmark 复用
├── NativeApplicabilityReporter.java      # 默认 no-op，benchmark 显式启用的 split/file 命中诊断采集器
├── NativeRejectReason.java               # 稳定拒绝原因枚举，benchmark/report 不解析日志
├── SupportsNativeIO.java                 # 轻量守门员：只包含不依赖 Arrow/JNR 的判断
└── NativeIOOptions.java                  # 从 CoreOptions / table options / conf 中解析 native 开关与 OBS 配置

paimon-native-io/src/main/java/org/apache/paimon/nativeio/
├── NativeRawFileSplitRead.java           # 平行实现 RawFileSplitRead，依赖 paimon-core
├── NativeFormatReaderFactory.java        # 适配 FormatReaderFactory，复用 DataFileRecordReader 容错语义
├── NativeParquetSchemaValidator.java     # 读取 Parquet footer，验证 physical/logical type 是否在 PoC 能力内
├── NativePreflightResult.java            # USE_NATIVE / USE_JAVA / EMPTY / THROW，表达 footer preflight 结果
├── NativeFileRecordReader.java           # 包装 PaimonNativeReader 为 FileRecordReader<InternalRow>
├── NativeFileRecordIterator.java         # Arrow VSR -> Paimon VectorizedColumnBatch -> InternalRow
├── NativeRawFileSplitReadProvider.java   # SplitReadProvider，优先匹配 native 支持的 raw split
└── PaimonNativeSplitReadProviderFactory.java

paimon-native-io/src/main/resources/META-INF/services/
└── org.apache.paimon.operation.nativeio.NativeSplitReadProviderFactory
```

修改文件：

- `paimon-core/.../KeyValueTableRead.java`：通过 `NativeSplitReadProviderLoader` 尝试加载 native provider；加载成功时插到 `PrimaryKeyTableRawFileSplitReadProvider` 之前，不支持 native 的 split 继续落到原 Java provider
- `paimon-core/.../KeyValueFileStore.java`：新增 `newNativeSplitReadContext()`，把构造 raw read 所需的 `fileIO`、`schemaManager`、`schema`、`valueType`、`formatDiscover`、`pathFactory`、`CoreOptions` 打包进 core SPI context；不直接 new native 实现，避免 core 对 `paimon-native-io` 的编译依赖
- `paimon-core/.../table/PrimaryKeyFileStoreTable.java`：`newRead()` 调用 `store().newNativeSplitReadContext()`，并传给 `KeyValueTableRead` 新构造器
- `paimon-api/src/main/java/org/apache/paimon/CoreOptions.java`：新增 `NATIVE_IO_ENABLED`、`NATIVE_IO_BATCH_SIZE` 等配置项；row index 内部列名只作为诊断/测试用内部选项，不作为普通用户调优项
- `paimon-core/.../operation/nativeio/NativeSplitReadContext.java`：新增 `engineName` / `engineSupportsNativeIO` 字段。PoC 只允许 Spark connector 设置为 supported；Flink/Core/未知引擎即使 table property 打开也返回 unsupported，避免共享 core 读路径误影响 Flink
- `paimon-core/.../schema/SchemaValidation.java`：在 `validateTableSchema(TableSchema schema)` 中拒绝持久化 `__paimon.internal.*` 前缀 table option，确保 Spark-only guard 只能由 connector 动态注入，不能由用户 DDL 绕过
- `paimon-bundle/pom.xml`：决定是否把 `paimon-native-io` 纳入 bundle；PoC 默认单独 jar，生产化前再纳入 bundle

为什么需要 SPI：

- `paimon-core` 目前只在 test scope 依赖 `paimon-arrow`；如果直接把 `NativeFileRecordReader` 放进 core，会把 Arrow Java、JNR 和 native loader 变成 core 主依赖
- `paimon-native-io` 需要依赖 `paimon-core` 的 `SplitRead` / `FileRecordReader` / `RawFileSplitRead` 语义；如果 core 再依赖 `paimon-native-io` 会形成模块环
- SPI 让 native jar 不在 classpath 时行为完全等同原 Paimon；native jar 在 classpath 且开关打开时才尝试匹配 native provider
- ServiceLoader 必须用线程上下文 classloader 优先加载（Spark executor 用户 jar 通常挂在 TCCL 上），捕获 `ServiceConfigurationError` 后记录 debug 并视为 provider unavailable；不能因为 native jar 缺失影响 Java reader
- ServiceLoader 结果缓存要按 `ClassLoader` 维度隔离，不能用单个 JVM 全局 static provider 覆盖所有 classloader。Spark executor 里用户 jar / Paimon bundle / REPL 可能由不同 loader 承载；一个 loader 缺 native jar 不能让另一个 loader 永久 unavailable
- provider factory 创建阶段不要主动加载 `.so`；native lib 加载延迟到 `PaimonNativeReader.initializeReader()`，否则 driver 规划或 executor 构造 read provider 时可能因为平台不匹配提前失败。`PaimonNativeSplitReadProviderFactory` 自身也要保持轻量：构造器和静态初始化不能触发 Arrow allocator、JNR interface binding、native loader 或 Rust symbol 解析；这些重依赖只允许在真正创建/初始化 `NativeFileRecordReader` 时触发。否则 classpath 缺 jnr/jffi/Arrow 时会在 ServiceLoader 阶段 `NoClassDefFoundError`，导致本应透明的 Java 路径失败
- Spark 会把 `ReadBuilder` 放进 `PaimonPartitionReaderFactory` 序列化到 executor；native 相关动态 option 可以随 table/read builder 传递，但不能把 `PaimonNativeReader`、JNR `Pointer`、Arrow allocator、`VectorSchemaRoot`、native lib load 状态等运行期对象放进可序列化对象图
- `NativeSplitReadContext` 在 executor 侧由 `ReadBuilder.newRead()` 创建；如果后续被缓存或序列化，字段必须只包含 Paimon 已可序列化/可重建对象，不持有 non-serializable native handle

`paimon-spark-common` 不承载 native reader 选择逻辑。Spark 侧只需要：

- 在创建 Spark `ReadBuilder` / table read 之前把 native context 标记为 `engineName=spark`、`engineSupportsNativeIO=true`；其他 connector 默认 false
- 把 `spark.paimon.native-io.enabled` 等 session conf 合并到 Paimon options（现有 `OptionUtils.copyWithSQLConf` 已覆盖 `spark.paimon.*`，见 §15）
- 把 `spark.hadoop.fs.obs.*` / `spark.paimon.fs.obs.*` 透传为 Paimon core 可见的 `fs.obs.*`（现有代码不会自动完成，必须补）
- 增加 Spark SQL 端到端 correctness / benchmark 测试

原因：Paimon raw/merge 读路径的真正分发点在 `paimon-core` 的 `KeyValueTableRead` 和 `SplitReadProvider`，如果 native reader 只放 Spark 模块，Flink / Core reader 不可见，且 `paimon-core` 无法依赖 Spark。

Spark-only engine 标记的可落地方式：

- Spark 现有入口在 `PaimonSparkTableBase.newScanBuilder` 中调用 `table.copy(options.asCaseSensitiveMap)`，随后 `BaseScan` 调 `table.newReadBuilder()`
- PoC 可在 Spark connector 合并 options 时注入一个**瞬时 read option**，例如 `__paimon.internal.native-io.engine=spark`，只存在于 `table.copy(...)` 的本次扫描对象中，不写入 catalog schema/table property
- `NativeIOOptions.engineSupportsNativeIO()` 只认这个内部瞬时 option；Flink/Core 默认没有该 option，因此即使表属性里打开 `native-io.enabled` 也不会进入 native
- 为避免用户通过 DDL 持久化同名 key 绕过引擎 guard，`SchemaValidation.validateTableSchema(...)` 需要拒绝创建/修改任何 `__paimon.internal.*` 前缀的 table option；`table.copy(dynamicOptions)` 仍允许 Spark connector 注入本次扫描动态 option
- Spark 读入口必须统一走一个 helper（例如 `OptionUtils.withSparkNativeReadOptions(...)`），负责合并 `spark.paimon.*`、补齐 `spark.hadoop.fs.obs.*` fallback、注入 `__paimon.internal.native-io.engine=spark`。真实调用点至少包括 V2 catalog、V1 path、V1 catalog 和 Spark 内部 KnownSplitsTable。
- V2 catalog：`SparkCatalog.loadSparkTable(...)` 当前已调用 `copyWithSQLConf(...)`，helper 应复用/扩展这个路径。
- V1 path：`SparkSource.loadTable` 非 catalog 分支当前 `copyWithSQLConf(FileStoreTableFactory.create(...), extraOptions=options)`，需要补 Hadoop OBS fallback 和 Spark-only marker。
- V1 catalog：`SparkSource.loadTable` catalog 分支当前 `loadTable(...).getTable.copy(options)`，不会合并 SQLConf；这个分支必须改为同一 helper，否则 `format("paimon").option("catalog", ...)` 的读法会漏掉 native 开关/OBS 配置。
- KnownSplitsTable：`PaimonSparkTableBase.newScanBuilder` 遇到 `KnownSplitsTable` 不会再 `table.copy(options)`；要求创建 `KnownSplitsTable` 的 origin table 已经带上 native dynamic options，或在 `KnownSplitsTable.create` / `PaimonSplitScanBuilder` 入口显式校验并补齐。否则 Spark 内部已知 split 扫描会绕过 native marker。
- Spark `PaimonPartitionReaderFactory.equals` 依赖 `ReadBuilder.equals`，而当前 `ReadBuilderImpl.equals/hashCode` 主要看 table name / filter / readType，不看 `table.copy(...)` 后的动态 option；并且 `ReadBuilderImpl.equals` 看 `partitionFilter`，hashCode 当前没有包含它。PoC 注入 `native-io.enabled`、OBS 配置和 `__paimon.internal.native-io.engine` 后，必须让 read builder/factory 的 equality 能区分 native on/off 和关键 native option fingerprint，避免 Spark 计划复用、缓存或测试断言把两条不同读路径当成同一个 reader factory。fingerprint 可以在内存中用不可逆摘要参与 equals/hashCode，但不能进入 `toString`、日志、assert message 或 benchmark 报告；可打印诊断只展示敏感 key 是否存在
- native dynamic options 会随 Spark reader factory 序列化到 executor，这是读取 OBS 的必要条件；但任何 `toString`、debug 日志、assert message、benchmark env/applicability 输出都不能打印完整 `table.options()` 或 `extraOptions`。如果 equality 测试失败需要输出差异，只输出 native 开关、OBS 配置 key 是否存在和 endpoint 脱敏值，不输出明文 AK/SK/token，也不输出 secret hash / fingerprint
- 如果后续 Paimon 增加正式 read context API，再把这个瞬时 option 迁移到 typed API；PoC 不为此扩大公共 `ReadBuilder` 接口。

---

## 5. Rust 侧改造清单

### 5.1 从 lakesoul-io 拷贝改造（保留）

| LakeSoul 源文件 | Paimon 目标文件 | 改造内容 |
|---|---|---|
| `lakesoul-io/src/object_store.rs:185-273` (`register_object_store`) | `paimon-io/src/object_store.rs` | 不复用 AWS/S3 backend；新增 `ObsObjectStore` 实现 `object_store::ObjectStore`，底层调用 `paimon-io/src/obs/` 内嵌 OBS client；只接受 `obs://` URL 和 `fs.obs.*` 配置，`s3://`、`oss://`、`hdfs://` 等 scheme 一律拒绝 |
| `obs-rust-sdk/src/*` | `paimon-io/src/obs/` | 将 OBS client 代码迁入 Paimon native IO，并裁剪为 read-path 所需能力：config、签名、head、range/stream get、可选 list、错误解析；迁入后不再作为外部 crate 依赖 |
| `lakesoul-io/src/session.rs:63-77` (`GLOBAL_RUNTIME`) | `paimon-io/src/session.rs` | 改 env var 名 `PAIMON_NATIVE_IO_WORKER_THREADS` |
| `lakesoul-io/src/session.rs:720-803` (`build_physical_plan`) | `paimon-io/src/session.rs` | 大幅简化：删 substrait / filter 分支、删 partition_cols 分支、保留 `FileScanConfigBuilder + ParquetSource → create_physical_plan` 主线 |
| `lakesoul-io/src/helpers/mod.rs` 中 `compute_projection_indices` / `infer_schema` / `get_file_object_meta` / `get_file_exist_col` | `paimon-io/src/helpers.rs` | 不动或微调 |
| `lakesoul-io/src/config/mod.rs` 中 `LakeSoulIOConfig` / Builder 的子集 | `paimon-io/src/config.rs` | 大幅删减，见 §5.2 |
| `lakesoul-io/src/reader.rs:89-94` (`LakeSoulReader`) | `paimon-io/src/reader.rs` (`PaimonReader`) | 字段对应 |
| `lakesoul-io/src/reader.rs:278-282` (`SyncSendableMutableLakeSoulReader`) | `paimon-io/src/reader.rs` (`SyncSendableMutablePaimonReader`) | 字段对应 |
| `lakesoul-io/src/reader.rs:107-113` (`new`) / `:223-229` (`start`) / `:317-326` (`start_blocked`) / `:355-363` (`next_rb_blocked`) / `:370-372` (`get_schema`) | 同上 | 重命名 + 简化（无 filter 分支、无 merge 分支） |
| `lakesoul-io-c/src/lib.rs` 中 IOConfig builder / Reader 生命周期 / FFI 输出 (40-50 个函数) | `paimon-io-c/src/lib.rs` | 大幅删减，见 §5.3 |

DataFusion 51 已经把不少 API 拆到了独立 crate，不能只看 `datafusion` umbrella crate。实现时以 LakeSoul 当前 `lakesoul-io/Cargo.toml` 为基线，先保留 `datafusion-datasource` / `datafusion-datasource-parquet` / `datafusion-physical-plan` / `datafusion-session` 等直接 import 需要的依赖，等编译 spike 通过后再删减未使用项。

### 5.2 从 lakesoul-io **完全删除**

| 删除项 | 原 LakeSoul 路径 | 为什么 |
|---|---|---|
| Merge 逻辑全部 | `physical_plan/merge/` | PoC 只做 DV 模式 PK 表，不做 sort-merge |
| Writer 全部 | `lakesoul-io/src/writer.rs` 等 | PoC 不做写 |
| ListingTable 路径 | `physical_plan/datasource/listing.rs` 的 `LakeSoulTableProvider` 部分 | 我们走更短的 `build_physical_plan` 直构路径 |
| Substrait filter | `with_filter_proto` / `with_filter_buf` / `with_filter_str` 全部 | PoC 阶段 1 不下推 SQL filter；带 data filter/topN/limit 的查询走 Java reader |
| **非 OBS object store 支持** | `object_store.rs` 的 s3 / oss / hdfs / file 等通用分支 | native IO 文件系统层只服务生产 OBS 场景，非 OBS 路径一律不进入 native reader |
| **HDFS 支持** | `lakesoul-io/src/hdfs/mod.rs` 全部 + object_store.rs:154-171 hdfs 分支 | 生产使用 OBS，不需要 HDFS |
| **hdrs / hdfs-sys 依赖** | `Cargo.toml` 的 `hdrs` git 依赖 | 仅 OBS，不需要 HDFS native 依赖 |
| 其他 crate | `lakesoul-datafusion` / `lakesoul-metadata*` / `lakesoul-flight` / `python` / `lakesoul-console` / `lakesoul-s3-proxy` | 与 IO 无关 |
| Merge 配置 | `merge_operators` / `primary_keys` / `hash_bucket_num` | LakeSoul merge 才用 |
| Schema evolution | `default_column_value` / `set_default_column_value` | PoC 不做 schema evolution |
| 异步 callback FFI | `next_record_batch` / `start_reader_with_data` 等 callback 版本 | 只保留 `_blocked` 同步版本，简单 |
| Java callback/临时 buffer 辅助 | `ObjectReferenceManager`、`fixedBuffer`、`mutableBuffer` | PoC 不保留异步 callback 和 proto filter，不需要这组长期 direct memory；避免从 LakeSoul 搬迁时留下无用堆外占用 |
| `cbindgen` build-dep | `lakesoul-io-c/Cargo.toml` `[build-dependencies] cbindgen` | PoC 不依赖 C 头文件；如果后续要给社区或外部 C consumer 发布 header，可再用 profile 恢复 |

### 5.3 paimon-io-c 暴露的 C ABI 接口（首版精简版）

PoC 用 **GLOBAL_RUNTIME**，不暴露 runtime builder。**用 schema FFI（C Data Interface）传 target schema**，不传 JSON 字符串。

```rust
#[repr(C)]
pub struct CStatus {
    err: *const c_char,
    status: c_int,
}

// ─────────────────────────────────────────────────────────────
// 配置构造（builder pattern）
// ─────────────────────────────────────────────────────────────
extern "C" fn new_paimon_io_config_builder() -> NonNull<IOConfigBuilder>;

extern "C" fn paimon_config_builder_add_file(
    builder: NonNull<IOConfigBuilder>,
    file: *const c_char,
) -> NonNull<IOConfigBuilder>;

// target_schema 用 Arrow C Schema FFI 传（避免 JSON 序列化）
extern "C" fn paimon_config_builder_with_target_schema(
    builder: NonNull<IOConfigBuilder>,
    schema_addr: c_ptrdiff_t,
) -> NonNull<IOConfigBuilder>;

extern "C" fn paimon_config_builder_with_batch_size(
    builder: NonNull<IOConfigBuilder>,
    batch_size: c_int,
) -> NonNull<IOConfigBuilder>;

// row_index 虚拟列开关 + 列名（必须有，DV 对齐用）
extern "C" fn paimon_config_builder_with_row_index_column(
    builder: NonNull<IOConfigBuilder>,
    column_name: *const c_char,
) -> NonNull<IOConfigBuilder>;

// OBS 配置：key/value 一对一传（fs.obs.endpoint / fs.obs.access.key / fs.obs.secret.key 等）
extern "C" fn paimon_config_builder_set_object_store_option(
    builder: NonNull<IOConfigBuilder>,
    key: *const c_char,
    value: *const c_char,
) -> NonNull<IOConfigBuilder>;

extern "C" fn create_paimon_io_config_from_builder(
    builder: NonNull<IOConfigBuilder>,
) -> NonNull<CResult<IOConfig>>;

extern "C" fn check_io_config_created(
    config: NonNull<CResult<IOConfig>>,
) -> *const c_char;

// ─────────────────────────────────────────────────────────────
// Reader 生命周期（GLOBAL_RUNTIME，不传 runtime）
// ─────────────────────────────────────────────────────────────
extern "C" fn create_paimon_reader_with_global_runtime(
    config: NonNull<CResult<IOConfig>>,
) -> NonNull<CResult<Reader>>;

extern "C" fn check_reader_created(
    reader: NonNull<CResult<Reader>>,
) -> *const c_char;

extern "C" fn start_reader(
    reader: NonNull<CResult<Reader>>,
) -> NonNull<CStatus>;

// 导出 Arrow Schema（含 row_index 列）
extern "C" fn paimon_reader_get_schema(
    reader: NonNull<CResult<Reader>>,
    schema_addr: c_ptrdiff_t,
) -> NonNull<CStatus>;

// 阻塞式取下一批 RecordBatch（含 row_index 列）
//   return CStatus.status = num_rows; 0 = EOF; <0 = error
extern "C" fn next_record_batch_blocked(
    reader: NonNull<CResult<Reader>>,
    array_addr: c_ptrdiff_t,
) -> NonNull<CStatus>;

extern "C" fn free_paimon_reader(reader: NonNull<CResult<Reader>>);

// ─────────────────────────────────────────────────────────────
// 资源释放
// ─────────────────────────────────────────────────────────────
extern "C" fn free_c_status(status: NonNull<CStatus>);
extern "C" fn free_paimon_io_config_builder(builder: NonNull<IOConfigBuilder>);
extern "C" fn free_paimon_io_config(config: NonNull<CResult<IOConfig>>);
```

**接口数预估：17 个。** 相比 LakeSoul 56 个函数仍砍掉约 3 倍。这里把 `check_*` 和 free 函数也算入 ABI；后续实现不能在不更新 Java binding / smoke test / benchmark deployment check 的情况下增删或改签名。

去掉的（相比 LakeSoul）：

- writer 全部（约 15 个函数）
- callback 异步 FFI 版本（`next_record_batch` / `start_reader_with_data` 等）
- `create_tokio_runtime_from_builder` / `new_tokio_runtime_builder`（用 global runtime）
- `lakesoul_config_builder_add_merge_op` / `with_primary_keys`（无 merge）
- `lakesoul_config_builder_set_default_column_value`（无 schema evolution）
- `lakesoul_config_builder_add_filter*` 三个变体（PoC 不下推 SQL filter）
- `lakesoul_config_builder_with_prefix`（路径前缀，PoC 不要）

FFI 内存与错误生命周期需要严格沿用 LakeSoul 模式：

- `CResult<T>` 同时持有 `ptr` 和 `err`；`free_paimon_reader` / `free_paimon_io_config*` 必须释放对象和错误 CString
- builder mutator 不允许 `unwrap()` panic 穿过 FFI；UTF-8/schema import 等错误写入 builder error state，并在 `create_paimon_io_config_from_builder` 返回 `CResult<IOConfig>` 时暴露给 Java
- `create_paimon_reader_with_global_runtime(config)` 成功创建 reader 后消费 `CResult<IOConfig>`，Java 侧必须把 config 置空，不能再调用 `free_paimon_io_config`；只有 config 创建失败或 reader 创建前失败时才由 Java 释放 config
- `check_io_config_created` / `check_reader_created` 返回的是 `CResult.err` 内部借用指针，Java 只读取字符串，不单独 `free_c_string`；随后通过 `free_paimon_io_config` / `free_paimon_reader` 释放 `CResult` 时统一释放错误 CString
- `CStatus` ABI 必须固定为 `#[repr(C)] { err: *const c_char, status: c_int }`，JNR 侧按 LakeSoul 形态用 `UTF8StringRef err` + `Signed32 status` 映射；字段顺序和宽度都要有 smoke test 覆盖，不能随手改成 `long` 或调换顺序
- `CStatus` 同时持有 `status` 和 `err`；Java 每次处理完 `start_reader` / `paimon_reader_get_schema` / `next_record_batch_blocked` 返回值后必须调用 `free_c_status`，Rust 侧释放 `err` 字符串和 `CStatus` 自身
- `CStatus` 不变量要固定：`status > 0` 表示成功返回 batch 且 `err == null`；`status == 0` 表示 EOF/void success 且 `err == null`；`status < 0` 表示错误且 `err` 必须非空、UTF-8、已脱敏。Java 侧遇到 `status >= 0 && err != null` 或 `status < 0 && err == null` 要按 native ABI bug 抛 `IOException`
- 所有 Java → Rust 的 `*const c_char` 入参（file path、row index column name、object store option key/value）必须在对应 builder 方法内立即校验 UTF-8 并复制进 Rust-owned `String` / `HashMap`。不能把 JNR 传入的临时 C 字符串指针保存到 config builder 或 reader 里；错误信息也不能包含 AK/SK/session token 明文
- 所有地址型入参必须先做 null/0 校验：`paimon_config_builder_with_target_schema(schema_addr)`、`paimon_reader_get_schema(schema_addr)`、`next_record_batch_blocked(array_addr)` 遇到 0 地址要返回 `CResult.err` / `CStatus(-1, ...)`，不能让 Rust Arrow FFI 或 JNR 解引用后直接 crash JVM
- 所有 free 函数必须 null-safe：`free_c_status(null)`、`free_paimon_reader(null)`、`free_paimon_io_config(null)` 直接返回。Java wrapper 在成功消费 config、close reader 或释放 status 后把本地指针/引用置空；C 侧 null-safe 是兜底，不代表允许 Java 重复释放同一个非 null 指针
- **JNR Java 签名要先镜像 LakeSoul 已验证的映射**：Rust 返回 `NonNull<CStatus>`，LakeSoul Java 侧声明为 `LibLakeSoulIO.CStatus start_reader(Pointer reader)` / `CStatus next_record_batch_blocked(...)`，并用 `free_c_status(@Pinned @In @Transient CStatus status)` 释放。Paimon 首版也按这个形态写，不要贸然改成 `Pointer` 返回；如果要改，必须先做 Java+Rust 最小 smoke test 验证 JNR struct-by-reference 映射和释放行为
- `paimon_reader_get_schema` 也返回 `CStatus`；Java 侧在 `Data.importSchema` 前先检查 status，并在 finally 中 `free_c_status`。不要把 schema export 写成 void + panic，因为 C Data schema 构造/字段转换错误应按初始化失败暴露
- 所有 `extern "C"` 导出函数都不能让 Rust panic 跨 FFI 边界；内部用 `catch_unwind` 或等价 wrapper 把 panic 转成 `CResult.err` / `CStatus(-1, ...)`，并在 Rust 日志中记录脱敏错误。`unwrap()` / `expect()` 只允许出现在进程启动即可失败的测试代码里，不能出现在 FFI 热路径
- `next_record_batch_blocked` 协议中 `status == 0` 专门表示 EOF，不能同时表示“返回了一个 0 行 RecordBatch”。native 侧如果 DataFusion/arrow-rs 产出 0 行 batch，应在 Rust 内部跳过并继续拉下一批，直到拿到 `num_rows > 0` 的 batch、EOF 或错误；否则 Java 会按 EOF 处理并不会 import ArrowArray
- `next_record_batch_blocked` 成功返回 batch 时 `status` 必须是本批 `RecordBatch::num_rows()`，且必须为正数。Java import `VectorSchemaRoot` 后要校验 `root.getRowCount() == status`、`_row_index.valueCount == status`、所有业务 vector valueCount 不小于 `status`；任一不一致都按 native bug 抛 `IOException`
- `next_record_batch_blocked` 导出的是 `RecordBatch -> StructArray -> FFI_ArrowArray`；native 侧 `std::mem::forget(ffi_array)` 后由 Arrow Java import 接管生命周期
- `paimon_reader_get_schema` 写入 `FFI_ArrowSchema`；Java `Data.importSchema` 后关闭 ArrowSchema。导出的 schema 是“物理业务列 + `_row_index`”的 native output schema，不能导出 Parquet 原始全量 schema，也不能导出没有 `_row_index` 的业务 schema；Java 初始化时要与 `NativeFormatReaderFactory` 根据 `physicalReadType` 计算出的期望 Arrow schema 做严格比对
- `paimon_config_builder_with_target_schema(schema_addr)` 必须在 native 侧立即 import/copy 成 Rust `SchemaRef`；不能保存 Java 传入的 `ArrowSchema` 指针，因为 Java 调用结束后会关闭临时 schema
- 所有 FFI C string 入参用 `CStr::from_ptr` 读取；Java 不传 null，native 侧遇到无效 UTF-8/空必填字段返回 `CStatus(-1)`，不要 panic

### 5.4 PaimonReader Rust 主结构（基于 LakeSoul 实际签名）

```rust
// paimon-io/src/reader.rs
// 镜像 lakesoul-io/src/reader.rs:89-94
pub struct PaimonReader {
    io_session: PaimonIOSession,
    stream: Option<SendableRecordBatchStream>,
    pub(crate) schema: Option<SchemaRef>,
}

impl PaimonReader {
    pub fn new(config: PaimonIOConfig) -> Result<Self> {
        if config.files.is_empty() {
            anyhow::bail!("no files configured");
        }
        Ok(Self {
            io_session: PaimonIOSession::try_new(config)?,
            stream: None,
            schema: None,
        })
    }

    // 镜像 lakesoul-io/src/reader.rs:223-229
    pub async fn start(&mut self) -> Result<()> {
        self.io_session
            .execution_props_mut()
            .mark_start_execution(self.io_session.config().options().clone());
        // 1. session.try_new() 内部已注册 obs:// object store 并规范化文件路径
        // 2. build_physical_plan(filters=vec![]) 直接构造 FileScanConfig
        //    + ParquetSource → ExecutionPlan
        // 3. execute_stream(plan, task_ctx)? 拿到 SendableRecordBatchStream
        let plan = self.io_session.build_physical_plan(vec![]).await?;
        let stream = execute_stream(plan, self.io_session.task_ctx())?;
        self.schema = Some(stream.schema());
        self.stream = Some(stream);
        Ok(())
    }

    // 镜像 lakesoul-io/src/reader.rs:355-363
    pub async fn next_rb(&mut self) -> Option<Result<RecordBatch>> {
        match self.stream.as_mut() {
            None => None,
            Some(s) => s.next().await.map(|r| r.map_err(Into::into)),
        }
    }

    pub fn get_schema(&self) -> Option<SchemaRef> { self.schema.clone() }
}

// 镜像 lakesoul-io/src/reader.rs:278-282
pub struct SyncSendableMutablePaimonReader {
    inner: Arc<Mutex<PaimonReader>>,
    runtime: Arc<Runtime>,  // 实际就是 GLOBAL_RUNTIME.clone()
}

impl SyncSendableMutablePaimonReader {
    pub fn new_with_global_runtime(reader: PaimonReader) -> Self {
        Self {
            inner: Arc::new(Mutex::new(reader)),
            runtime: GLOBAL_RUNTIME.clone(),
        }
    }

    // 镜像 lakesoul-io/src/reader.rs:317-326
    pub fn start_blocked(&mut self) -> Result<()> {
        let inner = self.inner.clone();
        self.runtime.block_on(async move {
            inner.lock().await.start().await
        })
    }

    // 镜像 lakesoul-io/src/reader.rs:355-363
    pub fn next_rb_blocked(&self) -> Option<Result<RecordBatch>> {
        let inner = self.inner.clone();
        self.runtime.block_on(async move {
            inner.lock().await.next_rb().await
        })
    }
}
```

### 5.5 PaimonIOSession 关键路径

```rust
// paimon-io/src/session.rs
// 镜像 lakesoul-io/src/session.rs:63-77
pub static GLOBAL_RUNTIME: LazyLock<Arc<Runtime>> = LazyLock::new(|| {
    let threads = std::env::var("PAIMON_NATIVE_IO_WORKER_THREADS")
        .ok().and_then(|v| v.parse().ok()).unwrap_or(4);
    Arc::new(Builder::new_multi_thread()
        .worker_threads(threads)
        .max_blocking_threads(threads * 2)
        .enable_all()
        .build()
        .expect("build tokio runtime"))
});

impl PaimonIOSession {
    pub fn try_new(mut config: PaimonIOConfig) -> Result<Self> {
        // 1. 构造 SessionConfig（见 §5.6）
        // 2. 构造 RuntimeEnv + metadata cache
        // 3. 对每个 file 校验 obs:// scheme，注册 obs://bucket -> ObsObjectStore
        // 4. 规范化文件路径，保证 ListingTableUrl 能解析
        // 5. 初始化 OnceCell caches: listing_metas / file_format / file_schema / table_schema
    }

    // 关键：镜像 lakesoul-io/src/session.rs:720-803 的核心路径
    //       绕过 ListingTable，直接构建 FileScanConfig
    pub async fn build_physical_plan(
        &self,
        _filters: Vec<Expr>, // PoC 总是 empty
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let listing_metas = self.list_files().await?;
        if listing_metas.table_paths.is_empty() {
            return Ok(empty_exec_with_target_schema_and_row_index(&self.config));
        }
        // 阶段 1 Java 侧保证一个 NativeFileRecordReader 只传一个 data file。
        // 如果后续放开多文件，这里必须按 object_store_url 分组构造多个 scan，
        // 或在发现多个 object_store_url 时拒绝，不能只取 first。
        let object_store_url = single_object_store_url(&listing_metas.table_paths)?;
        let file_format = ParquetFormat::default();  // 直接用 Arrow ParquetFormat
        let table_schema = self.io_table_schema().await?;
        let file_schema = table_schema.file_schema().clone();        // Parquet 物理业务列，不含 _row_index
        let output_schema = append_row_index_schema(                  // native 对 JVM 暴露的完整 schema
            &project_schema(&file_schema, &self.config.target_schema)?,
            &self.config.row_index_column,
        )?;

        let partition_files: Vec<PartitionedFile> = listing_metas.object_metas
            .iter()
            .map(|m| PartitionedFile::from(m.clone()))
            .collect();

        let projection_indices = compute_projection_indices(
            &self.config.target_schema,
            &file_schema,
        )?;

        let source = file_format.file_source();
        let scan_config = FileScanConfigBuilder::new(
                object_store_url,
                file_schema.clone(),
                source,
            )
            .with_file_groups(vec![FileGroup::new(partition_files)])
            .with_projection_indices(projection_indices)
            .with_statistics(Statistics::new_unknown(file_schema.as_ref()))
            .with_row_index_column(self.config.row_index_column.clone())  // ★ 关键
            .build();

        file_format.create_physical_plan(self, scan_config).await
    }
}
```

**注意 `with_row_index_column`**：DataFusion `FileScanConfigBuilder` 是否原生支持这个方法需要 verify（见 §11.1 第 1 项）。若不支持，需要在 arrow-rs Parquet reader 层加一层 wrapper，把 row_index 作为附加列拼到输出 RecordBatch 后。

这里有两个 schema 口径不能混淆：

- `file_schema` 是 Parquet reader 看到的物理业务列，不包含 `_row_index`；`projection_indices` 只能基于它计算，不能把虚拟列下推给 Parquet
- `output_schema` 是 native reader 对 JVM 暴露的 Arrow schema，等于投影后的业务列 + `_row_index`；`paimon_reader_get_schema` 和每个 `RecordBatch` 都必须使用这个口径
- `Statistics::new_unknown(...)` 传入的 schema 必须和 `FileScanConfig` 的文件 schema 口径一致；如果后续改成 wrapper plan 追加 `_row_index`，wrapper 的 `schema()` / `statistics()` 也要同步返回追加后的 output schema，不能出现 execution plan schema 不含 `_row_index`、实际 batch 含 `_row_index` 的漂移

空输入/空文件的 plan schema 也必须与 native 输出 schema 一致：业务列按 target schema，外加内部 row_index 列。不能用裸 `Schema::empty()` 作为通用 `EmptyExec` schema；否则非空 projection 但 0 行时，Java schema import / batch 校验会看到列集合不一致。对于单文件 0 行，native 可以直接 EOF，但 `paimon_reader_get_schema` 仍要返回完整 schema。

### 5.6 DataFusion Session / Runtime 配置（从 LakeSoul 补齐）

LakeSoul `LakeSoulIOSession::try_new` 里有几项不是业务逻辑，但会影响输出顺序、schema 行为和内存稳定性，Paimon PoC 也应显式保留：

```rust
let mut sess_conf = SessionConfig::default()
    .with_batch_size(io_config.batch_size)
    .with_parquet_pruning(true)
    .with_information_schema(true)
    .with_create_default_catalog_and_schema(true);

sess_conf.options_mut().optimizer.enable_round_robin_repartition = false;
sess_conf.options_mut().optimizer.prefer_hash_join = false;
sess_conf.options_mut().execution.target_partitions = 1;
sess_conf.options_mut().execution.parquet.dictionary_enabled = Some(false);
sess_conf.options_mut().execution.parquet.schema_force_view_types = false;
// 如果 DataFusion 51 暴露 timezone/session timezone 选项，固定为 UTC 或显式空值；
// 阶段 1 TIMESTAMP_WITHOUT_TIME_ZONE 不应受 Spark session timezone 影响。
```

保留理由：

- `enable_round_robin_repartition=false`：避免 RecordBatch 输出顺序被 repartition 打乱；DV 的 `returnedPosition()` 需要与 batch 内行顺序稳定对应
- `target_partitions=1`：PoC 里每个 native reader 只读一个文件/一组文件，先避免 DataFusion 内部并行切分导致 row_index 对齐复杂化
- 同一 data file 内必须保持物理扫描顺序输出 batch。不能开启会跨 row group / page 重排输出的并发读取策略；如果 arrow-rs/DataFusion 某个 async 配置可能乱序，PoC 必须显式关闭或限制为顺序流。JVM 侧的 row_index 非递减校验是最后防线，不是排序修复机制；发现乱序直接失败，不能在 JVM 缓存全文件后按 row_index 排序
- `parquet_pruning=true`：保留 DataFusion 默认能力，但阶段 1 native 不传 data filters，实际不会依赖谓词做 row group/page pruning；文件级跳过仍由 JVM `FileIndexEvaluator` 完成。row_index spike 可以单独构造 pruning/filter 场景验证未来兼容性，但 PoC 正式 native scan 不允许带 data filter/topN/limit
- `dictionary_enabled=false`：与 LakeSoul 保持一致，降低 Arrow Java 侧 dictionary vector 处理复杂度
- `schema_force_view_types=false`：避免 parquet view type 行为和 Paimon/Arrow Java 类型映射不一致
- timezone/session timezone：阶段 1 只支持 `TIMESTAMP_WITHOUT_TIME_ZONE`，DataFusion session 不应根据本机时区或 Spark session timezone 改写值。若 DataFusion 51 有显式 session timezone 配置，固定为 UTC 或空值，并在 timestamp correctness test 里覆盖不同 Spark session timezone

RuntimeEnv：

- 使用 `RuntimeEnvBuilder::new().with_cache_manager(...)`，至少保留 metadata cache 配置开关
- PoC 默认不启用 spill/memory pool；如果 benchmark 出现大 query 内存压力，再引入 LakeSoul 的 `GreedyMemoryPool` / temp dir 方案
- env var 改名为 `PAIMON_NATIVE_IO_WORKER_THREADS`、`PAIMON_NATIVE_IO_FILE_META_CACHE_LIMIT`

### 5.7 文件元数据 / schema 推断 / projection 细节（从 LakeSoul 补齐）

LakeSoul 在 `helpers::get_file_object_meta` 和 `file_format::infer_schema` 里有三点需要迁入：

1. **并发 head 文件元数据**
   - 用 DataFusion `TaskContext.session_config().options().execution.meta_fetch_concurrency` 控制并发
   - 对每个 `ListingTableUrl` 转成 object_store `Path` 后调用 `store.head(&path)`
   - 过滤 size `< 8` 的无效 Parquet 文件，避免 parquet metadata 读取 panic/无意义报错

2. **schema merge 前清理 field metadata**
   - LakeSoul `clear_metadata` 会清理 field metadata，再合并 schema
   - Paimon PoC 也应清理 field metadata，避免不同 parquet writer 写入的字段 metadata 差异导致 schema merge 失败
   - 顶层 schema metadata 如冲突则报错，保持显式失败

3. **projection indices 必须包含剩余 filter 列和 row_index 虚拟列**
   - PoC 不下推 SQL filter，但如果 JVM 侧未来传入 native filter，projection 不能只看 target schema
   - row_index 是虚拟列，不属于 parquet file schema；projection indices 只覆盖物理列，row_index 通过 FileScanConfig/reader wrapper 追加

### 5.8 依赖版本（与 LakeSoul workspace 对齐）

```toml
# paimon-native-io/rust/Cargo.toml (workspace)
[workspace.dependencies]
datafusion = "51"
datafusion-common = "51"
datafusion-common-runtime = "51"
datafusion-catalog-listing = "51"
datafusion-datasource = "51"
datafusion-datasource-parquet = "51"
datafusion-execution = "51"
datafusion-expr = "51"
datafusion-physical-expr = "51"
datafusion-physical-plan = "51"
datafusion-session = "51"
arrow = "57"
arrow-array = "57"
arrow-buffer = "57"
arrow-cast = "57"
arrow-schema = "57"
arrow-ipc = "57"
parquet = { version = "57", features = ["async", "zstd", "lz4", "snap"] }  # 去掉 encryption；codec feature 必须覆盖 Paimon 默认/常用压缩
object_store = "0.13"
tokio = { version = "1", features = ["rt-multi-thread", "macros"] }
tokio-stream = "0.1"
tokio-util = "0.7"
futures = "0.3"
async-trait = "0.1"
async-stream = "0.3"
anyhow = "1"
bytes = "1"
url = "2"  # 解析 obs:// URL
serde = { version = "1", features = ["derive"] }
serde_json = "1"
tracing = "0.1"
tracing-subscriber = { version = "0.3", features = ["env-filter"] }

# paimon-io/Cargo.toml
[dependencies]
datafusion = { workspace = true }
datafusion-common = { workspace = true }
datafusion-common-runtime = { workspace = true }
datafusion-datasource = { workspace = true }
datafusion-datasource-parquet = { workspace = true }
datafusion-execution = { workspace = true }
datafusion-physical-plan = { workspace = true }
datafusion-session = { workspace = true }
arrow = { workspace = true }
arrow-array = { workspace = true }
arrow-buffer = { workspace = true }
arrow-cast = { workspace = true }
arrow-schema = { workspace = true }
parquet = { workspace = true }
# 只依赖 object_store trait，不启用 aws backend；OBS client 内嵌在 paimon-io/src/obs
object_store = { workspace = true, default-features = false }
tokio = { workspace = true }
tokio-stream = { workspace = true }
tokio-util = { workspace = true }
futures = { workspace = true }
async-trait = { workspace = true }
async-stream = { workspace = true }
anyhow = { workspace = true }
bytes = { workspace = true }
url = { workspace = true }
serde = { workspace = true }
serde_json = { workspace = true }
tracing = { workspace = true }

# paimon-io-c/Cargo.toml
[lib]
name = "paimon_native_io"
crate-type = ["cdylib"]

[dependencies]
paimon-io = { path = "../paimon-io" }
arrow-array = { workspace = true }
arrow-schema = { workspace = true }
anyhow = { workspace = true }
tokio = { workspace = true }
```

**与 LakeSoul 差异**：
- 去掉 `datafusion-substrait`（不下推 SQL filter）
- 去掉 `proto` crate（不需要 Substrait Plan proto）
- 去掉 `rootcause`（用更通用的 `anyhow`）
- 去掉默认 `cbindgen` 生成步骤（PoC 不需要 C 头文件；必要时再用独立 profile 恢复）
- 去掉 parquet 的 `encryption` feature（PoC 不需要 Parquet 加密）
- **去掉 `hdrs` / `hdfs-sys` 等 HDFS 依赖**（仅 OBS）
- `paimon-io` 内嵌 OBS client（`src/obs/`），实现 `ObsObjectStore`；不启用 `object_store` 的 `aws` feature，不使用 `AmazonS3Builder`
- 现有 `/Users/opay-20240095/IdeaProjects/nativeio/obs-rust-sdk` 作为迁移来源和参考实现，核心代码迁入 Paimon 后由 Paimon native IO 统一版本/CI/发布
- native 层不接受 `s3://` 路径，也不解析 `fs.s3a.*` 配置

**编译 spike 要锁定的 DataFusion 51 API**：
- `FileScanConfigBuilder`、`FileGroup`、`PartitionedFile` 的准确 module path
- `ParquetFormat` / `ParquetSource` 的准确类型和构造方式
- `RuntimeEnv::register_object_store` 与 `ObjectStoreRegistry` 的 API 形态
- row index 虚拟列是否仍是 `with_row_index_column`；如果方法已移动或被删除，直接在 wrapper 层补列

- ✅ 保留 LakeSoul 的 DataFusion SessionConfig 稳定性设置：关闭 round-robin repartition、`target_partitions=1`、开启 parquet pruning、关闭 dictionary、`schema_force_view_types=false`
- ✅ 保留 LakeSoul 的 metadata/schema 处理经验：并发 head、过滤 size `< 8` 文件、schema merge 前清理 field metadata

---

## 6. Java 侧改造清单

### 6.1 PaimonNativeReader（从 LakeSoul `NativeIOReader` 改造）

去掉的方法：
- `addFilter` / `addFilterProto`（PoC 不下推 SQL filter）
- `addMergeOps`（无 merge）
- `setDefaultColumnValue`（无 schema evolution）
- **`tokioRuntime` / `tokioRuntimeBuilder` 相关初始化**（GLOBAL_RUNTIME，无须传）

保留并改名：
- `addFile(String file)`
- `initializeReader()`：内部调 `create_paimon_reader_with_global_runtime` + `start_reader` + `get_schema`
- `getSchema(): Schema`
- `nextBatchBlocked(long arrayAddr): int`
- `close()`

新增：
- `setTargetSchema(Schema arrowSchema)`：把 target schema 通过 Arrow C Schema FFI 传给 native（不是 JSON）
- `setBatchSize(int batchSize)`：行批大小，默认 4096
- `setRowIndexColumn(String colName)`：开启 row_index 虚拟列输出（**必须调用**，否则 DV 对齐会错）。列名由 `NativeIOOptions.rowIndexColumn()` 生成，默认建议 `__paimon_native_row_index`；如果用户表/read schema 已存在同名字段，必须生成不冲突的后缀名，不能硬编码 `_row_index`
- `setObjectStoreOption(String key, String value)`：OBS 配置 key/value 传递。JVM 侧把 Paimon 的 `fs.obs.*` 配置一对一塞给 native（核心键：`fs.obs.endpoint`、`fs.obs.access.key`、`fs.obs.secret.key`、可选 `fs.obs.session.token`、`fs.obs.region`）

LakeSoul Java 包装层经验需要保留：

- LakeSoul `NativeIOBase` 默认创建 32MB child allocator；Paimon 侧不要每个 reader 自建 root allocator，优先复用 Spark task / paimon-arrow 传入的 `BufferAllocator`，reader close 时只释放自己创建的 child allocator
- `Data.exportSchema` / `Data.importSchema` 后必须关闭临时 `ArrowSchema` 和临时 `CDataDictionaryProvider`；reader 持有的 provider 在 `close()` 里释放
- `nextBatchBlocked` 返回 `<0` 时立刻抛 `IOException`，并总是在 finally 中 `free_c_status`
- `nextBatchBlocked` 返回 `status == 0` 表示 EOF，Java 侧不能调用 `Data.importIntoVectorSchemaRoot` 读取未初始化 `ArrowArray`；应直接返回 `null` batch，并释放本次分配的临时 ArrowArray holder
- `PaimonNativeReader` / `NativeFileRecordReader` 对外优先抛 `IOException` 或其子类；不要把 `CStatus.err` 包成无语义的 `RuntimeException`。虽然 `DataFileRecordReader` 会把部分 `RuntimeException` 也按 corrupt 处理，但 native 读错误应保持文件 IO 语义，便于日志、测试和上层容错判断
- native reader 初始化失败时，调用 `free_paimon_reader(reader)` 后再抛异常，避免 `CResult.err` 泄漏
- `close()` 必须幂等，支持 partial-initialized 状态；`NativeFileRecordReader.close()` 先关 Arrow/VSR，再关 `PaimonNativeReader`，避免 native reader 释放后 Java vector 仍引用 C Data buffer
- `PaimonNativeReader` 需要一个简单状态机/锁：`nextBatchBlocked` 进入 native 前增加 in-flight 标记，返回后清除；`close/free_paimon_reader` 必须等待 in-flight 调用结束或在同一读线程串行执行，不能在另一个线程释放正在被 native 使用的 reader pointer。PoC 可以不实现主动 cancel token，但必须避免 use-after-free
- Spark task 取消时不能指望 `Thread.interrupt()` 打断正在进行的 OBS range read；`nextBatchBlocked` 进入 native 前后要检查线程中断状态，返回后若已中断应尽快 close reader 并抛 `IOException` / `InterruptedIOException`。Spark 侧 `PaimonPartitionReader.close()` 仍是主释放入口；native 层通过有限 OBS timeout 保证最坏阻塞时间有上界
- `PaimonJnrLoader` 沿用 LakeSoul 的 JNR 加载方式：从 classpath resource 复制 native lib 到 `java.io.tmpdir`，`URLConnection.setUseCaches(false)`，`LibraryOption.LoadNow=true`，`IgnoreError=true`；resource 路径采用 Paimon 平台目录规范（见 §8.3）
- Arrow Java 属性不能依赖 native lib 加载后才“修正”。`arrow.enable_unsafe_memory_access` / `arrow.allocation.manager.type` 等属性可能在 Arrow 类首次初始化时就被读取；Spark/Paimon 可能早于 `PaimonJnrLoader` 使用 Arrow。PoC 应把需要的 Arrow 属性作为 driver/executor 启动参数或 benchmark procedure 环境检查项；loader 只能做 best-effort 设置并校验当前 allocator manager，不应假设后置设置一定生效。Paimon 当前 `paimon-arrow` 使用 `arrow-memory-unsafe`，不要沿用 LakeSoul 的 Netty allocator 配置；如需显式设置 allocator manager，应使用 Paimon 现有依赖可用的 `Unsafe`

### 6.2 NativeFileRecordReader（新增 Java 类，实现 FileRecordReader<InternalRow>）

需要正确实现 `FileRecordIterator` 接口的 **`returnedPosition()`**——从 Arrow batch 的 `_row_index` 虚拟列读取真实物理位置，喂给上游 `ApplyDeletionFileRecordIterator`。

```text
class NativeFileRecordReader(
    file: Path,
    fileSize: Long,
    readType: RowType,
    nativeOptions: NativeIOOptions,
    arrowAllocator: BufferAllocator
) extends FileRecordReader[InternalRow] {

  private val nativeReader: PaimonNativeReader = {
    val r = new PaimonNativeReader()
    r.addFile(file.toString)
    r.setTargetSchema(toArrowSchema(readType))
    val rowIndexColumn = nativeOptions.rowIndexColumn()
    r.setRowIndexColumn(rowIndexColumn)  // ★ DV 对齐必需，且不能与业务列同名
    r.setBatchSize(4096)
    nativeOptions.objectStoreOptions().forEach { (k, v) =>
      r.setObjectStoreOption(k, v)
    }
    r.initializeReader()
    r
  }

  // 复用 paimon-arrow 模块的 ArrowBatchReader / Arrow2PaimonVectorConverter，
  // 但需要一个能返回 VectorizedColumnBatch 的内部适配器，不能只返回 Iterable<InternalRow>。
  private val arrowReader = new NativeArrowVectorBatchReader(nativeReader, readType, arrowAllocator)

  override def readBatch(): FileRecordIterator[InternalRow] = {
    if (!arrowReader.hasNext) return null
    val vsr: VectorSchemaRoot = arrowReader.next()
    // 把 row_index 列拆出来，业务列做 Paimon ColumnVector 包装
    val rowIndexVec = vsr.getVector(nativeOptions.rowIndexColumn()).asInstanceOf[BigIntVector]
    // readType 可以是 0 列（count(*) / exists）；此时仍要用 rowIndexVec.valueCount 生成空行 batch
    val columnarBatch = arrowReader.toPaimonBatchWithoutRowIndex(vsr)
    new NativeFileRecordIterator(columnarBatch, rowIndexVec, vsr, file)
  }

  override def close(): Unit = { arrowReader.close(); nativeReader.close() }
}

class NativeFileRecordIterator(
    batch: PaimonColumnarBatch,
    rowIndexVec: BigIntVector,
    owner: VectorSchemaRoot,
    path: Path
) extends FileRecordIterator[InternalRow] {
  private var currentIdx = -1
  private var lastReturnedIdx = -1
  private var released = false

  override def next(): InternalRow = {
    currentIdx += 1
    if (currentIdx >= batch.getNumRows) null
    else {
      lastReturnedIdx = currentIdx
      batch.getRow(currentIdx)
    }
  }

  // ★ DV 对齐的关键
  override def returnedPosition(): Long = {
    if (lastReturnedIdx < 0) {
      throw new IllegalStateException("returnedPosition called before next returned a row")
    }
    rowIndexVec.get(lastReturnedIdx)
  }

  override def filePath(): Path = path
  override def releaseBatch(): Unit = {
    if (!released) {
      released = true
      try {
        batch.release()
      } finally {
        owner.close()
      }
    }
  }
}
```

关键技术点：

- **复用 paimon-arrow 转换**：Paimon 已有 `paimon-arrow` 模块的 `ArrowBatchReader` / `Arrow2PaimonVectorConverter`，避免重复造轮子；但现有 `ArrowBatchReader.readBatch` 只返回 `Iterable<InternalRow>`，内部还复用一个 `VectorizedColumnBatch`，没有 `releaseBatch()` owner，也没有 `returnedPosition()`。native reader 不能直接把它的 iterator 暴露给上层，必须新增一个面向 `VectorSchemaRoot` owner 的小适配器或扩展方法，每个返回 batch 绑定自己的 Arrow root / dictionary / row_index 生命周期
- **`returnedPosition()` 从 row_index 列读**：不是计数器。该列必须是 Parquet 文件内真实物理行号；若 arrow-rs 只返回 row group 内相对行号，native 侧必须加 row group offset 后再输出
- **`returnedPosition()` 调用语义对齐 Paimon `ColumnarRowIterator`**：第一次 `next()` 前调用必须抛 `IllegalStateException`；同一行重复调用必须返回同一个 row_index，不能推进位置；`next()` 返回 `null` 后不再产生新的 position
- **row_index 列名不能污染业务 schema**：`_row_index` 只是文档里的逻辑名；实现默认用 `__paimon_native_row_index`，并在 `NativeIOOptions` 初始化时避开 read schema 中已有字段。导入 Arrow 后必须按该内部列名拆列，再把它从业务 batch 中剥离
- **每个 Arrow batch 都要校验 schema 和顺序**：JVM 导入 `VectorSchemaRoot` 后，必须确认业务列集合、顺序、Arrow 类型与 `physicalReadType` 对应，且内部 row index 列存在、类型为 signed 64-bit integer、无 null、行数与所有业务 vector 一致。row_index 值必须在 `[0, file.rowCount())` 范围内，且同一文件内跨 batch 按输出顺序非递减；`file.rowCount()==0` 时 native 应直接 EOF，不应返回任何带 row_index 的 batch。任何 mismatch 都抛 `IOException`，不能继续按位置读；这能尽早暴露 Rust projection / row_index 拼接错误或 DataFusion 并发乱序
- **String 零拷贝不是阶段 1 前置条件**：当前 `Arrow2PaimonVectorConverter` 对 `VarCharVector.get()` 会生成 byte[]，阶段 1 接受这部分 copy 以保证正确性；阶段 2 性能优化再补 Arrow buffer 直读的 `BytesColumnVector` 实现
- **空投影 / `count(*)` 是必测路径，但 Spark SQL 可能被 aggregate pushdown 绕开**：Paimon Spark 对 DV 表支持 `COUNT(*)` aggregate pushdown，可能直接生成 `PaimonLocalScan` 而不创建 `PaimonPartitionReader`。因此空投影正确性必须至少有 core/read 层测试直接构造 0 列 `readType`；Spark SQL `count(*)` 只能作为端到端补充，不能单独证明 native reader 支持空投影
- native 传给 DataFusion 的 physical projection 可以为空，输出给 JVM 的 Arrow batch 仍必须包含 `_row_index` 虚拟列；JVM 侧把 `_row_index` 从业务 schema 中剥离后，构造 0 列但 `numRows=rowIndexVec.getValueCount()` 的 Paimon batch，每个 `InternalRow` 为空行
- 空投影 batch 可以用 `new VectorizedColumnBatch(new ColumnVector[0])` 并设置 `numRows`；`ColumnarRow.getFieldCount()` 会返回 0，适合 `count(*)` 路径
- **阶段 1 一文件一个 native reader**：`FileRecordIterator.filePath()` 是 Spark metadata column 的来源，DV 也是按文件解释 row position。即使 Rust config builder 支持 add 多个 file，`NativeFileRecordReader` 首版也只能传一个 data file；split 内多文件由 `ConcatRecordReader` 串联
- Rust `PaimonIOSession.build_physical_plan` 也要按单文件假设实现。不能在 `FileScanConfigBuilder` 里对多个 OBS bucket/authority 只取第一个 `object_store_url`；如果未来允许一个 native reader 读多文件，必须按 `object_store_url` 分组生成多个 scan 再 concat，或直接拒绝多 store 输入
- **资源生命周期**：`NativeFileRecordIterator` 必须持有 `VectorSchemaRoot` owner；`VectorSchemaRoot` / row_index vector 必须活到 `releaseBatch()`，不能在 `readBatch()` 返回前关闭 VSR，否则 Paimon column vector 或 row_index vector 会悬空。`releaseBatch()` 要同时释放 Paimon batch wrapper 和 Arrow root，并做成幂等，避免上层异常路径重复释放
- **不能提前复用 batch buffer**：Paimon `RecordReader.readBatch()` 契约允许上层在一段时间内持有返回的 iterator 及其对象。`NativeFileRecordReader` 不能在上一批 `releaseBatch()` 前复用同一个 `VectorSchemaRoot` / Arrow buffer / Paimon column vector；如果采用对象池，也必须以 `releaseBatch()` 作为归还点，否则连续 `readBatch()` 可能覆盖仍被 Spark 持有的行对象
- **异常关闭兜底**：`NativeFileRecordReader.close()` 要释放仍未 `releaseBatch()` 的 outstanding iterator/root，并让后续 iterator 操作快速失败或返回 null；不能只关闭 native reader 而把 Java Arrow root 留给 GC。实现可维护一个当前 outstanding batch 引用，正常 `releaseBatch()` 清空，close 时幂等释放

### 6.3 NativeRawFileSplitRead（平行 RawFileSplitRead）

复制 `RawFileSplitRead.java` 的整体骨架，而不是只替换文件 reader。必须保留它对 Paimon 读语义的处理：

- `DeletionVector.Factory` 构造与 `ApplyDeletionVectorReader` 包装
- `FileIndexEvaluator.evaluate` 的文件级跳过
- `FormatReaderMapping` 的 schema id、projection、filter、topN、limit 计算
- `ignoreCorruptFiles` / `ignoreLostFiles` 的错误语义
- `rowTrackingEnabled` 等来自 `CoreOptions` 的读语义开关；PoC 不支持 row tracking reader 注入，因此必须在文件级 guard 中显式拒绝，不能只看 `FormatReaderMapping`

还要注意 `FormatReaderMapping.Builder.build(...)` 里有一个关键中间结果：`actualReadRowType`。它是 Java format reader 真正读取的物理/逻辑折中 schema，已经应用了：

- schema id 演进后的 `readDataFields`
- `_KEY_` 字段裁剪后的 `trimmedKeyPair`
- partition 字段裁剪后的 `trimmedResult`

当前 Paimon 代码只把局部变量 `actualReadRowType` 传给 `formatDiscover.discover(...).createReaderFactory(...)`，`FormatReaderMapping` 对象本身没有保存这个中间结果，也没有 getter。native 分支不能只用表层 `readType` 重建 Rust/Arrow projection，否则在 PK data file thin-mode、普通 PK data file、partition trim 或后续 schema evolution 场景会出现“Java 读 A schema，native 读 B schema”的隐性错误。PoC 需要在 core 中给 `FormatReaderMapping` 增加 `private final RowType actualReadRowType` 字段，构造器接收并保存 `Builder.build(...)` 计算出的同一个对象，再暴露只读 accessor（例如 `getActualReadRowType()`、`hasIdentityIndexMapping()`、`hasNoCastMapping()`），让 native guard 和 `NativeFormatReaderFactory` 使用同一份 mapping 结果。

但 PoC 不实现 Java reader 的全部功能。任何 native 未确认支持的语义必须在 `SupportsNativeIO` 中拒绝，并在 `NativeRawFileSplitRead` 内为该文件构造 Java `DataFileRecordReader`。这是“unsupported 不进入 native”，不是 native 失败后的 auto-fallback。

注意 Java fallback 不是简单调用 `formatReaderMapping.getReaderFactory()` 读一遍文件。`NativeRawFileSplitRead.createFileReader(...)` 已经算出的 `fileIndexResult`、`selection` 和 `deletionVector` 必须按 `RawFileSplitRead` 原顺序继续生效：`BitmapIndexResult` 时把 selection 放入 `FormatReaderContext`，创建 Java `DataFileRecordReader` 后再包 `ApplyBitmapIndexRecordReader`，最后如果 DV 非空再包 `ApplyDeletionVectorReader`。否则 bitmap 命中的 fallback 文件会读出超集，或者 DV 与 bitmap 顺序和 Java 路径不一致。

| 阶段 | Java 路径 | Native 路径 |
|---|---|---|
| `DeletionVector.Factory` 构造（line 179-184） | 保留 | 保留 |
| `FileIndexEvaluator.evaluate` | 保留 | 保留 |
| 选择性 (`fileIndexResult.remain() == false`) | 直接 return `EmptyFileRecordReader` | 同上 |
| `BitmapIndexResult` 选择 (`selection`) | 传给 `FormatReaderContext` 并再包 `ApplyBitmapIndexRecordReader` | **PoC 暂不支持**；一旦 `fileIndexResult instanceof BitmapIndexResult`，该文件走 Java reader |
| schema id / cast / index mapping | `FormatReaderMapping` 处理 | **PoC 只支持无需 cast、无需字段重排的 schema**；但必须用 `FormatReaderMapping.getActualReadRowType()` 作为 native physical read schema；否则走 Java reader |
| partition / system fields | `PartitionUtils` 和 system fields 注入 | **PoC 不支持**；读 schema 包含分区列、row tracking、metadata/system field 时走 Java reader |
| data filters / topN / limit | `FormatReaderFactory` 在文件 reader 内执行 | **PoC 不下推 SQL filter**，因此 `mapping.getDataFilters()` 非空、`topN` 非空或 `limit` 非空时走 Java reader |
| 创建文件级 reader | `DataFileRecordReader(...formatReaderMapping...)` | `DataFileRecordReader(NativeFormatReaderFactory, context, ignoreCorruptFiles, ignoreLostFiles, ...)` 包一层；`NativeFormatReaderFactory.createReader` 内构造 `NativeFileRecordReader`，复用 Paimon 对 lost/corrupt 文件的创建期与读取期容错语义 |
| `ApplyDeletionVectorReader` 包装（line 308-309） | 保留 | **保留**（前提：Native reader 实现 `FileRecordIterator.returnedPosition()`） |

注意 `ignoreLostFiles` / `ignoreCorruptFiles` 不能只靠 `DataFileRecordReader` 的第二个构造器。第二个构造器接收的 `FileRecordReader` 已经构造完，无法捕获 native reader 初始化阶段的 OBS head/open/parquet metadata 异常。PoC 应新增 `NativeFormatReaderFactory`，让 `DataFileRecordReader.createReader(...)` 调 `factory.createReader(context)` 时再初始化 native reader，这样文件丢失和损坏文件在创建期也能沿用原有判断和日志。

这不属于 native auto-fallback：如果 `ignoreCorruptFiles=false`，已进入 native 后的异常仍抛出；如果 `ignoreCorruptFiles=true`，行为与 Paimon 现有 Java reader 一致，按“忽略损坏文件”跳过该文件，而不是改走 Java reader 重读。

但要区分“文件损坏”和“外部 IO/权限故障”。当前 `DataFileRecordReader.ignoreCorruptException` 在 `ignoreCorruptFiles=true` 时会宽泛吞掉 `IOException` / `RuntimeException` / `InternalError`；native OBS 的鉴权失败、连接超时、服务端 5xx 不应被当成 corrupt file 静默跳过。PoC 需要在 core 中增加一个中立异常标记（例如 `NonCorruptFileReadException extends IOException` 或等价 marker），并调整 `ignoreCorruptException` 对该类返回 false。`NativeFileRecordReader` 把 OBS auth/timeout/5xx/range status 错误映射为该异常；Parquet footer/页损坏仍可按 corrupt 语义受 `ignoreCorruptFiles` 控制。

`NativeFormatReaderFactory` 只实现整文件 `createReader(Context)`。`FormatReaderFactory.createReader(Context, offset, length)` 目前主要给 format table text/csv/json 分片读取使用，默认实现会抛 `UnsupportedOperationException`；阶段 1 native raw read 不支持 byte-range split。若未来要支持 Parquet split 内切分，必须重新证明 row_index 是文件绝对位置且 DV/metadata column 顺序仍一致。

### 6.4 KeyValueTableRead 选择逻辑（含开关基础设施）

```java
// 在 KeyValueTableRead 构造 readProviders 时，native provider 必须排在 Java raw provider 前面。
// provider.match 只能做 split 级粗判断；文件级 schema/filter/index 判断在 NativeRawFileSplitRead 内完成。
List<SplitReadProvider> providers = new ArrayList<>();
NativeSplitReadProviderLoader.tryCreate(nativeContext, this::config)
    .ifPresent(providers::add);
providers.add(new PrimaryKeyTableRawFileSplitReadProvider(batchRawReadSupplier, this::config));
providers.add(new MergeFileSplitReadProvider(mergeReadSupplier, this::config));
providers.add(new IncrementalChangelogReadProvider(mergeReadSupplier, this::config));
providers.add(new IncrementalDiffReadProvider(mergeReadSupplier, this::config));
this.readProviders = Collections.unmodifiableList(providers);
```

不要用 `Arrays.asList(...)` 后再插入 provider；现有 `KeyValueTableRead` 需要改成 `ArrayList` 组装，才能在 native provider 不可用时保持原顺序完全不变。

`NativeRawFileSplitReadProvider` 不应继承现有 `RawFileSplitReadProvider`，因为后者的 `LazyField` 类型写死为 `RawFileSplitRead`。native 读类是平行实现，应该直接实现 `SplitReadProvider`：

```java
public final class NativeRawFileSplitReadProvider implements SplitReadProvider {
    private final LazyField<NativeRawFileSplitRead> splitRead;

    public NativeRawFileSplitReadProvider(
            Supplier<NativeRawFileSplitRead> supplier,
            SplitReadConfig splitReadConfig) {
        this.splitRead = new LazyField<>(() -> {
            NativeRawFileSplitRead read = supplier.get();
            splitReadConfig.config(read);
            return read;
        });
    }

    @Override
    public LazyField<NativeRawFileSplitRead> get() {
        return splitRead;
    }
}
```

这一点很关键：`KeyValueTableRead.forceKeepDelete` / `applyReadType` / `withFilter` / `withTopN` / `withLimit` / `withIOManager` 可能在 reader lazy 初始化前调用，也可能在初始化后调用。native provider 必须和 Java provider 一样通过 `SplitReadConfig` 对 lazy reader 补齐当前配置；`NativeRawFileSplitRead` 自身也必须实现并保存这些 `SplitRead` 状态，否则 guard 可能看不到 forceKeepDelete/filter/topN/limit，导致错误进入 native。即使 `provider.match(split, Context.forceKeepDelete=true)` 已经拒绝 native，`NativeRawFileSplitRead.forceKeepDelete()` 也不能做成无状态 no-op，避免直接单测或未来调用顺序变化时失守。

如果 `NativeRawFileSplitRead` 复制 `RawFileSplitRead` 的 `formatReaderMappings` cache，cache key 不能只用 `FormatKey(schemaId, formatIdentifier)` 后长期复用。native guard 依赖 `readRowType`、filters、topN、limit 和 `actualReadRowType` 等价性；这些配置变化后必须清空 mapping cache，或把 read type/filter/topN/limit fingerprint 纳入 cache key。否则先读宽 schema、后切窄 schema 的测试会复用旧 mapping，导致 native preflight 与实际 reader schema 不一致。

两层选择模型：

1. `NativeRawFileSplitReadProvider.match(split, context)` 只做 split 级粗判断：
   - 当前 connector 明确支持 native IO（PoC 为 Spark-only；Flink/Core/未知引擎拒绝）
   - native 开关打开
   - ServiceLoader 找到 native provider
   - `CoreOptions.deletionVectorsEnabled()` 为 true（PoC 只做 DV 模式 PK 表）
   - `split instanceof DataSplit`
   - `!context.forceKeepDelete()`
   - `!dataSplit.isStreaming()`
   - `dataSplit.rawConvertible()`
   - `allFilesHaveDeleteRowCount(split)`
2. `NativeRawFileSplitRead.createFileReader(...)` 对每个文件做完整判断。支持 native 的文件通过 `NativeFormatReaderFactory` 交给 `DataFileRecordReader` 创建 `NativeFileRecordReader`；不支持的文件构造 Java `DataFileRecordReader`。最终仍用 `ConcatRecordReader` 串起来，因此一个 split 可以是 native/Java 混合读。构造顺序必须严格保留 `dataSplit.dataFiles()` 原顺序，不能按 native/Java 分组重排；否则 Spark metadata file path、row index 以及未排序查询的自然输出顺序都会和 Java 路径漂移。`FormatReaderFactory.Context` / `FormatReaderContext` 只有 `fileIO/filePath/fileSize/selection`，没有 `DataFileMeta.rowCount()`；native 的 row_index 范围校验必须由 `NativeFormatReaderFactory` 显式持有当前 `DataFileMeta` 或 `rowCount`，不要为了 native PoC 扩展公共 `FormatReaderFactory.Context`

`SupportsNativeIO.checkNativeFile` 文件级守门员返回 `NativeApplicability`，业务上必须全部满足：

```java
public static NativeApplicability checkNativeFile(
        DataSplit split,
        DataFileMeta file,
        FormatReaderMapping mapping,
        @Nullable FileIndexResult fileIndexResult,
        RowType readType,
        boolean rowTrackingEnabled,
        NativeIOOptions options,
        Path actualDataPath) {
    if (!options.enabled()) return rejected(DISABLED);
    if (!"parquet".equals(formatIdentifier(file.fileName()))) return rejected(NON_PARQUET_FILE);
    if (!"obs".equals(actualDataPath.getScheme())) return rejected(NON_OBS_PATH);
    if (!options.hasRequiredObsConfig()) return rejected(MISSING_OBS_CONFIG);
    if (hasUnsupportedTypes(readType)) return rejected(UNSUPPORTED_TYPE);
    if (!samePhysicalAndLogicalReadType(mapping.getActualReadRowType(), readType)) {
        return rejected(READ_TYPE_MISMATCH);
    }
    if (requiresCastOrReorder(mapping)) return rejected(SCHEMA_CAST_OR_REORDER);
    if (requiresPartitionOrSystemFields(mapping, rowTrackingEnabled)) {
        return rejected(PARTITION_OR_SYSTEM_FIELDS);
    }
    if (!noDataFiltersTopNOrLimit(mapping)) return rejected(DATA_FILTER_TOPN_LIMIT);
    if (fileIndexResult instanceof BitmapIndexResult) return rejected(BITMAP_INDEX_SELECTION);
    return applicable();
}
```

任何一条不满足 → 该文件走 Java reader（透明不进入 native，不算 native 失败 fallback），并把 reason 写入 task-local applicability 统计。

注意：`NativeRawFileSplitRead` 只接 `rawConvertible` split；非 raw split 仍走 `MergeFileSplitRead`（不变）。
同时，split 级 guard 必须确认表配置 `deletion-vectors.enabled=true`。`KeyValueTableRead` 覆盖的是 PK 表读路径，但 Paimon 还有 `FIRST_ROW` / 单 level 等 raw-convertible 场景；这些不是本 PoC 范围，不能只靠 `rawConvertible=true` 放进 native。

---

## 7. 数据流（端到端单次 batch）

1. **Driver**：`PaimonScan.toBatch()` → `PaimonInputPartition(splits)`
2. **Executor**：`PaimonPartitionReader` 取一个 `DataSplit`
3. `KeyValueTableRead.createReader(split)`：
   - `SupportsNativeIO.checkNativeSplit` 做 split 级粗判断
   - 通过 → `NativeRawFileSplitRead`；不通过 → 原 `RawFileSplitRead`（透明）
4. `NativeRawFileSplitRead.createFileReader()` 对每个 `DataFileMeta`：
   - 构造 `DeletionVector`（如果存在）
   - 用 `DataFilePathFactory.toPath(file)` 得到真实 data file path；该路径可能来自 external path，不能自己拼 table path
   - `SupportsNativeIO.checkNativeFile` 做文件级完整判断，再做 `NativeParquetSchemaValidator` footer preflight
   - 全部通过 → 构造 `NativeFormatReaderFactory(file, file.rowCount(), mapping.getActualReadRowType(), readType, nativeOptions, allocator)`，再交给 `DataFileRecordReader` 创建 `NativeFileRecordReader`
   - 不通过 → 构造 Java `DataFileRecordReader`
   - native 分支仍必须经过 `DataFileRecordReader` 的 `FormatReaderFactory` 构造器，复用 `ignoreCorruptFiles` / `ignoreLostFiles` 包装；由于 PoC guard 已拒绝 cast / partition / row tracking / selection，这些参数传 null / false / empty
   - 用 `ApplyDeletionVectorReader` 包装
5. `NativeFileRecordReader` 构造时（初次）：
   - JNR 调 `new_paimon_io_config_builder`
   - JNR 调 `paimon_config_builder_add_file(file_path)`
   - JNR 调 `paimon_config_builder_with_target_schema(arrow_schema_addr)` ← Arrow C Schema FFI；`count(*)` 时 target schema 可以是 0 个业务列
   - JNR 调 `paimon_config_builder_with_row_index_column(nativeOptions.rowIndexColumn())` ← **必需**
   - JNR 调 `paimon_config_builder_set_object_store_option("fs.obs.endpoint", "...")` / `fs.obs.access.key` / `fs.obs.secret.key` 等多次
   - JNR 调 `create_paimon_io_config_from_builder`
   - JNR 调 `check_io_config_created(config)`；如果非空，释放 config 后抛 `IOException`
   - JNR 调 `create_paimon_reader_with_global_runtime(config)`
   - JNR 调 `start_reader(reader)`（同步阻塞，native 内 `GLOBAL_RUNTIME.block_on`）
  - JNR 调 `paimon_reader_get_schema(reader, schema_addr)`，检查 `CStatus` 并释放，再 `Data.importSchema(...)` 得到 `org.apache.arrow.vector.types.pojo.Schema`
6. 每次 `readBatch()`：
   - JVM 分配 `ArrowArray` FFI struct
   - JNR 调 `next_record_batch_blocked(reader, array_addr)`
   - Native：`GLOBAL_RUNTIME.block_on(stream.next())` → `RecordBatch`（含 `_row_index` 列）
   - Native：`RecordBatch` → `FFI_ArrowArray` 拷贝到 `array_addr`
   - JVM：`Data.importIntoVectorSchemaRoot` 得到 `VectorSchemaRoot`
   - JVM：校验 `CStatus.status`、`VectorSchemaRoot.getRowCount()`、`_row_index.valueCount` 三者一致且大于 0；`status == 0` 只按 EOF 处理，不能 import 本次 ArrowArray
   - JVM：拆出 `_row_index` 列，业务列做 Paimon `ColumnVector` 包装
   - 如果业务列为 0，则用 `_row_index.valueCount` 构造 0 列 Paimon batch，不能把该 batch 当 EOF
   - JVM：包装为 `NativeFileRecordIterator`，每次 `next()` 输出 InternalRow，`returnedPosition()` 从 `_row_index` 列读
7. `ApplyDeletionFileRecordIterator.next()` 用 `returnedPosition()` 查 DV bitmap，跳过被删除的行
8. `PaimonPartitionReader` 把 `InternalRow` → `SparkInternalRow` 给 Spark

---

## 8. 关键技术问题与处置

### 8.1 OBS 配置传递

**Paimon 现状**：Java 侧已有 `OBSFileIO`（`paimon-obs-impl/.../OBSFileIO.java`），配置走 `fs.obs.*` 前缀（`OBSFileIO.java:53`）。Manifest / snapshot 等元数据继续走 Java OBSFileIO，**native 仅接 Parquet 数据文件读取**，两条路径并存。

**需要补齐的 Paimon 侧配置来源**：`RawFileSplitRead` 当前只持有 `FileIO`、`CoreOptions`、`FileStorePathFactory`，并没有直接持有 Hadoop `Configuration`。因此不能把 `NativeFileRecordReader` 设计成依赖 `hadoopConf` 入参。PoC 使用 `NativeIOOptions` 显式收集配置：

1. `CoreOptions.toConfiguration()` / table options 中的 `fs.obs.*`
2. Spark session conf 中的 `spark.hadoop.fs.obs.*` 和 `spark.paimon.fs.obs.*`（进入 Paimon options 前去掉相应前缀）
3. catalog / warehouse 初始化时传入的 Paimon options

Spark 侧具体落点：

- `spark.paimon.*`：现有 `OptionUtils.copyWithSQLConf` 已经会去掉 `spark.paimon.` 前缀并合并到 table options，因此 `spark.paimon.native-io.enabled=true` 可以自然进入 core
- `spark.hadoop.fs.obs.*`：当前不会被 `OptionUtils.copyWithSQLConf` 合并；需要在 `SparkGenericCatalog.autoFillConfigurations` / `SparkSource.loadTable` 的 path-table 分支中从 `sessionState.newHadoopConf()` 抽取 `fs.obs.*`，放入 Paimon options
- `spark.paimon.fs.obs.*`：会被 `OptionUtils` 转成 `fs.obs.*`，遵循 Paimon 现有 dynamic option 语义，覆盖 table schema 中的同名 `fs.obs.*`
- `spark.hadoop.fs.obs.*`：只作为 fallback 补齐缺失键，不能覆盖已经由 table option、path/table extraOptions 或 `spark.paimon.fs.obs.*` 得到的 `fs.obs.*`
- 可实现优先级：DataSource/path extraOptions `fs.obs.*` > identifier-specific / global `spark.paimon.fs.obs.*` 动态配置 > 持久化 table/catalog option `fs.obs.*` > `spark.hadoop.fs.obs.*` fallback。这个顺序与 `OptionUtils.copyWithSQLConf` 的动态 option 行为一致，同时避免 Hadoop 默认配置覆盖 Paimon 显式配置

`NativeIOOptions` 只保留 `fs.obs.*` 和 native 自身配置，不读取 Hadoop 全量配置，避免把无关 secret 或 HDFS/S3 配置传入 native。必填键：

- `fs.obs.endpoint`
- `fs.obs.access.key`
- `fs.obs.secret.key`

可选键：

- `fs.obs.session.token`
- `fs.obs.region`
- `fs.obs.connection.timeout`
- `fs.obs.socket.timeout`
- `fs.obs.max.connections`

Paimon 官方 OBS 文档里的 `fs.obs.endpoint` 示例是裸 hostname（如 `obs-endpoint-hostname`），而部分内部环境可能配置成 `https://obs...`。Native OBS client 必须同时接受裸 hostname 和带 `http://` / `https://` scheme 的 endpoint：裸 hostname 默认按 HTTPS 访问；签名 canonical host 使用去掉 scheme 的 host[:port]；`env.json` 记录脱敏 endpoint 和是否 HTTPS。不能因为 endpoint 形式不同导致 Java OBSFileIO 可读、native 不可读。

超时必须有有限默认值：如果用户未配置，native OBS client 也要设置合理的 connect/read/socket timeout（例如 30s/60s，具体值在实现中固定并写入 `env.json` 的脱敏配置摘要）。PoC 暂不实现 native cancel token，不能允许 OBS range read 在 `next_record_batch_blocked` 中无限阻塞；超时错误按外部 IO 错误传播，`ignoreCorruptFiles` 只处理文件损坏语义，不吞掉鉴权、网络超时和服务端 5xx。由于现有 `DataFileRecordReader` 的 ignoreCorrupt 判断较宽，native 必须使用上文的非 corrupt 异常标记让这些错误逃逸。

如果 `FileIO` 是 `OBSFileIO` 但这些配置不在 table/catalog options 中，Spark 接入层必须在创建 catalog/table 时把 session conf 合并进 Paimon options；否则 native provider 的 `match` 返回 false，继续走 Java reader。

安全要求：`fs.obs.access.key`、`fs.obs.secret.key`、`fs.obs.session.token` 不能出现在 INFO/WARN 日志、异常 message、benchmark `env.json` 或 applicability 报告里；最多输出是否存在和脱敏后缀。

脱敏不能只靠调用方自觉：

- Java `NativeIOOptions` / `NativeSplitReadContext` / `NativeApplicability.detail` 必须实现受控 `toString()`，禁止打印完整 `options` map。敏感 key 判断要大小写不敏感，覆盖 `access.key`、`secret.key`、`session.token`、`password`、`credential` 等常见片段
- Rust `ObsConfig` / `PaimonIOConfig` / error context 不要 `#[derive(Debug)]` 直接输出全字段；需要 `Debug` 时手写 redacted 版本，只输出 endpoint、bucket、region、timeout、max connections、credential presence
- `fs.obs.endpoint` 不允许包含 URL userinfo（例如 `https://ak:sk@host`）；解析到 userinfo 时直接拒绝并返回配置错误，避免 endpoint 脱敏遗漏导致密钥进入日志或签名 canonical host
- panic 转错误、`anyhow::Context`、OBS request debug、JNR exception message 都要走同一 redaction helper；测试里要用假 AK/SK/token 做字符串扫描，确保异常和报告里都没有原文

**Native 侧实现专用 OBS 文件系统适配**：输入路径必须是 `obs://bucket/...`，配置只读取 `fs.obs.*`。Rust 侧实现 `ObsObjectStore` 并挂到 DataFusion 的 `ObjectStoreRegistry`，底层直接调用 `paimon-io/src/obs/` 内嵌 OBS client 的 `head_object` / `get_object_range` / `get_object_stream` 等 OBS 原生 API；不使用 `object_store::aws::AmazonS3Builder`，避免 AWS S3 与 OBS 在签名、endpoint、错误模型和边界行为上的差异。OBS client 代码从现有 obs-rust-sdk 迁入 Paimon native IO 后随 Paimon 一起演进，如果 DataFusion 接入需要补充 range/stream/list/error/retry 等能力，直接修改内嵌 OBS 模块。

```rust
// paimon-io/src/object_store.rs
use crate::obs::{Client as ObsClient, Config as ObsConfig};
use object_store::{GetOptions, GetResult, ObjectMeta, ObjectStore, path::Path};

#[derive(Debug, Clone)]
pub struct ObsObjectStoreConfig {
    bucket: String,
    endpoint: String,
    access_key: String,
    secret_key: String,
    session_token: Option<String>,
    region: Option<String>,
}

#[derive(Debug)]
pub struct ObsObjectStore {
    bucket: String,
    client: ObsClient,
}

pub fn register_obs_object_store(
    url: &Url,                        // obs://bucket-name
    config: &HashMap<String, String>, // fs.obs.*
    runtime: &RuntimeEnv,
) -> Result<()> {
    anyhow::ensure!(url.scheme() == "obs", "native IO only supports obs:// paths");
    let bucket = url.host_str().ok_or_else(|| anyhow::anyhow!("obs bucket is required"))?;
    let endpoint = config.get("fs.obs.endpoint")
        .ok_or_else(|| anyhow::anyhow!("fs.obs.endpoint required"))?;
    let access_key = config.get("fs.obs.access.key").ok_or(...)?;
    let secret_key = config.get("fs.obs.secret.key").ok_or(...)?;

    let obs_config = ObsConfig::builder()
        .access_key(access_key, secret_key)
        .endpoint(endpoint)
        .build()?;
    let store = ObsObjectStore {
        bucket: bucket.to_string(),
        client: ObsClient::from_config(obs_config)?,
    };

    runtime.register_object_store(url, Arc::new(store));
    Ok(())
}

#[async_trait]
impl ObjectStore for ObsObjectStore {
    async fn head(&self, location: &Path) -> object_store::Result<ObjectMeta> {
        // client.head_object().bucket(&self.bucket).key(location.as_ref()).send().await
        // 映射为 ObjectMeta { location, last_modified, size, e_tag, version }
    }

    async fn get_opts(&self, location: &Path, options: GetOptions) -> object_store::Result<GetResult> {
        // range -> client.get_object().range("bytes=start-end").send_streaming().await
        // no range -> client.get_object().send_streaming().await
        // 映射为 GetResult::Stream，供 DataFusion Parquet reader 做 range/async read
    }

    fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, object_store::Result<ObjectMeta>> {
        // PoC 直接构造 PartitionedFile，不依赖 list；可返回 unsupported，或仅为调试实现 list_objects_v2
    }
}
```

`ObjectStore` trait 实现要求：

- 即使 PoC 只读，也要实现当前 `object_store 0.13` trait 要求的所有方法；写入、删除、复制、multipart 等非读方法统一返回 `object_store::Error::NotImplemented` / `Unsupported`
- 读路径至少要保证 `head`、`get_opts`、`get_range` / `get_ranges`（如 trait 要求）、`list` / `list_with_delimiter`（可只用于诊断）编译通过并行为明确
- `ObjectMeta` 字段映射要稳定：`location` 必须是去掉 bucket 后的 object key；`size` 来自 OBS content-length；`e_tag` / version 可选；`last_modified` 如果 OBS 返回值缺失或解析失败，返回带 status/detail 的 external error，不能用本地当前时间伪造，否则 DataFusion metadata cache 和诊断会不可复现
- JVM footer preflight 与 native scan 之间要防对象被覆盖的 TOCTOU：preflight 成功后至少把 file size、footer row count、OBS `e_tag` / version（如果可得）放入 `NativeFormatReaderFactory` / native config；native `head` 返回的 size 必须与 `DataFileMeta.fileSize()` 和 preflight size 一致，若 e_tag/version 可用也必须一致。不一致时按外部一致性错误抛出，不能改走 Java reader，也不能继续按旧 DV row count 读新对象
- 错误映射必须稳定：OBS 404 → `NotFound`，403/签名失败 → 认证/权限错误，429/5xx/timeout → retryable 或保留 status code 的 external error；不要把所有 OBS 错误都包成普通 `Generic`
- range 响应必须校验返回长度和 HTTP status（206/200），否则 Parquet footer/range read 可能出现静默短读
- `GetOptions` 不能被静默忽略：PoC 至少正确处理 `range`；如果 DataFusion/object_store 传入 `if_match`、`if_none_match`、`version`、`head`、`extensions` 等 OBS client 暂不支持的条件或扩展参数，要返回明确 `Unsupported` / external error，而不是退化成无条件读。否则 Parquet metadata cache 或未来条件读取会读到不符合预期的对象版本
- 重试必须有上限并保留错误分类：429/5xx/连接 reset/timeout 可以按幂等读做有限重试；403、签名失败、404、range 不满足、短读校验失败不应盲目重试。最终错误要带脱敏 endpoint、bucket、status code / request id，不能带 AK/SK/token

**内嵌 OBS client 维护策略**：

- 现有 obs-rust-sdk 是迁移来源，不作为 Paimon native IO 的长期外部依赖
- 将 obs-rust-sdk 的认证、签名、HTTP、XML、错误解析等核心代码迁入 `paimon-native-io/rust/paimon-io/src/obs/`，并按 native read path 裁剪
- DataFusion 侧需要的对象存储语义（`head`、range read、streaming read、错误分类、重试、连接池、可选 list）优先在内嵌 OBS 模块中实现或修正
- `ObsObjectStore` 只负责把 `object_store::ObjectStore` trait 调用映射到内嵌 OBS client，不在 adapter 里复制 OBS 协议细节
- 如果发现内嵌 OBS client 与 DataFusion Parquet reader 的需求不匹配，修改 Paimon 内部 OBS 模块；不要切回 AWS/S3 backend 作为兼容捷径

**Native 内 URL scheme 处理**：

- Paimon 数据文件路径必须形如 `obs://bucket/warehouse/db/table/bucket-0/data-xxx.parquet`
- `register_obs_object_store` 只接受 `obs` scheme；遇到 `s3://`、`oss://`、`hdfs://`、`file://` 或无 scheme 路径直接报错，不尝试 fallback 或自动转换
- `object_store` 通过 `RuntimeEnv::register_object_store` 接受任意实现了 `ObjectStore` trait 的后端；我们注册 `obs://bucket` → `ObsObjectStore`，不走 DataFusion 的 S3 自动发现
- 转成 `object_store::path::Path` 时必须去掉 scheme 和 bucket，只保留 object key（如 `warehouse/db/table/.../data.parquet`）；不要把 `bucket` 重复拼进 key
- Java 传给 native 的文件路径建议使用 Paimon `Path.toString()`，而不是 `Path.toUri().toASCIIString()`。Paimon `Path` 本身允许未转义字符；object key 应在 native 侧按 `obs://authority/path` 结构拆出原始 key，再由 OBS HTTP client 在签名/请求阶段做一次编码。不要提前 percent-encode，也不要对已经编码的 `%xx` 再编码；`?query` / `#fragment` 不属于合法 Paimon object key 输入，遇到时直接拒绝并记录 `NON_OBS_PATH` 或路径非法 detail。若真实 OBS key 需要包含 `?` 或 `#` 字符，输入路径必须以 `%3F` / `%23` 形式表达并在 native 侧按“已编码片段”处理，不能把未转义 `?` / `#` 当作 key 的一部分
- PoC 的 `FileScanConfigBuilder` 直接拿已知数据文件构造 `PartitionedFile`，正常读路径主要依赖 `head` / `get_opts` / `get_range` / footer range read；`list` 不是主路径依赖，但 trait 方法仍要有可解释实现

**配置传递方式**：

- Java 侧从 `NativeIOOptions.objectStoreOptions()` 拿到所有 `fs.obs.*` 配置
- JNR 调 `paimon_config_builder_set_object_store_option(k, v)` 一对一传给 native（key/value，不用 JSON）
- `fs.obs.access.key` / `fs.obs.secret.key` / `fs.obs.session.token` 的 key 名大小写按 `OBSFileIO` 规则归一化，兼容用户用不同大小写写入 Paimon options；传给 native 前统一成标准小写 key
- Native 端解析 URL 后只挑当前 bucket 需要的几个配置注册 store

**OBS endpoint 区域映射**（典型）：

| 区域 | endpoint |
|---|---|
| 华北-北京一 | `https://obs.cn-north-1.myhuaweicloud.com` |
| 华东-上海一 | `https://obs.cn-east-3.myhuaweicloud.com` |
| 亚太-新加坡 | `https://obs.ap-southeast-3.myhuaweicloud.com` |

**潜在边界 case**：

- ⚠️ OBS list 接口与 DataFusion 主读路径关系弱：PoC 直接构造 `PartitionedFile`，不依赖 list；若后续调试/诊断需要 list，再把迁入的 `list_objects_v2` 映射到 `ObjectStore::list`
- ⚠️ OBS multipart upload 与 native read 无关——PoC 不写，不涉及
- ⚠️ 路径中含中文 / 特殊字符时必须覆盖 URL 编码测试（见 §9.1 “OBS 路径编码”）

### 8.2 文件系统边界（OBS-only）

Native IO 的文件系统层不做通用抽象，首版只包含 `ObsObjectStoreConfig` / `register_obs_object_store` 这一条路径：

- Java 侧只把 `fs.obs.*` 配置传给 native；不传 `fs.s3a.*`、`fs.oss.*`、`fs.defaultFS` 等其他文件系统配置
- Rust 侧只解析 `obs://bucket/key`，并从 `fs.obs.endpoint`、`fs.obs.access.key`、`fs.obs.secret.key`、可选 `fs.obs.session.token` / `fs.obs.region` 构造 store
- `SupportsNativeIO.canUseNative` 需要额外检查 data file path scheme 是否为 `obs`；非 OBS 文件直接走 Java reader，不进入 native
- 如果 `obs://` 路径缺少 bucket、endpoint 或 AK/SK，native reader 初始化失败并给出明确错误

### 8.3 Native lib 加载

Paimon 已有 `JNIUtils`，资源布局是 `/<os>/<arch>/<libName>.<so|dylib|dll>`；LakeSoul `JnrLoader` 资源布局是 classpath 根目录 `System.mapLibraryName("lakesoul_io_c")`。PoC 选择：

- **加载动作沿用 LakeSoul JNR 模式**：`LibraryLoader.loadLibrary(LibPaimonNativeIO.class, options, finalPath)`，因为我们走 C ABI + JNR-FFI，不是手写 JNI
- **资源布局沿用 Paimon `JNIUtils` 的平台目录**：`/linux/x86_64/libpaimon_native_io.so`、`/darwin/aarch64/libpaimon_native_io.dylib`，便于后续与 Paimon 现有 native artifact 规范统一
- `PaimonJnrLoader` 不能直接把 `System.getProperty("os.arch")` 拼进 resource path。JDK 在 Linux x86_64 上常返回 `amd64`，macOS Apple Silicon 上可能返回 `aarch64` 或 `arm64`；必须统一归一化为资源目录名：`amd64` / `x86_64` -> `x86_64`，`aarch64` / `arm64` -> `aarch64`。不支持的 arch 要返回明确 unavailable reason，不能退化成模糊的 `resource not found`
- `PaimonJnrLoader` 先按平台目录查找；找不到再尝试 classpath 根目录 `System.mapLibraryName("paimon_native_io")`，仅用于本地开发
- 解压到本地的文件名要包含 Paimon/native 版本或资源 hash，避免同一 executor JVM 里旧 jar 和新 jar 共用 `java.io.tmpdir/libpaimon_native_io.so`；如果 `java.io.tmpdir` 挂载为 `noexec`，允许通过 Java system property `-Dpaimon.native-io.tmpdir=...` 或 `spark.executorEnv.PAIMON_NATIVE_IO_TMPDIR=...` 指定可执行目录。这个 tmpdir 不是 `CoreOptions` / table option，不能持久化到表属性
- `PaimonJnrLoader` 必须线程安全。同一个 executor JVM 多个 task 可能并发首次初始化 native reader；resource 解压要用 per-lib synchronized / file lock / atomic rename，先写到唯一 `.tmp` 再 rename 到带 hash 的目标文件，避免一个线程加载到另一个线程未写完的 `.so`。加载结果按 `(resourcePath, hash)` 缓存；失败结果不能永久缓存到无法恢复，至少要允许后续不同 tmpdir/hash 重试
- `LibraryLoader.loadLibrary(LibPaimonNativeIO.class, ...)` 使用的 `LibPaimonNativeIO.class` 必须来自创建 `PaimonNativeReader` 的同一个 classloader。不要从系统 classloader 反射加载 JNR interface 后再强转，否则 Spark 多 classloader 场景会出现 provider 可见但 binding 类型不兼容。loader 缓存 key 至少包含 `ClassLoader`、`resourcePath`、`resourceHash`
- 不要直接复用当前 `org.apache.paimon.utils.JNIUtils.load(...)` 来加载 native IO：该类用单个 static `inited` 标记所有 JNI lib，语义不是按 `jniName` 维度隔离；如果未来别的 Paimon native lib 先调用过 `JNIUtils.load`，native IO 可能被误判已加载。PoC 用独立 `PaimonJnrLoader`，或先把 `JNIUtils` 改成按 libName 维护 loaded map 后再复用
- Arrow Java 属性按启动期配置/校验处理；`PaimonJnrLoader` 可以 best-effort 设置 `arrow.enable_unsafe_memory_access=true`、`arrow.enable_null_check_for_get=false`，但如果 Arrow 已初始化，必须记录当前实际 allocator manager 并在不兼容时快速失败。allocator manager 跟随 Paimon `paimon-arrow` 的 `arrow-memory-unsafe`，不额外引入 `arrow-memory-netty`

打包要求：

- Linux x86_64 是唯一生产目标，必须在 `paimon-native-io` jar 中包含 `/linux/x86_64/libpaimon_native_io.so`
- macOS arm64 只用于开发验证，可单独 profile 构建 `/darwin/aarch64/libpaimon_native_io.dylib`
- Maven 默认构建不强制 cargo build；新增 `-Pnative-io` profile 执行 cargo build 并复制产物，避免普通 Paimon 开发者没有 Rust 环境时无法编译
- `paimon-native-io/rust/rust-toolchain.toml` 必须固定 Rust toolchain。LakeSoul 当前 Rust crate 使用 edition 2024，DataFusion 51 也可能要求较新 Rust；PoC 先以 spike 实测的最低版本为准，避免不同开发机 cargo 版本漂移
- `Cargo.lock` 对内部 PoC 应提交以保证可复现；如果按 Apache library release 规则不提交 lockfile，需要在 release 策略里明确说明
- `paimon-bundle` PoC 阶段不默认打入 native jar；线上压测时显式把 `paimon-native-io` jar 加到 Spark classpath

### 8.4 Arrow Java 版本兼容

| 项目 | Arrow Java 版本 |
|---|---|
| Paimon 默认 | **15.0.0** |
| Paimon 某 profile | 18.1.0 |
| LakeSoul | **15.0.2** |

**15.0.0 vs 15.0.2 同 minor 版本，ABI 兼容**。Rust 侧 arrow-rs 57 通过 **Arrow C Data Interface** 与 Java 通信——这个 ABI 是 Arrow 项目级别的稳定 contract。LakeSoul 自己就是 arrow-rs 57（Rust） ↔ arrow-java 15（Java），证明跨大版本工作。**结论：直接用 Paimon 当前 Arrow Java 15.0.0，不冲突。**

但 Paimon Spark profile 可能把 Arrow Java 切到 18.1.0（例如 Spark 3.5 profile）。PoC 主目标是 Spark 3.4.4/JDK8，可先以默认 Arrow 15.0.0 做主线验收；如果 native jar 要跨 Spark 3.5 profile 复用，必须单独跑 Arrow 18.1.0 smoke test（schema export/import、`VectorSchemaRoot` import、row_index vector 类型、allocator close），不能只假设 C Data Interface 兼容。

LakeSoul 在加载后设置 Netty allocator，但 Paimon 当前 `paimon-arrow` 依赖的是 `arrow-memory-unsafe`。Paimon native IO 不应额外引入 `arrow-memory-netty`，避免和 Spark/Netty 依赖产生冲突；建议设置：
```java
System.setProperty("arrow.enable_unsafe_memory_access", "true");
System.setProperty("arrow.enable_null_check_for_get", "false");
System.setProperty("arrow.allocation.manager.type", "Unsafe"); // 如当前环境需要显式指定
```

这些属性最好通过 Spark driver/executor JVM 参数在 Arrow 类加载前设置；benchmark procedure 的 deployment check 要记录实际 Arrow Java 版本、allocator manager、unsafe/null-check 属性状态。若 Arrow 已经以不兼容 allocator 初始化，native IO 直接 unavailable，不在运行中切换 allocator。

### 8.5 Tokio runtime 生命周期

LakeSoul 用 **全局静态 `LazyLock<Arc<Runtime>>`**（`session.rs:63-77`），executor 进程级共享。我们镜像：

```rust
pub static GLOBAL_RUNTIME: LazyLock<Arc<Runtime>> = LazyLock::new(|| {
    let threads = std::env::var("PAIMON_NATIVE_IO_WORKER_THREADS")
        .ok().and_then(|v| v.parse().ok()).unwrap_or(4);
    Arc::new(Builder::new_multi_thread()
        .worker_threads(threads)
        .max_blocking_threads(threads * 2)
        .enable_all().build().expect("build tokio runtime"))
});
```

部署时通过 `spark.executorEnv.PAIMON_NATIVE_IO_WORKER_THREADS=<spark.executor.cores>` 把线程数对齐到 executor cores。

Rust 日志用 `tracing_subscriber::fmt().with_env_filter(...)` 进程内 `try_init()` 一次即可，不额外暴露 `rust_logger_init` FFI；filter env var 使用 `PAIMON_NATIVE_IO_LOG`。日志里禁止输出 OBS AK/SK/token。

runtime 配置必须可诊断、可收敛：

- `PAIMON_NATIVE_IO_WORKER_THREADS` 解析失败、为 0 或超过内部上限（建议 `min(32, available_parallelism * 2)`）时不要 panic；记录脱敏 warn 并回落到默认值。默认值仍建议 4，benchmark `env.json` 记录实际值
- `max_blocking_threads` 不能无界跟随用户输入放大；建议按 `worker_threads * 2` 后再加上限，避免同一 executor 多 task 并发时 native runtime 把机器线程打满
- Tokio runtime 初始化失败不能 `expect` 直接 abort JVM。FFI 首次使用 runtime 时如果构建失败，应转成 `CResult.err` / `CStatus(-1, ...)`，Java 将 native 标记 unavailable 或抛 `IOException`。示例代码里的 `expect("build tokio runtime")` 只表示伪代码，不允许进入生产实现
- 全局 runtime 只初始化一次，后续 task 改 env var 不应改变线程数；deployment check 必须记录 driver/executor 实际 runtime worker count，避免误以为动态调整已生效

### 8.6 DV 应用机制（基于 Parquet row_index）

**核心机制**：Paimon DV 判断需要的是「数据文件内真实物理行号」。Java Parquet 路径通过 `RowIndexGenerator.initFromPageReadStore` 调用 `pages.getRowIndexes()`，并结合 row group 起始 offset 生成 `ColumnarRowIterator.returnedPosition()`。

```
Parquet 文件
  ├─ row group 0 (物理 0~999)    row_index_offset=0
  ├─ row group 1 (物理 1000~1999) row_index_offset=1000
  └─ ...

  pushdown 后存活的行（本 RG 内相对位置）= [3, 5, 25, ...]
  真实物理 row_index = relative + row_index_offset = [1003, 1005, 1025, ...]
```

**Native 路径镜像同样机制**：让 arrow-rs/DataFusion Parquet reader 在输出 `RecordBatch` 时附加 `_row_index` 虚拟列。该列的 contract 是：值必须等于 Paimon Java 路径 `FileRecordIterator.returnedPosition()` 的值，即文件内真实物理行号。

需要 spike 明确 arrow-rs/DataFusion 输出语义：

- 如果原生 row index 已经是文件内绝对位置，直接输出
- 如果原生 row index 是 row group 内相对位置，native wrapper 必须加 row group offset
- 如果原生不支持 row index 输出，native wrapper 需要基于 row group metadata / selection 手动生成 `_row_index`

```
ApplyDeletionFileRecordIterator (复用，不改)
  ↑
NativeFileRecordIterator.returnedPosition()
  ↑
_row_index 列 (Arrow batch)
  ↑
arrow-rs Parquet reader (with_row_index_column)
  ↑
Parquet file metadata (PageReadStore.getRowIndexes())
```

**与后续 filter pushdown 的兼容前提**：阶段 1 不让带 data filter/topN/limit 的查询进入 native，因此正式 native scan 的 `_row_index` 只需要覆盖 projection、row group 顺序和 EOF/空 batch，不依赖 DataFusion predicate selection。阶段 2 如果开启 row group skip / page filter / predicate pushdown，`_row_index` 仍必须对应输出 batch 每一行的原始文件物理位置。row_index spike 仍建议提前覆盖多 row group 和 selection 场景，避免后续补 filter 时推翻设计。

**阶段 2 优化**：当 DV 删除率高（>20%）时，可以把 DV bitmap 一次性序列化传到 native，在 Arrow batch 出来前算 selection vector，跳过被删除行的 Arrow → InternalRow 转换。但 PoC 阶段不做。

### 8.7 Schema / Type 映射

Paimon 类型 → Arrow Schema 的映射已经有现成（`paimon-arrow` 模块）。Spec 阶段不展开，实现时直接复用。

Native 侧 schema 输入必须分成两层：

- `physicalReadType`：来自 `FormatReaderMapping.getActualReadRowType()`，用于 Rust/DataFusion Parquet projection 和 Arrow schema import
- `logicalOutputType`：来自当前 `RawFileSplitRead.withReadType(...)` 的 `readType`，用于 JVM 侧构造 Paimon `VectorizedColumnBatch` 并交给上层 Spark/Paimon

阶段 1 的 native guard 只允许 `physicalReadType` 与 `logicalOutputType` 在字段数、字段 id、字段名、Paimon 类型上完全一致的文件进入 native。这样可以明确拒绝 schema evolution cast/reorder、partition 字段注入和 key trim 后重排等复杂路径，同时避免把“没有公开 actualReadRowType”误实现成按 `readType` 猜 physical projection。后续如果要支持 schema evolution，必须先把 `FormatReaderMapping` 的 index/cast/partition 还原逻辑搬到 native 输出后处理，不能只改 Rust projection。

PK data file thin-mode 要单独覆盖：Paimon 普通 PK 文件可能包含 `_KEY_`、`_SEQUENCE_NUMBER`、`_VALUE_KIND` 和 value columns；开启 `data-file.thin-mode` 后可省掉重复 key columns。`RawFileSplitRead` 面向上层输出 value row，但 Java format reader 的实际读取 schema 由 `FormatReaderMapping` 决定。PoC 不要求支持所有历史写入形态；要求是：只有当 `FormatReaderMapping` 证明 physical/logical 等价时才走 native，否则 Java reader。

阶段 1 类型白名单要显式写死，不使用 `default: return false` 这种乐观逻辑。这里的白名单分两层：先按 Paimon logical type 粗判断，再按 Parquet physical/logical schema 做文件级 preflight。

- 支持：`CHAR` / `VARCHAR`、`BOOLEAN`、`BINARY` / `VARBINARY`、`DECIMAL(precision<=38)`、`TINYINT` / `SMALLINT` / `INTEGER` / `BIGINT`、`FLOAT` / `DOUBLE`、`DATE`、`TIME_WITHOUT_TIME_ZONE`、`TIMESTAMP_WITHOUT_TIME_ZONE(precision<=6)`
- 暂不支持：`TIMESTAMP_WITH_LOCAL_TIME_ZONE`（需要先验证 Arrow timezone 与 Spark/Paimon session timezone 一致）、`VARIANT`、`BLOB`、`ARRAY`、`VECTOR`、`MULTISET`、`MAP`、`ROW`
- `TIMESTAMP_WITHOUT_TIME_ZONE(precision>6)` 暂不支持。Paimon Parquet writer 对 precision <=3 写 INT64 millis，4-6 写 INT64 micros，>6 写 INT96；Java reader 对 INT96 有自己的 Julian day / nanos-of-day 转换。DataFusion/arrow-rs 的 INT96 解释、时区和纳秒语义必须先 spike 对齐，不能直接纳入 PoC 白名单
- Decimal 必须校验 Arrow decimal128 precision/scale 与 Paimon 一致；超过 38 位或 parquet physical/logical annotation 不匹配时走 Java reader
- Arrow schema 侧也要做二次校验：`TIMESTAMP_WITHOUT_TIME_ZONE` 对应 Arrow `Timestamp` 的 timezone 必须为 null/empty，time unit 只能是 millis 或 micros；任何带 timezone 的 Arrow timestamp 都视为 `TIMESTAMP_WITH_LOCAL_TIME_ZONE` 风险并拒绝。`DECIMAL` 对应 Arrow `Decimal` 必须是 128-bit，precision/scale 与 Paimon 完全一致；不要让 arrow-rs 自动提升到 decimal256 后再在 Java 侧截断
- Arrow 字符串/二进制类型必须与 `paimon-arrow` 现有转换器能力一致：`CHAR/VARCHAR` 只接受 `Utf8` / `VarCharVector`，`BINARY/VARBINARY` 只接受 `Binary` / `VarBinaryVector`；阶段 1 不接受 `LargeUtf8`、`LargeBinary`、`FixedSizeBinary` 或 dictionary-encoded vector。即使 DataFusion 配置了 `dictionary_enabled=false`，JVM import 后仍要校验并拒绝非预期 Arrow type，避免某些 parquet encoding 或 DataFusion 版本漂移绕过配置
- 内部 row_index 列必须是 signed 64-bit Arrow `Int(64, signed=true)` / `BigIntVector`。`UInt64`、`Int32`、nullable row_index 或 dictionary row_index 都必须拒绝；DV bitmap 使用的是 Java long 物理行号语义，不能把 unsigned 值在 Java 侧溢出解释
- Decimal physical schema 按 Paimon writer 的严格规则校验：precision <=9 必须是 `INT32 + DECIMAL(p,s)`，10-18 必须是 `INT64 + DECIMAL(p,s)`，19-38 必须是 `FIXED_LEN_BYTE_ARRAY(length=computeMinBytesForDecimalPrecision(p)) + DECIMAL(p,s)`。Paimon Java reader 能读部分历史/外部 `BINARY + DECIMAL`，但 PoC native 不接这个兼容面，统一 `PARQUET_PHYSICAL_TYPE` 走 Java
- Parquet physical schema preflight 必须确认：timestamp 只接受 INT64 + TIMESTAMP(MILLIS/MICROS, isAdjustedToUTC=false)，拒绝 INT96；decimal 只接受 arrow-rs/DataFusion 与 Paimon Java 都能等价读取的 physical encoding；字段 id/name/order 与 `physicalReadType` 一致
- Parquet codec 也必须 preflight。Paimon `file.compression` 默认是 `zstd`，并支持按 level 配置 `lz4` / `zstd` 等；Rust `parquet` crate 必须开启对应 codec feature。阶段 1 只允许本构建明确支持的 codec（至少覆盖 Paimon 默认 `zstd`、常见 `snappy/snap`、`lz4`、`uncompressed`）；检查粒度必须覆盖 footer 中每个 row group / column chunk 的 codec，不能只看一个文件级默认值；遇到未启用或 DataFusion/arrow-rs 不支持的 codec 返回 `PARQUET_UNSUPPORTED_FEATURE` 走 Java reader
- Parquet encryption 不在 PoC 范围内；Rust parquet crate 不启用 encryption feature。footer preflight 如果发现 encrypted footer / column crypto metadata，返回 `PARQUET_UNSUPPORTED_FEATURE` 走 Java reader，不能进入 native scan 后失败
- Footer preflight 还要比对 Parquet footer 总 row count 与 `DataFileMeta.rowCount()`。二者不一致时返回 `PARQUET_METADATA_MISMATCH` 走 Java reader 并记录 detail；native row_index 范围校验以 `DataFileMeta.rowCount()` 为上界，但不能在 footer 已证明不一致时继续 native
- `ArrowUtils.toArrowField` 会写入 `PARQUET:field_id` metadata；PoC 已拒绝 schema cast/reorder，但仍应保留 field metadata，后续 schema evolution 才有扩展空间

Parquet physical schema preflight 可以放在 `paimon-native-io` 模块中实现，原因是它可以依赖 Parquet/Arrow 相关类；core 的 `SupportsNativeIO` 只做不需要重依赖的 logical guard。preflight 返回“不适用”时，`NativeRawFileSplitRead` 应创建 Java `DataFileRecordReader` 并记录稳定 reason（例如 `PARQUET_PHYSICAL_TYPE`），不能等 native scan 已经启动后再失败。

注意：Paimon 的 `STRING` 类型在底层是 `BinaryString`（不可变 byte[]）。阶段 1 正确性不要求零拷贝，先复用 `paimon-arrow` 现有转换；阶段 2 如果 benchmark 显示字符串列成为瓶颈，再实现 Arrow Utf8 buffer 直读路径，避免每个值 `VarCharVector.get()` 复制 byte[]。

### 8.8 与 Paimon 现有 paimon-arrow 模块的关系

Paimon 已有 `paimon-arrow` 模块，里面有 `ArrowVectorizedBatchConverter` 等。**会复用其中的 Arrow ↔ Paimon 类型转换、ColumnVector 包装**，避免重复造轮子。但 native lib 加载、JNR 调用、FFI 是新增。

### 8.9 Apache 合规 / 打包发布边界

PoC 虽然定位内部验证，但代码会落在 Apache Paimon 仓库形态里，必须提前约束合规边界：

- 从 LakeSoul cherry-pick 的 Rust/Java 文件保留 Apache-2.0 license header，并在必要位置补 Paimon ASF header；不能混入未确认许可的第三方代码片段
- 迁入 `obs-rust-sdk` 前先确认许可证兼容性；当前本地 `obs-rust-sdk/Cargo.toml` 指向 `LICENSE-APACHE-2.0`，目录内也有 `LICENSE-MIT`，仍需要在 `LICENSE` / `NOTICE` 中按实际迁入文件来源记录清楚
- `LICENSE` / `NOTICE` 需要补充 native 依赖和迁入代码来源；Rust crates 的许可证清单要用 `cargo-deny` 或同类工具输出并归档
- `paimon-native-io-*-with-dependencies.jar` 如果包含 `jnr-ffi`、`jffi` 及其平台 native resource，也必须把这些 Java/native 依赖的 license/notice 纳入二进制 artifact 审核；不能只检查 Rust crate 许可证。shade 生成的 `META-INF/LICENSE` / `META-INF/NOTICE` 合并策略要在 build 中固定
- ASF source release 不应包含预编译 `.so` / `.dylib`；PoC 可以在内部二进制 jar 带 native lib，但上游发布需要拆成 profile/classifier 或只在 binary artifact 中包含
- Maven RAT 需要覆盖新增 `rust/` 源码、`Cargo.toml`、生成文件和 native resource；生成物、lockfile、二进制资源如需保留，要显式加 RAT exclude 并说明原因
- Rust 生成目录必须固定在 `target/` 或 Maven build 目录下，不能把 `cargo build`、`cargo vendor`、bindgen 产物、临时 `.so/.dylib` 写入 source tree。若为了离线构建引入 vendored crates，必须单独评审每个 crate license，并在 source release 策略里明确是否包含 vendor 目录；PoC 默认不 vendor crates
- `Cargo.lock` 是否提交要做一次明确决策：内部 PoC 推荐提交以保证 benchmark 可复现；若按 ASF library release 规则选择不提交，benchmark `env.json` 必须记录 `cargo metadata` 摘要或 dependency lock hash，避免同一代码在不同时间解析出不同 Rust 依赖
- `cargo-deny` / license 清单要在 CI 或 profile 中可重复生成，输出归档到 build artifact；不能只人工查看一次。对 dual-license crate 需要固定选择 Apache/MIT 等兼容路径，并同步到 NOTICE/README
- `paimon-bundle` 默认不打入 `paimon-native-io`，直到许可证、NOTICE、source/binary release 策略通过评审

---

## 9. 阶段 1 验收标准（正确性）

### 9.1 必须通过的测试

| 测试 | 数据 | 验证 |
|---|---|---|
| 单元：PaimonNativeReader 端到端 | 单文件 Parquet（100 行） | 行数、schema、字段值与 Java reader 一致 |
| 单元：投影下推 | 多列 Parquet，select 部分列 | 只读取请求列，schema 正确 |
| 单元：native option 校验 | `batch-size=0/-1/65537`、`max-batch-bytes=0/512GB`、row index 列名冲突 | 非法 batch size / byte limit 拒绝；row index 内部列名自动避让 |
| 集成：Spark SQL 全表扫描 | 1 partition × 10 文件 × 100k 行 | `select *` / 投影查询结果集一致；hash 比较必须 order-insensitive，或显式按 primary key / 全字段排序后再 hash；`count(*)` 只作补充，因为可能走 aggregate pushdown |
| 单元/Core：空投影 raw read | DV 表 + `ReadBuilder.withReadType(RowType.of())` 或直接 `NativeRawFileSplitRead.withReadType(RowType.of())` | native physical projection 为 0 业务列时仍输出 `_row_index`，空行数量和 DV 过滤结果与 Java 一致 |
| 单元：native 空文件/0 行 schema | 0 行 Parquet 或 native plan 空输入，分别用空投影和非空投影读取 | EOF 行为正确；`paimon_reader_get_schema` 仍返回 target schema + row_index，不返回裸空 schema |
| 单元：DataFusion output schema 口径 | mock `FileScanConfig` 原始 file schema、wrapper output schema、`paimon_reader_get_schema` 分别缺失/多出 `_row_index` | `projection_indices` 不包含虚拟列；execution plan schema、FFI schema、实际 batch schema 三者对 output schema 口径一致 |
| 集成：Spark count aggregate pushdown 识别 | `SELECT count(*)` 触发 `PaimonLocalScan` 时 | benchmark 标记为 `aggregate_pushdown_not_native_scan`，不计入 native IO speedup |
| 集成：Spark plan comparable 识别 | baseline/native 两轮因 AQE、pushdown、projection 或 split 文件数不同导致 scan plan 不一致 | 标记 `plan_not_comparable`，保留诊断但不计入 speedup |
| 单元：Spark plan signature canonicalize | 两个等价 `executedPlan` 仅 expression id/codegen id 不同，另构造 pushed filter/schema/file count 不同 | 等价 plan signature 相同；真实 scan 语义不同则标记 `plan_not_comparable` |
| 单元：correctness multiset hash | 构造重复行、重复 group 输出、hash xor 抵消样例 | order-insensitive 对比保留 occurrence count；重复数据差异不会被 set/xor 聚合吞掉 |
| 集成：非 Spark 引擎 guard | Flink/Core table read 或未标记 engine 的 `ReadBuilder` | 即使 table property 打开也不进入 native，稳定返回 `UNSUPPORTED_ENGINE` |
| 单元：Spark-only 瞬时 option | Spark `table.copy(...)` 注入 `__paimon.internal.native-io.engine=spark`，原 table schema 不包含该 key | Spark scan 支持 native；table property 不被污染；Core/Flink 仍拒绝 |
| 单元：内部 option 持久化保护 | `CREATE/ALTER TABLE ... WITH ('__paimon.internal.native-io.engine'='spark')` | `SchemaValidation` 拒绝，避免用户绕过 Spark-only guard |
| 集成：Spark 读入口覆盖 | Spark V2 catalog table、V1 path table、V1 catalog table（`format("paimon").option("catalog", ...)`）、KnownSplitsTable 内部扫描 | 各入口都注入 Spark-only option、合并 `spark.paimon.*`、补齐 `spark.hadoop.fs.obs.*` fallback；KnownSplitsTable origin 不丢 native dynamic options |
| 单元：ReadBuilder / PartitionReaderFactory equality | 同一表同一 projection，分别 `native-io.enabled=true/false`，OBS 配置不同，partitionFilter 不同，metadataColumns/blobAsDescriptor 不同 | `ReadBuilderImpl.equals/hashCode` 或 `PaimonPartitionReaderFactory.equals/hashCode` 能区分关键 native dynamic options，且 equals/hashCode 口径一致，Spark 不会复用错误读路径 |
| 单元：ReadBuilder serialization | native enabled + OBS options + Spark-only 瞬时 option 的 `PaimonPartitionReaderFactory` 做 Java/Spark closure serialization roundtrip | 序列化对象图不包含 native handle/JNR Pointer/Arrow allocator；executor 反序列化后仍能在 `newRead()` 构造 native context |
| 集成：DV 删除后查询 | 写 1M 行 → UPDATE 删一半 → 查询 | 结果与 JVM 路径完全一致 |
| 集成：partition pruning | 多 partition 表 | 只扫命中 partition，结果一致 |
| 集成：file index 命中 | 启用 bloom filter | file 跳过逻辑生效 |
| 单元：applicability skipped 口径 | `fileIndexResult.remain()==false` 或 preflight `EMPTY` | reporter 记录 `skipped_files/skipped_bytes`，不计入 native files，也不计入 Java fallback files |
| 单元：applicability task attempt 去重 | mock task failed retry、speculative duplicate success、同 partition 多 attempt 上报 | 只聚合 Spark 接受的 successful attempt；失败/重复 attempt 不重复计入 native/java/skipped bytes |
| 集成：bitmap index 命中 | 触发 `BitmapIndexResult` | native provider 拒绝，透明走 Java reader，结果一致 |
| 单元：split 内 native/Java 混合顺序 | 一个 split 多文件，部分文件 native、部分文件因 codec/schema/path 走 Java | `ConcatRecordReader` 按 `dataSplit.dataFiles()` 原顺序串联；metadata file path/row index 与 Java 路径一致 |
| 集成：PK data file 普通模式 | `data-file.thin-mode=false` 的 PK+DV Parquet 表 | 如果 `actualReadRowType` 与 `readType` 等价则 native 结果一致；不等价则按稳定 reason 走 Java |
| 集成：PK data file thin-mode | `data-file.thin-mode=true` 的 PK+DV Parquet 表 | 验证 `_KEY_` 裁剪/缺省不会让 native 读错字段；命中或拒绝都必须可解释 |
| 单元：FormatReaderMapping accessor | 构造 key trim、identity mapping、schema evolution mapping | `getActualReadRowType()`、`hasIdentityIndexMapping()`、`hasNoCastMapping()` 与 Java reader 现有行为一致 |
| 集成：schema cast | 写旧 schema 后读新 schema | native provider 拒绝，透明走 Java reader，结果一致 |
| 集成：data filter / topN / limit | `WHERE` / `LIMIT` / `ORDER BY ... LIMIT` | 阶段 1 native provider 拒绝，透明走 Java reader，结果一致 |
| 单元：SplitRead lazy config | 在 native reader 初始化前后分别调用 `applyReadType` / `withFilter` / `withTopN` / `withLimit` | `NativeRawFileSplitRead` 都能拿到最新配置；带 filter/topN/limit 不会误进 native |
| 单元：FormatReaderMapping cache 失效 | 同一个 `NativeRawFileSplitRead` 先后切换宽/窄 `readType` 或 filter/topN/limit | mapping cache 不复用旧 schema；guard 与实际 reader schema 一致 |
| 集成：partition / row tracking field | 查询需要 reader 注入分区字段或 row tracking system field | native provider 拒绝，透明走 Java reader，结果一致 |
| 集成：Spark metadata column | 查询 `__paimon_file_path` / `__paimon_row_index` | native `filePath()` / `returnedPosition()` 可供 Spark 拼接，结果与 Java 一致 |
| 集成：非 OBS 路径 | 本地 / HDFS / S3 测试表 | native provider 拒绝，透明走 Java reader |
| 单元：OBS 配置优先级 | 同时设置 table `fs.obs.*`、`spark.paimon.fs.obs.*`、`spark.hadoop.fs.obs.*`、path extraOptions | 最终 `NativeIOOptions` 符合 DataSource/path extraOptions > spark.paimon > table/catalog > spark.hadoop fallback |
| 单元：OBS endpoint 兼容 | `fs.obs.endpoint` 分别为裸 hostname、`https://host`、`http://host:port`，AK/SK key 大小写不同 | Native 归一化后可构造同等 OBS client；敏感 key 名按 `OBSFileIO` 规则兼容 |
| 单元：OBS external path / 多 store | `DataFilePathFactory.toPath(file)` 返回 external path，且 mock 多文件跨 bucket/authority | 阶段 1 每个 native reader 只接单文件；多文件由 Java split 层串联；Rust 不允许只取第一个 object_store_url 读全部文件 |
| 集成：OBS 路径编码 | bucket/key 含中文、空格、`+`，以及已编码 `%23` / `%3F`；另测未转义 `#` / `?` | JVM `Path`、OBS client、object_store `Path` 三侧转换一致；不能 double encode；未转义 query/fragment 被拒绝 |
| 单元：OBS ObjectStore 条件读取 | mock `GetOptions` 带 range、if_match、version、extensions，以及短读/416/range 长度不符 | range 正确；暂不支持的条件参数返回明确 unsupported/external error；短读和无效 range 不静默成功 |
| 单元：OBS preflight/native 一致性 | preflight footer 后 mock OBS 对象 size/eTag/version 改变 | native 初始化检测到 ObjectMeta 与 preflight 不一致并抛外部一致性错误；不走 Java fallback，不继续读 |
| 单元：OBS retry 分类 | mock 429/5xx/timeout、403、404、签名失败、短读 | 可重试错误有限重试后保留分类；不可重试错误不盲目重试；错误信息脱敏并保留 status/request id |
| 单元：配置/日志脱敏 | 使用假 AK/SK/session token/password，触发 option parse、ServiceLoader、OBS error、Rust panic、benchmark env/applicability 输出 | `toString`、异常、日志、JSON 报告都不包含敏感原文；endpoint userinfo 被拒绝 |
| 单元：row_index contract | 多 row group Parquet + pruning + mock 乱序 batch | native `_row_index` 与 Java `returnedPosition()` 完全一致；乱序 batch 被 JVM 校验拒绝，不能静默输出 |
| 单元：FileRecordIterator contract | 调用顺序 `returnedPosition(before next)`、`next -> returnedPosition -> returnedPosition -> next`，以及 `releaseBatch` 重复调用 | `next()` 前调用抛 `IllegalStateException`；同一行重复 `returnedPosition()` 稳定；重复 release 不 double free |
| 单元：RecordReader batch 持有契约 | 连续调用两次 `readBatch()`，第一批 iterator 在第二批返回后再读取/释放 | 第一批数据不被第二批 Arrow/VSR 复用覆盖；只有 `releaseBatch()` 后资源才能回收 |
| 单元：Parquet shaded 依赖边界 | native module 打包后在只含 Paimon shaded `paimon-format` 的 classpath 运行 preflight | 不出现 `ClassNotFoundException: org.apache.parquet.*`；preflight 通过 `paimon-format` helper 或同步 shade 后的类访问 footer |
| 单元：PaimonJnrLoader 隔离 | 先模拟/调用其他 Paimon native lib load，再加载 native IO | 不受 `JNIUtils` 单个 static `inited` 影响；native IO 按自身 libName/resource 独立加载 |
| 单元：ServiceLoader 缺依赖容错 | classpath 有 service 文件/provider factory，但缺少 jnr/jffi 或 Arrow runtime | ServiceLoader 阶段只判定 native unavailable，Java reader 可继续；factory 静态初始化不触发 native/JNR/Arrow |
| 单元：ServiceLoader classloader 隔离 | 两个 URLClassLoader 分别包含/不包含 native jar，或包含不同 native resource hash | provider 与 loader 缓存按 ClassLoader 隔离；一个 loader 的 unavailable 不污染另一个 loader；JNR interface/binding classloader 一致 |
| 单元：PaimonJnrLoader arch alias | mock `os.arch=amd64/x86_64/arm64/aarch64` | `amd64` 与 `x86_64` 都查 `/linux/x86_64/`；`arm64` 与 `aarch64` 都查 `/darwin/aarch64/`；不支持 arch 返回明确不可用原因 |
| 单元：PaimonJnrLoader 并发首次加载 | 多线程同时调用 `PaimonJnrLoader.load()`，tmpdir 初始为空 | native resource 只产生完整目标文件；没有半写文件被加载；所有线程拿到同一个可用 binding 或同一个可解释错误 |
| 单元：native 版本/hash 冲突 | driver/executor classpath mock 出 manifest 缺失、多个 native jar、多个 resource hash | manifest 缺失时版本为 `unknown` 但 hash 存在；多个版本/hash 冲突时 benchmark `failed`，不继续跑性能 |
| 单元：Tokio runtime env 校验 | `PAIMON_NATIVE_IO_WORKER_THREADS=0/-1/abc/9999`，以及初始化失败 mock | 非法值回落默认或上限并记录实际值；runtime 构建失败转成可诊断错误，不 abort JVM |
| 单元：Arrow batch schema 校验 | native mock 返回列顺序错、row_index 缺失/类型错、vector 行数不一致 | JVM 包装抛 `IOException`，不产生错误数据 |
| 单元：Arrow 非预期类型拒绝 | native mock 返回 `LargeUtf8`、`LargeBinary`、dictionary vector、`FixedSizeBinary`、row_index `UInt64/Int32/nullable` | JVM schema 校验拒绝，返回明确错误；不尝试截断/转换后继续读 |
| 单元：FFI batch row count 协议 | native mock 返回 `status>0` 但 `VectorSchemaRoot.rowCount` / `_row_index.valueCount` 不一致，或返回 `status=0` 同时写入 ArrowArray | Java 拒绝不一致 batch；`status=0` 只作为 EOF，不 import ArrowArray |
| 单元：FFI CStatus 不变量 | mock `status>=0` 携带 err、`status<0` 无 err、err 非 UTF-8 或含敏感配置 | Java 识别 ABI bug 并抛 `IOException`；错误字符串 UTF-8 且脱敏 |
| 单元：PaimonNativeReader EOF/lifecycle | mock native 返回 `status=0`、0 行 batch、初始化中途失败、`nextBatchBlocked` 期间触发 close | EOF 不 import 未初始化 `ArrowArray`；0 行 batch 在 native 内部跳过或明确禁止；partial init close 不泄漏；close 与 in-flight native 调用串行化，不能 use-after-free |
| 单元：Spark task interrupt 语义 | `nextBatchBlocked` 前线程已 interrupt，或 native 返回后线程被 interrupt | reader 尽快 close 并抛 `InterruptedIOException` / `IOException`；不继续产出半批数据；in-flight close 不 use-after-free |
| 单元：FFI panic 边界 | mock/exported Rust FFI 内部触发 panic | panic 被转换为 `CStatus(-1, ...)` / `CResult.err`，不会跨 FFI 直接 abort JVM；错误信息脱敏 |
| 单元：FFI free/null safety | `free_c_status(null)`、重复 close Java wrapper、reader 创建成功消费 config 后 close | null free 安全；Java 指针置空后不 double free；重复 close 幂等 |
| 类型：timestamp precision | `TIMESTAMP(0/3/6/9)` 分别写入 Parquet，并 mock Arrow timestamp 带 timezone；Spark session timezone 分别设 UTC/Asia-Shanghai | precision <=6 且 timezone empty 可对比 Java；precision >6/INT96 或 Arrow timezone 非空返回 `PARQUET_PHYSICAL_TYPE` 走 Java；timestamp_ntz 不受 session timezone 改写 |
| 单元：Parquet physical schema preflight | 构造 INT96 timestamp、decimal physical mismatch、decimal256、`BINARY + DECIMAL`、field id/order mismatch | native 文件级 guard 返回稳定 reason，不启动 native scan |
| 单元：Parquet codec/encryption preflight | zstd/snappy/lz4/uncompressed、未启用 codec、encrypted footer/column mock | 支持 codec 可 native；未支持 codec 或 encryption 返回 `PARQUET_UNSUPPORTED_FEATURE` 走 Java |
| 单元：Parquet metadata preflight | mock footer row count 与 `DataFileMeta.rowCount()` 不一致 | 返回 `PARQUET_METADATA_MISMATCH` 走 Java，不进入 native row_index 校验 |
| 容错：损坏文件 | 注入损坏 Parquet | native 路径抛出有效 IOException；关闭 native 后 Java 路径行为与原来一致 |
| 容错：native 读取期异常类型 | mock `next_record_batch_blocked` 返回 `CStatus(-1, "corrupt")` | Java 包装抛 `IOException`；`ignoreCorruptFiles=true` 时由 `DataFileRecordReader` 跳过 |
| 容错：native 外部 IO 异常 | mock OBS 鉴权失败、timeout、5xx、range status 异常，并打开 `ignoreCorruptFiles=true` | 异常通过非 corrupt marker 逃逸，不能被当成损坏文件静默跳过 |
| 容错：preflight 丢失/损坏文件 | footer preflight 阶段触发 missing/corrupt，分别打开 `ignoreLostFiles` / `ignoreCorruptFiles` | 行为与 `DataFileRecordReader` 一致，不能因 preflight 提前读 footer 而破坏容错 |
| 容错：OBS 连接异常 | 注入网络中断 / 5xx / 鉴权失败 | 错误传播，session 不死 |
| 类型：基础 | boolean/int/long/string/binary/decimal/date/time/timestamp_ntz(<=6) | 全部一致，decimal precision/scale 与 timestamp precision 要覆盖边界 |
| 类型：暂不支持 | timestamp_ltz/variant/blob/array/map/row/vector/multiset | native provider 拒绝，透明走 Java reader |
| 资源：Arrow batch release | 连续 read/release 1000 个 batch，以及未 release 直接 close | `VectorSchemaRoot`、row_index vector、native reader 无泄漏、无 double free；close 会兜底释放 outstanding batch |
| Memory：超宽 batch 上限 | 构造大 string/binary 列，单批 Arrow buffer 超过内部 byte 阈值 | Java import 后立即拒绝并释放资源；错误可诊断，不 OOM，不把半批数据交给上层 |
| Memory：长跑 | 50 个 batch 顺序读 | 无 native leak（valgrind/RSS 监控） |

### 9.2 验收方式

写一个 `NativeIOCorrectnessTestSuite`，参数化 `spark.paimon.native-io.enabled=true/false`，跑完后对比两路径输出。

正确性对比不能依赖 Spark scan 的自然输出顺序。默认规则：

- 有 primary key 的 synthetic PK 表，结果量可控时按 primary key（或 primary key + partition）排序后比较 row hash
- 宽表/大表不适合全量 collect 时，用 order-insensitive **multiset** 聚合校验：`count`、每列 null count、数值列 sum/min/max、字符串/二进制列稳定 hash 聚合；必要时按主键分桶做分段 hash，避免单个 hash 抵消错误。不能只比较 `distinct` set 或 commutative xor hash，否则重复行、重复 key 的中间结果或聚合输出可能被误判为一致
- 对没有唯一主键的 query 输出（例如 group by 后只投影非唯一列），correctness hash 要包含“canonical row bytes -> occurrence count”的 multiset 语义；大结果可按 hash bucket 聚合 `(rowHash, count, secondaryHash)`，但必须保留重复次数
- correctness hash 要使用类型稳定的 canonical encoding：decimal 用 unscaled value + scale，timestamp_ntz 用 UTC-independent epoch micros 或 Paimon `Timestamp` 的精确字段，binary 用原始 bytes，string 用 UTF-8 bytes。float/double 要显式规范化 `NaN`、`+0.0/-0.0` 和 infinities，避免 Java/Rust/Spark 字符串化差异导致误判或漏判
- 自定义 query 如果包含 `LIMIT` 但没有 deterministic `ORDER BY`，不能作为 correctness 对比 query；阶段 1 带 limit 本来也应被 native guard 拒绝，只能验证 fallback 语义
- 任何 benchmark query 计入 speedup 前，必须先通过同一 query 的 baseline/native correctness check；失败 query 不进入性能汇总

---

## 10. 阶段 2 Benchmark 设计

### 10.1 测试矩阵

| 维度 | 取值 |
|---|---|
| 表大小 | 100M 行 / 1B 行（生产典型） |
| 列数 | 20 列 / 200 列 |
| 投影率 | 10% / 50% / 100% |
| DV 比例 | 0% / 5% / 50% |
| 文件大小 | 128MB / 1GB |
| 并发 | executor cores = 4 / 16 |

### 10.2 衡量指标

- **Scan stage wall-time**（主指标）
- 端到端 query latency
- CPU utilization（executor）
- 堆外内存峰值
- GC 时间

### 10.3 目标

主指标 Scan stage wall-time **≥1.5x 提升**（PoC 通过标准）。如果在大列数 + 高投影率场景能到 2-3x，记录为亮点。

### 10.4 测试数据选择原则

性能验证的首要目标不是跑行业排名，而是稳定、可重复地测出 **Paimon PK+DV 读路径**在 Java reader 与 native IO reader 之间的差异。因此默认使用 procedure 自动生成的 synthetic PK+DV 数据集；TPC-DS/SSB 只作为可选派生数据集。

---

### 10.5 Spark 存储过程：线上一键跑 benchmark 并输出图表

新增一个 Spark procedure，用于在真实分布式 Spark + OBS 环境中自动准备测试数据、顺序运行 Java 原生路径和 native IO 路径，并把结果 JSON/CSV/柱状图写到指定 OBS 路径。一次调用可以完成两轮对比，但**不是同时跑**，而是顺序跑，并且每轮之间做 cache/session 隔离，尽量避免缓存干扰。

建议接口：

```sql
CALL sys.native_io_benchmark(
  mode => 'synthetic',                  -- synthetic / existing_table / tpcds / ssb
  scale => '1tb',                       -- synthetic 下控制生成数据规模；也可用 rows=...
  warehouse => 'obs://bucket/bench/warehouse',
  result_path => 'obs://bucket/bench/result/native-io/${run_id}',
  table_name => 'native_io_bench_pk_dv',
  query_set => 'default',               -- default / scan_only / projection / filter / custom
  run_order => 'baseline_first',         -- baseline_first / native_first / alternate
  repeat => 3,
  warmup => 1,
  dv_delete_ratio => 0.05,
  target_file_size => '512mb',
  output_chart => true
);
```

`mode` 说明：

- `synthetic`：默认推荐。procedure 自动生成 Paimon PK+DV 表，保证能命中 native IO 守门员，最适合线上环境快速验证
- `existing_table`：用户传入已有 Paimon 表，procedure 只校验是否满足 PK+DV/native 条件并跑查询
- `tpcds` / `ssb`：可选，作为标准数据集派生模式；需要先生成或导入标准数据，再转换为 Paimon PK+DV 表

输出内容：

- `env.json`：Spark conf、executor/core/memory、shuffle 分区、OBS endpoint、Paimon/native IO 版本、native resource hash、native runtime worker count、run_id（敏感配置脱敏）。阶段 1 不为版本探测新增 C ABI；native 版本从 jar manifest / Maven artifact version / 解压 resource hash 获取，保持 C ABI 17 个函数不扩张。若 manifest 版本缺失，版本字段写 `unknown` 但 resource hash 仍必须存在；若 driver/executor 发现多个不同版本或 hash 的 native jar/resource，benchmark 直接 `failed`
- `dataset.json`：数据规模、表 schema、文件数、文件大小、DV 比例、是否 rawConvertible
- `native_applicability.json`：每条 query 的 split/file 级 native 命中情况，包含 `native_files`、`java_fallback_files`、`skipped_files`、`native_bytes`、`java_bytes`、`skipped_bytes`、拒绝原因聚合；`skipped_*` 表示 Paimon file index / empty preflight 直接返回 empty reader，不算 native 命中，也不算 Java fallback
- `result.json`：每条 query 的 baseline/native wall time、scan stage time、输入文件数、输入字节数、输出行数；必须保留每次 repeat 的明细，不只输出 median
- `result.csv`：便于后续导入 BI / Excel
- `summary.json`：几何平均加速比、scan stage 平均加速比、失败 query 列表、`native_not_applicable` / `partial_native` 列表、每条 query 的 median/min/max/stddev/CV；CV 过高的 query 标记为 unstable，不纳入默认结论
- `latency_bar.png`：类似 LakeSoul 图的分组柱状图，蓝色 baseline，橙色 native IO
- `speedup_bar.png`：每条 query 的 speedup = baseline_time / native_time

### 10.6 顺序运行与缓存隔离

Procedure 一次调用内顺序跑两种路径：

1. baseline：`spark.paimon.native-io.enabled=false`
2. native：`spark.paimon.native-io.enabled=true`

为了减少缓存干扰：

- 两轮之间执行 `spark.catalog.clearCache()` / `spark.sharedState.cacheManager.clearCache()`，并 unpersist procedure 内创建的所有 DataFrame
- 每条 query 使用新的 `SQLExecution`，记录独立 executionId
- 可选 `isolate_session=true` 时，为 baseline/native 分别创建新的 SparkSession clone，并重新设置 conf
- warmup 结果不计入统计；repeat 结果取 median，同时保留全部 run 明细
- `run_order='alternate'` 时按 `baseline#1 -> native#1 -> baseline#2 -> native#2...` 交替执行，用于抵消对象存储冷热、集群负载波动
- `run_order='native_first'` 可用于验证顺序是否影响结论；最终报告记录 run_order
- procedure 不能可靠清理 OBS / OS / executor 本地磁盘缓存，所以报告必须记录“cache not fully controllable”；如果线上允许，可通过换 `run_id` 路径、重启 executor 或扩大数据集降低缓存影响
- baseline/native 两轮必须记录关键 Spark SQL conf 快照，包括 AQE、whole-stage codegen、shuffle partitions、broadcast threshold、dynamic partition pruning、aggregate pushdown/native IO 开关。除 native IO 开关和 procedure 必要的诊断开关外，两轮 conf 必须一致；如果 AQE 导致 baseline/native executedPlan scan 边界不一致，该 query 标记为 `plan_not_comparable`

Native jar 部署校验：

- benchmark procedure 开始前先在 driver 调 `NativeSplitReadProviderLoader` 做一次 provider 可见性检查
- 再通过一个小的 Spark job 在每个 executor 上调用 `PaimonJnrLoader.isAvailable()` / `NativeSplitReadProviderLoader`，确认 native jar、`jnr-ffi` 运行时依赖和 `.so` 都在 executor classpath 且可加载。校验 job 的分区数要至少覆盖当前 executor 数，并在结果里记录 executorId/host/nativeAvailable/nativeVersion/nativeResourceHash/runtimeWorkerThreads/error；如果 Spark 开启 dynamic allocation，procedure 要么临时固定 executor 数，要么在每轮 query 前后记录 executor 集合变化并对新增 executor 重新校验
- driver 与所有 executor 的 `nativeResourceHash` 必须一致；发现同一 Spark application 内不同 executor 加载了不同 native 资源 hash，benchmark 直接标记 `failed`，不能把混部 jar 或缓存旧 `.so` 的结果纳入性能比较
- 任一 executor 不可用时，benchmark 直接 fail fast，不能把静默 Java fallback 的结果写成 native 性能

### 10.7 synthetic 数据集设计（默认推荐）

不强制使用 TPC-DS/SSB。为了稳定命中当前 native IO 能力，默认 synthetic 数据集直接生成 Paimon **PK 表 + DV 模式 + Parquet + OBS**：

```sql
CREATE TABLE native_io_bench_pk_dv (
  id BIGINT,
  k1 BIGINT,
  k2 INT,
  k3 STRING,
  dim1 STRING,
  dim2 STRING,
  metric1 DOUBLE,
  metric2 BIGINT,
  event_date DATE,
  ts TIMESTAMP,
  PRIMARY KEY (id) NOT ENFORCED
) WITH (
  'bucket' = '...',
  'file.format' = 'parquet',
  'deletion-vectors.enabled' = 'true',
  'target-file-size' = '512MB'
);
```

生成逻辑：

- 用 Spark `range(numRows)` 分布式生成数据，不依赖外部数据文件
- 字段包含数值、字符串、日期、timestamp，覆盖 PoC 类型白名单
- 写入后执行一轮或多轮 DELETE/UPDATE 制造 DV，例如删除 `id % 100 < dv_delete_ratio * 100`
- 可选 `compact_before_benchmark=true` 控制是否先 compaction，保证文件大小更接近目标值
- 生成后检查 split 是否 `rawConvertible=true`；不满足则 fail fast

默认 query set：

| Query | 目的 | 阶段 1 处理 |
|---|---|---|
| `scan_count` | 全表行数校验，`count(*)` | 可能被 Spark/Paimon aggregate pushdown 成 `PaimonLocalScan`，默认只作正确性/元数据校验；如果 physical plan 不是 `BatchScan/PaimonPartitionReader`，标记 `aggregate_pushdown_not_native_scan`，不纳入 speedup |
| `scan_count_no_agg_pushdown` | 强制走 native scan 的行数校验 | procedure 应通过 core/read 层直接 0 列 `readType`，或构造 Spark 不可 aggregate-pushdown 的等价查询；用于覆盖空投影 + DV row_index |
| `projection_3_cols` | 窄列投影，验证 projection 下推收益 | native 适用 |
| `projection_wide` | 宽列扫描，验证 Arrow/Parquet decode 成本 | native 适用 |
| `filter_on_k1` | 简单过滤 | 标记为 `native_not_applicable`，用于验证带 data filter 查询不会误走 native；阶段 2 实现 filter pushdown 后再纳入 speedup |
| `group_by_dim` | scan + 轻聚合，保证下游 Spark 算子一致 | native 适用，只要 pushed data filter/topN/limit 为空 |

这些 query 不追求行业 benchmark 排名，只用于有效测试 Paimon Java read path vs native IO read path。

### 10.8 标准数据集适配判断

**TPC-DS**：适合作为可选 benchmark，不作为唯一要求。

- TPC-DS 是业内标准决策支持 benchmark，1TB scale factor 有明确数据规模和 99 条 SQL 模板
- 但原始 TPC-DS 是星型/雪花模型的 append 数据，并不天然是 Paimon **PK 表 + DV 模式**
- 如果使用 TPC-DS，procedure 必须把数据加载成 Paimon PK 表并开启 DV，再执行 UPDATE/DELETE 制造 DV

**SSB**：适合作为快速演示，不作为唯一验收。

- SSB 是业内常用 OLAP benchmark，查询少、模型简单，适合快速展示 scan 性能差异
- 但 SSB 更偏 append-only 星型模型；直接建 append 表不会命中当前 PK+DV native IO
- 若使用 SSB，也必须转换为 Paimon PK+DV 表并制造 DV

**结论**：

- 线上验证默认用 synthetic PK+DV 数据集，保证有效命中 native IO
- TPC-DS/SSB 作为可选“derived PK+DV dataset”，报告必须明确标注，不宣称等同原始标准 benchmark 排名
- 结果只用于比较 Paimon Java read path vs Paimon native IO read path，不与 Doris/ClickHouse/Spark 原生 Parquet 等系统横向对比

### 10.9 Benchmark 表构造规则

为了确保 native IO 真的生效，procedure 在构造或校验数据时必须检查：

- 表类型：Paimon primary-key table
- `'deletion-vectors.enabled' = 'true'`
- 文件格式：Parquet
- 数据路径：`obs://...`
- 查询 split 中 `rawConvertible=true`
- 至少一轮 DELETE/UPDATE 后存在 deletion vector 文件或 DV 元数据
- read schema 类型在 PoC 白名单内
- native 文件级守门员命中率：`native_bytes / total_bytes`，用于区分 full-native、partial-native、not-applicable

如果 split 级条件不满足，procedure 应把该 query 标记为 `native_not_applicable`，而不是把 Java 路径结果误算成 native IO 性能。如果 split 级通过但文件级 **0 个文件 / 0 字节** 命中 native，应标记为 `native_not_applicable_file_level`，不是 `partial_native`；这通常说明 schema/type/filter/path 等文件级条件全部拒绝。如果 split 级通过且文件级只有部分文件命中 native，应标记为 `partial_native`，结果可以保留用于诊断，但默认不纳入几何平均 speedup，除非报告里明确展示 native 文件/字节命中率。

applicability 拒绝原因使用稳定枚举，便于聚合：

`DISABLED`、`UNSUPPORTED_ENGINE`、`NO_PROVIDER`、`NOT_DV_TABLE`、`STREAMING_SPLIT`、`FORCE_KEEP_DELETE`、`NOT_RAW_CONVERTIBLE`、`MISSING_DELETE_ROW_COUNT`、`NON_PARQUET_FILE`、`NON_OBS_PATH`、`MISSING_OBS_CONFIG`、`UNSUPPORTED_TYPE`、`PARQUET_PHYSICAL_TYPE`、`PARQUET_UNSUPPORTED_FEATURE`、`PARQUET_METADATA_MISMATCH`、`SCHEMA_CAST_OR_REORDER`、`READ_TYPE_MISMATCH`、`PARTITION_OR_SYSTEM_FIELDS`、`DATA_FILTER_TOPN_LIMIT`、`BITMAP_INDEX_SELECTION`。

`READ_TYPE_MISMATCH` 专门表示 `FormatReaderMapping.getActualReadRowType()` 与上层 `readType` 不等价，例如 PK data file 普通模式/历史文件/schema trim 导致的实际读取字段与输出字段不完全一致。这个 reason 和 `SCHEMA_CAST_OR_REORDER` 分开，便于 benchmark 判断是 schema evolution 问题，还是 native 暂未支持 Paimon KV/trim 投影问题。

Spark 物理计划也要进入报告：如果查询被 `PaimonLocalScan`、aggregate pushdown、metadata-only scan 或其他不创建 `PaimonPartitionReader` 的路径接管，该 query 必须标记为 `aggregate_pushdown_not_native_scan` / `not_native_scan_plan`，不能把它计入 native IO speedup。procedure 至少要记录 `executedPlan` 摘要和是否出现 Paimon scan reader。baseline/native 的 scan 节点数量、输出 schema、pushed filters、partition filters、文件数/字节数应一致；只有 reader 实现和 native applicability 可以不同。若不一致，标记 `plan_not_comparable`，不进入 speedup 几何平均。

plan comparable 不能直接比较 Spark `executedPlan.toString` 原文，因为 expression id、plan id、codegen id、runtime statistics 文案可能每轮不同。procedure 应抽取稳定 scan signature：scan node class/name、Paimon table identifier、output field name/type/nullability、pushed data filters、partition filters、bucket/partition count、input file count/bytes、是否 metadata-only、是否 `PaimonLocalScan`、是否创建 `PaimonPartitionReader`。raw `executedPlan` 仍保存到报告用于诊断，但比较用 canonical signature。

query status 使用稳定枚举：`ok`、`correctness_failed`、`native_not_applicable`、`native_not_applicable_file_level`、`partial_native`、`aggregate_pushdown_not_native_scan`、`not_native_scan_plan`、`plan_not_comparable`、`unstable`、`failed`。

### 10.10 Benchmark 图表口径

柱状图建议同时输出两张，样式参考 LakeSoul IO benchmark 图：白底、网格线、蓝色 baseline、橙色 native IO、每组 query 两根柱子，标题使用 `Spark 读 OBS 性能对比 - Paimon Native IO`。

1. `latency_bar.png`：每条 query 的 native off/on 绝对耗时
2. `speedup_bar.png`：每条 query 的 speedup = off_time / on_time

图表标题必须包含：dataset、scale factor、DV 删除比例、文件大小、executor 配置、run_id。这样后续不同环境结果可比较。

### 10.11 不要做的对比

- 不和 LakeSoul 直接对比（场景不同）
- 不和 Comet 对比（PoC 是 LakeSoul 路径）
- 不和 Spark 自带 vectorized parquet reader 对比（Paimon 没用它）

---

## 11. 风险与开放问题

### 11.1 高风险（PoC 第一周必须做 spike 验证）

1. **arrow-rs Parquet reader 的 row_index 列支持**：
   - 验证内容：arrow-rs `ParquetRecordBatchStreamBuilder` 或 DataFusion `FileScanConfigBuilder` 是否原生支持把 row_index 作为虚拟列附加到输出 RecordBatch，以及该 row_index 是文件绝对位置还是 row group 相对位置
   - 失败回退：如果不支持，需要在 arrow-rs Parquet reader 上写一层 wrapper，从 page metadata 读 row indexes 后手工拼一列
   - 验证方式：写 Rust spike，读一个带多 row group 的 Parquet 文件，开 row group skip / page filter，确认输出 `_row_index` 与 Paimon Java `returnedPosition()` 完全一致
   - **Spike 失败 → 需要写 wrapper（多 2-3 天工作量），但 PoC 整体仍可继续**

2. **OBS-only 文件系统适配正确性**：
   - 验证内容：`obs://` 路径 + `fs.obs.*` 配置能通过 `ObsObjectStore` + 内嵌 OBS client 读取 Paimon 写出的 Parquet 文件；非 OBS scheme 不进入 native 或明确报错
   - 关注点：OBS 原生鉴权、endpoint、range read 性能、特殊字符 URL 编码、内嵌 OBS client 到 `object_store::ObjectStore` 错误映射
   - 验证方式：写 30 行 Rust 代码，注册 `obs://bucket` 对应的 `ObsObjectStore`，从 OBS 读取一个 Paimon 表的某个 Parquet data file，验证 schema / 行数与 Java 路径一致；再用 `s3://` 或 `file://` 路径验证 native 守门员拒绝
   - **Spike 失败 → 优先修 Paimon 内嵌 OBS client，其次修 `ObsObjectStore` adapter；不回退到 AWS S3 backend，工作量 +2~5 天**

3. **Paimon core 模块依赖边界**：
   - 验证内容：`paimon-core` 只新增 SPI / loader / guard 后，main compile scope 能否在不依赖 `paimon-native-io`、`paimon-arrow`、JNR 和 Spark 的情况下编译；当前 `paimon-core` 对 `paimon-arrow` 只有 test scope 依赖，不能因为 native IO 变成 main 依赖；`paimon-native-io` jar 在 classpath 时能否通过 ServiceLoader 被发现
   - 失败回退：把 SPI 再拆到更底层轻量模块，或调整 ServiceLoader 注册点；仍避免 `paimon-core -> paimon-native-io` 的模块环
   - **Spike 失败 → 需要重排 Maven 模块依赖，工作量 +1~2 天**

4. **OBS 配置能否在 core 层拿全**：
   - 验证内容：真实 Spark catalog/table 创建后，`CoreOptions` / table options 中是否能拿到 `fs.obs.endpoint`、`fs.obs.access.key`、`fs.obs.secret.key`
   - 失败回退：Spark catalog 初始化时显式把 `spark.hadoop.fs.obs.*` 合并进 Paimon catalog options；core 层仍只读 `NativeIOOptions`
   - **Spike 失败 → 需要补 Spark conf 透传逻辑，工作量 +1 天**

5. **DataFusion 51 拆分 crate / API 漂移**：
   - 验证内容：`FileScanConfigBuilder`、`ParquetFormat` / `ParquetSource`、`PartitionedFile`、`RuntimeEnv::register_object_store`、row index 虚拟列 API 的实际 module path 和签名
   - 验证方式：先写最小 Rust crate，只构造一个 OBS/file mock `ObjectStore` + 单 Parquet 文件 physical plan，不接 Java
   - **Spike 失败 → 调整到当前 DataFusion 51 API；如果 row_index API 缺失，落到 wrapper 方案**

6. **JNR `CStatus` 返回值映射**：
   - 验证内容：Rust `NonNull<CStatus>` 返回值在 JNR Java 侧按 LakeSoul 的 `CStatus` struct 声明是否稳定可用，`free_c_status(@Pinned @In @Transient CStatus)` 是否正确释放
   - 验证方式：写一个只返回成功/失败 `CStatus` 的 cdylib smoke test，在 JDK 8 + Linux x86_64 和 macOS arm64 各跑一次
   - **Spike 失败 → Java 签名改成 `Pointer` 并手动读 struct 字段，同时调整释放函数**

7. **空投影 / `count(*)` 与 DV row_index 共存**：
   - 验证内容：0 业务列 projection 时 native 仍输出 `_row_index`，JVM 能构造 0 列 `InternalRow` batch，`ApplyDeletionVectorReader` 仍按 row position 过滤。注意 Spark SQL `count(*)` 可能被 aggregate pushdown 成 `PaimonLocalScan`，不能单独作为验证
   - 失败回退：短期拒绝空投影 query 走 Java reader；benchmark 中只有强制经过 scan reader 的 `scan_count_no_agg_pushdown` 才能计入 native speedup
   - **建议作为第一周 correctness case，不拖到性能阶段**

8. **`FormatReaderMapping.actualReadRowType` 暴露与等价性判断**：
   - 验证内容：PK 普通 data file、`data-file.thin-mode=true`、partition 表、schema evolution 表分别构造 mapping，确认 `actualReadRowType` 与 Java reader 实际 `createReaderFactory` 输入一致
   - 失败回退：先只允许 mapping 完全 identity 的薄数据文件进入 native；其他文件稳定返回 `READ_TYPE_MISMATCH` 走 Java reader
   - **Spike 失败 → 不影响正确性，但 native 命中面会收窄；需要补 core accessor / 单测，工作量 +0.5~1 天**

9. **Parquet physical schema / timestamp INT96 语义**：
   - 验证内容：Paimon Java 对 timestamp precision >6 写 INT96，native/DataFusion 对 INT96 的 Julian day、nanos-of-day、时区和 nanosecond 精度解释是否完全一致
   - 失败回退：阶段 1 只支持 timestamp precision <=6；INT96 和 physical annotation 不匹配统一 `PARQUET_PHYSICAL_TYPE` 走 Java reader
   - **Spike 失败 → 正确性不受影响，但 timestamp(9) 表不会命中 native；后续支持需补专门转换或 wrapper**

### 11.2 中风险

1. **Cross-compile / build 流程**：Paimon 当前是纯 Java 构建（Maven）。引入 Rust cargo build 后，CI / 发布脚本需要改。Linux x86_64 单平台 PoC 阶段 OK，但流程上要确认能跑通。**注意：只支持 OBS 后 Rust 不引入 HDFS C 库依赖，cross-compile 大幅简化**。

2. **JNR-FFI 在 JDK 8 + arm64 macOS 的兼容性**：开发同学可能用 M 系列 Mac。需要确认能本地编译 cdylib + JNR 能在 macOS arm64 上加载。

3. **Tokio runtime 在 Spark executor 长生命周期的稳定性**：global runtime 共享，runtime 死了整个 executor 废。需要兜底重启逻辑（PoC 阶段先不做，观察问题）。

4. **OBS 凭据轮换**：生产 AK/SK 如果定期轮换，native reader 持有的 store 实例会带过期凭据。PoC 阶段先用静态 AK/SK，生产化时再处理动态轮换。

5. **Apache/RAT/NOTICE 发布合规**：内部 PoC 可以先单独 jar 灰度，但进入正式 Paimon 发布前必须完成第三方许可证、NOTICE、source release 不含二进制产物、RAT exclude 等检查。

6. **Spark task cancel / timeout**：`next_record_batch_blocked` 是阻塞式 FFI；Spark 取消 task 时 Java 线程中断不一定能打断 OBS range read。PoC 先依赖 OBS client timeout、reader close、FFI 前后 interrupt 检查来保证可收敛，生产化再评估 native cancel token。

### 11.3 已决策（用户已确认）

- ✅ **Q-A**：Native lib 分发 → 打入 paimon-native-io.jar resources，沿用 LakeSoul `JnrLoader`
- ✅ **Q-B**：OBS 配置传递 → key/value 一对一传（沿用 LakeSoul `object_store_options` 模式），不用 JSON。Paimon 侧把 `fs.obs.*` 配置拍平传 native
- ✅ **Q-C**：Column pruning → **做**。native 侧拿 target schema 算 indices，只读请求列
- ✅ **Q-D**：Rust crate 来源 → 在 Paimon 内 `paimon-native-io/rust/` 下新建，从 lakesoul-io cherry-pick 文件，保留 license header
- ✅ **Q-E**：Fallback 机制 → **PoC 不做**，native 失败直接抛异常，便于暴露 bug
- ✅ **Q-F**：开关支持 → **做**，多层 override（session > table > cluster > default(false)）
- ✅ **Q-G**：Metric 配套 → **PoC 不做**，等通过后再加
- ✅ **Q-H**：错误类型 → 用 `anyhow::Error`，不引入 `rootcause`（LakeSoul 用的）
- ✅ **Q-I**：文件系统范围 → **仅支持 OBS**。native IO 只接受 `obs://` 路径和 `fs.obs.*` 配置；S3 / OSS / HDFS / file 等非 OBS 文件系统不做、不规划在本 PoC 内扩展

### 11.4 仍待澄清

- **Q-J**：DV 模式 PK 表在生产里有多少？建议先 sample 几张典型表（宽表 / 窄表 / 大文件 / 小文件）作为 PoC fixture
- **Q-K**：OBS endpoint 和 region 是什么？AK/SK 怎么注入 PoC 测试环境（环境变量 / Spark conf）
- **Q-L**：Paimon 表的 OBS 路径是否统一为 `obs://bucket/...`；如果存在历史 scheme，需要 Java 路径读取或迁移路径，不纳入 native IO 兼容层

---

## 12. 工作量预估（粗略）

| 阶段 | 任务 | 人天 |
|---|---|---|
| **Spike** | arrow-rs row_index 列支持验证 | 1 |
| **Spike** | DataFusion 51 split crate / API 最小编译验证 | 1 |
| **Spike** | JNR `CStatus` 返回值映射 smoke test（JDK 8 + Linux/macOS） | 1 |
| **Spike** | OBS-only `ObsObjectStore` 端到端 read 验证（含非 OBS scheme 拒绝） | 1 |
| **Spike** | Maven/SPI/shade 依赖边界验证：core 不引入 Arrow/JNR 编译依赖，native jar 通过 ServiceLoader 生效，Parquet shaded classpath 不冲突 | 1 |
| **Spike** | `FormatReaderMapping.actualReadRowType` accessor 与 PK thin/non-thin 等价性验证 | 1 |
| Rust | paimon-io crate：config + session + reader + helpers + 内嵌 OBS client + `ObsObjectStore`（适配 DataFusion ObjectStore） | 4 |
| Rust | paimon-io-c crate：17 个 C ABI 函数 | 2 |
| Build | Maven 集成 cargo build，cdylib 打入 jar resources，with-dependencies jar / ServiceLoader resource merge | 3 |
| Java | `JnrLoader` + `LibPaimonNativeIO` + `PaimonNativeReader` | 3 |
| Java | `NativeFileRecordReader` + `NativeFileRecordIterator` + Arrow → Paimon ColumnVector | 4 |
| Java | `NativeFormatReaderFactory` + 空投影/count batch 适配 | 1 |
| Java/native module | `NativeParquetSchemaValidator` physical schema preflight（timestamp/decimal/field id/order） | 1 |
| Java | `CoreOptions.NATIVE_IO_ENABLED` + 多层 override 支持 | 1 |
| Java/Core | SPI 接口 / loader / `SupportsNativeIO` 守门员 + `KeyValueTableRead` 接入 + 非 corrupt 异常分类 | 3 |
| Java/native module | `NativeRawFileSplitRead` + `NativeRawFileSplitReadProvider` 实现 | 2 |
| Spark | session conf 透传 + 四类读入口覆盖（V2/V1 path/V1 catalog/KnownSplitsTable）+ equality/hashCode 修正 + Spark SQL correctness/benchmark 接入 | 2 |
| 测试 | 正确性测试套件（参数化 native=true/false 双跑对比，含 reader EOF/lifecycle、serialization/equality、FFI panic、CStatus 不变量、loader arch/classloader/shaded dependency、Arrow 非预期类型、preflight/native 一致性、外部 IO 异常分类、plan signature、multiset correctness、OBS endpoint/path mock） | 9 |
| 测试 | benchmark 存储过程：synthetic PK+DV 自动造数、顺序隔离跑 baseline/native、结果图表写 OBS | 4 |
| 测试 | benchmark 框架基础设施与指标采集（含 applicability reporter、task attempt 去重、native resource hash/runtime worker 校验、plan comparable signature） | 3 |
| Compliance | 第三方 license / NOTICE / RAT / source-binary release 策略检查（含 cargo-deny、Cargo.lock/vendor 策略、with-dependencies jar NOTICE 合并） | 2 |
| Buffer | 集成调试 / 内存泄漏排查 / DV 边界 case | 5 |
| **合计** | | **55 人天** |

阶段 2 性能调优另算（预估 5-10 天）。

潜在调整：
- 阶段 1 spike 暴露 row_index 列需要写 arrow-rs wrapper → +2-3 天
- DataFusion 51 API 与 LakeSoul 当前代码差异较大，需要重写 scan config 接入 → +1~3 天
- JNR `CStatus` 映射不能复用 LakeSoul 形态，需要改 Pointer 手动映射并补释放测试 → +1 天
- `FormatReaderMapping` accessor 或 PK thin/non-thin 等价性验证发现命中面不足 → +0.5~1 天，短期可收窄 native 命中条件
- timestamp INT96 / Parquet physical schema 语义无法短期对齐 → 不增加阶段 1 工作量，保持 `PARQUET_PHYSICAL_TYPE` 走 Java；若要支持 INT96 另估 +1~2 天
- OBS-only spike 暴露内嵌 OBS client 或 `ObsObjectStore` adapter 问题 → +2~5 天（优先修 Paimon 内部 OBS 模块，再修 adapter；不切 AWS S3 backend，不扩展到 S3/OSS/HDFS）

---

## 13. 后续演进路径

PoC 通过后，可以渐进扩展：

1. **覆盖 append-only 表**：复用大部分代码，更简单
2. **下推 filter 到 native**：用 Substrait 协议（LakeSoul 已有现成实现可借鉴）
3. **下推 DV 到 native**：消除 JVM 侧逐行判断
4. **支持 ORC**：DataFusion 也有 ORC 支持
5. **OBS reader 生产化**：优先在 Paimon 内嵌 OBS 模块完善 range read、streaming read、错误分类、重试和连接池行为；`ObsObjectStore` 保持轻量映射层，native IO 文件系统范围仍为 OBS-only
6. **非 DV PK 表的 native sort-merge**：移植 LakeSoul `sorted_stream_merger`
7. **接入 Spark `ColumnarBatch`**：实现 `SupportsColumnarReads`，跳过 Arrow → InternalRow → SparkInternalRow 三次转换

每一步都是独立可发布的增量。

---

## 14. 决策记录

| 议题 | 决策 | 日期 |
|---|---|---|
| 选 LakeSoul vs Comet | LakeSoul（JDK 8 兼容 + 内部使用无社区约束） | 2026-05-16 |
| 阶段划分 | 阶段 1 正确性，阶段 2 性能 | 2026-05-16 |
| 起点表类型 | DV 模式 PK 表 | 2026-05-16 |
| Spark 版本 | 3.4.4 | 2026-05-16 |
| JDK 版本 | 8（runtime） | 2026-05-16 |
| 文件格式 | Parquet（PoC 阶段） | 2026-05-16 |
| 文件系统 | **仅 OBS（华为云对象存储）**；native IO 只接受 `obs://` + `fs.obs.*` | 2026-05-16 |
| 不使用非 OBS 文件系统 | S3 / OSS / HDFS / file 等全部不进入 native IO；生产只要求 OBS | 2026-05-17 |
| OBS client 来源 | 将现有 `obs-rust-sdk/src/*` 迁入 `paimon-native-io/rust/paimon-io/src/obs/`，作为 Paimon 内部模块维护；不保留外部 crate 依赖 | 2026-05-17 |
| Kerberos | 不考虑（线上无 Kerberos） | 2026-05-16 |
| Arrow Java 版本 | 沿用 Paimon 当前 15.0.0，不冲突 | 2026-05-16 |
| Arrow 18 profile | Spark 3.5 等 profile 切到 Arrow 18.1.0 时，需要额外 smoke test；PoC Spark 3.4.4/JDK8 主线先按 Arrow 15.0.0 验收 | 2026-05-18 |
| Arrow Java allocator | 跟随 Paimon `paimon-arrow` 的 `arrow-memory-unsafe`；不沿用 LakeSoul 的 Netty allocator 设置 | 2026-05-18 |
| DV 对齐机制 | Parquet row_index 虚拟列 + `FileRecordIterator.returnedPosition()` | 2026-05-16 |
| Pushdown 策略 | 阶段 1 只做 projection；SQL data filter/topN/limit 不进 native。阶段 2 再做 Substrait / Paimon Predicate filter pushdown | 2026-05-16 |
| Native scan 路径 | 直接 `FileScanConfigBuilder + ParquetSource → create_physical_plan`（不走 `ListingTable`） | 2026-05-16 |
| Tokio Runtime | 全局 `LazyLock<Arc<Runtime>>`，env var 调线程数 | 2026-05-16 |
| Projection 表达 | 传 target Schema，indices 在 native 内部算 | 2026-05-16 |
| OBS 配置传递 | key/value 一对一传（`fs.obs.endpoint` / `fs.obs.access.key` / `fs.obs.secret.key` 等） | 2026-05-16 |
| OBS 配置优先级 | DataSource/path extraOptions > `spark.paimon.fs.obs.*` 动态配置 > 持久化 table/catalog `fs.obs.*` > `spark.hadoop.fs.obs.*` fallback | 2026-05-18 |
| Rust crate 来源 | Paimon 内新建 `paimon-native-io/rust/`，cherry-pick lakesoul-io 文件 | 2026-05-16 |
| Error 类型 | `anyhow::Error`（不引入 `rootcause`） | 2026-05-16 |
| 开关 | 支持，多层 override（session > table > cluster > default(false)） | 2026-05-16 |
| Auto-fallback | PoC 不做 | 2026-05-16 |
| Metric | PoC 读链路不做 runtime metric；benchmark procedure 单独采集 query/stage 指标并输出报告 | 2026-05-17 |
| Native applicability 采集 | 默认 no-op；benchmark procedure 显式启用 `NativeApplicabilityReporter`，从真实 split/file guard 采集命中率和拒绝 reason，不作为生产 runtime metric | 2026-05-18 |
| Benchmark 数据集 | 默认 synthetic PK+DV 自动造数，保证命中 native IO；TPC-DS/SSB 仅作为可选 derived PK+DV 模式，不宣称等同原始标准 benchmark 排名 | 2026-05-18 |
| Benchmark 执行方式 | 一次 procedure 顺序跑 baseline/native，两轮之间清理 Spark cache，可选隔离 SparkSession；不并发同时跑 | 2026-05-18 |
| Benchmark native 命中口径 | 输出 split/file 级 native 命中率；`partial_native` 默认不纳入几何平均 speedup | 2026-05-18 |
| 文件级 0 命中口径 | split 级通过但 file 级 0 native bytes 时标记 `native_not_applicable_file_level`，不算 `partial_native` | 2026-05-18 |
| Benchmark 物理计划口径 | `PaimonLocalScan` / aggregate pushdown / metadata-only plan 不算 native IO scan；必须记录 executedPlan 并从 speedup 几何平均中排除 | 2026-05-18 |
| C ABI 接口数 | 17 个（LakeSoul 56 个砍约 3 倍） | 2026-05-16 |
| Paimon 接入层位置 | core 放可选 SPI 接入点；真实 `NativeRawFileSplitRead` / provider 放 `paimon-native-io`，Spark 只负责 conf 透传与端到端测试 | 2026-05-18 |
| Paimon native 模块依赖边界 | `paimon-core` 只放 SPI / loader / guard，不直接依赖 Arrow/JNR/native；`paimon-native-io` 通过 ServiceLoader 提供真实 provider | 2026-05-18 |
| Paimon native 模块格式依赖 | `paimon-native-io` 显式依赖 `paimon-format`，用于 Parquet footer preflight；core 仍不新增 Arrow/JNR/native 依赖 | 2026-05-18 |
| ServiceLoader 行为 | 使用线程上下文 classloader，provider 创建阶段不加载 native lib；缺失 provider 只让 native unavailable | 2026-05-18 |
| Native lib resource 布局 | JNR 加载方式沿用 LakeSoul，classpath resource 布局采用 Paimon 平台目录 `/<os>/<arch>/libpaimon_native_io.*`，loader 归一化 `amd64/x86_64` 与 `arm64/aarch64` alias | 2026-05-18 |
| PoC 分发 jar 形态 | 线上压测使用 `paimon-native-io-*-with-dependencies.jar` 或等价显式依赖清单，确保 `jnr-ffi` 在 driver/executor classpath；不把 Paimon/Spark/Arrow 主依赖重复打入 native jar | 2026-05-18 |
| 引擎范围 | PoC 只允许 Spark connector 标记 `engineSupportsNativeIO=true`；Flink/Core 共享读路径默认拒绝，避免 table property 误影响非 Spark 作业 | 2026-05-18 |
| Spark-only 标记方式 | Spark connector 通过 `table.copy(...)` 注入瞬时 read option（如 `__paimon.internal.native-io.engine=spark`），不扩大公共 `ReadBuilder` API；`SchemaValidation` 拒绝持久化 `__paimon.internal.*` table option | 2026-05-18 |
| Spark 读入口统一 | V2 catalog、V1 path、V1 catalog 读都必须走同一 Spark native read option helper；不能只改 `PaimonSparkTableBase.newScanBuilder` | 2026-05-18 |
| String 零拷贝 | 阶段 1 不作为正确性前置；先复用 `paimon-arrow` 转换，阶段 2 再优化 Arrow Utf8 buffer 直读 | 2026-05-18 |
| row_index 内部列名 | 不硬编码 `_row_index`；默认 `__paimon_native_row_index` 并避开业务字段冲突 | 2026-05-18 |
| native reader 文件粒度 | 阶段 1 一文件一个 `NativeFileRecordReader`；split 内多文件仍由 `ConcatRecordReader` 串联 | 2026-05-18 |
| 类型支持策略 | 阶段 1 使用显式基础类型白名单；timestamp_ltz、variant/blob 和所有嵌套/集合类型先走 Java reader | 2026-05-18 |
| Timestamp 精度 | 阶段 1 只支持 `TIMESTAMP_WITHOUT_TIME_ZONE(precision<=6)`；Paimon Parquet INT96（precision>6）先拒绝，等 arrow-rs/DataFusion 语义 spike 后再放开 | 2026-05-18 |
| Parquet physical preflight | native 文件进入 scan 前必须校验 footer physical/logical schema；unsupported physical type 走 Java，missing/corrupt/IO 错误按原 `ignoreLostFiles` / `ignoreCorruptFiles` 语义处理 | 2026-05-18 |
| Preflight footer 读取位置 | 阶段 1 JVM 侧用 Paimon `FileIO` 读 footer，接受额外一次 range read；后续如需优化再下沉到 Rust metadata load | 2026-05-18 |
| Unsupported 场景处理 | 不进入 native provider，透明走 Java reader；只有已进入 native 后失败才直接抛异常 | 2026-05-18 |
| DV scope guard | 即使 split `rawConvertible=true`，也必须要求 `deletion-vectors.enabled=true`，避免 FIRST_ROW/单 level raw 场景误进 PoC native | 2026-05-18 |
| `ignoreLostFiles` / `ignoreCorruptFiles` | native reader 通过 `NativeFormatReaderFactory` 交给 `DataFileRecordReader` 创建，复用原有创建期和读取期容错语义；外部 IO/权限类错误用非 corrupt marker 逃逸，不受 `ignoreCorruptFiles` 静默跳过 | 2026-05-18 |
| 空投影 / `count(*)` | 0 业务列 projection 仍必须输出 `_row_index`，JVM 构造 0 列 batch 后交给 DV 过滤 | 2026-05-18 |
| `FormatReaderMapping` 复用 | native physical read schema 必须来自 core 暴露的 `actualReadRowType`；阶段 1 只允许它与 logical `readType` 完全等价，否则 `READ_TYPE_MISMATCH` 走 Java | 2026-05-18 |
| JNR `CStatus` 映射 | 首版镜像 LakeSoul：Rust 返回 `NonNull<CStatus>`，Java 声明 struct 返回并用 `free_c_status` 释放；改 Pointer 前必须 smoke test | 2026-05-18 |
| DataFusion 51 依赖 | 依赖 split crates（`datafusion-datasource*` / `physical-plan` / `session` 等），先按 LakeSoul 编译通过再删减 | 2026-05-18 |
| Apache 发布合规 | PoC jar 可内部分发；正式进入 Paimon 发布前必须完成 license/NOTICE/RAT/source-binary release 策略 | 2026-05-18 |

---

## 15. 开关基础设施详细设计

### 15.1 四层 override

```
spark.paimon.native-io.enabled = true       ← 最高（session conf）
   ↓ override
table property 'native-io.enabled' = true   ← 次高（per-table）
   ↓ override
paimon-config.yaml native-io.enabled: true  ← 低（cluster default）
   ↓ default
hardcode false                              ← 兜底
```

实现约束：

- `CoreOptions` 只直接看到 Paimon table/catalog options；Spark session conf 必须在 Spark connector 创建 table read 前合并进 Paimon options
- session conf key 使用 `spark.paimon.native-io.enabled`；进入 core 后统一转换为 `native-io.enabled`
- OBS 配置同理：`spark.hadoop.fs.obs.*` / `spark.paimon.fs.obs.*` 在 Spark 层合并为 core 可见的 `fs.obs.*`
- core 层不依赖 SparkConf / Hadoop Configuration，只读取 `NativeIOOptions`
- 四层 override 只决定“是否请求 native IO”；PoC 还必须经过 `engineSupportsNativeIO` guard。只有 Spark connector 在构造 `NativeSplitReadContext` 时置 true，Flink/Core 默认 false，避免 table/catalog property 对非 Spark 引擎生效
- `__paimon.internal.native-io.engine` 是 Spark connector 注入的内部瞬时 read option，不是用户-facing table option；不要写入文档生成的公共配置表；持久化 table option 中出现 `__paimon.internal.*` 必须由 `SchemaValidation` 拒绝

### 15.2 配置项定义

```java
// CoreOptions.java
public static final ConfigOption<Boolean> NATIVE_IO_ENABLED =
    key("native-io.enabled")
        .booleanType()
        .defaultValue(false)
        .withDescription(
            "Whether to use native IO (Rust-based Parquet reader via JNR-FFI) "
            + "for raw-convertible splits (DV mode PK tables). "
            + "Falls back to Java path for unsupported scenarios.");
```

`native-io.batch-size`：

- 默认 `4096` 行，先对齐 LakeSoul 常见 batch 粒度，后续 benchmark 再调
- 合法范围建议 `[1, 65536]`；小于 1 直接配置校验失败，大于 65536 拒绝或 clamp 都可以，但必须记录清楚，推荐直接拒绝
- 该配置控制 DataFusion/arrow-rs 每个 `RecordBatch` 的目标行数，不保证输出 batch 一定等于该大小（row group 边界、EOF、过滤都会变小）
- 每个 Arrow batch 必须额外带内部 row index 列；内存估算要按 `业务列 + 1` 计算
- 行数不是内存上限。宽 string/binary/decimal 列可能让单个 Arrow batch 远大于预期；PoC 至少要在 JVM import 后统计 `VectorSchemaRoot` 近似 buffer bytes（或 Arrow allocator 当前分配增量），超过内部默认阈值（建议 64MB，可通过内部/benchmark option 调整）时抛出明确 `IOException` 并释放 batch。后续如要生产化，再考虑下推 DataFusion/Parquet reader 的 byte-size adaptive batch
- benchmark 的 `env.json` 记录 batch size；生产日志不要逐 batch 打印，避免噪音

`native-io.max-batch-bytes`：

- 内部/benchmark 诊断选项，不建议作为用户-facing 调优项写入公共配置表；默认建议 `64MB`
- 合法范围建议 `[1MB, 512MB]`；小于下限或大于上限直接配置校验失败，不做静默 clamp
- JVM import Arrow batch 后按所有 vector buffer 近似字节数统计；超过阈值时释放 batch 并抛 `IOException`。这个错误表示 native 输出批太大，不是 Parquet corrupt，也不能被 `ignoreCorruptFiles` 静默吞掉
- benchmark `env.json` 记录实际阈值；`result.json` 可记录是否触发过 batch byte limit

`native-io.row-index-column` 不建议暴露给普通用户配置；作为内部选项默认 `__paimon_native_row_index`，仅用于测试/诊断。`NativeIOOptions` 初始化时必须检查 read schema 中是否已有同名字段，如果冲突就追加后缀生成内部列名。

### 15.3 守门员 `SupportsNativeIO`

`SupportsNativeIO` 不建议只返回 boolean。PoC 需要给 benchmark 和诊断报告输出稳定拒绝原因，因此 core 侧定义轻量结果对象：

```java
public final class NativeApplicability {
    private final boolean applicable;
    private final NativeRejectReason reason; // applicable=true 时为 null / NONE
    private final String detail;             // 不包含敏感配置值
}
```

`SplitReadProvider.match(...)` 仍然只能返回 boolean；实现上先调用 `SupportsNativeIO.checkNativeSplit(...)`，把 `NativeApplicability` 缓存在 provider 或 task-local 诊断收集器里，再把 `applicable` 返回给 Paimon 原有分发逻辑。benchmark procedure 复用同一套 checker，不能另写一套近似判断。

为避免“读链路没有 runtime metric”与 benchmark 需要 `native_applicability.json` 互相矛盾，PoC 增加一个默认关闭的轻量诊断采集器：

- `NativeApplicabilityReporter` 放在 core SPI 附近，只暴露 `reportSplit(...)` / `reportFile(...)` / `snapshotAndReset()` 这类最小接口；默认实现 no-op，生产读路径不产生 Spark metric 和日志噪音
- benchmark procedure 开启诊断时，在 driver/executor 侧安装 task-local 或 Spark accumulator backed reporter；`NativeRawFileSplitRead` 在 split/file guard 后记录 file path、file size、是否 native、reason、detail
- reporter 只能记录脱敏后的 path/reason/字节数/文件数，不能记录 OBS AK/SK/session token；detail 字段也不能包含完整密钥或 Hadoop conf dump
- applicability 统计必须来自真实读路径的 reporter，而不是 procedure 预扫描后自己推断；否则会和实际 Spark scan、projection、file index、preflight 结果漂移
- `fileIndexResult.remain()==false`、preflight 返回 `EMPTY` 这类路径要记录为 `skipped_files/skipped_bytes`，不能记成 Java fallback；否则带 file index 或 ignoreLost/ignoreCorrupt 的 query 会把 native 命中率算低
- 已选择 native 并进入 `NativeFormatReaderFactory.createReader(Context)` 之后，如果发生 native 初始化/读取异常，不再记录为 `USE_JAVA` fallback；成功读完前已记录的 native 文件如果最终 task 失败，要依赖 attempt 去重/成功 attempt 过滤，不能把失败 attempt 混入最终命中率
- 如果 reporter 通过 Spark accumulator 汇总，必须处理 task retry / speculative execution：记录 `executionId`、`stageId`、`partitionId`、`taskAttemptId`、`attemptNumber` 或等价 task attempt key，只聚合成功 attempt；至少要在报告中标记是否发生 retry。不能把失败 attempt 的 split/file 统计重复计入 `native_applicability.json`
- executor 侧 reporter 建议先把 split/file 事件缓存在 task-local buffer，等 `TaskCompletionListener` 确认本 attempt 成功结束后再 flush 到 accumulator；如果无法可靠判断成功/失败，driver 聚合时必须按 Spark listener 的 task end reason 过滤，只保留每个 `(stageId, partitionId)` 被 Spark 接受的 successful attempt。speculative execution 下同一 partition 多个成功 attempt 只能保留最终 winner，不能简单求和
- baseline(native off) 和 native(native on) 两轮都要 reset reporter；报告中只把 native on 轮的命中率用于判定 `full_native` / `partial_native` / `native_not_applicable`

`provider.match` 只能做 split 级粗判断：

```java
public static NativeApplicability checkNativeSplit(
        DataSplit split,
        NativeIOOptions options,
        SplitReadProvider.Context context) {
    if (!options.enabled()) return rejected(DISABLED);
    if (!options.engineSupportsNativeIO()) return rejected(UNSUPPORTED_ENGINE);
    if (!options.providerAvailable()) return rejected(NO_PROVIDER);
    if (!options.deletionVectorsEnabled()) return rejected(NOT_DV_TABLE);
    if (context.forceKeepDelete()) return rejected(FORCE_KEEP_DELETE);
    if (split.isStreaming()) return rejected(STREAMING_SPLIT);
    if (!split.rawConvertible()) return rejected(NOT_RAW_CONVERTIBLE);
    if (!allFilesHaveDeleteRowCount(split)) return rejected(MISSING_DELETE_ROW_COUNT);
    return applicable();
}
```

文件级完整判断在 `NativeRawFileSplitRead.createFileReader` 里做：

```java
public static NativeApplicability checkNativeFile(
        DataSplit split,
        DataFileMeta file,
        FormatReaderMapping mapping,
        @Nullable FileIndexResult fileIndexResult,
        RowType readType,
        boolean rowTrackingEnabled,
        NativeIOOptions options,
        Path actualDataPath) {
    if (!options.enabled()) return rejected(DISABLED);
    if (!"parquet".equals(DataFilePathFactory.formatIdentifier(file.fileName()))) {
        return rejected(NON_PARQUET_FILE);
    }
    if (!"obs".equals(actualDataPath.getScheme())) return rejected(NON_OBS_PATH);
    if (!options.hasRequiredObsConfig()) return rejected(MISSING_OBS_CONFIG);
    if (hasUnsupportedTypes(readType)) return rejected(UNSUPPORTED_TYPE);
    if (!samePhysicalAndLogicalReadType(mapping.getActualReadRowType(), readType)) {
        return rejected(READ_TYPE_MISMATCH);
    }
    if (requiresCastOrReorder(mapping)) return rejected(SCHEMA_CAST_OR_REORDER);
    if (requiresPartitionOrSystemFields(mapping, rowTrackingEnabled)) {
        return rejected(PARTITION_OR_SYSTEM_FIELDS);
    }
    if (!noDataFiltersTopNOrLimit(mapping)) return rejected(DATA_FILTER_TOPN_LIMIT);
    if (fileIndexResult instanceof BitmapIndexResult) return rejected(BITMAP_INDEX_SELECTION);
    return applicable();
}

// NativeRawFileSplitRead.createFileReader(...) 中：
// checkNativeFile 通过后，还要在 paimon-native-io 模块做 Parquet footer preflight。
// 这个步骤检查 physical/logical parquet schema，不启动 native scan。
NativePreflightResult preflight = NativeParquetSchemaValidator.validate(
    fileIO,
    actualDataPath,
    file.fileSize(),
    mapping.getActualReadRowType(),
    ignoreLostFiles,
    ignoreCorruptFiles,
    nativeOptions);
switch (preflight.action()) {
    case USE_NATIVE:
        break;
    case USE_JAVA:
        return javaReader(preflight.reason()); // 例如 PARQUET_PHYSICAL_TYPE
    case EMPTY:
        return new EmptyFileRecordReader<>();
    case THROW:
        throw preflight.exception();
}

private static boolean hasUnsupportedTypes(RowType type) {
    return type.getFields().stream().anyMatch(f -> {
        switch (f.type().getTypeRoot()) {
            case CHAR:
            case VARCHAR:
            case BOOLEAN:
            case BINARY:
            case VARBINARY:
            case TINYINT:
            case SMALLINT:
            case INTEGER:
            case BIGINT:
            case FLOAT:
            case DOUBLE:
            case DATE:
            case TIME_WITHOUT_TIME_ZONE:
                return false;
            case TIMESTAMP_WITHOUT_TIME_ZONE:
                return ((TimestampType) f.type()).getPrecision() > 6;
            case DECIMAL:
                return ((DecimalType) f.type()).getPrecision() > 38;
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
            case VARIANT:
            case BLOB:
            case ARRAY:
            case VECTOR:
            case MULTISET:
            case MAP:
            case ROW:
                return true;  // PoC 不支持这些类型
            default:
                return true;  // 新增类型默认拒绝，避免静默读错
        }
    });
}
```

Parquet footer preflight 属于“进入 native scan 前的适用性判断”，不是 native 失败 fallback。这里要特别保护 `ignoreLostFiles` / `ignoreCorruptFiles` 语义：

- preflight 在 JVM 侧用 Paimon `FileIO` 读取 footer，不直接用 Rust `ObsObjectStore`；这样 external path、Paimon 文件系统封装、`ignoreLostFiles` / `ignoreCorruptFiles` 行为都与 Java reader 对齐
- 阶段 1 接受 native 路径多一次 footer range read 的开销。benchmark 需要记录这是 correctness guard 成本；如果这部分成为瓶颈，阶段 2 再把 validation 下沉到 Rust metadata load 并返回结构化 not-applicable
- preflight 读取 footer 时如果发现 file not found，且 `ignoreLostFiles=true`，返回 `EmptyFileRecordReader`，行为对齐 `DataFileRecordReader`
- preflight 读取 footer 时如果发现 Parquet footer 损坏，且 `ignoreCorruptFiles=true`，返回 `EmptyFileRecordReader`
- preflight 读取 footer 时如果是 OBS 403、5xx、timeout 等真实外部错误，按现有 IOException 传播，不改走 Java reader
- 只有 footer 成功读取后判断出 schema/physical type 不满足 PoC 能力，才返回 Java reader，并记录 `PARQUET_PHYSICAL_TYPE`
- preflight result 要携带 `validatedFileSize`、`validatedFooterRowCount`、可选 `validatedETag/version`。native reader 初始化时把这些值传给 Rust；Rust `ObsObjectStore.head` / DataFusion metadata load 读到的 `ObjectMeta` 必须与这些值一致。不一致说明数据文件在 preflight 与 native scan 之间被覆盖或 metadata 不一致，必须抛外部一致性错误，不能把旧 footer 的 schema/row count 套到新对象上继续读

也就是说，`NativeParquetSchemaValidator` 不能只抛普通 IOException，也不能只返回 boolean / `NativeApplicability`；需要区分“不适用”“可 native”“按 ignore 语义返回空 reader”“真实 IO 错误”。建议定义 `NativePreflightResult`：

```java
enum NativePreflightAction {
    USE_NATIVE,
    USE_JAVA,
    EMPTY,
    THROW
}
```

其中 `USE_JAVA` 携带 `NativeRejectReason` 并汇总进 benchmark applicability；`EMPTY` 对应 `ignoreLostFiles` / `ignoreCorruptFiles` 已生效，不计作 Java fallback；`THROW` 保留原 IOException。

`requiresCastOrReorder(mapping)` 首版保守处理：

- `mapping.getIndexMapping()` 不是 identity → 不支持；`null` 视为 identity，非 null 时必须逐项等于当前位置
- `mapping.getCastMapping()` 非空 → 不支持
- `mapping.getActualReadRowType()` 与 `readType` 字段数、字段 id、字段名、类型不一致 → 不支持

为避免每个模块重复猜测 mapping 语义，core 侧建议给 `FormatReaderMapping` 增加最小只读 API：

```java
public RowType getActualReadRowType();
public boolean hasIdentityIndexMapping();
public boolean hasNoCastMapping();
```

`actualReadRowType` 在 `Builder.build(...)` 里已经存在，只需要保存到 final 字段；getter 不能用 `dataSchema` / 上层 `readType` 重新推导一份近似结果。这属于 core SPI 边界增强，不引入 Arrow/JNR/native 依赖。

`requiresPartitionOrSystemFields(mapping, rowTrackingEnabled)` 首版保守处理：

- `mapping.getPartitionPair()` 非空 → 不支持
- `mapping.getSystemFields()` 非空 → 不支持
- `rowTrackingEnabled` 打开 → 不支持
- Spark metadata columns `__paimon_row_index` / `__paimon_file_path` 本身不在 Paimon `readType` 内，由 `PaimonRecordReaderIterator` 基于 `FileRecordIterator.returnedPosition()` 和 `filePath()` 拼接；native iterator 已实现这两个方法后可以支持 path/index metadata。`__paimon_partition` / `__paimon_bucket` 也由 Spark split 信息拼接，不要求 native reader 注入字段。

这些场景不是错误，也不是 native fallback；文件级判断返回 false 后，`NativeRawFileSplitRead` 为该文件创建 Java `DataFileRecordReader`。

`noDataFiltersTopNOrLimit(mapping)` 首版保守处理：

- `mapping.getDataFilters()` 非空 → 不支持
- `mapping.getTopN()` 非空 → 不支持
- `mapping.getLimit()` 非空 → 不支持

原因：Paimon pushed data filters/topN/limit 当前由 format reader 消费；如果 native 不实现等价过滤而直接返回整文件数据，会产生错误结果。阶段 1 不做 Substrait / Paimon Predicate 翻译，因此必须拒绝这些查询。

### 15.4 A/B 对比测试基础设施

```scala
class NativeIOCorrectnessSuite extends ParameterizedTest {
  @ParameterizedTest
  @ValueSource(booleans = Array(true, false))
  def testFixture_DvDeleteThenQuery(nativeEnabled: Boolean): Unit = {
    spark.conf.set("spark.paimon.native-io.enabled", nativeEnabled.toString)
    val result = spark.sql(query).collect()
    val resultHash = MurmurHash3.arrayHash(result)
    assertThat(resultHash).isEqualTo(expectedHash)
  }
}
```

### 15.5 不做（PoC 阶段）

- ❌ Auto-fallback（出错直接抛异常）
- ❌ Metric（CustomTaskMetric）
- ❌ 灰度比例控制
- ❌ 运行时动态切换（必须 SET conf 后新查询生效）

---

*Spec end. 待用户审阅。*
