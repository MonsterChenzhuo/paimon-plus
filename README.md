# Paimon Plus

Paimon Plus 是基于 Apache Paimon 拷贝并继续维护的增强版本。上游 Paimon 仍然是基础代码来源，本仓库后续用于维护面向 Spark、Native IO、宽表性能和写入稳定性的 plus 改动。

当前代码基于 Apache Paimon 1.4 系列开发，仍保持 JDK 8 和 Scala 2.12 兼容。

## Plus 增强

### Spark `export_parquet`

新增 Spark procedure：

```sql
CALL sys.export_parquet(
  table => 'default.T',
  columns => 'id,name',
  output_path => 's3://bucket/export/t',
  where => "dt = '2026-05-14' and id >= 10",
  parallelism => 100,
  compression => 'zstd',
  overwrite => true,
  target_file_size => '128 MB'
);
```

用途：

- 将 Paimon 表直接导出为外部 Parquet 目录。
- 支持列裁剪，`columns => '*'` 可导出全部列。
- 支持简单 `AND` 条件过滤。
- 支持 `parallelism`、`compression`、`overwrite`。
- 支持 `target_file_size` 控制导出文件滚动，避免生成过多小文件或单文件过大。

详细说明见 [docs/superpowers/export_parquet-usage.md](docs/superpowers/export_parquet-usage.md)。

### Native IO

新增 `paimon-native-io` 模块，提供基于 Rust native library 和 JNR 的 Paimon Native IO 能力。

当前重点：

- `paimon-native-io` Java 封装与 native library 加载。
- Spark 读路径可通过动态参数启用 native IO。
- Native reader 通过 Arrow batch 返回数据，降低 JVM row-by-row 读取开销。
- OBS 相关配置可从 Spark Hadoop conf、Paimon options 或环境变量透传。
- `export_parquet` native fast path 的设计与计划文档已在 `docs/superpowers` 下维护。

常用开关：

```sql
SET spark.paimon.native-io.enabled=true;
SET spark.paimon.native-io.batch-size=4096;
SET spark.paimon.native-io.max-batch-bytes='64 MB';
```

`export_parquet` native fast path 相关配置：

```sql
SET spark.paimon.native-io.export.enabled=true;
SET spark.paimon.native-io.export.fallback.enabled=true;
SET spark.paimon.native-io.export.metrics.enabled=true;
SET spark.paimon.native-io.export.memory-limit='512 MB';
```

Native IO 设计入口：

- `docs/superpowers/specs/2026-05-16-paimon-native-io-poc-design.md`
- `docs/superpowers/specs/2026-05-19-paimon-export-parquet-native-fast-path-design.md`
- `docs/superpowers/plans/2026-05-18-paimon-native-io-core-spark.md`
- `docs/superpowers/plans/2026-05-19-paimon-export-parquet-native-fast-path.md`

### 宽 schema 校验优化

优化 `[schema] Avoid repeated logical row type validation`。

问题背景：

创建 scan builder 时会校验表 schema。对于非常宽的 schema，`validateMergeFunctionFactory` 原先会在字段循环中反复调用 `schema.logicalRowType()`，导致重复构建 `RowType`，并在每次构建时重复执行全字段重名校验。约 20k 列的表在 Spark job 真正提交前，会在 driver 侧形成明显 CPU 热点。

优化内容：

- 缓存 `TableSchema.logicalRowType()`，避免在同一次校验中重复构建逻辑行类型。
- 在 merge function 校验中复用字段名列表。
- 对同一个 aggregate function 避免重复发现 aggregator factory。
- 将 duplicate fields 检查改为单次线性 `HashSet` 扫描，避免 grouping collector 带来的额外开销。

### 宽行写 buffer page size 校验

优化 `[core] Validate page size for wide write buffer rows`。

该改动在 core 写入路径中增加宽行场景下的 page size 校验，避免单行或宽表写入 buffer 与 page size 配置不匹配时进入更隐蔽的失败路径。宽表写入时可以更早暴露配置问题，便于定位和调整。

## 与上游 Apache Paimon 的关系

Paimon Plus 不是替代 Apache Paimon 的独立实现，而是在 Apache Paimon 代码基础上的增强维护分支。

本仓库保留 Apache Paimon 的模块结构、构建方式和 Apache License 2.0。后续 plus 特性会优先在本仓库维护，必要时再评估是否向上游提交。

上游文档地址：https://paimon.apache.org

## 构建

基础要求：

- JDK 8 或 JDK 11。
- Maven 3.6.3 或更高版本。
- Scala 2.12。
- 构建 Native IO native library 时需要 Rust toolchain。

推荐在项目固定镜像内构建：

```bash
docker run --rm -it \
  -v "$PWD":/workspace \
  -w /workspace \
  monster830/paimon-plus:spark344-java8 \
  bash
```

常用构建命令：

```bash
mvn clean install -DskipTests
```

快速打包：

```bash
mvn -Pfast-build -DskipTests package
```

只编译单个模块：

```bash
mvn -pl paimon-spark/paimon-spark-common -DskipTests compile
```

构建 Native IO 模块和 native library：

```bash
mvn -pl paimon-native-io -am -Pnative-io -DskipTests package
```

格式化代码：

```bash
mvn spotless:apply
```

IDE 配置：

- 将 `paimon-common/target/generated-sources/antlr4` 标记为 Sources Root。

## 测试

优先运行最小范围测试。

Java 单测：

```bash
mvn -pl <module> -Dtest=TestClassName#methodName test
```

Scala 单测使用 `scalatest-maven-plugin`，需要通过 `-DwildcardSuites` 指定 suite，并设置 `-Dtest=none`：

```bash
mvn -pl paimon-spark/paimon-spark-ut -am -Pfast-build -DfailIfNoTests=false \
  -DwildcardSuites=org.apache.paimon.spark.procedure.ExportParquetProcedureTest \
  -Dtest=none test
```

## 许可证

本仓库代码基于 Apache Paimon，遵循 [Apache Software License 2.0](LICENSE)。
