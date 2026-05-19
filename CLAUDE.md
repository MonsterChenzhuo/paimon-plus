# Paimon Plus 协作说明

本仓库面向 Apache Paimon 相关开发与设计工作。所有面向用户的解释、总结和计划默认使用中文。

## 基本约束

- 代码兼容 JDK 8 和 Scala 2.12，不使用更高版本语法。
- 修改代码前先阅读现有实现和测试，优先沿用本仓库已有模块边界、命名和构建方式。
- 工作区可能存在用户改动，不能回滚或覆盖非本任务相关文件。
- 文档、计划和设计说明优先放在 `docs/superpowers` 下。

## Superpowers 文档

- 设计文档放在 `docs/superpowers/specs/`。
- 实施计划放在 `docs/superpowers/plans/`。
- `export_parquet` native fast path 当前设计入口：
  - `docs/superpowers/specs/2026-05-19-paimon-export-parquet-native-fast-path-design.md`
  - `docs/superpowers/plans/2026-05-19-paimon-export-parquet-native-fast-path.md`

维护这些文档时，需要同步检查 spec 和 plan，避免设计和实施步骤不一致。

## 构建与测试

测试、打包都使用同一个镜像：

```text
monster830/paimon-plus:spark344-java8
```

推荐从仓库根目录执行：

```bash
docker run --rm -it \
  -v "$PWD":/workspace \
  -w /workspace \
  monster830/paimon-plus:spark344-java8 \
  bash
```

进入容器后再运行 Maven 命令。不要在宿主机上混用其他 JDK、Scala 或 Spark 环境做最终验证。

优先运行最小范围测试：

```bash
mvn -pl <module> -Dtest=TestClassName test
```

Scala 测试使用 `scalatest-maven-plugin`，通常需要：

```bash
mvn -pl paimon-spark/paimon-spark-ut -am -Pfast-build -DfailIfNoTests=false \
  -DwildcardSuites=org.apache.paimon.spark.sql.WriteMergeSchemaTest \
  -Dtest=none test
```

本地快速迭代可用 `-Pfast-build` 跳过 checkstyle、spotless、enforcer 和 rat；最终验证时不要只依赖 fast-build。

打包也必须在 `monster830/paimon-plus:spark344-java8` 镜像内执行，例如：

```bash
mvn -Pfast-build -DskipTests package
```

## Native IO 设计注意事项

- Spark 侧只负责规划、调度和语义兜底；native fast path 的适用性必须在 driver preflight 阶段明确。
- Rust native export 需要关注 OBS/object_store 兼容、multipart 写出、row group pruning、DV 过滤、predicate literal 编码、内存上限和指标可观测。
- native library 加载应复用现有 `PaimonJnrLoader`，不要新增独立 loader。
- 出现不支持的 schema evolution、credential provider、predicate 或 DV 表达时，应给出稳定 reject reason，并按配置回退 Java 路径或 fail fast。
