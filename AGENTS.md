# Apache Paimon

面向 AI 编码助手的 Apache Paimon 项目协作指南。

## 响应要求

- 默认使用中文解释实现思路、变更内容、验证结果和风险点。
- 输出应直接、具体，优先说明可执行结论。

## 语法要求

- 项目基于 JDK 8 和 Scala 2.12，不能使用更高版本才支持的语法特性。
- 新增 Java、Scala 代码时，应保持与现有模块的代码风格一致。

## 项目实现

- 修改前先阅读相关模块的现有实现、测试和调用路径，优先沿用已有抽象与工具方法。
- 变更范围应尽量收敛，只改动完成需求所必需的文件。
- 新增行为或修复缺陷时，优先补充最小但有效的测试覆盖。
- 不做无关格式化、重命名或大范围重构，除非这是完成需求的必要条件。

## 官网与博客

- 当需求属于“大需求”或新增/改变用户可见能力时，交付范围必须包含官网和文档传播设计，不能只提交代码实现。
- 需要同步评估并更新项目首页入口，例如 `docs/content/_index.md` 或对应官网首页内容，让用户能从首页发现该能力。
- 需要新增或更新博客/文章，内容至少覆盖：使用场景、现有痛点、方案设计、配置与命令示例、诊断/排障流程、限制和风险。
- 如果仓库暂时没有现成 blog 目录，应在 `docs/content` 下创建合适的内容入口，或在最终说明中明确建议发布路径。
- 对 AI 友好能力（如诊断 API、CLI、观测数据、结构化 JSON 输出）还必须同步说明 Codex、Claude Code、spark-cli 等工具如何消费这些数据。

## 分支与提交

- 新代码默认直接在 `main` 分支实现、提交并推送到远端 `main`。
- 完成需求并验证通过后，将本次任务相关提交推送到远端 `main` 分支。
- 如果用户明确要求使用功能分支、PR 或暂不推送，则以用户要求为准。
- 提交前检查工作区状态，只暂存和提交本次任务相关文件，避免带入无关改动。

## 构建和测试

优先使用最小可行的构建和测试范围，配合下面的加速方式缩短反馈周期。

### 构建

- 编译单个模块：`mvn -pl <module> -DskipTests compile`
- 编译多个模块：`mvn -pl <module1>,<module2> -DskipTests compile`

### 测试

优先运行最窄范围的测试。

#### Java 测试（surefire）

- 单个方法：`mvn -pl <module> -Dtest=TestClassName#methodName test`
- 单个类：`mvn -pl <module> -Dtest=TestClassName test`

#### Scala 测试（scalatest）

Scala 测试使用 `scalatest-maven-plugin`，不是 surefire。运行时使用 `-DwildcardSuites` 和 `-Dtest=none`：

```shell
mvn -pl paimon-spark/paimon-spark-ut -am -Pfast-build -DfailIfNoTests=false \
  -DwildcardSuites=org.apache.paimon.spark.sql.WriteMergeSchemaTest \
  -Dtest=none test
```

### 本地迭代加速

本地迭代时可使用 `-Pfast-build` 跳过 checkstyle、spotless、enforcer 和 rat 检查；最终验证不要依赖该选项：

```shell
mvn clean install -DskipTests -Pfast-build
```

如果需要同时构建 Native IO Rust native library，并打进 `paimon-native-io` artifact，需要额外启用 `native-io` profile：

```shell
mvn clean install -DskipTests -Pfast-build,native-io
```

如果需要构建 Spark 3.4 模块及其依赖，并同时启用 Native IO native library：

```shell
mvn clean install -DskipTests -Pfast-build,native-io,spark3 -pl paimon-spark/paimon-spark-3.4 -am
```

单模块测试示例：

```shell
mvn -pl <module> -Pfast-build -Dtest=TestClassName#methodName test
```

如果目标模块依赖本地已修改模块，使用 `-am`。如遇到无测试模块导致失败，可增加 `-DfailIfNoTests=false`：

```shell
mvn -pl <module> -am -Pfast-build -DfailIfNoTests=false -Dtest=TestClassName#methodName test
```
