# export_parquet 参数与使用说明

`CALL sys.export_parquet` 用于在 Spark 中把 Paimon 表的数据导出到外部 Parquet 目录。当前实现只保留标准 Java 导出路径：driver 规划 Paimon splits，Spark tasks 通过 Paimon Java reader 读取行，再用 Paimon Parquet writer 写出文件。

## 基本语法

```sql
CALL sys.export_parquet(
  table => 'db.table',
  columns => 'id,name,dt',
  output_path => 's3://bucket/export/table',
  where => "dt = '2026-05-14' and id >= 100",
  parallelism => 32,
  compression => 'zstd',
  overwrite => true,
  target_file_size => '128 MB',
  partitioned_output => true,
  partition_job_parallelism => 4
);
```

最小调用只需要 `table`、`columns` 和 `output_path`：

```sql
CALL sys.export_parquet(
  table => 'default.T',
  columns => '*',
  output_path => 's3://bucket/export/t'
);
```

返回结果：

| 字段 | 类型 | 说明 |
| --- | --- | --- |
| `result` | `BOOLEAN` | 导出成功时为 `true`。失败时 procedure 抛出异常。 |
| `rows` | `BIGINT` | 实际写入 Parquet 文件的行数。 |

## 参数总览

| 参数 | 类型 | 必填 | 默认值 | 说明 |
| --- | --- | --- | --- | --- |
| `table` | `STRING` | 是 | 无 | 目标 Paimon 表标识符，例如 `'T'`、`'default.T'`。 |
| `columns` | `STRING` | 是 | 无 | 导出的列列表，使用逗号分隔；传 `'*'` 导出全部列。 |
| `output_path` | `STRING` | 是 | 无 | Parquet 输出目录。成功写出后包含 `part-*.parquet`、`_manifest.json` 和 `_SUCCESS`。 |
| `where` | `STRING` | 否 | 空，表示全量导出 | 行过滤条件。当前只支持简单条件通过 `AND` 连接。 |
| `parallelism` | `INT` | 否 | `spark.sparkContext.defaultParallelism` | Spark 导出任务并行度上限。小于 1 时按 1 处理。 |
| `compression` | `STRING` | 否 | `'zstd'` | Parquet 压缩 codec，直接传给 Parquet writer。 |
| `overwrite` | `BOOLEAN` | 否 | `false` | 输出目录已存在时是否删除后重写。 |
| `target_file_size` | `STRING` | 否 | 空，表示按 Paimon split 写文件 | 目标 Parquet 文件大小，例如 `'128 MB'`。配置后启用 rolling writer。 |
| `partitioned_output` | `BOOLEAN` | 否 | `false` | 是否按 Paimon 分区分别输出到 `output_path/分区路径`。 |
| `partition_job_parallelism` | `INT` | 否 | 实际导出分区数 | `partitioned_output=true` 时，并发提交分区导出 job 的上限。 |

`compact_output` 已移除。传入 `compact_output => true` 会被 Spark procedure 参数解析拒绝。

## 执行路径

`export_parquet` 现在只有 Java export 路径，不再提供 Native export fast path，也不再读取 `spark.paimon.native-io.export.*` 配置。即使这些旧配置仍存在于 Spark session 中，也不会改变 `export_parquet` 的行为。

核心流程：

1. 解析 `table` 并加载 Paimon 表。
2. 根据 `columns` 生成输出列投影。
3. 根据 `where` 生成 Paimon predicate。
4. 读取列自动包含输出列和过滤列。
5. 使用 Paimon scan 规划 splits。
6. 如果启用 `partitioned_output`，按 split 的分区值生成分区输出目录。
7. 准备输出目录，必要时按 `overwrite` 删除旧目录。
8. Spark tasks 读取 Paimon split，应用过滤条件，写出 Parquet 文件。
9. driver 汇总写出行数，生成 `_manifest.json` 和 `_SUCCESS`。

## 文件大小

不设置 `target_file_size` 时：

- 每个有数据输出的 Paimon split 最多生成一个 Parquet 文件。
- 如果某个 split 的所有行都被 `where` 过滤掉，该 split 不会创建空 Parquet 文件。

设置 `target_file_size` 时：

- 每个 Spark partition 使用一个 rolling writer。
- writer 达到目标大小后滚动生成新 `part-*.parquet` 文件。
- 文件数量由数据量、压缩率、目标大小和实际 Spark partition 数共同决定。

## 分区输出

`partitioned_output => true` 适用于 Paimon 分区表。导出时会按 Paimon scan 规划出的 split 分组，只创建实际有数据的分区目录。

```sql
CALL sys.export_parquet(
  table => 'default.orders',
  columns => 'order_id,user_id,amount',
  output_path => 's3://bucket/export/orders_range',
  where => "dt >= '2026-05-01' and dt <= '2026-05-06'",
  partitioned_output => true,
  partition_job_parallelism => 4,
  target_file_size => '256 MB',
  overwrite => true
);
```

输出目录示例：

```text
s3://bucket/export/orders_range/
  dt=2026-05-01/
    part-....parquet
    _SUCCESS
  dt=2026-05-02/
    part-....parquet
    _SUCCESS
  _manifest.json
  _SUCCESS
```

## Manifest

`_manifest.json` 写在输出根目录，记录输出根路径和相对 Parquet 文件路径。下游 Python、Arrow、Spark 或 AI 工具可以先读 manifest，再按文件列表消费本次导出的稳定快照，避免递归 list 对象存储目录。

示例结构：

```json
{
  "base_path": "s3://bucket/export/orders_range",
  "files": [
    {"path": "dt=2026-05-01/part-a.parquet"},
    {"path": "dt=2026-05-02/part-b.parquet"}
  ]
}
```

## 常见错误

| 错误现象 | 常见原因 | 处理方式 |
| --- | --- | --- |
| `Columns should not be empty.` | `columns` 为空字符串或空白字符串 | 传入 `'*'` 或明确列列表。 |
| `Cannot find column 'xxx'.` | `columns` 中存在表里没有的列 | 检查列名和大小写。 |
| `Cannot find filter column 'xxx'.` | `where` 中存在表里没有的字段 | 检查过滤字段名。 |
| `Output path already exists...` | 输出目录已存在且未设置 `overwrite => true` | 换新目录，或确认可覆盖后设置 `overwrite => true`。 |
| `Unsupported predicate condition` | `where` 使用了不支持的表达式 | 改写为简单条件，并只用 `AND` 连接。 |
| `Unsupported filter literal type` | `where` 对复杂类型或暂不支持类型做过滤 | 改为支持的基础类型字段过滤。 |
| `Invalid IN predicate` | `IN` 后面不是括号包裹的列表 | 使用 `col in ('a', 'b')` 格式。 |
| `Target file size should be larger than 0 bytes.` | `target_file_size` 解析后小于等于 0 | 使用 `'128 MB'`、`'512 MB'` 等正数大小。 |
| `compact_output` 参数错误 | 使用了已移除的旧参数 | 删除 `compact_output`，用 `target_file_size` 控制 rolling writer 输出大小。 |

## 使用建议

- 导出到对象存储时，建议设置 `target_file_size`，避免单个任务写出过大的 Parquet 文件。
- 首次导出建议使用新目录；只有确认目录可删除时才使用 `overwrite => true`。
- `where` 尽量包含分区列或高选择性字段，便于 Paimon scan 和 reader 减少读取量。
- `parallelism` 应结合 Paimon split 数和 Spark executor 资源设置，单纯调大不一定提升速度。
- 如果需要复杂 SQL 逻辑、表达式计算、join 或聚合，建议先用 Spark SQL 写临时表或 DataFrame，再另行导出。

## AI 工具消费

Codex、Claude Code、spark-cli 或其他诊断工具可以直接消费以下结构化产物：

- procedure 返回的 `rows`：判断导出是否产生数据。
- `_manifest.json`：获取本次导出的相对 Parquet 文件列表。
- Spark stage/task metrics：判断导出慢在读取、编码还是对象存储写入。
- executor thread dump：定位是否停在 `ExportParquetProcedure.exportSplits`、`RollingParquetWriter.addElement` 或 Parquet writer。
