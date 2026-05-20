# export_parquet 参数与使用说明

`CALL sys.export_parquet` 用于在 Spark 中把 Paimon 表的数据导出到外部 Parquet 目录。它直接基于 Paimon 读路径做列裁剪和条件过滤，不需要先构造 Spark SQL 的宽 `Project`，适合将 Paimon 表的部分列或部分行批量落盘到对象存储、本地文件系统或其他 Paimon `FileIO` 支持的路径。

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
  target_file_size => '128 MB'
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

返回结果包含两列：

| 字段 | 类型 | 说明 |
| --- | --- | --- |
| `result` | `BOOLEAN` | 导出成功时为 `true`。失败时 procedure 会抛出异常，不返回 `false`。 |
| `rows` | `BIGINT` | 实际写入 Parquet 文件的行数。 |

## 参数总览

| 参数 | 类型 | 必填 | 默认值 | 说明 |
| --- | --- | --- | --- | --- |
| `table` | `STRING` | 是 | 无 | 目标 Paimon 表标识符，例如 `'T'`、`'default.T'`。 |
| `columns` | `STRING` | 是 | 无 | 导出的列列表，使用逗号分隔；也可以传 `'*'` 导出全部列。 |
| `output_path` | `STRING` | 是 | 无 | Parquet 输出目录。目录成功写出后会包含 `part-*.parquet` 和 `_SUCCESS`。 |
| `where` | `STRING` | 否 | 空，表示全量导出 | 行过滤条件。当前只支持简单条件通过 `AND` 连接。 |
| `parallelism` | `INT` | 否 | `spark.sparkContext.defaultParallelism` | Spark 导出任务并行度上限。小于 1 时会按 1 处理。 |
| `compression` | `STRING` | 否 | `'zstd'` | Parquet 压缩 codec，直接传给 Parquet writer。 |
| `overwrite` | `BOOLEAN` | 否 | `false` | 输出目录已存在时是否删除后重写。 |
| `target_file_size` | `STRING` | 否 | 空，表示按 Paimon split 写文件 | 目标 Parquet 文件大小，例如 `'128 MB'`。配置后会启用滚动写文件。 |

## Native fast path

`export_parquet` 当前有两条执行路径：

| 路径 | 触发条件 | 状态 |
| --- | --- | --- |
| Java export | 默认路径，或 `spark.paimon.native-io.export.enabled=false` | 可用；通过 Paimon Java reader 读取，再用 Paimon Parquet writer 写出。 |
| Native export | `spark.paimon.native-io.enabled=true` 且 `spark.paimon.native-io.export.enabled=true` | 已接入 ServiceLoader SPI、driver preflight、配置校验、Spark split 到 raw Parquet source file 的规划、DV position 下沉、predicate JSON 转换、JNR FFI 和 Rust 读写 pipeline。 |

默认仍使用 Java export 路径。联调或压测 native export 时显式开启 native export，并建议设置 `spark.paimon.native-io.export.fail-on-fallback=true`，避免误以为已命中 native。

开启方式：

```sql
SET spark.paimon.native-io.enabled=true;
SET spark.paimon.native-io.export.enabled=true;
```

启用 native export 时还需要在 driver 和 executor classpath 中包含 `paimon-native-io` jar 及其 native library resource。若 classpath 中没有 native export provider，会以 `NO_PROVIDER` 拒绝。

`spark.paimon.native-io.export.fallback.enabled` 默认是 `true`，driver preflight 不适用时会回退 Java export。设置 `spark.paimon.native-io.export.fail-on-fallback=true` 后，preflight 不适用会抛出包含 reject reason 和 detail 的异常。

当前 native preflight 的已实现限制：

- 必须同时开启 `native-io.enabled` 和 `native-io.export.enabled`。
- 只接受 Spark engine 内部标记；该标记由 Spark procedure 自动注入。
- 当前只支持 `compression => 'zstd'`。
- 当前只接受 `obs://` 输出路径。
- 必须提供 OBS endpoint、access key 和 secret key。
- native export 配置必须落在合法范围内。
- 规划出的 split 必须能转换为 raw Parquet source file；否则以 `NOT_RAW_CONVERTIBLE` 拒绝并按 fallback 配置处理。

Native export 配置项：

| Spark 配置名 | 默认值 | 合法范围 / 说明 |
| --- | --- | --- |
| `spark.paimon.native-io.enabled` | `false` | native IO 总开关；native export 也依赖它。 |
| `spark.paimon.native-io.export.enabled` | `false` | 是否尝试 `export_parquet` native fast path。 |
| `spark.paimon.native-io.export.fallback.enabled` | `true` | driver preflight 不适用时是否回退 Java export。 |
| `spark.paimon.native-io.export.fail-on-fallback` | `false` | preflight 不适用时是否强制失败；压测 native 时建议打开。 |
| `spark.paimon.native-io.export.metrics.enabled` | `true` | native task 是否返回并记录 metrics。 |
| `spark.paimon.native-io.export.obs.read-buffer-size` | `8 MB` | `1 MB` 到 `128 MB`；native OBS 顺序读 buffer 大小。 |
| `spark.paimon.native-io.export.obs.read-concurrency` | `4` | `1` 到 `64`；每个 native task 的 OBS 读并发上限。 |
| `spark.paimon.native-io.export.writer.batch-size` | `8192` | `1` 到 `65536`；native writer 使用的 Arrow batch 行数。 |
| `spark.paimon.native-io.export.writer.row-group-size` | `250000` | `1024` 到 `10000000`；native writer 的 row group 行数上限。 |
| `spark.paimon.native-io.export.writer.multipart-part-size` | `64 MB` | `5 MB` 到 `512 MB`；multipart upload part 大小。 |
| `spark.paimon.native-io.export.memory-limit` | `512 MB` | `64 MB` 到 `16 GB`；单 native export task 的软内存上限。 |
| `spark.paimon.native-io.export.runtime-threads` | `4` | `1` 到 `64`；executor 进程级 native runtime worker 数。 |
| `spark.paimon.native-io.export.metadata-cache.enabled` | `true` | 是否允许 native export 使用进程级 Parquet metadata cache。 |

OBS 配置来源：

- Spark Hadoop conf：`spark.hadoop.fs.obs.endpoint`、`spark.hadoop.fs.obs.access.key`、`spark.hadoop.fs.obs.secret.key` 等。
- Paimon/Spark option：`spark.paimon.fs.obs.*` 或表 options 中的 `fs.obs.*`。
- 环境变量 fallback：`OBS_ENDPOINT`、`HUAWEICLOUD_OBS_ENDPOINT`、`OBS_ACCESS_KEY_ID`、`OBS_ACCESS_KEY`、`HUAWEICLOUD_OBS_ACCESS_KEY_ID`、`AWS_ACCESS_KEY_ID`、`OBS_SECRET_ACCESS_KEY`、`OBS_SECRET_KEY`、`HUAWEICLOUD_OBS_SECRET_ACCESS_KEY`、`AWS_SECRET_ACCESS_KEY`。
- Access key alias：`fs.obs.accessKey`、`fs.obs.ak` 会归一化为 `fs.obs.access.key`。
- Secret key alias：`fs.obs.secretKey`、`fs.obs.sk` 会归一化为 `fs.obs.secret.key`。

## 参数详细说明

### table

`table` 是要导出的 Paimon 表名，由当前 Spark catalog 解析。

```sql
CALL sys.export_parquet(
  table => 'orders',
  columns => '*',
  output_path => 's3://bucket/export/orders'
);
```

```sql
CALL sys.export_parquet(
  table => 'ods.orders',
  columns => '*',
  output_path => 's3://bucket/export/ods/orders'
);
```

如果表不存在或表标识符无法解析，调用会失败。

### columns

`columns` 控制导出的输出 schema。

支持两种形式：

| 写法 | 说明 |
| --- | --- |
| `'*'` | 导出表的全部列，顺序与 Paimon 表 schema 一致。 |
| `'id,name,dt'` | 只导出指定列，输出列顺序与参数中出现的顺序一致。 |

列名可以使用反引号包裹，适合列名包含特殊字符或关键字的场景：

```sql
CALL sys.export_parquet(
  table => 'T',
  columns => '`user`,id,`event time`',
  output_path => 's3://bucket/export/t'
);
```

注意事项：

- `columns` 不能为空。
- 指定不存在的列会失败，错误信息类似 `Cannot find column 'xxx'.`
- 重复列会被去重，并保留首次出现的位置。例如 `'id,name,id'` 等价于 `'id,name'`。
- `where` 中使用的过滤列不要求出现在 `columns` 中。procedure 会自动把过滤列加入读取列，再只把 `columns` 指定的列写出。

### output_path

`output_path` 是导出目录，不是单个 Parquet 文件路径。

导出成功后，目录结构类似：

```text
s3://bucket/export/t/
  part-0b2b7e7d-....parquet
  part-8d78cc21-....parquet
  _SUCCESS
```

输出路径行为：

- 路径末尾多余的 `/` 会被忽略，例如 `s3://bucket/export/t/` 会按 `s3://bucket/export/t` 处理。
- 如果目录不存在，会创建目录。
- 如果目录已存在且 `overwrite` 不是 `true`，调用会失败。
- 如果目录已存在且 `overwrite => true`，会先递归删除该目录，然后重新写出。
- 文件名由 `part-` 加 UUID 组成，不能通过参数指定文件名前缀。

### where

`where` 是可选过滤条件。为空或只包含空白字符时表示不过滤，导出全部行。

当前支持的条件形式如下：

| 条件类型 | 示例 |
| --- | --- |
| 等于 | `id = 10` |
| 不等于 | `id != 10`、`id <> 10` |
| 大于 / 大于等于 | `score > 80`、`score >= 80` |
| 小于 / 小于等于 | `score < 100`、`score <= 100` |
| `IN` | `dt in ('2026-05-13', '2026-05-14')` |
| `IS NULL` | `deleted_at is null` |
| `IS NOT NULL` | `name is not null` |
| 多条件 `AND` | `dt = '2026-05-14' and score >= 20` |

示例：

```sql
CALL sys.export_parquet(
  table => 'default.orders',
  columns => 'order_id,user_id,amount',
  output_path => 's3://bucket/export/orders/dt=2026-05-14',
  where => "dt = '2026-05-14' and amount >= 100"
);
```

支持的字面量类型：

| Paimon 类型 | 字面量示例 |
| --- | --- |
| `BOOLEAN` | `true`、`false` |
| `TINYINT` / `SMALLINT` / `INTEGER` / `BIGINT` | `1`、`100` |
| `FLOAT` / `DOUBLE` | `1.5`、`3.14` |
| `DECIMAL` | `123.45` |
| `CHAR` / `VARCHAR` | `'abc'`、`"abc"` |
| `DATE` | `'2026-05-14'` |
| `TIME` | `'12:30:00'` |
| `TIMESTAMP` | `'2026-05-14 12:30:00'`、`'2026-05-14T12:30:00'` |
| `TIMESTAMP WITH LOCAL TIME ZONE` | `'2026-05-14T12:30:00Z'`、`'2026-05-14T12:30:00+08:00'`、`'2026-05-14 12:30:00'` |

限制：

- 只支持 `AND` 连接，不支持 `OR`。
- 不支持 `LIKE`、`BETWEEN`、`NOT IN`、函数表达式、算术表达式、子查询。
- 不支持复杂类型字面量，例如 `ARRAY`、`MAP`、`ROW`。
- 条件左侧必须是字段名，不能是表达式。
- 可以用反引号引用字段名，例如 `` `event time` >= '2026-05-14 00:00:00' ``。
- 字符串字面量支持单引号或双引号；若需要在单引号字符串中表示单引号，可以使用两个单引号。

### parallelism

`parallelism` 是 Spark 任务并行度上限，不一定等于最终 task 数。

实际分区数计算规则：

- 基础上限是 `min(parallelism, Paimon split 数量)`，最小为 1。
- 如果没有设置 `target_file_size`，通常每个 Spark task 处理一个或多个 split，每个有输出数据的 split 最多写出一个 Parquet 文件。
- 如果设置了 `target_file_size`，procedure 会根据 Paimon split 的总文件大小估算需要的分区数：`ceil(totalSplitSize / targetFileSize)`，再与基础上限取较小值。
- 如果传入 `parallelism <= 0`，会按 1 处理。

建议：

- 小表或 split 数很少时，调大 `parallelism` 不会继续增加并发。
- 大表导出到对象存储时，可以根据 executor 资源、对象存储写吞吐和目标文件大小综合设置。
- 若希望控制最终文件大小，优先设置 `target_file_size`，不要只依赖 `parallelism`。

### compression

`compression` 控制 Parquet 文件压缩方式，默认是 `zstd`。

示例：

```sql
CALL sys.export_parquet(
  table => 'default.T',
  columns => '*',
  output_path => 's3://bucket/export/t',
  compression => 'snappy'
);
```

该值会直接传给 Parquet writer。可用 codec 取决于当前 Parquet 依赖和运行环境，常见值包括 `zstd`、`snappy`、`gzip`、`uncompressed`。如果传入的 codec 不被支持，写文件阶段会失败。

### overwrite

`overwrite` 控制输出目录已存在时的行为。

| 值 | 行为 |
| --- | --- |
| 不填或 `false` | 如果 `output_path` 已存在，直接失败，避免误覆盖已有数据。 |
| `true` | 如果 `output_path` 已存在，先删除整个目录，再重新写出。 |

示例：

```sql
CALL sys.export_parquet(
  table => 'default.T',
  columns => '*',
  output_path => 's3://bucket/export/t',
  overwrite => true
);
```

注意：`overwrite => true` 会删除整个输出目录。不要把 `output_path` 指向仍在使用的业务目录。

### target_file_size

`target_file_size` 用于控制导出文件滚动，格式由 Paimon `MemorySize` 解析。

支持的单位包括：

| 单位 | 含义 |
| --- | --- |
| `b`、`bytes` | 字节 |
| `k`、`kb`、`kibibytes` | 1024 字节 |
| `m`、`mb`、`mebibytes` | 1024 KB |
| `g`、`gb`、`gibibytes` | 1024 MB |
| `t`、`tb`、`tebibytes` | 1024 GB |

示例：

```sql
CALL sys.export_parquet(
  table => 'default.orders',
  columns => '*',
  output_path => 's3://bucket/export/orders',
  target_file_size => '256 MB',
  parallelism => 64,
  overwrite => true
);
```

行为说明：

- 不设置 `target_file_size` 时，导出按 Paimon split 写文件。有输出数据的 split 会生成一个 Parquet 文件。
- 设置后，同一个 Spark partition 内会使用滚动 writer，当当前 Parquet writer 达到目标大小后关闭，并打开新的 `part-*.parquet`。
- `target_file_size` 必须大于 0，否则会失败。
- 该参数是目标值，不保证每个文件都精确等于该大小。实际大小会受 row group、压缩率、单行大小、split 分布等影响。

## 常用示例

### 导出全表全部列

```sql
CALL sys.export_parquet(
  table => 'default.orders',
  columns => '*',
  output_path => 's3://bucket/export/orders',
  overwrite => true
);
```

### 只导出部分列

```sql
CALL sys.export_parquet(
  table => 'default.orders',
  columns => 'order_id,user_id,amount,dt',
  output_path => 's3://bucket/export/orders_columns',
  overwrite => true
);
```

### 按分区和普通列过滤

```sql
CALL sys.export_parquet(
  table => 'default.orders',
  columns => 'order_id,user_id,amount',
  output_path => 's3://bucket/export/orders_dt_20260514',
  where => "dt = '2026-05-14' and amount >= 100",
  overwrite => true
);
```

### 使用 IN 过滤多天数据

```sql
CALL sys.export_parquet(
  table => 'default.orders',
  columns => '*',
  output_path => 's3://bucket/export/orders_2days',
  where => "dt in ('2026-05-13', '2026-05-14')",
  parallelism => 64,
  overwrite => true
);
```

### 控制压缩格式和文件大小

```sql
CALL sys.export_parquet(
  table => 'default.orders',
  columns => '*',
  output_path => 's3://bucket/export/orders_sized',
  compression => 'zstd',
  target_file_size => '512 MB',
  parallelism => 128,
  overwrite => true
);
```

## 执行流程

`export_parquet` 的核心流程如下：

1. 解析 `table` 并加载 Paimon 表。
2. 根据 `columns` 生成输出列投影。
3. 根据 `where` 生成 Paimon predicate。
4. 读取列会自动包含输出列和过滤列。
5. 使用 Paimon scan 规划 splits。
6. 准备输出目录，必要时按 `overwrite` 删除旧目录。
7. Spark 根据 split 和参数启动导出任务。
8. 每个任务读取 Paimon split，应用过滤条件，写出 Parquet 文件。
9. driver 汇总写出行数，并创建 `_SUCCESS` 文件。

## 输出文件与文件数

不设置 `target_file_size` 时：

- 每个有数据输出的 Paimon split 最多生成一个 Parquet 文件。
- 如果某个 split 的所有行都被 `where` 过滤掉，该 split 不会创建空 Parquet 文件。

设置 `target_file_size` 时：

- 每个 Spark partition 使用一个 rolling writer。
- writer 达到目标大小后滚动生成新文件。
- 文件数量主要由数据量、压缩率、目标大小和实际 Spark partition 数决定。

## 常见错误

| 错误现象 | 常见原因 | 处理方式 |
| --- | --- | --- |
| `Columns should not be empty.` | `columns` 为空字符串或空白字符串 | 传入 `'*'` 或明确列列表。 |
| `Cannot find column 'xxx'.` | `columns` 中存在表里没有的列 | 检查列名和大小写。 |
| `Cannot find filter column 'xxx'.` | `where` 中存在表里没有的字段 | 检查过滤字段名。 |
| `Output path already exists...` | 输出目录已存在且未设置 `overwrite => true` | 换新目录，或确认可以覆盖后设置 `overwrite => true`。 |
| `Unsupported predicate condition` | `where` 使用了不支持的表达式 | 改写为简单条件，并只用 `AND` 连接。 |
| `Unsupported filter literal type` | `where` 对复杂类型或暂不支持类型做过滤 | 改为支持的基础类型字段过滤。 |
| `Invalid IN predicate` | `IN` 后面不是括号包裹的列表 | 使用 `col in ('a', 'b')` 格式。 |
| `Target file size should be larger than 0 bytes.` | `target_file_size` 解析后小于等于 0 | 使用 `'128 MB'`、`'512 MB'` 等正数大小。 |
| `Native export is enabled but not applicable. reason=NO_PROVIDER` | 开启了 native export，但 classpath 中没有 `paimon-native-io` provider | 补齐 `paimon-native-io` jar，或关闭 `spark.paimon.native-io.export.enabled` 使用 Java 路径。 |
| `Native export is enabled but not applicable. reason=NOT_RAW_CONVERTIBLE` | 规划出的 split 不能转换为 native 可直接读取的 raw Parquet source file | 检查表文件格式、split 类型和表特性；必要时关闭 native export 或允许 fallback。 |
| `Native export is enabled but not applicable. reason=EXPORT_UNSUPPORTED_COMPRESSION` | native export 当前只接受 `zstd` | 使用 `compression => 'zstd'`，或关闭 native export。 |
| `Native export is enabled but not applicable. reason=NON_OBS_PATH` | native export 当前只接受 `obs://` 输出路径 | 改用 `obs://` 输出路径，或关闭 native export。 |
| `Native export is enabled but not applicable. reason=MISSING_OBS_CONFIG` | native export 缺少 OBS endpoint、access key 或 secret key | 补齐 `fs.obs.*` 配置或相关环境变量。 |
| `Native export is enabled but not applicable. reason=EXPORT_INVALID_CONFIG` | native export 参数超出合法范围 | 对照 native 配置表修正参数。 |

## 使用建议

- 导出到对象存储时，建议设置 `target_file_size`，避免生成过多小文件。
- 当前生产导出建议关闭 `spark.paimon.native-io.export.enabled`，保持 Java export 路径；native export 适合开发联调和后续压测验证。
- 首次导出建议使用新目录；只有确认目录可删除时才使用 `overwrite => true`。
- `where` 尽量包含分区列或高选择性字段，便于 Paimon scan 和 reader 减少读取量。
- `parallelism` 应结合 Paimon split 数和 Spark executor 资源设置。单纯调大该值不一定能提升速度。
- 如果需要复杂 SQL 逻辑、表达式计算、join 或聚合，建议先用 Spark SQL 写临时表或 DataFrame，再另行导出；`export_parquet` 更适合直接导出 Paimon 表的行列子集。
