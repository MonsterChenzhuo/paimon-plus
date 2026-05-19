# Paimon Native IO Correctness Benchmark Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Prove Native IO correctness against the Java path and add a Spark procedure that runs repeatable baseline/native OBS benchmarks with applicability reporting and charts.

**Architecture:** Correctness tests run the same fixtures twice with `spark.paimon.native-io.enabled=false/true` and compare canonical multiset hashes. The benchmark procedure creates or validates PK+DV Parquet OBS datasets, runs baseline/native sequentially with cache isolation, collects scan signatures and native applicability from the real read path, writes JSON/CSV outputs, and generates two bar charts.

**Tech Stack:** Spark 3.4.4, Paimon Spark procedures, JUnit 5/ScalaTest, Spark listeners, Jackson/CSV, Java AWT or a lightweight chart utility already present in the project.

---

### File Structure

- Create pure helper tests under `paimon-spark/paimon-spark-common/src/test/java/org/apache/paimon/spark/nativeio/`
- Create Spark SQL end-to-end correctness tests in the existing Spark SQL test module/style after inspecting current bases, likely `paimon-spark/paimon-spark-ut` or `paimon-spark/paimon-spark-3.4`
- Create: `paimon-spark/paimon-spark-common/src/main/java/org/apache/paimon/spark/procedure/NativeIOBenchmarkProcedure.java`
- Create: `paimon-spark/paimon-spark-common/src/main/java/org/apache/paimon/spark/nativeio/NativeIOBenchmarkRunner.java`
- Create: `paimon-spark/paimon-spark-common/src/main/java/org/apache/paimon/spark/nativeio/NativeIOBenchmarkReport.java`
- Create: `paimon-spark/paimon-spark-common/src/main/java/org/apache/paimon/spark/nativeio/NativeIOPlanSignature.java`
- Create: `paimon-spark/paimon-spark-common/src/main/java/org/apache/paimon/spark/nativeio/NativeIOMultisetHash.java`
- Create: `paimon-spark/paimon-spark-common/src/main/java/org/apache/paimon/spark/nativeio/NativeIOChartWriter.java`
- Modify: `paimon-spark/paimon-spark-common/src/main/java/org/apache/paimon/spark/SparkProcedures.java` and related procedure catalog paths to expose `sys.native_io_benchmark`.
- Test: `NativeIOPlanSignatureTest.java`, `NativeIOMultisetHashTest.java`, `NativeIOBenchmarkProcedureTest.java`.

### Execution Notes

- Start commands with `PAIMON_ROOT=/Users/opay-20240095/IdeaProjects/nativeio/paimon`; use `$PAIMON_ROOT` in executable shell snippets.
- Add `-am` to Maven commands when running a module that depends on changed upstream modules.
- Spark 3.4.x is the PoC acceptance baseline. Spark 3.5/4.0 checks are compatibility smoke tests and must be labeled as non-baseline.
- Use Paimon `FileIO` for writing `result_path` artifacts to `obs://...`; do not use local `java.io.File` APIs except for temporary chart files that are uploaded and then deleted.
- Exclude `target/` from source discovery.

### Task 1: Canonical Correctness Hash

**Files:**
- Create: `NativeIOMultisetHash.java`
- Test: `NativeIOMultisetHashTest.java`

- [ ] **Step 1: Write hash tests**

```java
@Test
void preservesDuplicateCounts() {
    NativeIOMultisetHash left = NativeIOMultisetHash.create();
    left.add(row("a", 1));
    left.add(row("a", 1));
    NativeIOMultisetHash right = NativeIOMultisetHash.create();
    right.add(row("a", 1));
    assertThat(left.finish()).isNotEqualTo(right.finish());
}
```

Also cover decimal scale, timestamp NTZ micros, binary bytes, `NaN`, and `+0.0/-0.0`.

- [ ] **Step 2: Run test to verify failure**

Run: `cd $PAIMON_ROOT && mvn -pl paimon-spark/paimon-spark-common -am -Dtest=NativeIOMultisetHashTest test`

Expected: FAIL because hash helper does not exist.

- [ ] **Step 3: Implement hash helper**

Encode each row into canonical bytes:

```text
type tag + null marker + value bytes
decimal = unscaled value bytes + scale
timestamp_ntz = epoch micros or exact Paimon timestamp fields
binary = raw bytes
string = UTF-8 bytes
float/double = normalized NaN and signed zero handling
```

Store `(rowHash, occurrenceCount, secondaryHash)` buckets so duplicate counts cannot be lost.

- [ ] **Step 4: Verify hash tests**

Run: `cd $PAIMON_ROOT && mvn -pl paimon-spark/paimon-spark-common -am -Dtest=NativeIOMultisetHashTest test`

Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add paimon-spark/paimon-spark-common/src/main/java/org/apache/paimon/spark/nativeio/NativeIOMultisetHash.java paimon-spark/paimon-spark-common/src/test/java/org/apache/paimon/spark/nativeio/NativeIOMultisetHashTest.java
git commit -m "test(native-io): add canonical multiset hash"
```

### Task 2: Correctness Test Suite

**Files:**
- Create: Spark SQL correctness suite in the existing Spark SQL test module selected after inspecting the current Spark test bases.
- Create: pure helper tests in `paimon-spark/paimon-spark-common/src/test/java/org/apache/paimon/spark/nativeio/`

- [ ] **Step 1: Select the correct Spark test module**

Inspect existing Spark SQL test bases before creating the suite:

```bash
cd $PAIMON_ROOT
find paimon-spark -path '*/target/*' -prune -o -type f -name '*Test.scala' -print | grep '/spark/sql/' | head -40
find paimon-spark -path '*/target/*' -prune -o -type f -name '*ITCase.scala' -print | head -40
```

Expected: choose the same module/style used by existing Spark SQL end-to-end tests, not an arbitrary Java test under `paimon-spark-common`.

- [ ] **Step 2: Write parameterized native on/off harness**

For each query, run:

```java
spark.conf().set("spark.paimon.native-io.enabled", enabled);
Dataset<Row> result = spark.sql(query);
NativeIOMultisetHash hash = collectOrAggregateHash(result);
```

Compare Java and native hashes only after confirming the physical plan creates a real Paimon scan reader for native-applicable queries.

- [ ] **Step 3: Add required fixtures**

Fixtures must include:

```text
DV delete/update table
projection query
empty projection raw read through core/read layer
0-row Parquet
partition pruning
bitmap index fallback
schema cast fallback
data filter/topN/limit fallback
non-OBS path fallback
timestamp precision <= 6
unsupported nested type fallback
metadata file path and row index columns
split with mixed native/Java files preserving order
```

- [ ] **Step 4: Verify suite without native profile**

Run: `cd $PAIMON_ROOT && mvn -pl paimon-spark/paimon-spark-3.4 -am -Dtest=NativeIOCorrectnessTestSuite test`

Expected: PASS for fallback-only and guard tests; native smoke cases are skipped when native module/resource is unavailable.

- [ ] **Step 5: Verify suite with native profile**

Run: `cd $PAIMON_ROOT && mvn -pl paimon-spark/paimon-spark-3.4,paimon-native-io -am -Pnative-io -Dtest=NativeIOCorrectnessTestSuite test`

Expected: PASS; native-applicable fixtures compare equal to Java path.

- [ ] **Step 6: Commit**

```bash
git add paimon-spark/paimon-spark-common/src/test/java/org/apache/paimon/spark/nativeio/NativeIOCorrectnessTestSuite.java
git commit -m "test(native-io): add spark correctness suite"
```

### Task 3: Plan Signature

**Files:**
- Create: `NativeIOPlanSignature.java`
- Test: `NativeIOPlanSignatureTest.java`

- [ ] **Step 1: Write signature tests**

Two plans that only differ by expression id/codegen id must produce the same signature. Plans with different output schema, pushed filters, partition filters, file count, metadata-only flag, or `PaimonLocalScan` status must differ.

- [ ] **Step 2: Implement signature extraction**

Extract:

```text
scan node class/name
table identifier
output field name/type/nullability
pushed data filters
partition filters
bucket/partition count
input file count and bytes
metadata-only flag
PaimonLocalScan flag
PaimonPartitionReader creation flag
```

Keep raw `executedPlan` text for diagnosis, but never use it for equality.

- [ ] **Step 3: Verify signature tests**

Run: `cd /Users/opay-20240095/IdeaProjects/nativeio/paimon && mvn -pl paimon-spark/paimon-spark-common -Dtest=NativeIOPlanSignatureTest test`

Expected: PASS.

- [ ] **Step 4: Commit**

```bash
git add paimon-spark/paimon-spark-common/src/main/java/org/apache/paimon/spark/nativeio/NativeIOPlanSignature.java paimon-spark/paimon-spark-common/src/test/java/org/apache/paimon/spark/nativeio/NativeIOPlanSignatureTest.java
git commit -m "feat(native-io): add stable spark scan signature"
```

### Task 4: Applicability Reporter Aggregation

**Files:**
- Modify: `paimon-core/src/main/java/org/apache/paimon/operation/nativeio/NativeApplicabilityReporter.java`
- Create Spark-side reporter integration under `paimon-spark/paimon-spark-common/src/main/java/org/apache/paimon/spark/nativeio/`

- [ ] **Step 1: Write reporter tests**

Mock task retry, speculative duplicate success, skipped file, Java fallback file, and native file. Assert only the successful accepted attempt contributes to final totals.

- [ ] **Step 2: Implement task-local buffering**

Executor-side reporter records split/file events in task-local buffer. Flush only on successful task completion. Include `executionId`, `stageId`, `partitionId`, `taskAttemptId`, and `attemptNumber`.

- [ ] **Step 3: Implement driver aggregation**

Aggregate:

```text
native_files/native_bytes
java_fallback_files/java_bytes
skipped_files/skipped_bytes
reason counts
retry/speculation markers
```

Do not record OBS AK/SK/token or unredacted options in details.

- [ ] **Step 4: Verify reporter tests**

Run: `cd /Users/opay-20240095/IdeaProjects/nativeio/paimon && mvn -pl paimon-spark/paimon-spark-common,paimon-core -Dtest='*NativeApplicability*Test' test`

Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add paimon-core/src/main/java/org/apache/paimon/operation/nativeio/NativeApplicabilityReporter.java paimon-spark/paimon-spark-common/src/main/java/org/apache/paimon/spark/nativeio paimon-spark/paimon-spark-common/src/test/java/org/apache/paimon/spark/nativeio
git commit -m "feat(native-io): aggregate native applicability for benchmark"
```

### Task 5: Benchmark Procedure API

**Files:**
- Create: `NativeIOBenchmarkProcedure.java`
- Create: `NativeIOBenchmarkRunner.java`
- Create: `NativeIOBenchmarkReport.java`
- Modify: procedure registration.
- Test: `NativeIOBenchmarkProcedureTest.java`

- [ ] **Step 1: Write procedure parser tests**

Validate arguments:

```sql
mode => synthetic|existing_table|tpcds|ssb
scale => non-empty
warehouse => obs://...
result_path => obs://...
table_name => non-empty
query_set => default|scan_only|projection|filter|custom
run_order => baseline_first|native_first|alternate
repeat >= 1
warmup >= 0
0 <= dv_delete_ratio <= 1
output_chart => true|false
```

- [ ] **Step 2: Implement procedure registration**

Follow existing `SparkProcedures`, `ProcedureCatalog`, `ProcedureBuilder`, `ProcedureParameter`, and `BaseProcedure` patterns. Expose:

```sql
CALL sys.native_io_benchmark(
  mode => 'synthetic',
  scale => '1tb',
  warehouse => 'obs://bucket/bench/warehouse',
  result_path => 'obs://bucket/bench/result/native-io/run_id',
  table_name => 'native_io_bench_pk_dv',
  query_set => 'default',
  run_order => 'baseline_first',
  repeat => 3,
  warmup => 1,
  dv_delete_ratio => 0.05,
  target_file_size => '512mb',
  output_chart => true
)
```

- [ ] **Step 3: Implement native jar deployment check**

Before benchmark, run a driver check and a Spark executor job that records executor id, host, native availability, native resource hash, runtime worker threads, and error. Fail fast if any executor lacks native support or hashes differ.

If Spark dynamic allocation is enabled, record executor set before and after every query repeat. Re-run native loader checks for newly observed executors or mark the benchmark `failed`.

- [ ] **Step 4: Verify parser and deployment tests**

Run: `cd $PAIMON_ROOT && mvn -pl paimon-spark/paimon-spark-common -am -Dtest=NativeIOBenchmarkProcedureTest test`

Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add paimon-spark/paimon-spark-common/src/main/java/org/apache/paimon/spark/procedure/NativeIOBenchmarkProcedure.java paimon-spark/paimon-spark-common/src/main/java/org/apache/paimon/spark/nativeio/NativeIOBenchmark*.java paimon-spark/paimon-spark-common/src/test/java/org/apache/paimon/spark/nativeio/NativeIOBenchmarkProcedureTest.java
git commit -m "feat(native-io): add benchmark procedure shell"
```

### Task 6: Synthetic PK+DV Dataset Builder

**Files:**
- Modify: `NativeIOBenchmarkRunner.java`

- [ ] **Step 1: Write dataset builder tests**

In local Spark test mode, create a small PK table with DV enabled and assert table options include:

```text
file.format=parquet
deletion-vectors.enabled=true
target-file-size=<requested>
```

After DELETE/UPDATE, assert at least one split is `rawConvertible=true` and DV metadata exists.

- [ ] **Step 2: Implement synthetic generator**

Use Spark `range(numRows)` to create columns `id`, `k1`, `k2`, `k3`, `dim1`, `dim2`, `metric1`, `metric2`, `event_date`, and `ts`. Write into a Paimon primary-key table on OBS. Execute delete/update based on `dv_delete_ratio`. Optionally compact when `compact_before_benchmark=true`.

For phase 1, explicitly reject `mode=tpcds` and `mode=ssb` with `UNSUPPORTED_MODE`, or add full derived PK+DV conversion tasks before enabling them. Reports must not claim standard TPC-DS/SSB comparability unless conversion is implemented and labeled as derived.

- [ ] **Step 3: Verify builder tests**

Run: `cd $PAIMON_ROOT && mvn -pl paimon-spark/paimon-spark-common -am -Dtest=NativeIOBenchmarkDatasetTest test`

Expected: PASS.

- [ ] **Step 4: Commit**

```bash
git add paimon-spark/paimon-spark-common/src/main/java/org/apache/paimon/spark/nativeio/NativeIOBenchmarkRunner.java paimon-spark/paimon-spark-common/src/test/java/org/apache/paimon/spark/nativeio
git commit -m "feat(native-io): generate benchmark pk dv dataset"
```

### Task 7: Sequential Baseline/Native Runner

**Files:**
- Modify: `NativeIOBenchmarkRunner.java`
- Modify: `NativeIOBenchmarkReport.java`

- [ ] **Step 1: Implement query set**

Default queries:

```text
scan_count
scan_count_no_agg_pushdown
projection_3_cols
projection_wide
filter_on_k1
group_by_dim
```

Mark `filter_on_k1` native-not-applicable in phase 1 when pushed data filters are present. Detect aggregate pushdown for `scan_count`.

- [ ] **Step 2: Implement run order and cache isolation**

Support `baseline_first`, `native_first`, and `alternate`. Between runs call Spark cache clearing APIs, unpersist procedure-created DataFrames, reset applicability reporter, and start a new SQL execution.

If Spark runtime version is not Spark 3.4.x, mark the run as `non_baseline_spark_version` in `env.json` and exclude it from PoC acceptance summaries unless explicitly requested.

- [ ] **Step 3: Implement correctness gate**

For each query, run Java and native correctness hash before recording speedup. Queries with mismatch get `correctness_failed` and are excluded from speedup summary.

- [ ] **Step 4: Implement status classification**

Use stable statuses: `ok`, `correctness_failed`, `native_not_applicable`, `native_not_applicable_file_level`, `partial_native`, `aggregate_pushdown_not_native_scan`, `not_native_scan_plan`, `plan_not_comparable`, `unstable`, `failed`.

- [ ] **Step 5: Verify runner tests**

Run: `cd $PAIMON_ROOT && mvn -pl paimon-spark/paimon-spark-common -am -Dtest=NativeIOBenchmarkRunnerTest test`

Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add paimon-spark/paimon-spark-common/src/main/java/org/apache/paimon/spark/nativeio/NativeIOBenchmarkRunner.java paimon-spark/paimon-spark-common/src/main/java/org/apache/paimon/spark/nativeio/NativeIOBenchmarkReport.java paimon-spark/paimon-spark-common/src/test/java/org/apache/paimon/spark/nativeio
git commit -m "feat(native-io): run sequential baseline native benchmark"
```

### Task 8: Report Writers and Charts

**Files:**
- Create: `NativeIOChartWriter.java`
- Modify: `NativeIOBenchmarkReport.java`

- [ ] **Step 1: Write report serialization tests**

Assert report writes:

```text
env.json
dataset.json
native_applicability.json
result.json
result.csv
summary.json
latency_bar.png
speedup_bar.png
```

Assert env/report output does not contain fake AK/SK/token strings.

Assert reports include `comparison_scope = "paimon_java_reader_vs_paimon_native_io"` and generated summary text does not mention LakeSoul, Comet, ClickHouse, Doris, or Spark native Parquet as benchmark competitors.

- [ ] **Step 2: Implement JSON and CSV**

`env.json` includes Spark conf snapshot, Spark runtime version, Scala binary version, executor/core/memory, shuffle partitions, OBS endpoint redacted, Arrow version/allocator manager, native max batch bytes, Paimon/native Maven artifact coordinates, native resource hash, Cargo lock hash or cargo metadata summary, runtime worker count, and run id. `summary.json` includes median/min/max/stddev/CV and geometric mean speedup for eligible full-native queries.

Write `env.json`, `dataset.json`, `native_applicability.json`, `result.json`, `result.csv`, `summary.json`, and PNG files through Paimon `FileIO` resolved from `result_path`. Temporary local chart files must be deleted after upload.

- [ ] **Step 3: Implement charts**

White background, grid lines, blue baseline, orange native IO. Titles include dataset, scale, DV ratio, file size, executor config, and run id. `speedup_bar.png` uses speedup = baseline median / native median.

- [ ] **Step 4: Verify report tests**

Run: `cd $PAIMON_ROOT && mvn -pl paimon-spark/paimon-spark-common -am -Dtest=NativeIOBenchmarkReportTest test`

Expected: PASS and generated PNG files are non-empty.

- [ ] **Step 5: Commit**

```bash
git add paimon-spark/paimon-spark-common/src/main/java/org/apache/paimon/spark/nativeio/NativeIOChartWriter.java paimon-spark/paimon-spark-common/src/main/java/org/apache/paimon/spark/nativeio/NativeIOBenchmarkReport.java paimon-spark/paimon-spark-common/src/test/java/org/apache/paimon/spark/nativeio
git commit -m "feat(native-io): write benchmark reports and charts"
```

### Final Verification

- [ ] **Step 1: Run Spark correctness tests**

Run: `cd $PAIMON_ROOT && mvn -pl paimon-spark/paimon-spark-3.4,paimon-spark/paimon-spark-common,paimon-native-io -am -Pnative-io -Dtest='NativeIOCorrectnessTestSuite,*NativeIOBenchmark*Test,*NativeIOPlanSignatureTest,*NativeIOMultisetHashTest' test`

Expected: PASS.

- [ ] **Step 2: Run a local tiny benchmark smoke**

Run:

```sql
CALL sys.native_io_benchmark(
  mode => 'synthetic',
  scale => '1000',
  warehouse => 'obs://bucket/bench/warehouse',
  result_path => 'obs://bucket/bench/result/native-io/smoke',
  table_name => 'native_io_bench_pk_dv_smoke',
  query_set => 'scan_only',
  run_order => 'alternate',
  repeat => 1,
  warmup => 0,
  dv_delete_ratio => 0.05,
  target_file_size => '128mb',
  output_chart => true
)
```

Expected: `summary.json` reports `ok` or clearly classified non-applicable statuses; no query with failed correctness is included in speedup.

- [ ] **Step 3: Commit verification fixes**

```bash
git add paimon-spark/paimon-spark-common
git commit -m "test(native-io): verify correctness and benchmark procedure"
```

### Self-Review Checklist

- [ ] Correctness comparison is order-insensitive and preserves duplicate counts.
- [ ] `count(*)` aggregate pushdown is detected and excluded from native scan speedup.
- [ ] Benchmark uses real read-path applicability reporter output, not a duplicate predictor.
- [ ] Partial native and zero file-level native hits are excluded from default geometric mean speedup.
- [ ] Reports and errors are scanned for raw AK/SK/session token before writing.
- [ ] Spark 3.4.x is marked as the baseline runtime; non-3.4 runs are labeled non-baseline.
- [ ] Benchmark artifacts are written through Paimon `FileIO`, not local file APIs.
- [ ] Generated reports enforce `comparison_scope = "paimon_java_reader_vs_paimon_native_io"`.
