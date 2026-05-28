/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.paimon.spark.procedure;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.catalog.CatalogContext;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.Path;
import org.apache.paimon.spark.SparkCatalog;
import org.apache.paimon.spark.SparkTable;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.Table;
import org.apache.paimon.table.source.DataSplit;
import org.apache.paimon.table.source.Split;
import org.apache.paimon.utils.JsonSerdeUtil;
import org.apache.paimon.utils.Preconditions;
import org.apache.paimon.utils.StringUtils;

import org.apache.spark.SparkEnv;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.connector.catalog.CatalogPlugin;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.connector.catalog.TableCatalog;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.unsafe.types.UTF8String;

import java.io.IOException;
import java.io.Serializable;
import java.lang.reflect.Method;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import scala.Option;

import static org.apache.spark.sql.types.DataTypes.BooleanType;
import static org.apache.spark.sql.types.DataTypes.DoubleType;
import static org.apache.spark.sql.types.DataTypes.IntegerType;
import static org.apache.spark.sql.types.DataTypes.LongType;
import static org.apache.spark.sql.types.DataTypes.StringType;

/**
 * Creates a synthetic PK+DV table, benchmarks Paimon Java and native-IO read paths, and writes
 * JSON/CSV reports.
 */
public class NativeIOBenchmarkProcedure extends BaseProcedure {

    private static final String NATIVE_IO_ENABLED_CONF = "spark.paimon.native-io.enabled";
    private static final long DEFAULT_ROWS = 10_000_000L;
    private static final int DEFAULT_REPEAT = 3;
    private static final int DEFAULT_WARMUP = 1;
    private static final double DEFAULT_DV_DELETE_RATIO = 0.10D;
    private static final String DEFAULT_TARGET_FILE_SIZE = "128 mb";
    private static final int DEFAULT_BUCKET = 4;
    private static final int DELETE_MODULUS = 10_000;
    private static final String MEASURE_MODE_NOOP = "noop";
    private static final String MEASURE_MODE_ROW_DIGEST = "row_digest";
    private static final String DEFAULT_MEASURE_MODE = MEASURE_MODE_NOOP;
    private static final long ROW_COUNT_UNAVAILABLE = -1L;
    private static final String EXCLUDED_RULES_CONF = "spark.sql.optimizer.excludedRules";

    private static final ProcedureParameter[] PARAMETERS =
            new ProcedureParameter[] {
                ProcedureParameter.required("warehouse", StringType),
                ProcedureParameter.required("result_path", StringType),
                ProcedureParameter.required("table_name", StringType),
                ProcedureParameter.optional("rows", LongType),
                ProcedureParameter.optional("repeat", IntegerType),
                ProcedureParameter.optional("warmup", IntegerType),
                ProcedureParameter.optional("dv_delete_ratio", DoubleType),
                ProcedureParameter.optional("target_file_size", StringType),
                ProcedureParameter.optional("fail_if_native_unavailable", BooleanType),
                ProcedureParameter.optional("overwrite", BooleanType),
                ProcedureParameter.optional("bucket", IntegerType),
                ProcedureParameter.optional("measure_mode", StringType)
            };

    private static final StructType OUTPUT_TYPE =
            new StructType(
                    new StructField[] {
                        new StructField("result", DataTypes.BooleanType, false, Metadata.empty()),
                        new StructField("result_path", StringType, false, Metadata.empty()),
                        new StructField("status", StringType, false, Metadata.empty())
                    });

    protected NativeIOBenchmarkProcedure(TableCatalog tableCatalog) {
        super(tableCatalog);
    }

    @Override
    public ProcedureParameter[] parameters() {
        return PARAMETERS;
    }

    @Override
    public StructType outputType() {
        return OUTPUT_TYPE;
    }

    @Override
    public InternalRow[] call(InternalRow args) {
        Config config = parseConfig(args);
        Option<String> previousNativeIOEnabled = spark().conf().getOption(NATIVE_IO_ENABLED_CONF);
        try {
            BenchmarkReport report = runBenchmark(config);
            writeReports(report);
            return new InternalRow[] {
                newInternalRow(
                        true, UTF8String.fromString(config.resultPath), UTF8String.fromString("ok"))
            };
        } catch (Exception e) {
            throw new RuntimeException("Failed to run native IO benchmark", e);
        } finally {
            restoreConf(NATIVE_IO_ENABLED_CONF, previousNativeIOEnabled);
        }
    }

    private Config parseConfig(InternalRow args) {
        String warehouse = args.getString(0);
        String resultPath = args.getString(1);
        String tableName = args.getString(2);
        long rows = args.isNullAt(3) ? DEFAULT_ROWS : args.getLong(3);
        int repeat = args.isNullAt(4) ? DEFAULT_REPEAT : args.getInt(4);
        int warmup = args.isNullAt(5) ? DEFAULT_WARMUP : args.getInt(5);
        double dvDeleteRatio = args.isNullAt(6) ? DEFAULT_DV_DELETE_RATIO : args.getDouble(6);
        String targetFileSize = args.isNullAt(7) ? DEFAULT_TARGET_FILE_SIZE : args.getString(7);
        boolean failIfNativeUnavailable = args.isNullAt(8) || args.getBoolean(8);
        boolean overwrite = args.isNullAt(9) || args.getBoolean(9);
        int bucket = args.isNullAt(10) ? DEFAULT_BUCKET : args.getInt(10);
        String measureMode =
                args.isNullAt(11)
                        ? DEFAULT_MEASURE_MODE
                        : normalizeMeasureMode(args.getString(11));

        Preconditions.checkArgument(
                !StringUtils.isNullOrWhitespaceOnly(warehouse), "warehouse should not be empty.");
        Preconditions.checkArgument(
                !StringUtils.isNullOrWhitespaceOnly(resultPath),
                "result_path should not be empty.");
        Preconditions.checkArgument(
                !StringUtils.isNullOrWhitespaceOnly(tableName), "table_name should not be empty.");
        Preconditions.checkArgument(rows > 0, "rows should be greater than 0.");
        Preconditions.checkArgument(repeat > 0, "repeat should be greater than 0.");
        Preconditions.checkArgument(warmup >= 0, "warmup should be greater than or equal to 0.");
        Preconditions.checkArgument(
                dvDeleteRatio >= 0D && dvDeleteRatio < 1D, "dv_delete_ratio should be in [0, 1).");
        Preconditions.checkArgument(
                !StringUtils.isNullOrWhitespaceOnly(targetFileSize),
                "target_file_size should not be empty.");
        Preconditions.checkArgument(bucket > 0, "bucket should be greater than 0.");
        Preconditions.checkArgument(
                MEASURE_MODE_NOOP.equals(measureMode)
                        || MEASURE_MODE_ROW_DIGEST.equals(measureMode),
                "measure_mode should be 'noop' or 'row_digest'.");

        Identifier identifier = toIdentifier(tableName, PARAMETERS[2].name());
        validatePathParts(identifier);
        Path tablePath = tablePath(warehouse, identifier);
        String catalogName = benchmarkCatalogName(tableName, resultPath);
        String runId = runId(tableName, resultPath);

        return new Config(
                trimTrailingSlash(warehouse),
                trimTrailingSlash(resultPath),
                tableName,
                runId,
                catalogName,
                identifier,
                tablePath,
                rows,
                repeat,
                warmup,
                dvDeleteRatio,
                targetFileSize,
                failIfNativeUnavailable,
                overwrite,
                bucket,
                measureMode);
    }

    private BenchmarkReport runBenchmark(Config config) throws Exception {
        NativeCheck driverNative = checkNativeAvailability();
        List<NativeCheck> executorNative = checkExecutorNativeAvailability();
        if (config.failIfNativeUnavailable && !nativeAvailable(driverNative, executorNative)) {
            throw new IllegalStateException(
                    "Native IO is not available on driver or executors. "
                            + "Set fail_if_native_unavailable => false to run a fallback smoke "
                            + "benchmark.");
        }

        prepareSyntheticTable(config);

        SparkTable sparkTable = loadBenchmarkTable(config);
        Table table = sparkTable.getTable();
        List<QuerySpec> queries = benchmarkQueries(config);
        List<RunResult> results = new ArrayList<>();

        for (QuerySpec query : queries) {
            results.addAll(runQueryMode("java", false, query, config));
            results.addAll(runQueryMode("native", true, query, config));
        }

        List<Summary> summaries = summarize(queries, results);
        return new BenchmarkReport(config, table, driverNative, executorNative, results, summaries);
    }

    private void prepareSyntheticTable(Config config) {
        configureBenchmarkCatalog(config);
        String tableSql = sqlIdentifier(config.catalogName, config.identifier);
        createNamespaceIfNeeded(config.catalogName, config.identifier.namespace());
        if (config.overwrite) {
            spark().sql("DROP TABLE IF EXISTS " + tableSql);
        }

        spark().sql(
                        "CREATE TABLE "
                                + tableSql
                                + " ("
                                + "id BIGINT, "
                                + "k1 BIGINT, "
                                + "k2 INT, "
                                + "k3 STRING, "
                                + "dim1 STRING, "
                                + "dim2 STRING, "
                                + "metric1 DOUBLE, "
                                + "metric2 BIGINT, "
                                + "event_date DATE, "
                                + "ts TIMESTAMP"
                                + ") USING paimon "
                                + "TBLPROPERTIES ("
                                + "'"
                                + CoreOptions.PRIMARY_KEY.key()
                                + "' = 'id', "
                                + "'"
                                + CoreOptions.BUCKET.key()
                                + "' = '"
                                + config.bucket
                                + "', "
                                + "'"
                                + CoreOptions.FILE_FORMAT.key()
                                + "' = 'parquet', "
                                + "'"
                                + CoreOptions.DELETION_VECTORS_ENABLED.key()
                                + "' = 'true', "
                                + "'"
                                + CoreOptions.TARGET_FILE_SIZE.key()
                                + "' = '"
                                + escapeSqlString(config.targetFileSize)
                                + "')");

        spark().sql(
                        "INSERT INTO "
                                + tableSql
                                + " SELECT "
                                + "id, "
                                + "CAST(id % 100000 AS BIGINT) AS k1, "
                                + "CAST(id % 1000 AS INT) AS k2, "
                                + "concat('payload_', CAST(id AS STRING)) AS k3, "
                                + "concat('dim_', CAST(id % 100 AS STRING)) AS dim1, "
                                + "concat('group_', CAST(id % 32 AS STRING)) AS dim2, "
                                + "CAST(id * 0.01D AS DOUBLE) AS metric1, "
                                + "CAST(id * 3 AS BIGINT) AS metric2, "
                                + "date_add(DATE '2026-01-01', CAST(id % 365 AS INT)) "
                                + "AS event_date, "
                                + "CAST(date_add(DATE '2026-01-01', CAST(id % 365 AS INT)) "
                                + "AS TIMESTAMP) AS ts "
                                + "FROM range("
                                + config.rows
                                + ")");

        int deleteThreshold = deleteThreshold(config.dvDeleteRatio);
        if (deleteThreshold > 0) {
            spark().sql(
                            "DELETE FROM "
                                    + tableSql
                                    + " WHERE pmod(id, "
                                    + DELETE_MODULUS
                                    + ") < "
                                    + deleteThreshold);
        }
    }

    private List<RunResult> runQueryMode(
            String mode, boolean nativeEnabled, QuerySpec query, Config config) throws Exception {
        setNativeIOEnabled(nativeEnabled);
        List<RunResult> results = new ArrayList<>();
        int totalRuns = config.warmup + config.repeat;
        for (int i = 0; i < totalRuns; i++) {
            boolean warmup = i < config.warmup;
            spark().catalog().clearCache();
            spark().sparkContext()
                    .setJobDescription(
                            "paimon native_io_benchmark "
                                    + mode
                                    + " "
                                    + query.name
                                    + " "
                                    + (warmup ? "warmup" : "repeat"));
            long started = System.nanoTime();
            QueryDigest digest = materialize(query.sql, config.measureMode);
            long durationMs = (System.nanoTime() - started) / 1_000_000L;
            if (!warmup) {
                results.add(
                        new RunResult(
                                mode,
                                query.name,
                                results.size(),
                                durationMs,
                                digest.rows,
                                digest.checksum,
                                digest.checksumAvailable,
                                digest.planInfo));
            }
        }
        return results;
    }

    private QueryDigest materialize(String sql, String measureMode) {
        Dataset<Row> data = spark().sql(sql);
        PlanInfo planInfo = PlanInfo.from(data.queryExecution().executedPlan().toString());
        if (MEASURE_MODE_NOOP.equals(measureMode)) {
            data.write().format("noop").mode("overwrite").save();
            return new QueryDigest(ROW_COUNT_UNAVAILABLE, 0L, false, planInfo);
        }

        List<QueryDigest> partitions =
                data.javaRDD().mapPartitions(new DigestRowsFunction()).collect();
        long rows = 0L;
        long checksum = 0L;
        for (QueryDigest partition : partitions) {
            rows += partition.rows;
            checksum += partition.checksum;
        }
        return new QueryDigest(rows, checksum, true, planInfo);
    }

    private static QueryDigest digest(Iterator<Row> rows) {
        long rowCount = 0L;
        long checksum = 0L;
        while (rows.hasNext()) {
            Row row = rows.next();
            rowCount++;
            checksum += rowDigest(row);
        }
        return new QueryDigest(rowCount, checksum, true, PlanInfo.EMPTY);
    }

    private static long rowDigest(Row row) {
        long checksum = 1469598103934665603L;
        for (int i = 0; i < row.size(); i++) {
            Object value = row.get(i);
            checksum = 1099511628211L * checksum + i;
            checksum =
                    1099511628211L * checksum + (value == null ? 0 : value.toString().hashCode());
        }
        return checksum;
    }

    private List<QuerySpec> benchmarkQueries(Config config) {
        String table = sqlIdentifier(config.catalogName, config.identifier);
        return Arrays.asList(
                new QuerySpec("projection_3_cols", "SELECT id, k1, metric1 FROM " + table),
                new QuerySpec(
                        "projection_wide",
                        "SELECT id, k1, k2, k3, dim1, dim2, metric1, metric2, event_date, ts "
                                + "FROM "
                                + table),
                new QuerySpec(
                        "group_by_dim",
                        "SELECT dim1, count(*) AS cnt, sum(metric2) AS metric2_sum FROM "
                                + table
                                + " GROUP BY dim1"));
    }

    private List<Summary> summarize(List<QuerySpec> queries, List<RunResult> results) {
        List<Summary> summaries = new ArrayList<>();
        for (QuerySpec query : queries) {
            List<RunResult> javaResults = filter(results, "java", query.name);
            List<RunResult> nativeResults = filter(results, "native", query.name);
            RunResult javaMedian = median(javaResults);
            RunResult nativeMedian = median(nativeResults);
            double speedup =
                    nativeMedian.durationMs == 0
                            ? 0D
                            : (double) javaMedian.durationMs / nativeMedian.durationMs;
            summaries.add(
                    new Summary(
                            query.name,
                            javaMedian.durationMs,
                            nativeMedian.durationMs,
                            speedup,
                            javaMedian.rows,
                            nativeMedian.rows,
                            checksumMatch(javaMedian, nativeMedian),
                            javaMedian.planInfo.nativeColumnarCandidate,
                            nativeMedian.planInfo.nativeColumnarCandidate));
        }
        return summaries;
    }

    private static List<RunResult> filter(List<RunResult> results, String mode, String queryName) {
        List<RunResult> filtered = new ArrayList<>();
        for (RunResult result : results) {
            if (result.mode.equals(mode) && result.query.equals(queryName)) {
                filtered.add(result);
            }
        }
        return filtered;
    }

    private static RunResult median(List<RunResult> results) {
        Preconditions.checkArgument(!results.isEmpty(), "No benchmark result to summarize.");
        List<RunResult> sorted = new ArrayList<>(results);
        Collections.sort(sorted, (left, right) -> Long.compare(left.durationMs, right.durationMs));
        return sorted.get(sorted.size() / 2);
    }

    private static Boolean checksumMatch(RunResult javaResult, RunResult nativeResult) {
        if (!javaResult.checksumAvailable || !nativeResult.checksumAvailable) {
            return null;
        }
        return javaResult.rows == nativeResult.rows && javaResult.checksum == nativeResult.checksum;
    }

    private void writeReports(BenchmarkReport report) throws IOException {
        Path resultDir = new Path(report.config.resultPath);
        FileIO fileIO = outputFileIO(report.table, resultDir);
        prepareOutputDirectory(fileIO, resultDir, report.config.overwrite);
        writeJson(fileIO, resultDir, "env.json", envReport(report));
        writeJson(fileIO, resultDir, "dataset.json", datasetReport(report));
        writeJson(fileIO, resultDir, "result.json", resultReport(report));
        writeJson(fileIO, resultDir, "summary.json", summaryReport(report));
        fileIO.writeFile(new Path(resultDir, "result.csv"), csvReport(report.results), true);
        fileIO.writeFile(new Path(resultDir, "duration_bar.svg"), durationChart(report), true);
        fileIO.writeFile(new Path(resultDir, "speedup_bar.svg"), speedupChart(report), true);
        fileIO.writeFile(new Path(resultDir, "summary.html"), htmlReport(report), true);
    }

    private static Map<String, Object> envReport(BenchmarkReport report) {
        Map<String, Object> env = new LinkedHashMap<>();
        env.put("run_id", report.config.runId);
        env.put("created_at", Instant.now().toString());
        env.put("spark_version", report.sparkVersion);
        env.put("application_id", report.applicationId);
        env.put("master", report.master);
        env.put("default_parallelism", report.defaultParallelism);
        env.put("measure_mode", report.config.measureMode);
        env.put("warnings", report.warnings);
        env.put("driver_native_io", report.driverNative.asMap());
        List<Map<String, Object>> executors = new ArrayList<>();
        for (NativeCheck check : report.executorNative) {
            executors.add(check.asMap());
        }
        env.put("executor_native_io", executors);
        return env;
    }

    private static Map<String, Object> datasetReport(BenchmarkReport report) {
        Config config = report.config;
        Map<String, Object> dataset = new LinkedHashMap<>();
        dataset.put("run_id", config.runId);
        dataset.put("table_name", config.tableName);
        dataset.put("catalog_name", config.catalogName);
        dataset.put("table_identifier", sqlIdentifier(config.catalogName, config.identifier));
        dataset.put("warehouse", config.warehouse);
        dataset.put("table_path", config.tablePath.toString());
        dataset.put("rows_requested", config.rows);
        dataset.put("dv_delete_ratio", config.dvDeleteRatio);
        dataset.put("bucket", config.bucket);
        dataset.put("measure_mode", config.measureMode);
        dataset.put("delete_threshold", deleteThreshold(config.dvDeleteRatio));
        dataset.put(
                "deleted_rows_estimate", estimatedDeletedRows(config.rows, config.dvDeleteRatio));
        dataset.put(
                "rows_after_delete_estimate",
                config.rows - estimatedDeletedRows(config.rows, config.dvDeleteRatio));
        dataset.put("target_file_size", config.targetFileSize);
        dataset.put("planned_splits", report.datasetStats.plannedSplits);
        dataset.put("data_splits", report.datasetStats.dataSplits);
        dataset.put("raw_convertible_splits", report.datasetStats.rawConvertibleSplits);
        dataset.put("data_files", report.datasetStats.dataFiles);
        return dataset;
    }

    private static Map<String, Object> resultReport(BenchmarkReport report) {
        Map<String, Object> result = new LinkedHashMap<>();
        List<Map<String, Object>> runs = new ArrayList<>();
        for (RunResult run : report.results) {
            runs.add(run.asMap());
        }
        result.put("runs", runs);
        return result;
    }

    private static Map<String, Object> summaryReport(BenchmarkReport report) {
        Map<String, Object> result = new LinkedHashMap<>();
        List<Map<String, Object>> summaries = new ArrayList<>();
        for (Summary summary : report.summaries) {
            summaries.add(summary.asMap());
        }
        result.put("summaries", summaries);
        return result;
    }

    private static String csvReport(List<RunResult> results) {
        StringBuilder builder = new StringBuilder();
        builder.append("mode,query,iteration,duration_ms,rows,checksum")
                .append(",checksum_available,columnar_to_row,batch_scan")
                .append(",paimon_scan,native_columnar_candidate\n");
        for (RunResult result : results) {
            builder.append(result.mode)
                    .append(',')
                    .append(result.query)
                    .append(',')
                    .append(result.iteration)
                    .append(',')
                    .append(result.durationMs)
                    .append(',')
                    .append(result.rows)
                    .append(',')
                    .append(result.checksum)
                    .append(',')
                    .append(result.checksumAvailable)
                    .append(',')
                    .append(result.planInfo.hasColumnarToRow)
                    .append(',')
                    .append(result.planInfo.hasBatchScan)
                    .append(',')
                    .append(result.planInfo.hasPaimonScan)
                    .append(',')
                    .append(result.planInfo.nativeColumnarCandidate)
                    .append('\n');
        }
        return builder.toString();
    }

    private static String htmlReport(BenchmarkReport report) {
        StringBuilder builder = new StringBuilder();
        builder.append("<!DOCTYPE html>\n<html>\n<head>\n<meta charset=\"utf-8\">\n")
                .append("<title>")
                .append(xmlEscape(report.config.runId))
                .append("</title>\n")
                .append("<style>")
                .append(
                        "body{font-family:Arial,sans-serif;margin:24px;"
                                + "background:#fff;color:#111;}")
                .append("h1{font-size:20px;margin:0 0 16px;}")
                .append("section{margin:24px 0;}")
                .append("table{border-collapse:collapse;font-size:13px;}")
                .append("td,th{border:1px solid #ddd;padding:6px 8px;text-align:right;}")
                .append("td:first-child,th:first-child{text-align:left;}")
                .append("</style>\n</head>\n<body>\n<h1>")
                .append(xmlEscape(chartTitle(report)))
                .append("</h1>\n")
                .append(warningsHtml(report.warnings))
                .append("<section>\n")
                .append(durationChart(report))
                .append("\n</section>\n<section>\n")
                .append(speedupChart(report))
                .append("\n</section>\n<section>\n<table>\n")
                .append("<tr><th>query</th><th>java median ms</th><th>native median ms</th>")
                .append("<th>speedup</th><th>checksum match</th>")
                .append("<th>java columnar</th><th>native columnar</th></tr>\n");
        for (Summary summary : report.summaries) {
            builder.append("<tr><td>")
                    .append(xmlEscape(summary.query))
                    .append("</td><td>")
                    .append(summary.javaMedianMs)
                    .append("</td><td>")
                    .append(summary.nativeMedianMs)
                    .append("</td><td>")
                    .append(formatDouble(summary.speedup))
                    .append("x</td><td>")
                    .append(displayBoolean(summary.checksumMatch))
                    .append("</td><td>")
                    .append(summary.javaColumnar)
                    .append("</td><td>")
                    .append(summary.nativeColumnar)
                    .append("</td></tr>\n");
        }
        builder.append("</table>\n</section>\n</body>\n</html>\n");
        return builder.toString();
    }

    private static String warningsHtml(List<String> warnings) {
        if (warnings.isEmpty()) {
            return "";
        }
        StringBuilder builder = new StringBuilder();
        builder.append("<section><table>\n<tr><th>warning</th></tr>\n");
        for (String warning : warnings) {
            builder.append("<tr><td>").append(xmlEscape(warning)).append("</td></tr>\n");
        }
        return builder.append("</table></section>\n").toString();
    }

    private static String durationChart(BenchmarkReport report) {
        int width = 960;
        int left = 170;
        int top = 92;
        int chartWidth = 650;
        int groupHeight = 70;
        int barHeight = 18;
        int height = top + groupHeight * report.summaries.size() + 52;
        long maxDuration = 1L;
        for (Summary summary : report.summaries) {
            maxDuration =
                    Math.max(maxDuration, Math.max(summary.javaMedianMs, summary.nativeMedianMs));
        }

        StringBuilder builder = svgHeader(width, height, chartTitle(report));
        appendGrid(builder, left, top - 10, chartWidth, height - top - 30, maxDuration, "ms");
        for (int i = 0; i < report.summaries.size(); i++) {
            Summary summary = report.summaries.get(i);
            int y = top + i * groupHeight;
            builder.append(text(16, y + 24, summary.query, "#111", 13, "start"));
            appendBar(
                    builder,
                    left,
                    y,
                    scaled(summary.javaMedianMs, maxDuration, chartWidth),
                    barHeight,
                    "#2f6fed");
            appendBar(
                    builder,
                    left,
                    y + barHeight + 6,
                    scaled(summary.nativeMedianMs, maxDuration, chartWidth),
                    barHeight,
                    "#f28e2b");
            builder.append(
                    text(
                            left + chartWidth + 12,
                            y + 14,
                            summary.javaMedianMs + " ms",
                            "#333",
                            12,
                            "start"));
            builder.append(
                    text(
                            left + chartWidth + 12,
                            y + barHeight + 20,
                            summary.nativeMedianMs + " ms",
                            "#333",
                            12,
                            "start"));
        }
        appendLegend(builder, left, 58, "Java baseline", "#2f6fed", "Native IO", "#f28e2b");
        return builder.append("</svg>\n").toString();
    }

    private static String speedupChart(BenchmarkReport report) {
        int width = 960;
        int left = 170;
        int top = 86;
        int chartWidth = 650;
        int groupHeight = 48;
        int barHeight = 20;
        int height = top + groupHeight * report.summaries.size() + 48;
        double maxSpeedup = 1D;
        for (Summary summary : report.summaries) {
            maxSpeedup = Math.max(maxSpeedup, summary.speedup);
        }

        StringBuilder builder =
                svgHeader(width, height, "Native IO speedup | " + chartTitle(report));
        appendGrid(builder, left, top - 10, chartWidth, height - top - 26, maxSpeedup, "x");
        for (int i = 0; i < report.summaries.size(); i++) {
            Summary summary = report.summaries.get(i);
            int y = top + i * groupHeight;
            int barWidth = scaled(summary.speedup, maxSpeedup, chartWidth);
            builder.append(text(16, y + 18, summary.query, "#111", 13, "start"));
            appendBar(builder, left, y, barWidth, barHeight, "#2ca02c");
            builder.append(
                    text(
                            left + barWidth + 8,
                            y + 15,
                            formatDouble(summary.speedup) + "x",
                            "#333",
                            12,
                            "start"));
        }
        return builder.append("</svg>\n").toString();
    }

    private static StringBuilder svgHeader(int width, int height, String title) {
        StringBuilder builder = new StringBuilder();
        builder.append("<svg xmlns=\"http://www.w3.org/2000/svg\" width=\"")
                .append(width)
                .append("\" height=\"")
                .append(height)
                .append("\" viewBox=\"0 0 ")
                .append(width)
                .append(' ')
                .append(height)
                .append("\">\n")
                .append("<rect width=\"100%\" height=\"100%\" fill=\"#fff\"/>\n")
                .append(text(24, 30, title, "#111", 16, "start"));
        return builder;
    }

    private static void appendGrid(
            StringBuilder builder,
            int left,
            int top,
            int chartWidth,
            int chartHeight,
            double maxValue,
            String suffix) {
        for (int i = 0; i <= 4; i++) {
            int x = left + chartWidth * i / 4;
            double value = maxValue * i / 4D;
            builder.append("<line x1=\"")
                    .append(x)
                    .append("\" y1=\"")
                    .append(top)
                    .append("\" x2=\"")
                    .append(x)
                    .append("\" y2=\"")
                    .append(top + chartHeight)
                    .append("\" stroke=\"#e5e7eb\"/>\n")
                    .append(text(x, top - 8, formatDouble(value) + suffix, "#666", 11, "middle"));
        }
    }

    private static void appendBar(
            StringBuilder builder, int x, int y, int width, int height, String color) {
        builder.append("<rect x=\"")
                .append(x)
                .append("\" y=\"")
                .append(y)
                .append("\" width=\"")
                .append(Math.max(1, width))
                .append("\" height=\"")
                .append(height)
                .append("\" fill=\"")
                .append(color)
                .append("\"/>\n");
    }

    private static void appendLegend(
            StringBuilder builder,
            int x,
            int y,
            String leftLabel,
            String leftColor,
            String rightLabel,
            String rightColor) {
        appendBar(builder, x, y - 12, 14, 14, leftColor);
        builder.append(text(x + 20, y, leftLabel, "#333", 12, "start"));
        appendBar(builder, x + 130, y - 12, 14, 14, rightColor);
        builder.append(text(x + 150, y, rightLabel, "#333", 12, "start"));
    }

    private static String text(
            int x, int y, String value, String color, int fontSize, String anchor) {
        return "<text x=\""
                + x
                + "\" y=\""
                + y
                + "\" fill=\""
                + color
                + "\" font-size=\""
                + fontSize
                + "\" text-anchor=\""
                + anchor
                + "\">"
                + xmlEscape(value)
                + "</text>\n";
    }

    private static int scaled(double value, double maxValue, int chartWidth) {
        return (int) Math.round(value / Math.max(maxValue, 1D) * chartWidth);
    }

    private static String chartTitle(BenchmarkReport report) {
        Config config = report.config;
        return "Spark read benchmark - Paimon Native IO | dataset="
                + config.tableName
                + ", rows="
                + config.rows
                + ", dv="
                + formatDouble(config.dvDeleteRatio)
                + ", bucket="
                + config.bucket
                + ", measure="
                + config.measureMode
                + ", file="
                + config.targetFileSize
                + ", parallelism="
                + report.defaultParallelism
                + ", run_id="
                + config.runId;
    }

    private static String formatDouble(double value) {
        return String.format(Locale.ROOT, "%.2f", value);
    }

    private static String displayBoolean(Boolean value) {
        return value == null ? "n/a" : value.toString();
    }

    private static String xmlEscape(String value) {
        return value.replace("&", "&amp;")
                .replace("<", "&lt;")
                .replace(">", "&gt;")
                .replace("\"", "&quot;");
    }

    private static void writeJson(FileIO fileIO, Path resultDir, String fileName, Object value)
            throws IOException {
        fileIO.writeFile(new Path(resultDir, fileName), JsonSerdeUtil.toJson(value), true);
    }

    private void configureBenchmarkCatalog(Config config) {
        spark().conf().set("spark.sql.catalog." + config.catalogName, SparkCatalog.class.getName());
        spark().conf()
                .set("spark.sql.catalog." + config.catalogName + ".warehouse", config.warehouse);
    }

    private void createNamespaceIfNeeded(String catalogName, String[] namespace) {
        if (namespace.length == 0) {
            return;
        }
        spark().sql("CREATE DATABASE IF NOT EXISTS " + sqlIdentifier(catalogName, namespace));
    }

    private static void prepareOutputDirectory(FileIO fileIO, Path outputDir, boolean overwrite)
            throws IOException {
        if (fileIO.exists(outputDir)) {
            if (!overwrite) {
                throw new IOException(
                        "Output path already exists, set overwrite => true to replace it: "
                                + outputDir);
            }
            fileIO.delete(outputDir, true);
        }
        fileIO.mkdirs(outputDir);
    }

    private static FileIO outputFileIO(Table table, Path outputPath) throws IOException {
        if (table instanceof FileStoreTable) {
            CatalogContext context = ((FileStoreTable) table).catalogEnvironment().catalogContext();
            if (context != null) {
                return FileIO.get(outputPath, context);
            }
        }
        return table.fileIO();
    }

    private SparkTable loadBenchmarkTable(Config config) {
        try {
            CatalogPlugin catalog =
                    spark().sessionState().catalogManager().catalog(config.catalogName);
            Preconditions.checkArgument(
                    catalog instanceof TableCatalog,
                    "%s is not %s",
                    config.catalogName,
                    TableCatalog.class.getName());
            org.apache.spark.sql.connector.catalog.Table table =
                    ((TableCatalog) catalog).loadTable(config.identifier);
            Preconditions.checkArgument(
                    table instanceof SparkTable,
                    "%s is not %s",
                    config.identifier,
                    SparkTable.class.getName());
            return (SparkTable) table;
        } catch (Exception e) {
            throw new RuntimeException(
                    "Couldn't load benchmark table '"
                            + sqlIdentifier(config.catalogName, config.identifier)
                            + "'",
                    e);
        }
    }

    private List<NativeCheck> checkExecutorNativeAvailability() {
        JavaSparkContext javaSparkContext =
                JavaSparkContext.fromSparkContext(spark().sparkContext());
        int parallelism = Math.max(1, spark().sparkContext().defaultParallelism());
        List<Integer> probes = new ArrayList<>();
        for (int i = 0; i < parallelism; i++) {
            probes.add(i);
        }
        return javaSparkContext
                .parallelize(probes, parallelism)
                .map(
                        ignored -> {
                            NativeCheck check = checkNativeAvailability();
                            SparkEnv env = SparkEnv.get();
                            return check.withExecutorId(env == null ? "unknown" : env.executorId());
                        })
                .collect();
    }

    private static NativeCheck checkNativeAvailability() {
        try {
            Class<?> loaderClass = Class.forName("org.apache.paimon.nativeio.jnr.PaimonJnrLoader");
            Object loader = loaderClass.getMethod("current").invoke(null);
            boolean resourcePresent = optionalPresent(loaderClass, loader, "nativeResource");
            String resourcePath = stringMethod(loaderClass, loader, "resourcePath");
            boolean available = (Boolean) loaderClass.getMethod("available").invoke(loader);
            String failure = optionalThrowable(loaderClass, loader, "loadFailure");
            return new NativeCheck(
                    "driver", true, resourcePresent, resourcePath, available, failure);
        } catch (Throwable e) {
            return new NativeCheck("driver", false, false, null, false, e.toString());
        }
    }

    private static boolean optionalPresent(Class<?> loaderClass, Object loader, String methodName)
            throws Exception {
        Method method = loaderClass.getMethod(methodName);
        Object optional = method.invoke(loader);
        if (optional instanceof java.util.Optional) {
            return ((java.util.Optional<?>) optional).isPresent();
        }
        return optional != null;
    }

    private static String optionalThrowable(Class<?> loaderClass, Object loader, String methodName)
            throws Exception {
        Method method = loaderClass.getMethod(methodName);
        Object optional = method.invoke(loader);
        if (optional instanceof java.util.Optional) {
            java.util.Optional<?> value = (java.util.Optional<?>) optional;
            return value.isPresent() ? value.get().toString() : null;
        }
        return optional == null ? null : optional.toString();
    }

    private static String stringMethod(Class<?> loaderClass, Object loader, String methodName)
            throws Exception {
        Object value = loaderClass.getMethod(methodName).invoke(loader);
        return value == null ? null : value.toString();
    }

    private static boolean nativeAvailable(
            NativeCheck driverNative, List<NativeCheck> executorNative) {
        if (!driverNative.available) {
            return false;
        }
        for (NativeCheck check : executorNative) {
            if (!check.available) {
                return false;
            }
        }
        return true;
    }

    private void setNativeIOEnabled(boolean enabled) {
        spark().conf().set(NATIVE_IO_ENABLED_CONF, Boolean.toString(enabled));
    }

    private void restoreConf(String key, Option<String> previous) {
        if (previous.isDefined()) {
            spark().conf().set(key, previous.get());
        } else {
            spark().conf().unset(key);
        }
    }

    private static void validatePathParts(Identifier identifier) {
        for (String part : identifierParts(identifier)) {
            Preconditions.checkArgument(
                    !part.contains("/") && !part.contains("\\"),
                    "table_name should not contain path separators.");
        }
    }

    private static Path tablePath(String warehouse, Identifier identifier) {
        Path path = new Path(trimTrailingSlash(warehouse));
        for (String part : identifierParts(identifier)) {
            path = new Path(path, part);
        }
        return path;
    }

    private static List<String> identifierParts(Identifier identifier) {
        List<String> parts = new ArrayList<>();
        Collections.addAll(parts, identifier.namespace());
        parts.add(identifier.name());
        return parts;
    }

    private static String sqlIdentifier(Identifier identifier) {
        List<String> parts = identifierParts(identifier);
        return sqlIdentifier(parts.toArray(new String[0]));
    }

    private static String sqlIdentifier(String catalogName, Identifier identifier) {
        List<String> parts = identifierParts(identifier);
        parts.add(0, catalogName);
        return sqlIdentifier(parts.toArray(new String[0]));
    }

    private static String sqlIdentifier(String catalogName, String[] namespace) {
        List<String> parts = new ArrayList<>();
        parts.add(catalogName);
        Collections.addAll(parts, namespace);
        return sqlIdentifier(parts.toArray(new String[0]));
    }

    private static String sqlIdentifier(String[] parts) {
        StringBuilder builder = new StringBuilder();
        for (int i = 0; i < parts.length; i++) {
            if (i > 0) {
                builder.append('.');
            }
            builder.append('`').append(parts[i].replace("`", "``")).append('`');
        }
        return builder.toString();
    }

    private static String escapeSqlString(String value) {
        return value.replace("\\", "\\\\").replace("'", "\\'");
    }

    private static String trimTrailingSlash(String path) {
        String result = path.trim();
        while (result.length() > 1 && result.endsWith("/")) {
            result = result.substring(0, result.length() - 1);
        }
        return result;
    }

    private static String normalizeMeasureMode(String measureMode) {
        String normalized = measureMode.trim().toLowerCase(Locale.ROOT).replace('-', '_');
        if ("scan".equals(normalized) || "scan_only".equals(normalized)) {
            return MEASURE_MODE_NOOP;
        }
        if ("checksum".equals(normalized) || "digest".equals(normalized)) {
            return MEASURE_MODE_ROW_DIGEST;
        }
        return normalized;
    }

    private static int deleteThreshold(double deleteRatio) {
        return (int) Math.round(deleteRatio * DELETE_MODULUS);
    }

    private static long estimatedDeletedRows(long rows, double deleteRatio) {
        int threshold = deleteThreshold(deleteRatio);
        long cycles = rows / DELETE_MODULUS;
        long remainder = rows % DELETE_MODULUS;
        return cycles * threshold + Math.min(remainder, threshold);
    }

    private static String benchmarkCatalogName(String tableName, String resultPath) {
        long hash = (tableName + "\n" + resultPath).hashCode() & 0xffffffffL;
        return "paimon_native_io_benchmark_" + Long.toString(hash, 36);
    }

    private static String runId(String tableName, String resultPath) {
        long hash = (tableName + "\n" + resultPath).hashCode() & 0xffffffffL;
        return "native_io_benchmark_"
                + Long.toString(System.currentTimeMillis(), 36)
                + "_"
                + Long.toString(hash, 36);
    }

    private DatasetStats datasetStats(Table table) {
        List<Split> splits = table.newReadBuilder().newScan().plan().splits();
        int dataSplits = 0;
        int rawConvertibleSplits = 0;
        int dataFiles = 0;
        for (Split split : splits) {
            if (split instanceof DataSplit) {
                DataSplit dataSplit = (DataSplit) split;
                dataSplits++;
                if (dataSplit.rawConvertible()) {
                    rawConvertibleSplits++;
                }
                dataFiles += dataSplit.dataFiles().size();
            }
        }
        return new DatasetStats(splits.size(), dataSplits, rawConvertibleSplits, dataFiles);
    }

    private List<String> benchmarkWarnings(Config config) {
        List<String> warnings = new ArrayList<>();
        if (MEASURE_MODE_ROW_DIGEST.equals(config.measureMode)) {
            warnings.add(
                    "measure_mode=row_digest includes Spark row conversion and Java checksum "
                            + "cost; use measure_mode=noop for scan-only timing.");
        }
        Option<String> excludedRules = spark().conf().getOption(EXCLUDED_RULES_CONF);
        if (excludedRules.isDefined()) {
            String rules = excludedRules.get();
            if (containsRule(rules, "V2ScanRelationPushDown")) {
                warnings.add(
                        EXCLUDED_RULES_CONF
                                + " excludes V2ScanRelationPushDown; Spark may not push "
                                + "required columns to Paimon scans.");
            }
            if (containsRule(rules, "ColumnPruning")) {
                warnings.add(
                        EXCLUDED_RULES_CONF
                                + " excludes ColumnPruning; projection benchmarks can read "
                                + "more columns than requested.");
            }
            if (containsRule(rules, "SchemaPruning")) {
                warnings.add(
                        EXCLUDED_RULES_CONF
                                + " excludes SchemaPruning; nested or projected schema pruning "
                                + "can be disabled.");
            }
        }
        return warnings;
    }

    private static boolean containsRule(String rules, String simpleName) {
        for (String rule : rules.split(",")) {
            if (rule.trim().endsWith(simpleName)) {
                return true;
            }
        }
        return false;
    }

    public static ProcedureBuilder builder() {
        return new BaseProcedure.Builder<NativeIOBenchmarkProcedure>() {
            @Override
            public NativeIOBenchmarkProcedure doBuild() {
                return new NativeIOBenchmarkProcedure(tableCatalog());
            }
        };
    }

    @Override
    public String description() {
        return "NativeIOBenchmarkProcedure";
    }

    private static class Config {

        private final String warehouse;
        private final String resultPath;
        private final String tableName;
        private final String runId;
        private final String catalogName;
        private final Identifier identifier;
        private final Path tablePath;
        private final long rows;
        private final int repeat;
        private final int warmup;
        private final double dvDeleteRatio;
        private final String targetFileSize;
        private final boolean failIfNativeUnavailable;
        private final boolean overwrite;
        private final int bucket;
        private final String measureMode;

        private Config(
                String warehouse,
                String resultPath,
                String tableName,
                String runId,
                String catalogName,
                Identifier identifier,
                Path tablePath,
                long rows,
                int repeat,
                int warmup,
                double dvDeleteRatio,
                String targetFileSize,
                boolean failIfNativeUnavailable,
                boolean overwrite,
                int bucket,
                String measureMode) {
            this.warehouse = warehouse;
            this.resultPath = resultPath;
            this.tableName = tableName;
            this.runId = runId;
            this.catalogName = catalogName;
            this.identifier = identifier;
            this.tablePath = tablePath;
            this.rows = rows;
            this.repeat = repeat;
            this.warmup = warmup;
            this.dvDeleteRatio = dvDeleteRatio;
            this.targetFileSize = targetFileSize;
            this.failIfNativeUnavailable = failIfNativeUnavailable;
            this.overwrite = overwrite;
            this.bucket = bucket;
            this.measureMode = measureMode;
        }
    }

    private static class QuerySpec {

        private final String name;
        private final String sql;

        private QuerySpec(String name, String sql) {
            this.name = name;
            this.sql = sql;
        }
    }

    private static class QueryDigest implements Serializable {

        private static final long serialVersionUID = 1L;

        private final long rows;
        private final long checksum;
        private final boolean checksumAvailable;
        private final PlanInfo planInfo;

        private QueryDigest(
                long rows, long checksum, boolean checksumAvailable, PlanInfo planInfo) {
            this.rows = rows;
            this.checksum = checksum;
            this.checksumAvailable = checksumAvailable;
            this.planInfo = planInfo;
        }
    }

    private static class PlanInfo implements Serializable {

        private static final long serialVersionUID = 1L;

        private static final PlanInfo EMPTY = new PlanInfo(false, false, false, false, "");

        private final boolean hasColumnarToRow;
        private final boolean hasBatchScan;
        private final boolean hasPaimonScan;
        private final boolean nativeColumnarCandidate;
        private final String physicalPlan;

        private PlanInfo(
                boolean hasColumnarToRow,
                boolean hasBatchScan,
                boolean hasPaimonScan,
                boolean nativeColumnarCandidate,
                String physicalPlan) {
            this.hasColumnarToRow = hasColumnarToRow;
            this.hasBatchScan = hasBatchScan;
            this.hasPaimonScan = hasPaimonScan;
            this.nativeColumnarCandidate = nativeColumnarCandidate;
            this.physicalPlan = physicalPlan;
        }

        private static PlanInfo from(String physicalPlan) {
            boolean hasColumnarToRow = physicalPlan.contains("ColumnarToRow");
            boolean hasBatchScan = physicalPlan.contains("BatchScan");
            boolean hasPaimonScan = physicalPlan.contains("PaimonScan");
            return new PlanInfo(
                    hasColumnarToRow,
                    hasBatchScan,
                    hasPaimonScan,
                    hasColumnarToRow && hasBatchScan && hasPaimonScan,
                    physicalPlan);
        }

        private Map<String, Object> asMap() {
            Map<String, Object> map = new LinkedHashMap<>();
            map.put("columnar_to_row", hasColumnarToRow);
            map.put("batch_scan", hasBatchScan);
            map.put("paimon_scan", hasPaimonScan);
            map.put("native_columnar_candidate", nativeColumnarCandidate);
            map.put("physical_plan", physicalPlan);
            return map;
        }
    }

    private static class DigestRowsFunction implements FlatMapFunction<Iterator<Row>, QueryDigest> {

        private static final long serialVersionUID = 1L;

        @Override
        public Iterator<QueryDigest> call(Iterator<Row> rows) {
            return Collections.singletonList(digest(rows)).iterator();
        }
    }

    private static class RunResult {

        private final String mode;
        private final String query;
        private final int iteration;
        private final long durationMs;
        private final long rows;
        private final long checksum;
        private final boolean checksumAvailable;
        private final PlanInfo planInfo;

        private RunResult(
                String mode,
                String query,
                int iteration,
                long durationMs,
                long rows,
                long checksum,
                boolean checksumAvailable,
                PlanInfo planInfo) {
            this.mode = mode;
            this.query = query;
            this.iteration = iteration;
            this.durationMs = durationMs;
            this.rows = rows;
            this.checksum = checksum;
            this.checksumAvailable = checksumAvailable;
            this.planInfo = planInfo;
        }

        private Map<String, Object> asMap() {
            Map<String, Object> map = new LinkedHashMap<>();
            map.put("mode", mode);
            map.put("query", query);
            map.put("iteration", iteration);
            map.put("duration_ms", durationMs);
            map.put("rows", rows);
            map.put("checksum", checksum);
            map.put("checksum_available", checksumAvailable);
            map.put("plan", planInfo.asMap());
            return map;
        }
    }

    private static class Summary {

        private final String query;
        private final long javaMedianMs;
        private final long nativeMedianMs;
        private final double speedup;
        private final long javaRows;
        private final long nativeRows;
        private final Boolean checksumMatch;
        private final boolean javaColumnar;
        private final boolean nativeColumnar;

        private Summary(
                String query,
                long javaMedianMs,
                long nativeMedianMs,
                double speedup,
                long javaRows,
                long nativeRows,
                Boolean checksumMatch,
                boolean javaColumnar,
                boolean nativeColumnar) {
            this.query = query;
            this.javaMedianMs = javaMedianMs;
            this.nativeMedianMs = nativeMedianMs;
            this.speedup = speedup;
            this.javaRows = javaRows;
            this.nativeRows = nativeRows;
            this.checksumMatch = checksumMatch;
            this.javaColumnar = javaColumnar;
            this.nativeColumnar = nativeColumnar;
        }

        private Map<String, Object> asMap() {
            Map<String, Object> map = new LinkedHashMap<>();
            map.put("query", query);
            map.put("java_median_ms", javaMedianMs);
            map.put("native_median_ms", nativeMedianMs);
            map.put("speedup", speedup);
            map.put("java_rows", javaRows);
            map.put("native_rows", nativeRows);
            map.put("checksum_match", checksumMatch);
            map.put("java_columnar", javaColumnar);
            map.put("native_columnar", nativeColumnar);
            return map;
        }
    }

    private static class DatasetStats {

        private final int plannedSplits;
        private final int dataSplits;
        private final int rawConvertibleSplits;
        private final int dataFiles;

        private DatasetStats(
                int plannedSplits, int dataSplits, int rawConvertibleSplits, int dataFiles) {
            this.plannedSplits = plannedSplits;
            this.dataSplits = dataSplits;
            this.rawConvertibleSplits = rawConvertibleSplits;
            this.dataFiles = dataFiles;
        }
    }

    private static class NativeCheck implements Serializable {

        private static final long serialVersionUID = 1L;

        private final String executorId;
        private final boolean classPresent;
        private final boolean resourcePresent;
        private final String resourcePath;
        private final boolean available;
        private final String failure;

        private NativeCheck(
                String executorId,
                boolean classPresent,
                boolean resourcePresent,
                String resourcePath,
                boolean available,
                String failure) {
            this.executorId = executorId;
            this.classPresent = classPresent;
            this.resourcePresent = resourcePresent;
            this.resourcePath = resourcePath;
            this.available = available;
            this.failure = failure;
        }

        private NativeCheck withExecutorId(String executorId) {
            return new NativeCheck(
                    executorId, classPresent, resourcePresent, resourcePath, available, failure);
        }

        private Map<String, Object> asMap() {
            Map<String, Object> map = new LinkedHashMap<>();
            map.put("executor_id", executorId);
            map.put("class_present", classPresent);
            map.put("resource_present", resourcePresent);
            map.put("resource_path", resourcePath);
            map.put("available", available);
            map.put("failure", failure);
            return map;
        }
    }

    private class BenchmarkReport {

        private final Config config;
        private final Table table;
        private final NativeCheck driverNative;
        private final List<NativeCheck> executorNative;
        private final List<RunResult> results;
        private final List<Summary> summaries;
        private final DatasetStats datasetStats;
        private final List<String> warnings;
        private final String sparkVersion;
        private final String applicationId;
        private final String master;
        private final int defaultParallelism;

        private BenchmarkReport(
                Config config,
                Table table,
                NativeCheck driverNative,
                List<NativeCheck> executorNative,
                List<RunResult> results,
                List<Summary> summaries) {
            this.config = config;
            this.table = table;
            this.driverNative = driverNative;
            this.executorNative = executorNative;
            this.results = results;
            this.summaries = summaries;
            this.datasetStats = datasetStats(table);
            this.warnings = benchmarkWarnings(config);
            this.sparkVersion = spark().version();
            this.applicationId = spark().sparkContext().applicationId();
            this.master = spark().sparkContext().master();
            this.defaultParallelism = spark().sparkContext().defaultParallelism();
        }
    }
}
