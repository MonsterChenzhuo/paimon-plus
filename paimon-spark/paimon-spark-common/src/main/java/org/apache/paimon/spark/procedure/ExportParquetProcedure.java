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
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.deletionvectors.DeletionVector;
import org.apache.paimon.format.FormatWriter;
import org.apache.paimon.format.parquet.ParquetWriterFactory;
import org.apache.paimon.format.parquet.writer.RowDataParquetBuilder;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.FileStatus;
import org.apache.paimon.fs.Path;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.operation.nativeio.NativeRejectReason;
import org.apache.paimon.operation.nativeio.export.NativeExportContext;
import org.apache.paimon.operation.nativeio.export.NativeExportPlanDescriptor;
import org.apache.paimon.operation.nativeio.export.NativeExportPreflightResult;
import org.apache.paimon.operation.nativeio.export.NativeExportProvider;
import org.apache.paimon.operation.nativeio.export.NativeExportProviderFactory;
import org.apache.paimon.operation.nativeio.export.NativeExportSourceFile;
import org.apache.paimon.operation.nativeio.export.NativeExportTaskResult;
import org.apache.paimon.options.MemorySize;
import org.apache.paimon.options.Options;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.predicate.PredicateBuilder;
import org.apache.paimon.predicate.PredicateProjectionConverter;
import org.apache.paimon.predicate.PredicateVisitor;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.Table;
import org.apache.paimon.table.source.DataSplit;
import org.apache.paimon.table.source.IncrementalSplit;
import org.apache.paimon.table.source.RawFile;
import org.apache.paimon.table.source.ReadBuilder;
import org.apache.paimon.table.source.Split;
import org.apache.paimon.table.source.TableRead;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.CloseableIterator;
import org.apache.paimon.utils.InternalRowPartitionComputer;
import org.apache.paimon.utils.PartitionPathUtils;
import org.apache.paimon.utils.Preconditions;
import org.apache.paimon.utils.ProjectedRow;
import org.apache.paimon.utils.Projection;
import org.apache.paimon.utils.StringUtils;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.connector.catalog.TableCatalog;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.math.BigDecimal;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.stream.Collectors;

import scala.collection.JavaConverters;

import static org.apache.spark.sql.types.DataTypes.BooleanType;
import static org.apache.spark.sql.types.DataTypes.IntegerType;
import static org.apache.spark.sql.types.DataTypes.LongType;
import static org.apache.spark.sql.types.DataTypes.StringType;

/**
 * Export projected Paimon rows to parquet files without constructing a Spark SQL wide Project.
 *
 * <pre><code>
 * CALL sys.export_parquet(
 *   table => 'db.tbl',
 *   columns => 'id,f1,f2',
 *   output_path => 'obs://bucket/path',
 *   where => "dt = '2026-05-14' and user_id >= 100")
 * </code></pre>
 */
public class ExportParquetProcedure extends BaseProcedure {

    private static final Logger LOG = LoggerFactory.getLogger(ExportParquetProcedure.class);
    private static final String PAIMON_OPTION_PREFIX = "spark.paimon.";
    private static final String SPARK_HADOOP_PREFIX = "spark.hadoop.";
    private static final String OBS_CONF_PREFIX = "fs.obs.";

    private static final ProcedureParameter[] PARAMETERS =
            new ProcedureParameter[] {
                ProcedureParameter.required("table", StringType),
                ProcedureParameter.required("columns", StringType),
                ProcedureParameter.required("output_path", StringType),
                ProcedureParameter.optional("where", StringType),
                ProcedureParameter.optional("parallelism", IntegerType),
                ProcedureParameter.optional("compression", StringType),
                ProcedureParameter.optional("overwrite", BooleanType),
                ProcedureParameter.optional("target_file_size", StringType),
                ProcedureParameter.optional("partitioned_output", BooleanType),
                ProcedureParameter.optional("partition_job_parallelism", IntegerType),
                ProcedureParameter.optional("compact_output", BooleanType)
            };

    private static final StructType OUTPUT_TYPE =
            new StructType(
                    new StructField[] {
                        new StructField("result", DataTypes.BooleanType, false, Metadata.empty()),
                        new StructField("rows", LongType, false, Metadata.empty())
                    });

    protected ExportParquetProcedure(TableCatalog tableCatalog) {
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
        Identifier tableIdent = toIdentifier(args.getString(0), PARAMETERS[0].name());
        String columns = args.getString(1);
        String outputPath = args.getString(2);
        String where = args.isNullAt(3) ? null : args.getString(3);
        int parallelism =
                args.isNullAt(4)
                        ? spark().sparkContext().defaultParallelism()
                        : Math.max(1, args.getInt(4));
        String compression = args.isNullAt(5) ? "zstd" : args.getString(5);
        boolean overwrite = !args.isNullAt(6) && args.getBoolean(6);
        Long targetFileSize = args.isNullAt(7) ? null : parseTargetFileSize(args.getString(7));
        boolean partitionedOutput = !args.isNullAt(8) && args.getBoolean(8);
        int partitionJobParallelism = args.isNullAt(9) ? 1 : Math.max(1, args.getInt(9));
        boolean compactOutput = !args.isNullAt(10) && args.getBoolean(10);

        Table table = loadSparkTable(tableIdent).getTable();
        try {
            long rows =
                    export(
                            table,
                            columns,
                            outputPath,
                            where,
                            parallelism,
                            compression,
                            overwrite,
                            targetFileSize,
                            partitionedOutput,
                            partitionJobParallelism,
                            compactOutput);
            return new InternalRow[] {newInternalRow(true, rows)};
        } catch (Exception e) {
            throw new RuntimeException("Failed to export parquet files", e);
        }
    }

    private long export(
            Table table,
            String columns,
            String outputPath,
            @Nullable String where,
            int parallelism,
            String compression,
            boolean overwrite,
            @Nullable Long targetFileSize,
            boolean partitionedOutput,
            int partitionJobParallelism,
            boolean compactOutput)
            throws Exception {
        RowType tableRowType = table.rowType();
        int[] outputProjection = parseOutputProjection(tableRowType, columns);
        Predicate predicate = parsePredicate(tableRowType, where);
        int[] readProjection = readProjection(tableRowType, outputProjection, predicate);
        RowType outputType = Projection.of(outputProjection).project(tableRowType);

        Predicate projectedPredicate =
                predicate == null
                        ? null
                        : predicate
                                .visit(PredicateProjectionConverter.fromProjection(readProjection))
                                .orElseThrow(
                                        () ->
                                                new IllegalArgumentException(
                                                        "Cannot project predicate to read columns."));

        ReadBuilder readBuilder = table.newReadBuilder();
        if (predicate != null) {
            readBuilder = readBuilder.withFilter(predicate);
        }
        readBuilder = readBuilder.withProjection(readProjection);

        final ReadBuilder finalReadBuilder = readBuilder;
        List<Split> plannedSplits = finalReadBuilder.newScan().plan().splits();
        Path outputDir = new Path(trimTrailingSlash(outputPath));

        if (partitionedOutput) {
            Preconditions.checkArgument(
                    !table.partitionKeys().isEmpty(),
                    "partitioned_output requires a partitioned table.");
            return partitionedExport(
                    table,
                    finalReadBuilder,
                    plannedSplits,
                    outputDir,
                    outputType,
                    projectedPredicate,
                    outputProjection.length,
                    parallelism,
                    partitionJobParallelism,
                    compression,
                    overwrite,
                    targetFileSize,
                    compactOutput);
        }

        long rows =
                exportPlannedSplits(
                        table,
                        finalReadBuilder,
                        plannedSplits,
                        outputDir,
                        outputType,
                        projectedPredicate,
                        outputProjection.length,
                        parallelism,
                        compression,
                        overwrite,
                        targetFileSize);
        if (compactOutput) {
            compactOutputDirectory(table, outputDir, compression);
        }
        return rows;
    }

    private long exportPlannedSplits(
            Table table,
            ReadBuilder readBuilder,
            List<Split> plannedSplits,
            Path outputDir,
            RowType outputType,
            @Nullable Predicate projectedPredicate,
            int outputFieldCount,
            int parallelism,
            String compression,
            boolean overwrite,
            @Nullable Long targetFileSize)
            throws Exception {
        List<SerializedSplit> splits =
                plannedSplits.stream()
                        .map(ExportParquetProcedure::copySplit)
                        .map(ExportParquetProcedure::serializeSplit)
                        .collect(Collectors.toList());
        FileIO outputFileIO = outputFileIO(table, outputDir);

        Optional<NativeExportAttempt> nativeExportAttempt =
                nativeExportAttempt(
                        table,
                        outputDir.toString(),
                        compression,
                        targetFileSize,
                        outputType,
                        plannedSplits,
                        projectedPredicate);
        if (nativeExportAttempt.isPresent()) {
            NativeExportAttempt attempt = nativeExportAttempt.get();
            if (attempt.preflight.applicable()) {
                prepareOutputDirectory(outputFileIO, outputDir, overwrite);
                long rows = nativeExport(attempt.provider, attempt.context, parallelism);
                outputFileIO.newOutputStream(new Path(outputDir, "_SUCCESS"), true).close();
                return rows;
            }
            LOG.warn(
                    "Native export requested but rejected. reason={}, detail={}",
                    attempt.preflight.reason(),
                    attempt.preflight.detail());
            if (!attempt.context.options().get(CoreOptions.NATIVE_IO_EXPORT_FALLBACK_ENABLED)
                    || attempt.context
                            .options()
                            .get(CoreOptions.NATIVE_IO_EXPORT_FAIL_ON_FALLBACK)) {
                throw new UnsupportedOperationException(
                        "Native export is enabled but not applicable. reason="
                                + attempt.preflight.reason()
                                + ", detail="
                                + attempt.preflight.detail());
            }
        }

        prepareOutputDirectory(outputFileIO, outputDir, overwrite);

        return javaExport(
                table,
                readBuilder,
                splits,
                plannedSplits,
                outputDir,
                outputType,
                projectedPredicate,
                outputFieldCount,
                parallelism,
                compression,
                targetFileSize);
    }

    private long partitionedExport(
            Table table,
            ReadBuilder readBuilder,
            List<Split> plannedSplits,
            Path outputDir,
            RowType outputType,
            @Nullable Predicate projectedPredicate,
            int outputFieldCount,
            int parallelism,
            int partitionJobParallelism,
            String compression,
            boolean overwrite,
            @Nullable Long targetFileSize,
            boolean compactOutput)
            throws Exception {
        FileIO outputFileIO = outputFileIO(table, outputDir);
        prepareOutputDirectory(outputFileIO, outputDir, overwrite);

        List<PartitionExportPlan> partitions =
                partitionExportPlans(table, plannedSplits, outputDir);
        if (partitions.isEmpty()) {
            outputFileIO.newOutputStream(new Path(outputDir, "_SUCCESS"), true).close();
            return 0L;
        }

        int jobParallelism =
                Math.max(1, Math.min(Math.max(1, partitionJobParallelism), partitions.size()));
        long rows;
        if (jobParallelism == 1) {
            rows = 0L;
            for (PartitionExportPlan partition : partitions) {
                rows +=
                        exportPartition(
                                table,
                                readBuilder,
                                partition,
                                outputType,
                                projectedPredicate,
                                outputFieldCount,
                                parallelism,
                                compression,
                                targetFileSize,
                                compactOutput);
            }
        } else {
            rows =
                    exportPartitionsConcurrently(
                            table,
                            readBuilder,
                            partitions,
                            outputType,
                            projectedPredicate,
                            outputFieldCount,
                            parallelism,
                            jobParallelism,
                            compression,
                            targetFileSize,
                            compactOutput);
        }

        outputFileIO.newOutputStream(new Path(outputDir, "_SUCCESS"), true).close();
        return rows;
    }

    private long exportPartitionsConcurrently(
            Table table,
            ReadBuilder readBuilder,
            List<PartitionExportPlan> partitions,
            RowType outputType,
            @Nullable Predicate projectedPredicate,
            int outputFieldCount,
            int parallelism,
            int jobParallelism,
            String compression,
            @Nullable Long targetFileSize,
            boolean compactOutput)
            throws Exception {
        ExecutorService executor = Executors.newFixedThreadPool(jobParallelism);
        try {
            List<Future<Long>> futures = new ArrayList<>(partitions.size());
            for (PartitionExportPlan partition : partitions) {
                futures.add(
                        executor.submit(
                                new Callable<Long>() {
                                    @Override
                                    public Long call() throws Exception {
                                        return exportPartition(
                                                table,
                                                readBuilder,
                                                partition,
                                                outputType,
                                                projectedPredicate,
                                                outputFieldCount,
                                                parallelism,
                                                compression,
                                                targetFileSize,
                                                compactOutput);
                                    }
                                }));
            }

            long rows = 0L;
            for (Future<Long> future : futures) {
                rows += future.get();
            }
            return rows;
        } catch (ExecutionException e) {
            Throwable cause = e.getCause();
            if (cause instanceof Exception) {
                throw (Exception) cause;
            }
            throw new RuntimeException(cause);
        } finally {
            executor.shutdownNow();
        }
    }

    private long exportPartition(
            Table table,
            ReadBuilder readBuilder,
            PartitionExportPlan partition,
            RowType outputType,
            @Nullable Predicate projectedPredicate,
            int outputFieldCount,
            int parallelism,
            String compression,
            @Nullable Long targetFileSize,
            boolean compactOutput)
            throws Exception {
        LOG.info(
                "Exporting partition for sys.export_parquet. partition={}, outputPath={}, splits={}",
                partition.partitionPath,
                partition.outputDir,
                partition.splits.size());
        long rows =
                exportPlannedSplits(
                        table,
                        readBuilder,
                        partition.splits,
                        partition.outputDir,
                        outputType,
                        projectedPredicate,
                        outputFieldCount,
                        parallelism,
                        compression,
                        false,
                        targetFileSize);
        if (compactOutput) {
            compactOutputDirectory(table, partition.outputDir, compression);
        }
        return rows;
    }

    private static List<PartitionExportPlan> partitionExportPlans(
            Table table, List<Split> splits, Path outputDir) {
        CoreOptions options = new CoreOptions(table.options());
        RowType partitionType = table.rowType().project(table.partitionKeys());
        InternalRowPartitionComputer partitionComputer =
                new InternalRowPartitionComputer(
                        options.partitionDefaultName(),
                        partitionType,
                        table.partitionKeys().toArray(new String[0]),
                        options.legacyPartitionName());
        Map<String, PartitionExportPlan> plans = new LinkedHashMap<>();
        for (Split split : splits) {
            String partitionPath =
                    trimTrailingSlash(
                            PartitionPathUtils.generatePartitionPath(
                                    partitionComputer.generatePartValues(splitPartition(split))));
            PartitionExportPlan plan = plans.get(partitionPath);
            if (plan == null) {
                plan = new PartitionExportPlan(partitionPath, new Path(outputDir, partitionPath));
                plans.put(partitionPath, plan);
            }
            plan.splits.add(split);
        }
        return new ArrayList<>(plans.values());
    }

    private static BinaryRow splitPartition(Split split) {
        if (split instanceof DataSplit) {
            return ((DataSplit) split).partition();
        }
        if (split instanceof IncrementalSplit) {
            return ((IncrementalSplit) split).partition();
        }
        throw new UnsupportedOperationException(
                "partitioned_output only supports DataSplit and IncrementalSplit, but got "
                        + split.getClass().getName());
    }

    private void compactOutputDirectory(Table table, Path outputDir, String compression)
            throws IOException {
        FileIO fileIO = outputFileIO(table, outputDir);
        int parquetFiles = parquetFileCount(fileIO, outputDir);
        if (parquetFiles <= 1) {
            return;
        }

        Path parent = outputDir.getParent();
        Preconditions.checkArgument(
                parent != null, "Cannot compact root output directory: %s", outputDir);
        Path tempDir =
                new Path(
                        parent,
                        "." + outputDir.getName() + "-compact-" + UUID.randomUUID().toString());
        Path backupDir =
                new Path(
                        parent, "." + outputDir.getName() + "-before-compact-" + UUID.randomUUID());
        if (fileIO.exists(tempDir)) {
            fileIO.delete(tempDir, true);
        }
        if (fileIO.exists(backupDir)) {
            fileIO.delete(backupDir, true);
        }

        boolean committed = false;
        boolean backedUp = false;
        try {
            spark().read()
                    .parquet(outputDir.toString())
                    .coalesce(1)
                    .write()
                    .mode(SaveMode.Overwrite)
                    .option("compression", compression)
                    .parquet(tempDir.toString());
            if (!fileIO.rename(outputDir, backupDir)) {
                throw new IOException(
                        "Failed to backup output directory "
                                + outputDir
                                + " before compacting to "
                                + backupDir);
            }
            backedUp = true;
            if (!fileIO.rename(tempDir, outputDir)) {
                fileIO.rename(backupDir, outputDir);
                throw new IOException(
                        "Failed to replace compacted output directory "
                                + outputDir
                                + " with "
                                + tempDir);
            }
            committed = true;
            fileIO.deleteQuietly(backupDir);
        } finally {
            if (!committed) {
                fileIO.deleteQuietly(tempDir);
                if (backedUp && !fileIO.exists(outputDir)) {
                    fileIO.rename(backupDir, outputDir);
                }
            }
        }
    }

    private static int parquetFileCount(FileIO fileIO, Path outputDir) throws IOException {
        int count = 0;
        for (FileStatus status : fileIO.listFiles(outputDir, false)) {
            if (!status.isDir() && status.getPath().getName().endsWith(".parquet")) {
                count++;
            }
        }
        return count;
    }

    private long javaExport(
            Table table,
            ReadBuilder readBuilder,
            List<SerializedSplit> splits,
            List<Split> plannedSplits,
            Path outputDir,
            RowType outputType,
            @Nullable Predicate projectedPredicate,
            int outputFieldCount,
            int parallelism,
            String compression,
            @Nullable Long targetFileSize)
            throws Exception {
        final Table exportTable = table;
        final RowType finalOutputType = outputType;
        final Predicate finalProjectedPredicate = projectedPredicate;
        final String finalCompression = compression;
        final Long finalTargetFileSize = targetFileSize;
        final int numPartitions =
                numPartitions(parallelism, splits.size(), plannedSplits, targetFileSize);
        JavaSparkContext jsc = JavaSparkContext.fromSparkContext(spark().sparkContext());
        List<Long> counts;
        if (targetFileSize == null) {
            counts =
                    jsc.parallelize(splits, numPartitions)
                            .map(
                                    serializedSplit ->
                                            exportSplit(
                                                    exportTable,
                                                    readBuilder,
                                                    serializedSplit.split(),
                                                    outputDir,
                                                    finalOutputType,
                                                    finalProjectedPredicate,
                                                    outputFieldCount,
                                                    finalCompression))
                            .collect();
        } else {
            counts =
                    jsc.parallelize(splits, numPartitions)
                            .mapPartitions(
                                    serializedSplits ->
                                            Collections.singletonList(
                                                            exportSplits(
                                                                    exportTable,
                                                                    readBuilder,
                                                                    serializedSplits,
                                                                    outputDir,
                                                                    finalOutputType,
                                                                    finalProjectedPredicate,
                                                                    outputFieldCount,
                                                                    finalCompression,
                                                                    finalTargetFileSize))
                                                    .iterator())
                            .collect();
        }

        long rows = 0L;
        for (Long count : counts) {
            rows += count;
        }
        FileIO outputFileIO = outputFileIO(table, outputDir);
        outputFileIO.newOutputStream(new Path(outputDir, "_SUCCESS"), true).close();
        return rows;
    }

    private long nativeExport(
            NativeExportProvider provider, NativeExportContext context, int parallelism)
            throws Exception {
        installNativeIODiagnostics();
        NativeExportPlanDescriptor plan = provider.plan(context);
        List<byte[]> taskPayloads = plan.taskPayloads();
        LOG.info(
                "Using native export for sys.export_parquet. outputPath={}, tasks={}, projection={}",
                context.outputPath(),
                taskPayloads.size(),
                context.projectedFieldNames());
        if (taskPayloads.isEmpty()) {
            LOG.info(
                    "Native export completed. outputPath={}, rows=0, tasks=0",
                    context.outputPath());
            return 0L;
        }
        JavaSparkContext jsc = JavaSparkContext.fromSparkContext(spark().sparkContext());
        List<NativeExportTaskResult> results =
                jsc.parallelize(taskPayloads, nativeTaskCount(parallelism, taskPayloads.size()))
                        .map(provider::executeTask)
                        .collect();
        long rows = 0L;
        for (NativeExportTaskResult result : results) {
            rows += result.rowsOutput();
        }
        LOG.info(
                "Native export completed. outputPath={}, rows={}, tasks={}",
                context.outputPath(),
                rows,
                results.size());
        return rows;
    }

    private void installNativeIODiagnostics() {
        try {
            Class<?> diagnostics =
                    Class.forName(
                            "org.apache.paimon.spark.nativeio.diagnostics.NativeIODiagnostics",
                            true,
                            Thread.currentThread().getContextClassLoader());
            diagnostics
                    .getMethod("install", org.apache.spark.sql.SparkSession.class)
                    .invoke(null, spark());
        } catch (Throwable e) {
            LOG.debug("Native IO Spark UI diagnostics are not available.", e);
        }
    }

    static int nativeTaskCount(int parallelism, int taskCount) {
        return Math.max(1, Math.min(Math.max(1, parallelism), Math.max(1, taskCount)));
    }

    private Optional<NativeExportAttempt> nativeExportAttempt(
            Table table,
            String outputPath,
            String compression,
            @Nullable Long targetFileSize,
            RowType outputType,
            List<Split> plannedSplits,
            @Nullable Predicate projectedPredicate)
            throws IOException {
        Options options = exportOptions(table);
        if (!options.get(CoreOptions.NATIVE_IO_EXPORT_ENABLED)) {
            return Optional.empty();
        }

        List<NativeExportProviderFactory> factories =
                NativeExportProviderFactory.discover(
                        Thread.currentThread().getContextClassLoader());
        NativeExportContext context =
                new NativeExportContext(
                        options,
                        outputPath,
                        compression,
                        targetFileSize,
                        outputType.getFieldNames(),
                        plannedSplits.size(),
                        nativeSourceFiles(table, plannedSplits),
                        projectedPredicate);
        LOG.info(
                "Native export requested for sys.export_parquet. outputPath={}, splits={}, projection={}",
                outputPath,
                plannedSplits.size(),
                outputType.getFieldNames());
        if (factories.isEmpty()) {
            return Optional.of(
                    new NativeExportAttempt(
                            nullProvider(),
                            context,
                            NativeExportPreflightResult.rejected(
                                    NativeRejectReason.NO_PROVIDER,
                                    "no NativeExportProviderFactory found on classpath")));
        }

        NativeExportProvider provider = factories.get(0).create();
        NativeExportPreflightResult preflight = provider.preflight(context);
        return Optional.of(new NativeExportAttempt(provider, context, preflight));
    }

    private List<NativeExportSourceFile> nativeSourceFiles(Table table, List<Split> plannedSplits)
            throws IOException {
        List<NativeExportSourceFile> files = new ArrayList<>();
        for (Split split : plannedSplits) {
            if (!(split instanceof DataSplit)) {
                return Collections.emptyList();
            }
            DataSplit dataSplit = (DataSplit) split;
            Optional<List<RawFile>> rawFiles = dataSplit.convertToRawFiles();
            if (!rawFiles.isPresent()) {
                return Collections.emptyList();
            }
            Map<String, String> partition = partitionValues(table, dataSplit);
            DeletionVector.Factory dvFactory =
                    DeletionVector.factory(
                            table.fileIO(),
                            dataSplit.dataFiles(),
                            dataSplit.deletionFiles().orElse(null));
            List<RawFile> raw = rawFiles.get();
            for (int i = 0; i < raw.size(); i++) {
                RawFile rawFile = raw.get(i);
                DataFileMeta dataFile = dataSplit.dataFiles().get(i);
                files.add(
                        new NativeExportSourceFile(
                                rawFile.path(),
                                rawFile.format(),
                                rawFile.rowCount(),
                                rawFile.fileSize(),
                                rawFile.schemaId(),
                                partition,
                                deletedPositions(dvFactory, dataFile)));
            }
        }
        return files;
    }

    private static Map<String, String> partitionValues(Table table, DataSplit split) {
        Map<String, String> values = new LinkedHashMap<>();
        List<String> partitionKeys = table.partitionKeys();
        if (partitionKeys.isEmpty()) {
            return values;
        }
        RowType partitionType = table.rowType().project(partitionKeys);
        for (int i = 0; i < partitionKeys.size(); i++) {
            Object value =
                    org.apache.paimon.utils.InternalRowUtils.get(
                            split.partition(), i, partitionType.getTypeAt(i));
            values.put(partitionKeys.get(i), value == null ? null : value.toString());
        }
        return values;
    }

    private static List<Long> deletedPositions(
            DeletionVector.Factory dvFactory, DataFileMeta dataFile) throws IOException {
        Optional<DeletionVector> deletionVector = dvFactory.create(dataFile.fileName());
        if (!deletionVector.isPresent() || deletionVector.get().isEmpty()) {
            return Collections.emptyList();
        }
        List<Long> positions = new ArrayList<>();
        DeletionVector vector = deletionVector.get();
        for (long position = 0; position < dataFile.rowCount(); position++) {
            if (vector.isDeleted(position)) {
                positions.add(position);
            }
        }
        return positions;
    }

    private Options exportOptions(Table table) {
        Options options = new Options();
        for (Map.Entry<String, String> entry :
                JavaConverters.mapAsJavaMap(spark().sessionState().conf().getAllConfs())
                        .entrySet()) {
            if (entry.getKey().startsWith(PAIMON_OPTION_PREFIX)) {
                options.setString(
                        entry.getKey().substring(PAIMON_OPTION_PREFIX.length()), entry.getValue());
            }
        }
        for (scala.Tuple2<String, String> entry : spark().sparkContext().getConf().getAll()) {
            if (entry._1().startsWith(PAIMON_OPTION_PREFIX)) {
                options.setString(entry._1().substring(PAIMON_OPTION_PREFIX.length()), entry._2());
            }
        }
        options.set(CoreOptions.NATIVE_IO_INTERNAL_ENGINE, "spark");
        for (Map.Entry<String, String> entry : spark().sparkContext().hadoopConfiguration()) {
            if (entry.getKey().startsWith(OBS_CONF_PREFIX)) {
                options.setString(entry.getKey(), entry.getValue());
            } else if (entry.getKey().startsWith(SPARK_HADOOP_PREFIX + OBS_CONF_PREFIX)) {
                options.setString(
                        entry.getKey().substring(SPARK_HADOOP_PREFIX.length()), entry.getValue());
            }
        }
        for (Map.Entry<String, String> entry : table.options().entrySet()) {
            options.setString(entry.getKey(), entry.getValue());
        }
        return options;
    }

    private static NativeExportProvider nullProvider() {
        return new NativeExportProvider() {
            private static final long serialVersionUID = 1L;

            @Override
            public NativeExportPreflightResult preflight(NativeExportContext context) {
                return NativeExportPreflightResult.rejected(
                        NativeRejectReason.NO_PROVIDER,
                        "no NativeExportProviderFactory found on classpath");
            }

            @Override
            public NativeExportPlanDescriptor plan(NativeExportContext context) {
                throw new UnsupportedOperationException("No native export provider available.");
            }

            @Override
            public NativeExportTaskResult executeTask(byte[] taskPayload) {
                throw new UnsupportedOperationException("No native export provider available.");
            }
        };
    }

    private static long exportSplit(
            Table table,
            ReadBuilder readBuilder,
            Split split,
            Path outputDir,
            RowType outputType,
            @Nullable Predicate predicate,
            int outputFieldCount,
            String compression)
            throws Exception {
        TableRead read = readBuilder.newRead();
        FileIO fileIO = outputFileIO(table, outputDir);
        String fileName = "part-" + UUID.randomUUID() + ".parquet";
        Path filePath = new Path(outputDir, fileName);
        ProjectedRow outputRow = ProjectedRow.from(identityProjection(outputFieldCount));

        FormatWriter writer = null;
        long count = 0L;
        try (RecordReader<org.apache.paimon.data.InternalRow> reader = read.createReader(split);
                CloseableIterator<org.apache.paimon.data.InternalRow> iterator =
                        reader.toCloseableIterator()) {
            while (iterator.hasNext()) {
                org.apache.paimon.data.InternalRow row = iterator.next();
                if (predicate == null || predicate.test(row)) {
                    if (writer == null) {
                        writer =
                                new ParquetWriterFactory(
                                                new RowDataParquetBuilder(
                                                        outputType, new Options()))
                                        .create(
                                                fileIO.newOutputStream(filePath, false),
                                                compression);
                    }
                    writer.addElement(outputRow.replaceRow(row));
                    count++;
                }
            }
        } finally {
            if (writer != null) {
                writer.close();
            }
        }
        return count;
    }

    private static long exportSplits(
            Table table,
            ReadBuilder readBuilder,
            Iterator<SerializedSplit> splits,
            Path outputDir,
            RowType outputType,
            @Nullable Predicate predicate,
            int outputFieldCount,
            String compression,
            long targetFileSize)
            throws Exception {
        TableRead read = readBuilder.newRead();
        FileIO fileIO = outputFileIO(table, outputDir);
        ProjectedRow outputRow = ProjectedRow.from(identityProjection(outputFieldCount));

        long count = 0L;
        try (RollingParquetWriter writer =
                new RollingParquetWriter(fileIO, outputDir, outputType, compression)) {
            while (splits.hasNext()) {
                try (RecordReader<org.apache.paimon.data.InternalRow> reader =
                                read.createReader(splits.next().split());
                        CloseableIterator<org.apache.paimon.data.InternalRow> iterator =
                                reader.toCloseableIterator()) {
                    while (iterator.hasNext()) {
                        org.apache.paimon.data.InternalRow row = iterator.next();
                        if (predicate == null || predicate.test(row)) {
                            writer.addElement(outputRow.replaceRow(row), targetFileSize);
                            count++;
                        }
                    }
                }
            }
        }
        return count;
    }

    private static int numPartitions(
            int parallelism, int splitCount, List<Split> splits, @Nullable Long targetFileSize) {
        int maxPartitions = Math.max(1, Math.min(parallelism, Math.max(1, splitCount)));
        if (targetFileSize == null) {
            return maxPartitions;
        }

        long totalSize = totalSplitSize(splits);
        if (totalSize <= 0) {
            return maxPartitions;
        }

        long targetPartitions = (totalSize + targetFileSize - 1) / targetFileSize;
        return Math.max(
                1, Math.min(maxPartitions, (int) Math.min(Integer.MAX_VALUE, targetPartitions)));
    }

    private static long totalSplitSize(List<Split> splits) {
        long totalSize = 0L;
        for (Split split : splits) {
            totalSize += splitSize(split);
        }
        return totalSize;
    }

    private static long splitSize(Split split) {
        long size = 0L;
        if (split instanceof DataSplit) {
            for (DataFileMeta file : ((DataSplit) split).dataFiles()) {
                size += file.fileSize();
            }
        } else if (split instanceof IncrementalSplit) {
            IncrementalSplit incrementalSplit = (IncrementalSplit) split;
            for (DataFileMeta file : incrementalSplit.beforeFiles()) {
                size += file.fileSize();
            }
            for (DataFileMeta file : incrementalSplit.afterFiles()) {
                size += file.fileSize();
            }
        }
        return size;
    }

    private static SerializedSplit serializeSplit(Split split) {
        try {
            ByteArrayOutputStream bytes = new ByteArrayOutputStream();
            try (ObjectOutputStream out = new ObjectOutputStream(bytes)) {
                out.writeObject(split);
            }
            return new SerializedSplit(bytes.toByteArray());
        } catch (IOException e) {
            throw new RuntimeException("Failed to serialize Paimon split.", e);
        }
    }

    private static Split copySplit(Split split) {
        if (split instanceof DataSplit) {
            DataSplit dataSplit = (DataSplit) split;
            DataSplit.Builder builder =
                    DataSplit.builder()
                            .withSnapshot(dataSplit.snapshotId())
                            .withPartition(dataSplit.partition().copy())
                            .withBucket(dataSplit.bucket())
                            .withBucketPath(dataSplit.bucketPath())
                            .withTotalBuckets(dataSplit.totalBuckets())
                            .withDataFiles(dataSplit.dataFiles())
                            .rawConvertible(dataSplit.rawConvertible())
                            .isStreaming(dataSplit.isStreaming());
            if (dataSplit.deletionFiles().isPresent()) {
                builder.withDataDeletionFiles(dataSplit.deletionFiles().get());
            }
            return builder.build();
        }
        if (split instanceof IncrementalSplit) {
            IncrementalSplit incrementalSplit = (IncrementalSplit) split;
            return new IncrementalSplit(
                    incrementalSplit.snapshotId(),
                    incrementalSplit.partition().copy(),
                    incrementalSplit.bucket(),
                    incrementalSplit.totalBuckets(),
                    incrementalSplit.beforeFiles(),
                    incrementalSplit.beforeDeletionFiles(),
                    incrementalSplit.afterFiles(),
                    incrementalSplit.afterDeletionFiles(),
                    incrementalSplit.isStreaming());
        }
        return split;
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

    private static int[] parseOutputProjection(RowType rowType, String columns) {
        Preconditions.checkArgument(
                !StringUtils.isNullOrWhitespaceOnly(columns), "Columns should not be empty.");
        if ("*".equals(columns.trim())) {
            int[] projection = new int[rowType.getFieldCount()];
            for (int i = 0; i < projection.length; i++) {
                projection[i] = i;
            }
            return projection;
        }

        List<String> names =
                splitComma(columns).stream()
                        .map(ExportParquetProcedure::unquoteIdentifier)
                        .collect(Collectors.toList());
        Set<String> deduplicated = new LinkedHashSet<>(names);
        int[] projection = new int[deduplicated.size()];
        int pos = 0;
        for (String name : deduplicated) {
            int index = rowType.getFieldIndex(name);
            Preconditions.checkArgument(index >= 0, "Cannot find column '%s'.", name);
            projection[pos++] = index;
        }
        return projection;
    }

    private static int[] readProjection(
            RowType rowType, int[] outputProjection, @Nullable Predicate predicate) {
        Set<Integer> indexes = new LinkedHashSet<>();
        for (int index : outputProjection) {
            indexes.add(index);
        }
        for (String fieldName : PredicateVisitor.collectFieldNames(predicate)) {
            int index = rowType.getFieldIndex(fieldName);
            Preconditions.checkArgument(index >= 0, "Cannot find filter column '%s'.", fieldName);
            indexes.add(index);
        }

        int[] projection = new int[indexes.size()];
        int pos = 0;
        for (Integer index : indexes) {
            projection[pos++] = index;
        }
        return projection;
    }

    @Nullable
    private static Predicate parsePredicate(RowType rowType, @Nullable String where) {
        if (StringUtils.isNullOrWhitespaceOnly(where)) {
            return null;
        }

        List<Predicate> predicates = new ArrayList<>();
        PredicateBuilder builder = new PredicateBuilder(rowType);
        for (String condition : splitAnd(where)) {
            predicates.add(parseCondition(rowType, builder, condition));
        }
        return PredicateBuilder.and(predicates);
    }

    private static long parseTargetFileSize(String targetFileSize) {
        long bytes = MemorySize.parse(targetFileSize).getBytes();
        Preconditions.checkArgument(bytes > 0, "Target file size should be larger than 0 bytes.");
        return bytes;
    }

    private static Predicate parseCondition(
            RowType rowType, PredicateBuilder builder, String condition) {
        String trimmed = condition.trim();
        String lower = trimmed.toLowerCase(Locale.ROOT);
        if (lower.endsWith(" is not null")) {
            String field =
                    unquoteIdentifier(trimmed.substring(0, lower.lastIndexOf(" is not null")));
            return builder.isNotNull(fieldIndex(rowType, field));
        }
        if (lower.endsWith(" is null")) {
            String field = unquoteIdentifier(trimmed.substring(0, lower.lastIndexOf(" is null")));
            return builder.isNull(fieldIndex(rowType, field));
        }

        int inPos = indexOfKeyword(trimmed, "in");
        if (inPos > 0) {
            String field = unquoteIdentifier(trimmed.substring(0, inPos));
            String values = trimmed.substring(inPos + 2).trim();
            Preconditions.checkArgument(
                    values.startsWith("(") && values.endsWith(")"),
                    "Invalid IN predicate: %s",
                    condition);
            int index = fieldIndex(rowType, field);
            DataType type = rowType.getTypeAt(index);
            List<Object> literals =
                    splitComma(values.substring(1, values.length() - 1)).stream()
                            .map(value -> parseLiteral(type, value))
                            .collect(Collectors.toList());
            return builder.in(index, literals);
        }

        for (String op : Arrays.asList(">=", "<=", "!=", "<>", "=", ">", "<")) {
            int opPos = indexOfOperator(trimmed, op);
            if (opPos > 0) {
                String field = unquoteIdentifier(trimmed.substring(0, opPos));
                int index = fieldIndex(rowType, field);
                Object literal =
                        parseLiteral(
                                rowType.getTypeAt(index), trimmed.substring(opPos + op.length()));
                switch (op) {
                    case "=":
                        return literal == null
                                ? builder.isNull(index)
                                : builder.equal(index, literal);
                    case "!=":
                    case "<>":
                        return literal == null
                                ? builder.isNotNull(index)
                                : builder.notEqual(index, literal);
                    case ">":
                        return builder.greaterThan(index, literal);
                    case ">=":
                        return builder.greaterOrEqual(index, literal);
                    case "<":
                        return builder.lessThan(index, literal);
                    case "<=":
                        return builder.lessOrEqual(index, literal);
                    default:
                        throw new IllegalArgumentException("Unsupported operator: " + op);
                }
            }
        }

        throw new IllegalArgumentException("Unsupported predicate condition: " + condition);
    }

    private static int fieldIndex(RowType rowType, String field) {
        int index = rowType.getFieldIndex(field);
        Preconditions.checkArgument(index >= 0, "Cannot find filter column '%s'.", field);
        return index;
    }

    @Nullable
    private static Object parseLiteral(DataType type, String literal) {
        String value = stripQuotes(literal.trim());
        if ("null".equalsIgnoreCase(value)) {
            return null;
        }

        Object javaObject;
        switch (type.getTypeRoot()) {
            case BOOLEAN:
                javaObject = Boolean.parseBoolean(value);
                break;
            case TINYINT:
            case SMALLINT:
            case INTEGER:
            case BIGINT:
                javaObject = Long.parseLong(value);
                break;
            case FLOAT:
            case DOUBLE:
                javaObject = Double.parseDouble(value);
                break;
            case DECIMAL:
                javaObject = new BigDecimal(value);
                break;
            case CHAR:
            case VARCHAR:
                javaObject = value;
                break;
            case DATE:
                javaObject = LocalDate.parse(value);
                break;
            case TIME_WITHOUT_TIME_ZONE:
                javaObject = LocalTime.parse(value);
                break;
            case TIMESTAMP_WITHOUT_TIME_ZONE:
                javaObject = LocalDateTime.parse(normalizeDateTime(value));
                break;
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                javaObject =
                        hasTimeZone(value)
                                ? Instant.parse(value)
                                : Timestamp.valueOf(normalizeTimestamp(value));
                break;
            default:
                throw new UnsupportedOperationException(
                        "Unsupported filter literal type: " + type.getTypeRoot());
        }
        return PredicateBuilder.convertJavaObject(type, javaObject);
    }

    private static int[] identityProjection(int fieldCount) {
        int[] projection = new int[fieldCount];
        for (int i = 0; i < fieldCount; i++) {
            projection[i] = i;
        }
        return projection;
    }

    private static String normalizeDateTime(String value) {
        return value.indexOf('T') >= 0 ? value : value.replace(' ', 'T');
    }

    private static String normalizeTimestamp(String value) {
        return value.indexOf('T') >= 0 ? value.replace('T', ' ') : value;
    }

    private static boolean hasTimeZone(String value) {
        return value.endsWith("Z") || value.matches(".*[+-][0-9]{2}:[0-9]{2}$");
    }

    private static String stripQuotes(String value) {
        if (value.length() >= 2) {
            char first = value.charAt(0);
            char last = value.charAt(value.length() - 1);
            if ((first == '\'' && last == '\'') || (first == '"' && last == '"')) {
                return value.substring(1, value.length() - 1)
                        .replace(String.valueOf(first) + first, String.valueOf(first));
            }
        }
        return value;
    }

    private static String unquoteIdentifier(String identifier) {
        String value = identifier.trim();
        if (value.length() >= 2
                && value.charAt(0) == '`'
                && value.charAt(value.length() - 1) == '`') {
            return value.substring(1, value.length() - 1).replace("``", "`");
        }
        return value;
    }

    private static List<String> splitAnd(String input) {
        List<String> result = new ArrayList<>();
        int start = 0;
        char quote = 0;
        int parens = 0;
        for (int i = 0; i < input.length(); i++) {
            char c = input.charAt(i);
            if (quote != 0) {
                if (c == quote) {
                    quote = 0;
                }
            } else if (c == '\'' || c == '"') {
                quote = c;
            } else if (c == '(') {
                parens++;
            } else if (c == ')') {
                parens--;
            } else if (parens == 0 && matchesKeyword(input, i, "and")) {
                result.add(input.substring(start, i).trim());
                start = i + 3;
                i += 2;
            }
        }
        result.add(input.substring(start).trim());
        return result.stream().filter(s -> !s.isEmpty()).collect(Collectors.toList());
    }

    private static List<String> splitComma(String input) {
        List<String> result = new ArrayList<>();
        int start = 0;
        char quote = 0;
        int parens = 0;
        for (int i = 0; i < input.length(); i++) {
            char c = input.charAt(i);
            if (quote != 0) {
                if (c == quote) {
                    quote = 0;
                }
            } else if (c == '\'' || c == '"' || c == '`') {
                quote = c;
            } else if (c == '(') {
                parens++;
            } else if (c == ')') {
                parens--;
            } else if (c == ',' && parens == 0) {
                result.add(input.substring(start, i).trim());
                start = i + 1;
            }
        }
        result.add(input.substring(start).trim());
        return result.stream().filter(s -> !s.isEmpty()).collect(Collectors.toList());
    }

    private static int indexOfKeyword(String input, String keyword) {
        char quote = 0;
        int parens = 0;
        for (int i = 0; i < input.length(); i++) {
            char c = input.charAt(i);
            if (quote != 0) {
                if (c == quote) {
                    quote = 0;
                }
            } else if (c == '\'' || c == '"') {
                quote = c;
            } else if (c == '(') {
                parens++;
            } else if (c == ')') {
                parens--;
            } else if (parens == 0 && matchesKeyword(input, i, keyword)) {
                return i;
            }
        }
        return -1;
    }

    private static int indexOfOperator(String input, String op) {
        char quote = 0;
        for (int i = 0; i <= input.length() - op.length(); i++) {
            char c = input.charAt(i);
            if (quote != 0) {
                if (c == quote) {
                    quote = 0;
                }
            } else if (c == '\'' || c == '"') {
                quote = c;
            } else if (input.startsWith(op, i)) {
                return i;
            }
        }
        return -1;
    }

    private static boolean matchesKeyword(String input, int pos, String keyword) {
        int end = pos + keyword.length();
        if (end > input.length()) {
            return false;
        }
        if (!input.regionMatches(true, pos, keyword, 0, keyword.length())) {
            return false;
        }
        return isBoundary(input, pos - 1) && isBoundary(input, end);
    }

    private static boolean isBoundary(String input, int pos) {
        return pos < 0
                || pos >= input.length()
                || !Character.isLetterOrDigit(input.charAt(pos)) && input.charAt(pos) != '_';
    }

    private static String trimTrailingSlash(String path) {
        String result = path.trim();
        while (result.length() > 1 && result.endsWith("/")) {
            result = result.substring(0, result.length() - 1);
        }
        return result;
    }

    private static class SerializedSplit implements Serializable {

        private static final long serialVersionUID = 1L;

        private final byte[] bytes;

        private SerializedSplit(byte[] bytes) {
            this.bytes = bytes;
        }

        private Split split() {
            try (ObjectInputStream in = new ObjectInputStream(new ByteArrayInputStream(bytes))) {
                return (Split) in.readObject();
            } catch (IOException | ClassNotFoundException e) {
                throw new RuntimeException("Failed to deserialize Paimon split.", e);
            }
        }
    }

    private static class PartitionExportPlan {

        private final String partitionPath;
        private final Path outputDir;
        private final List<Split> splits = new ArrayList<>();

        private PartitionExportPlan(String partitionPath, Path outputDir) {
            this.partitionPath = partitionPath;
            this.outputDir = outputDir;
        }
    }

    private static class NativeExportAttempt {

        private final NativeExportProvider provider;
        private final NativeExportContext context;
        private final NativeExportPreflightResult preflight;

        private NativeExportAttempt(
                NativeExportProvider provider,
                NativeExportContext context,
                NativeExportPreflightResult preflight) {
            this.provider = provider;
            this.context = context;
            this.preflight = preflight;
        }
    }

    private static class RollingParquetWriter implements AutoCloseable {

        private final FileIO fileIO;
        private final Path outputDir;
        private final RowType outputType;
        private final String compression;

        private FormatWriter writer;

        private RollingParquetWriter(
                FileIO fileIO, Path outputDir, RowType outputType, String compression) {
            this.fileIO = fileIO;
            this.outputDir = outputDir;
            this.outputType = outputType;
            this.compression = compression;
        }

        private void addElement(org.apache.paimon.data.InternalRow row, long targetFileSize)
                throws IOException {
            if (writer == null) {
                writer =
                        new ParquetWriterFactory(
                                        new RowDataParquetBuilder(outputType, new Options()))
                                .create(
                                        fileIO.newOutputStream(newOutputPath(), false),
                                        compression);
            }
            writer.addElement(row);
            if (writer.reachTargetSize(true, targetFileSize)) {
                closeCurrentWriter();
            }
        }

        private Path newOutputPath() {
            return new Path(outputDir, "part-" + UUID.randomUUID() + ".parquet");
        }

        private void closeCurrentWriter() throws IOException {
            if (writer != null) {
                writer.close();
                writer = null;
            }
        }

        @Override
        public void close() throws IOException {
            closeCurrentWriter();
        }
    }

    public static ProcedureBuilder builder() {
        return new Builder<ExportParquetProcedure>() {
            @Override
            public ExportParquetProcedure doBuild() {
                return new ExportParquetProcedure(tableCatalog());
            }
        };
    }

    @Override
    public String description() {
        return "ExportParquetProcedure";
    }
}
