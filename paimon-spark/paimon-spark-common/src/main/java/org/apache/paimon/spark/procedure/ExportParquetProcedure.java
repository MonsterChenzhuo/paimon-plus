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
import org.apache.paimon.format.FormatWriter;
import org.apache.paimon.format.parquet.ParquetWriterFactory;
import org.apache.paimon.format.parquet.writer.RowDataParquetBuilder;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.FileStatus;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.PositionOutputStream;
import org.apache.paimon.io.DataFileMeta;
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
import org.apache.paimon.table.source.ReadBuilder;
import org.apache.paimon.table.source.Split;
import org.apache.paimon.table.source.TableRead;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.CloseableIterator;
import org.apache.paimon.utils.InternalRowPartitionComputer;
import org.apache.paimon.utils.JsonSerdeUtil;
import org.apache.paimon.utils.PartitionPathUtils;
import org.apache.paimon.utils.Preconditions;
import org.apache.paimon.utils.ProjectedRow;
import org.apache.paimon.utils.Projection;
import org.apache.paimon.utils.StringUtils;

import org.apache.spark.api.java.JavaSparkContext;
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
import java.util.Comparator;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletionService;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;

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
    private static final String SUCCESS_FILE_NAME = "_SUCCESS";
    private static final String MANIFEST_FILE_NAME = "_manifest.json";
    private static final String SPARK_SCHEDULER_MODE = "spark.scheduler.mode";
    private static final String SPARK_SCHEDULER_MODE_FAIR = "FAIR";
    private static final String SPARK_SCHEDULER_POOL = "spark.scheduler.pool";
    private static final String PARTITION_EXPORT_POOL_PREFIX = "paimon-export-parquet-partition-";

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
                ProcedureParameter.optional("partition_job_parallelism", IntegerType)
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
        Integer partitionJobParallelism = args.isNullAt(9) ? null : args.getInt(9);

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
                            partitionJobParallelism);
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
            @Nullable Integer partitionJobParallelism)
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
        String normalizedOutputPath = trimTrailingSlash(outputPath);
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
                    normalizedOutputPath);
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
        writeManifestAndSuccess(table, outputDir, normalizedOutputPath);
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
            @Nullable Integer partitionJobParallelism,
            String compression,
            boolean overwrite,
            @Nullable Long targetFileSize,
            String basePath)
            throws Exception {
        FileIO outputFileIO = outputFileIO(table, outputDir);
        prepareOutputDirectory(outputFileIO, outputDir, overwrite);

        List<PartitionExportPlan> partitions =
                partitionExportPlans(table, plannedSplits, outputDir);
        if (partitions.isEmpty()) {
            writeManifestAndSuccess(table, outputDir, basePath);
            return 0L;
        }

        int jobParallelism = partitionJobParallelism(partitionJobParallelism, partitions.size());
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
                                targetFileSize);
            }
        } else {
            configurePartitionExportFairScheduling(jobParallelism);
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
                            targetFileSize);
        }

        writeManifestAndSuccess(table, outputDir, basePath);
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
            @Nullable Long targetFileSize)
            throws Exception {
        ExecutorService exportExecutor = Executors.newFixedThreadPool(jobParallelism);
        try {
            CompletionService<PartitionExportResult> exportCompletion =
                    new ExecutorCompletionService<>(exportExecutor);
            for (int i = 0; i < partitions.size(); i++) {
                final int partitionIndex = i;
                PartitionExportPlan partition = partitions.get(i);
                String schedulerPool = PARTITION_EXPORT_POOL_PREFIX + i;
                exportCompletion.submit(
                        new Callable<PartitionExportResult>() {
                            @Override
                            public PartitionExportResult call() throws Exception {
                                Long rows =
                                        withSparkSchedulerPool(
                                                schedulerPool,
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
                                                                targetFileSize);
                                                    }
                                                });
                                return new PartitionExportResult(partitionIndex, partition, rows);
                            }
                        });
            }

            long rows = 0L;
            for (int i = 0; i < partitions.size(); i++) {
                PartitionExportResult result = exportCompletion.take().get();
                rows += result.rows;
            }
            return rows;
        } catch (ExecutionException e) {
            Throwable cause = e.getCause();
            if (cause instanceof Exception) {
                throw (Exception) cause;
            }
            throw new RuntimeException(cause);
        } finally {
            exportExecutor.shutdownNow();
        }
    }

    private Long withSparkSchedulerPool(String schedulerPool, Callable<Long> callable)
            throws Exception {
        String previousPool = spark().sparkContext().getLocalProperty(SPARK_SCHEDULER_POOL);
        spark().sparkContext().setLocalProperty(SPARK_SCHEDULER_POOL, schedulerPool);
        try {
            return callable.call();
        } finally {
            spark().sparkContext().setLocalProperty(SPARK_SCHEDULER_POOL, previousPool);
        }
    }

    private void configurePartitionExportFairScheduling(int jobParallelism) {
        String schedulerMode = spark().sparkContext().conf().get(SPARK_SCHEDULER_MODE, "FIFO");
        if (SPARK_SCHEDULER_MODE_FAIR.equalsIgnoreCase(schedulerMode)) {
            LOG.info(
                    "Submitting partitioned sys.export_parquet jobs with FAIR scheduler pools. "
                            + "jobParallelism={}",
                    jobParallelism);
        } else {
            LOG.warn(
                    "Submitting partitioned sys.export_parquet jobs concurrently, but Spark scheduler "
                            + "mode is {}. Set {}={} before creating SparkContext to schedule "
                            + "independent partition jobs fairly.",
                    schedulerMode,
                    SPARK_SCHEDULER_MODE,
                    SPARK_SCHEDULER_MODE_FAIR);
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
            @Nullable Long targetFileSize)
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
        writeSuccessFile(outputFileIO(table, partition.outputDir), partition.outputDir);
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

    private static void writeManifestAndSuccess(Table table, Path outputDir, String basePath)
            throws IOException {
        FileIO fileIO = outputFileIO(table, outputDir);
        writeManifest(fileIO, outputDir, basePath);
        writeSuccessFile(fileIO, outputDir);
    }

    static void writeManifest(FileIO fileIO, Path outputDir, String basePath) throws IOException {
        List<FileStatus> parquetFiles = new ArrayList<>();
        for (FileStatus status : fileIO.listFiles(outputDir, true)) {
            if (!status.isDir() && status.getPath().getName().endsWith(".parquet")) {
                parquetFiles.add(status);
            }
        }
        parquetFiles.sort(Comparator.comparing(status -> status.getPath().toString()));

        List<Map<String, String>> files = new ArrayList<>();
        for (FileStatus parquetFile : parquetFiles) {
            Map<String, String> file = new LinkedHashMap<>();
            file.put("path", relativePath(outputDir, parquetFile.getPath()));
            files.add(file);
        }

        Map<String, Object> manifest = new LinkedHashMap<>();
        manifest.put("base_path", basePath);
        manifest.put("files", files);
        fileIO.writeFile(
                new Path(outputDir, MANIFEST_FILE_NAME), JsonSerdeUtil.toJson(manifest), true);
    }

    private static String relativePath(Path base, Path file) {
        String basePath = trimTrailingSlash(base.toString());
        String filePath = file.toString();
        String prefix = basePath + Path.SEPARATOR;
        if (filePath.startsWith(prefix)) {
            return filePath.substring(prefix.length());
        }

        String baseUriPath = trimTrailingSlash(base.toUri().getPath());
        String fileUriPath = file.toUri().getPath();
        String uriPrefix = baseUriPath + Path.SEPARATOR;
        if (fileUriPath.startsWith(uriPrefix)) {
            return fileUriPath.substring(uriPrefix.length());
        }

        return file.getName();
    }

    private static void writeSuccessFile(FileIO fileIO, Path outputDir) throws IOException {
        try (PositionOutputStream ignored =
                fileIO.newOutputStream(new Path(outputDir, SUCCESS_FILE_NAME), true)) {
            // close creates the marker file.
        }
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
        return rows;
    }

    static int partitionJobParallelism(
            @Nullable Integer configuredParallelism, int partitionCount) {
        if (configuredParallelism == null) {
            return Math.max(1, partitionCount);
        }
        return Math.max(
                1, Math.min(Math.max(1, configuredParallelism), Math.max(1, partitionCount)));
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

    private static class PartitionExportResult {

        private final int partitionIndex;
        private final PartitionExportPlan partition;
        private final long rows;

        private PartitionExportResult(
                int partitionIndex, PartitionExportPlan partition, long rows) {
            this.partitionIndex = partitionIndex;
            this.partition = partition;
            this.rows = rows;
        }
    }

    private static class ParquetFileGroup {

        private final List<FileStatus> files = new ArrayList<>();
        private long size;
    }

    private static class ParquetCompactTask implements Serializable {

        private static final long serialVersionUID = 1L;

        private final List<ParquetCompactFile> inputFiles;
        private final String outputFile;

        private ParquetCompactTask(List<ParquetCompactFile> inputFiles, String outputFile) {
            this.inputFiles = inputFiles;
            this.outputFile = outputFile;
        }
    }

    private static class ParquetCompactFile implements Serializable {

        private static final long serialVersionUID = 1L;

        private final String path;
        private final long length;

        private ParquetCompactFile(String path, long length) {
            this.path = path;
            this.length = length;
        }
    }

    private static class CompactFileStatus implements FileStatus, Serializable {

        private static final long serialVersionUID = 1L;

        private final String path;
        private final long length;

        private CompactFileStatus(String path, long length) {
            this.path = path;
            this.length = length;
        }

        @Override
        public long getLen() {
            return length;
        }

        @Override
        public boolean isDir() {
            return false;
        }

        @Override
        public Path getPath() {
            return new Path(path);
        }

        @Override
        public long getModificationTime() {
            return 0L;
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
