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

package org.apache.paimon.nativeio;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.deletionvectors.ApplyDeletionVectorReader;
import org.apache.paimon.deletionvectors.DeletionVector;
import org.apache.paimon.disk.IOManager;
import org.apache.paimon.fileindex.FileIndexResult;
import org.apache.paimon.fileindex.bitmap.ApplyBitmapIndexRecordReader;
import org.apache.paimon.fileindex.bitmap.BitmapIndexResult;
import org.apache.paimon.format.FormatKey;
import org.apache.paimon.format.FormatReaderContext;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.io.DataFilePathFactory;
import org.apache.paimon.io.DataFileRecordReader;
import org.apache.paimon.io.FileIndexEvaluator;
import org.apache.paimon.mergetree.compact.ConcatRecordReader;
import org.apache.paimon.operation.SplitRead;
import org.apache.paimon.operation.nativeio.NativeApplicability;
import org.apache.paimon.operation.nativeio.NativeRejectReason;
import org.apache.paimon.operation.nativeio.NativeSplitReadContext;
import org.apache.paimon.operation.nativeio.SupportsNativeIO;
import org.apache.paimon.partition.PartitionUtils;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.predicate.TopN;
import org.apache.paimon.reader.EmptyFileRecordReader;
import org.apache.paimon.reader.FileRecordReader;
import org.apache.paimon.reader.ReaderSupplier;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.table.source.DataSplit;
import org.apache.paimon.table.source.DeletionFile;
import org.apache.paimon.table.source.Split;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.FormatReaderMapping;
import org.apache.paimon.utils.IOExceptionSupplier;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.paimon.predicate.PredicateBuilder.splitAnd;
import static org.apache.paimon.table.SpecialFields.rowTypeWithRowTracking;

/** Native raw file split read. */
public class NativeRawFileSplitRead implements SplitRead<InternalRow> {

    private final NativeSplitReadContext context;
    private final Map<FormatKey, FormatReaderMapping> formatReaderMappings = new HashMap<>();
    @Nullable private RowType readType;
    @Nullable private List<Predicate> filters;
    @Nullable private TopN topN;
    @Nullable private Integer limit;
    private boolean forceKeepDelete;

    public NativeRawFileSplitRead(NativeSplitReadContext context) {
        this.context = context;
    }

    @Override
    public SplitRead<InternalRow> forceKeepDelete() {
        this.forceKeepDelete = true;
        return this;
    }

    @Override
    public SplitRead<InternalRow> withIOManager(@Nullable IOManager ioManager) {
        return this;
    }

    @Override
    public SplitRead<InternalRow> withReadType(RowType readType) {
        this.readType = readType;
        this.formatReaderMappings.clear();
        return this;
    }

    @Override
    public SplitRead<InternalRow> withFilter(@Nullable Predicate predicate) {
        if (predicate != null) {
            this.filters = splitAnd(predicate);
            this.formatReaderMappings.clear();
        }
        return this;
    }

    @Override
    public SplitRead<InternalRow> withTopN(@Nullable TopN topN) {
        this.topN = topN;
        this.formatReaderMappings.clear();
        return this;
    }

    @Override
    public SplitRead<InternalRow> withLimit(@Nullable Integer limit) {
        this.limit = limit;
        this.formatReaderMappings.clear();
        return this;
    }

    @Override
    public RecordReader<InternalRow> createReader(Split split) throws IOException {
        DataSplit dataSplit = (DataSplit) split;
        return createReader(
                dataSplit.partition(),
                dataSplit.bucket(),
                dataSplit.dataFiles(),
                dataSplit.deletionFiles().orElse(null));
    }

    boolean supportsCurrentReadConfig() {
        return currentReadConfigApplicability().applicable();
    }

    NativeApplicability currentReadConfigApplicability() {
        if (!context.coreOptions().deletionVectorsEnabled()) {
            return NativeApplicability.rejected(
                    NativeRejectReason.NOT_DV_TABLE,
                    "native IO PoC only supports deletion-vector primary-key tables");
        }
        if (forceKeepDelete) {
            return NativeApplicability.rejected(
                    NativeRejectReason.FORCE_KEEP_DELETE, "force keep delete is enabled");
        }
        if (filters != null || topN != null || limit != null) {
            return NativeApplicability.rejected(
                    NativeRejectReason.DATA_FILTER_TOPN_LIMIT,
                    "native IO does not support filter, topN, or limit pushdown yet");
        }
        if (SupportsNativeIO.hasUnsupportedTypes(currentReadType())) {
            return NativeApplicability.rejected(
                    NativeRejectReason.UNSUPPORTED_TYPE,
                    "native IO does not support at least one requested field type");
        }
        return NativeApplicability.yes();
    }

    private RecordReader<InternalRow> createReader(
            BinaryRow partition,
            int bucket,
            List<DataFileMeta> files,
            List<DeletionFile> deletionFiles)
            throws IOException {
        DeletionVector.Factory dvFactory =
                DeletionVector.factory(context.fileIO(), files, deletionFiles);
        Map<String, IOExceptionSupplier<DeletionVector>> dvFactories = new HashMap<>();
        for (DataFileMeta file : files) {
            dvFactories.put(file.fileName(), () -> dvFactory.create(file.fileName()).orElse(null));
        }

        DataFilePathFactory dataFilePathFactory =
                context.pathFactory().createDataFilePathFactory(partition, bucket);
        List<ReaderSupplier<InternalRow>> suppliers = new ArrayList<>();

        FormatReaderMapping.Builder formatReaderMappingBuilder =
                new FormatReaderMapping.Builder(
                        context.fileFormatDiscover(),
                        currentReadType().getFields(),
                        schema -> {
                            if (context.coreOptions().rowTrackingEnabled()) {
                                return rowTypeWithRowTracking(schema.logicalRowType(), true, true)
                                        .getFields();
                            }
                            return schema.fields();
                        },
                        filters,
                        topN,
                        limit);

        for (DataFileMeta file : files) {
            suppliers.add(
                    () ->
                            createFileReader(
                                    partition,
                                    file,
                                    dataFilePathFactory,
                                    formatReaderMapping(file, formatReaderMappingBuilder),
                                    dvFactories.get(file.fileName())));
        }
        return ConcatRecordReader.create(suppliers);
    }

    private FormatReaderMapping formatReaderMapping(
            DataFileMeta file, FormatReaderMapping.Builder formatReaderMappingBuilder) {
        String formatIdentifier = DataFilePathFactory.formatIdentifier(file.fileName());
        long schemaId = file.schemaId();
        TableSchema tableSchema = context.tableSchema();
        return formatReaderMappings.computeIfAbsent(
                new FormatKey(schemaId, formatIdentifier),
                key ->
                        formatReaderMappingBuilder.build(
                                formatIdentifier,
                                tableSchema,
                                schemaId == tableSchema.id()
                                        ? tableSchema
                                        : context.schemaManager().schema(schemaId)));
    }

    private FileRecordReader<InternalRow> createFileReader(
            BinaryRow partition,
            DataFileMeta file,
            DataFilePathFactory dataFilePathFactory,
            FormatReaderMapping formatReaderMapping,
            @Nullable IOExceptionSupplier<DeletionVector> dvFactory)
            throws IOException {
        RowType actualReadRowType = formatReaderMapping.getActualReadRowType();
        if (actualReadRowType == null) {
            actualReadRowType = currentReadType();
        }
        DeletionVector deletionVector = dvFactory == null ? null : dvFactory.get();
        FileIndexResult fileIndexResult =
                evaluateFileIndex(file, dataFilePathFactory, formatReaderMapping, deletionVector);
        if (fileIndexResult != null && !fileIndexResult.remain()) {
            return new EmptyFileRecordReader<>();
        }

        FileRecordReader<InternalRow> fileRecordReader;
        NativeApplicability indexApplicability =
                SupportsNativeIO.checkFileIndexResult(fileIndexResult);
        NativeApplicability fileApplicability =
                SupportsNativeIO.checkNativeFile(
                        file,
                        formatReaderMapping,
                        actualReadRowType,
                        context.coreOptions().rowTrackingEnabled(),
                        context.nativeIOOptions(),
                        dataFilePathFactory.toPath(file));
        if (indexApplicability.applicable() && fileApplicability.applicable()) {
            FormatReaderContext formatReaderContext =
                    new FormatReaderContext(
                            context.fileIO(), dataFilePathFactory.toPath(file), file.fileSize());
            NativeFormatReaderFactory readerFactory =
                    new NativeFormatReaderFactory(
                            actualReadRowType,
                            file.rowCount(),
                            context.nativeIOOptions().batchSize(),
                            context.nativeIOOptions().maxBatchBytes().getBytes(),
                            context.nativeIOOptions().objectStoreOptions());
            CoreOptions coreOptions = context.coreOptions();
            fileRecordReader =
                    new DataFileRecordReader(
                            context.tableSchema().logicalRowType(),
                            readerFactory,
                            formatReaderContext,
                            coreOptions.scanIgnoreCorruptFile(),
                            coreOptions.scanIgnoreLostFile(),
                            formatReaderMapping.getIndexMapping(),
                            formatReaderMapping.getCastMapping(),
                            PartitionUtils.create(
                                    formatReaderMapping.getPartitionPair(), partition),
                            coreOptions.rowTrackingEnabled(),
                            file.firstRowId(),
                            file.maxSequenceNumber(),
                            formatReaderMapping.getSystemFields());
        } else {
            context.reporter()
                    .report(
                            indexApplicability.applicable()
                                    ? fileApplicability
                                    : indexApplicability);
            fileRecordReader =
                    javaFileRecordReader(
                            partition,
                            file,
                            dataFilePathFactory,
                            formatReaderMapping,
                            fileIndexResult);
        }

        if (deletionVector != null && !deletionVector.isEmpty()) {
            return new ApplyDeletionVectorReader(fileRecordReader, deletionVector);
        }
        return fileRecordReader;
    }

    @Nullable
    private FileIndexResult evaluateFileIndex(
            DataFileMeta file,
            DataFilePathFactory dataFilePathFactory,
            FormatReaderMapping formatReaderMapping,
            @Nullable DeletionVector deletionVector)
            throws IOException {
        if (!context.coreOptions().fileIndexReadEnabled()) {
            return null;
        }
        return FileIndexEvaluator.evaluate(
                context.fileIO(),
                formatReaderMapping.getDataSchema(),
                formatReaderMapping.getDataFilters(),
                formatReaderMapping.getTopN(),
                formatReaderMapping.getLimit(),
                dataFilePathFactory,
                file,
                deletionVector);
    }

    private FileRecordReader<InternalRow> javaFileRecordReader(
            BinaryRow partition,
            DataFileMeta file,
            DataFilePathFactory dataFilePathFactory,
            FormatReaderMapping formatReaderMapping,
            @Nullable FileIndexResult fileIndexResult)
            throws IOException {
        FormatReaderContext formatReaderContext =
                new FormatReaderContext(
                        context.fileIO(),
                        dataFilePathFactory.toPath(file),
                        file.fileSize(),
                        fileIndexResult instanceof BitmapIndexResult
                                ? ((BitmapIndexResult) fileIndexResult).get()
                                : null);
        CoreOptions coreOptions = context.coreOptions();
        FileRecordReader<InternalRow> fileRecordReader =
                new DataFileRecordReader(
                        context.tableSchema().logicalRowType(),
                        formatReaderMapping.getReaderFactory(),
                        formatReaderContext,
                        coreOptions.scanIgnoreCorruptFile(),
                        coreOptions.scanIgnoreLostFile(),
                        formatReaderMapping.getIndexMapping(),
                        formatReaderMapping.getCastMapping(),
                        PartitionUtils.create(formatReaderMapping.getPartitionPair(), partition),
                        coreOptions.rowTrackingEnabled(),
                        file.firstRowId(),
                        file.maxSequenceNumber(),
                        formatReaderMapping.getSystemFields());
        if (fileIndexResult instanceof BitmapIndexResult) {
            return new ApplyBitmapIndexRecordReader(
                    fileRecordReader, (BitmapIndexResult) fileIndexResult);
        }
        return fileRecordReader;
    }

    private RowType currentReadType() {
        return readType == null ? context.rowType() : readType;
    }

    NativeSplitReadContext context() {
        return context;
    }

    @Nullable
    RowType readType() {
        return readType;
    }

    @Nullable
    Predicate predicate() {
        return filters == null || filters.size() != 1 ? null : filters.get(0);
    }

    @Nullable
    TopN topN() {
        return topN;
    }

    @Nullable
    Integer limit() {
        return limit;
    }
}
