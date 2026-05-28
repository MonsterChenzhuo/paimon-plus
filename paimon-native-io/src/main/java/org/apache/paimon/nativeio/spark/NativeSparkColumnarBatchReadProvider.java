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

package org.apache.paimon.nativeio.spark;

import org.apache.paimon.fs.Path;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.io.DataFilePathFactory;
import org.apache.paimon.nativeio.jnr.PaimonJnrLoader;
import org.apache.paimon.operation.nativeio.NativeApplicability;
import org.apache.paimon.operation.nativeio.NativeRejectReason;
import org.apache.paimon.operation.nativeio.NativeSplitReadContext;
import org.apache.paimon.operation.nativeio.SupportsNativeIO;
import org.apache.paimon.spark.NativeColumnarBatchReadProvider;
import org.apache.paimon.spark.PaimonInputPartition;
import org.apache.paimon.spark.schema.PaimonMetadataColumn;
import org.apache.paimon.table.source.DataSplit;
import org.apache.paimon.table.source.ReadBuilder;
import org.apache.paimon.table.source.Split;
import org.apache.paimon.table.source.splitread.SplitReadProvider;

import org.apache.spark.sql.connector.read.PartitionReader;
import org.apache.spark.sql.vectorized.ColumnarBatch;

import java.util.List;

import scala.collection.JavaConverters;

/** Spark ColumnarBatch provider backed by paimon-native-io Arrow batches. */
public class NativeSparkColumnarBatchReadProvider implements NativeColumnarBatchReadProvider {

    @Override
    public boolean supportColumnarReads(
            ReadBuilder readBuilder,
            PaimonInputPartition partition,
            List<PaimonMetadataColumn> metadataColumns) {
        NativeSplitReadContext context = readBuilder.nativeSplitReadContext();
        if (context == null) {
            return false;
        }
        NativeApplicability applicability =
                supportApplicability(readBuilder, partition, context, metadataColumns);
        context.reporter().report(applicability);
        return applicability.applicable();
    }

    @Override
    public PartitionReader<ColumnarBatch> createColumnarReader(
            ReadBuilder readBuilder,
            PaimonInputPartition partition,
            List<PaimonMetadataColumn> metadataColumns) {
        NativeSplitReadContext context = readBuilder.nativeSplitReadContext();
        if (context == null) {
            throw new IllegalStateException("Native split read context is not available.");
        }
        return new NativeSparkColumnarBatchReader(
                context,
                readBuilder.readType(),
                JavaConverters.seqAsJavaListConverter(partition.splits()).asJava());
    }

    private NativeApplicability supportApplicability(
            ReadBuilder readBuilder,
            PaimonInputPartition partition,
            NativeSplitReadContext context,
            List<PaimonMetadataColumn> metadataColumns) {
        if (!metadataColumns.isEmpty()) {
            return NativeApplicability.rejected(
                    NativeRejectReason.PARTITION_OR_SYSTEM_FIELDS,
                    "native columnar IO does not support metadata columns yet");
        }
        if (!context.nativeIOOptions().columnarEnabled()) {
            return NativeApplicability.rejected(
                    NativeRejectReason.DISABLED, "native columnar IO is disabled");
        }
        NativeApplicability engine = SupportsNativeIO.checkEngine(context.nativeIOOptions());
        if (!engine.applicable()) {
            return engine;
        }
        if (!readBuilder.supportsNativeColumnarRead()) {
            return NativeApplicability.rejected(
                    NativeRejectReason.DATA_FILTER_TOPN_LIMIT,
                    "native columnar IO does not support filter, topN, limit, or row ranges yet");
        }
        if (!PaimonJnrLoader.current().hasNativeResource()) {
            return NativeApplicability.rejected(
                    NativeRejectReason.NO_PROVIDER, "paimon native IO library is not available");
        }
        if (!context.tableSchema().partitionKeys().isEmpty()) {
            return NativeApplicability.rejected(
                    NativeRejectReason.PARTITION_OR_SYSTEM_FIELDS,
                    "native columnar IO does not support partition fields yet");
        }
        if (context.coreOptions().rowTrackingEnabled()) {
            return NativeApplicability.rejected(
                    NativeRejectReason.PARTITION_OR_SYSTEM_FIELDS,
                    "native columnar IO does not support row tracking fields yet");
        }
        if (SupportsNativeIO.hasUnsupportedTypes(readBuilder.readType())) {
            return NativeApplicability.rejected(
                    NativeRejectReason.UNSUPPORTED_TYPE,
                    "native columnar IO does not support at least one requested field type");
        }

        List<Split> splits = JavaConverters.seqAsJavaListConverter(partition.splits()).asJava();
        for (Split split : splits) {
            if (!(split instanceof DataSplit)) {
                return NativeApplicability.rejected(
                        NativeRejectReason.NOT_RAW_CONVERTIBLE,
                        "native columnar IO only supports data splits");
            }
            DataSplit dataSplit = (DataSplit) split;
            NativeApplicability splitApplicability =
                    SupportsNativeIO.checkNativeSplit(
                            dataSplit,
                            context.nativeIOOptions(),
                            new SplitReadProvider.Context(false),
                            true);
            if (!splitApplicability.applicable()) {
                return splitApplicability;
            }
            for (DataFileMeta file : dataSplit.dataFiles()) {
                if (file.schemaId() != context.tableSchema().id()) {
                    return NativeApplicability.rejected(
                            NativeRejectReason.SCHEMA_CAST_OR_REORDER,
                            "native columnar IO does not support schema evolution yet");
                }
                DataFilePathFactory pathFactory =
                        context.pathFactory()
                                .createDataFilePathFactory(
                                        dataSplit.partition(), dataSplit.bucket());
                Path actualDataPath = pathFactory.toPath(file);
                NativeApplicability fileApplicability =
                        SupportsNativeIO.checkNativePhysicalFile(
                                file,
                                readBuilder.readType(),
                                context.coreOptions().rowTrackingEnabled(),
                                false,
                                context.nativeIOOptions(),
                                actualDataPath);
                if (!fileApplicability.applicable()) {
                    return fileApplicability;
                }
            }
        }
        return NativeApplicability.yes();
    }
}
