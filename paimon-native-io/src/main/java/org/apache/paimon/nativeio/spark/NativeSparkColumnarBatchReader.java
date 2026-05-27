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

import org.apache.paimon.deletionvectors.DeletionVector;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.io.DataFilePathFactory;
import org.apache.paimon.nativeio.NativeFileRecordReader;
import org.apache.paimon.nativeio.PaimonNativeReader;
import org.apache.paimon.operation.nativeio.NativeSplitReadContext;
import org.apache.paimon.spark.SparkTypeUtils;
import org.apache.paimon.table.source.DataSplit;
import org.apache.paimon.table.source.Split;
import org.apache.paimon.types.RowType;

import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.spark.sql.connector.read.PartitionReader;
import org.apache.spark.sql.vectorized.ColumnVector;
import org.apache.spark.sql.vectorized.ColumnarBatch;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.List;
import java.util.Optional;
import java.util.Queue;
import java.util.concurrent.atomic.AtomicBoolean;

/** Spark {@link ColumnarBatch} reader backed directly by native Arrow batches. */
class NativeSparkColumnarBatchReader implements PartitionReader<ColumnarBatch> {

    private final NativeSplitReadContext context;
    private final RowType readType;
    private final Queue<NativeFile> files;

    @Nullable private PaimonNativeReader currentReader;
    @Nullable private NativeFile currentFile;
    @Nullable private ColumnarBatch currentBatch;
    private long totalReadBatchTimeMs;

    NativeSparkColumnarBatchReader(
            NativeSplitReadContext context, RowType readType, List<Split> splits) {
        this.context = context;
        this.readType = readType;
        this.files = new ArrayDeque<>();
        for (Split split : splits) {
            DataSplit dataSplit = (DataSplit) split;
            DeletionVector.Factory dvFactory =
                    DeletionVector.factory(
                            context.fileIO(),
                            dataSplit.dataFiles(),
                            dataSplit.deletionFiles().orElse(null));
            DataFilePathFactory pathFactory =
                    context.pathFactory()
                            .createDataFilePathFactory(dataSplit.partition(), dataSplit.bucket());
            for (DataFileMeta file : dataSplit.dataFiles()) {
                this.files.add(new NativeFile(pathFactory.toPath(file).toString(), file, dvFactory));
            }
        }
    }

    @Override
    public boolean next() throws IOException {
        closeCurrentBatch();
        long startNs = System.nanoTime();
        try {
            while (true) {
                if (currentReader == null && !openNextFile()) {
                    return false;
                }

                VectorSchemaRoot root = currentReader.nextBatch();
                if (root == null) {
                    closeCurrentReader();
                    continue;
                }

                ColumnarBatch batch = toColumnarBatch(root);
                if (batch.numRows() == 0) {
                    batch.close();
                    continue;
                }
                currentBatch = batch;
                return true;
            }
        } finally {
            totalReadBatchTimeMs += (System.nanoTime() - startNs) / 1_000_000L;
        }
    }

    @Override
    public ColumnarBatch get() {
        return currentBatch;
    }

    @Override
    public void close() throws IOException {
        closeCurrentBatch();
        closeCurrentReader();
    }

    private boolean openNextFile() throws IOException {
        currentFile = files.poll();
        if (currentFile == null) {
            return false;
        }
        PaimonNativeReader reader = new PaimonNativeReader();
        try {
            reader.addFile(currentFile.path);
            reader.setBatchSize(context.nativeIOOptions().batchSize());
            reader.setRowIndexColumn(NativeFileRecordReader.ROW_INDEX_COLUMN);
            reader.setTargetColumns(readType);
            reader.setObjectStoreOptions(context.nativeIOOptions().objectStoreOptions());
            NativeFileRecordReader.addDeletionVector(
                    reader, currentFile.path, currentFile.deletionVector());
            reader.initializeReader();
            NativeFileRecordReader.validateSchema(
                    reader.schema(), readType, NativeFileRecordReader.ROW_INDEX_COLUMN);
            currentReader = reader;
            return true;
        } catch (IOException | RuntimeException e) {
            try {
                reader.close();
            } catch (IOException closeException) {
                e.addSuppressed(closeException);
            }
            throw e;
        }
    }

    private ColumnarBatch toColumnarBatch(VectorSchemaRoot root) {
        int rowCount = root.getRowCount();
        AtomicBoolean closed = new AtomicBoolean(false);
        Runnable closeRoot =
                () -> {
                    if (closed.compareAndSet(false, true)) {
                        root.close();
                    }
                };
        ColumnVector[] vectors = new ColumnVector[readType.getFieldCount()];
        org.apache.spark.sql.types.StructType sparkType =
                SparkTypeUtils.fromPaimonRowType(readType);
        for (int i = 0; i < vectors.length; i++) {
            ArrowSparkColumnVector vector =
                    new ArrowSparkColumnVector(
                            sparkType.fields()[i].dataType(),
                            root.getFieldVectors().get(i),
                            closeRoot);
            vectors[i] = vector;
        }
        return new ColumnarBatch(vectors, rowCount);
    }

    private void closeCurrentBatch() {
        if (currentBatch != null) {
            currentBatch.close();
            currentBatch = null;
        }
    }

    private void closeCurrentReader() throws IOException {
        if (currentReader != null) {
            currentReader.close();
            currentReader = null;
            currentFile = null;
        }
    }

    private static class NativeFile {

        private final String path;
        private final DataFileMeta file;
        private final DeletionVector.Factory dvFactory;
        @Nullable private DeletionVector deletionVector;
        private boolean deletionVectorLoaded;

        private NativeFile(String path, DataFileMeta file, DeletionVector.Factory dvFactory) {
            this.path = path;
            this.file = file;
            this.dvFactory = dvFactory;
        }

        @Nullable
        private DeletionVector deletionVector() {
            if (!deletionVectorLoaded) {
                try {
                    Optional<DeletionVector> optional = dvFactory.create(file.fileName());
                    deletionVector = optional.orElse(null);
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
                deletionVectorLoaded = true;
            }
            return deletionVector;
        }
    }
}
