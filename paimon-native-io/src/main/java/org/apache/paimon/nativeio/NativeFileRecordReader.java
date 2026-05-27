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

import org.apache.paimon.arrow.reader.ArrowBatchReader;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.deletionvectors.DeletionVector;
import org.apache.paimon.fs.Path;
import org.apache.paimon.io.NonCorruptFileReadException;
import org.apache.paimon.reader.FileRecordIterator;
import org.apache.paimon.reader.FileRecordReader;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.DecimalType;
import org.apache.paimon.types.RowType;
import org.apache.paimon.types.TimestampType;

import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.BitVector;
import org.apache.arrow.vector.DateDayVector;
import org.apache.arrow.vector.DecimalVector;
import org.apache.arrow.vector.Float4Vector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.SmallIntVector;
import org.apache.arrow.vector.TimeMilliVector;
import org.apache.arrow.vector.TimeStampVector;
import org.apache.arrow.vector.TinyIntVector;
import org.apache.arrow.vector.ValueVector;
import org.apache.arrow.vector.VarBinaryVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.DateUnit;
import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.TimeUnit;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.ArrowType.ArrowTypeID;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/** FileRecordReader backed by libpaimon_native_io Arrow batches. */
public class NativeFileRecordReader implements FileRecordReader<InternalRow> {

    public static final String ROW_INDEX_COLUMN = "__paimon_native_row_index";
    private static final int DELETED_POSITION_CHUNK_SIZE = 8192;

    private final NativeBatchReader nativeReader;
    private final Path filePath;
    private final RowType readRowType;
    private final String rowIndexColumn;
    private final long fileRowCount;
    private final long maxBatchBytes;
    private final List<NativeFileRecordIterator> outstandingBatches = new ArrayList<>();
    private long lastRowIndex = -1L;
    private boolean closed;

    public NativeFileRecordReader(
            String file,
            RowType readRowType,
            long fileRowCount,
            int batchSize,
            Map<String, String> objectStoreOptions)
            throws IOException {
        this(file, readRowType, fileRowCount, batchSize, Long.MAX_VALUE, objectStoreOptions);
    }

    public NativeFileRecordReader(
            String file,
            RowType readRowType,
            long fileRowCount,
            int batchSize,
            long maxBatchBytes,
            Map<String, String> objectStoreOptions)
            throws IOException {
        this(file, readRowType, fileRowCount, batchSize, maxBatchBytes, objectStoreOptions, null);
    }

    public NativeFileRecordReader(
            String file,
            RowType readRowType,
            long fileRowCount,
            int batchSize,
            long maxBatchBytes,
            Map<String, String> objectStoreOptions,
            @Nullable DeletionVector deletionVector)
            throws IOException {
        this.filePath = new Path(file);
        this.readRowType = readRowType;
        this.rowIndexColumn = rowIndexColumn(readRowType);
        this.fileRowCount = fileRowCount;
        this.maxBatchBytes = maxBatchBytes;
        PaimonNativeReader reader = new PaimonNativeReader();
        try {
            reader.addFile(file);
            reader.setBatchSize(batchSize);
            reader.setRowIndexColumn(rowIndexColumn);
            reader.setTargetColumns(readRowType);
            reader.setObjectStoreOptions(objectStoreOptions);
            addDeletionVector(reader, file, deletionVector);
            reader.initializeReader();
            validateSchema(reader.schema(), readRowType, rowIndexColumn);
            this.nativeReader = reader;
        } catch (IOException | RuntimeException e) {
            try {
                reader.close();
            } catch (IOException closeException) {
                e.addSuppressed(closeException);
            }
            throw e;
        }
    }

    public NativeFileRecordReader(
            String file, RowType readRowType, int batchSize, Map<String, String> objectStoreOptions)
            throws IOException {
        this(file, readRowType, -1L, batchSize, objectStoreOptions);
    }

    public static void addDeletionVector(
            PaimonNativeReader reader, String file, @Nullable DeletionVector deletionVector)
            throws IOException {
        if (deletionVector == null || deletionVector.isEmpty()) {
            return;
        }
        IOException[] failure = new IOException[1];
        long[] positions = new long[DELETED_POSITION_CHUNK_SIZE];
        int[] positionCount = new int[1];
        deletionVector.forEachDeletedPosition(
                position -> {
                    if (failure[0] != null) {
                        return;
                    }
                    positions[positionCount[0]++] = position;
                    if (positionCount[0] == positions.length) {
                        flushDeletedPositions(reader, file, positions, positionCount, failure);
                    }
                });
        flushDeletedPositions(reader, file, positions, positionCount, failure);
        if (failure[0] != null) {
            throw failure[0];
        }
    }

    private static void flushDeletedPositions(
            PaimonNativeReader reader,
            String file,
            long[] positions,
            int[] positionCount,
            IOException[] failure) {
        if (positionCount[0] == 0 || failure[0] != null) {
            return;
        }
        try {
            reader.addDeletedPositions(file, positions, positionCount[0]);
            positionCount[0] = 0;
        } catch (IOException e) {
            failure[0] = e;
        }
    }

    NativeFileRecordReader(
            NativeBatchReader nativeReader, Path filePath, RowType readRowType, long fileRowCount) {
        this(nativeReader, filePath, readRowType, fileRowCount, Long.MAX_VALUE);
    }

    NativeFileRecordReader(
            NativeBatchReader nativeReader,
            Path filePath,
            RowType readRowType,
            long fileRowCount,
            long maxBatchBytes) {
        this.nativeReader = nativeReader;
        this.filePath = filePath;
        this.readRowType = readRowType;
        this.rowIndexColumn = rowIndexColumn(readRowType);
        this.fileRowCount = fileRowCount;
        this.maxBatchBytes = maxBatchBytes;
    }

    @Override
    public FileRecordIterator<InternalRow> readBatch() throws IOException {
        if (closed) {
            throw nativeReadException("native file record reader is closed");
        }
        VectorSchemaRoot root = nativeReader.nextBatch();
        if (root == null) {
            return null;
        }
        BigIntVector rowIndexVector;
        try {
            rowIndexVector = validateBatch(root);
        } catch (IOException | RuntimeException e) {
            root.close();
            throw e;
        }
        NativeFileRecordIterator iterator =
                new NativeFileRecordIterator(
                        new ArrowBatchReader(readRowType, true).readBatch(root).iterator(),
                        rowIndexVector,
                        filePath,
                        root,
                        outstandingBatches);
        outstandingBatches.add(iterator);
        return iterator;
    }

    @Override
    public void close() throws IOException {
        if (closed) {
            return;
        }
        closed = true;
        for (NativeFileRecordIterator iterator : new ArrayList<>(outstandingBatches)) {
            iterator.releaseBatch();
        }
        nativeReader.close();
    }

    private BigIntVector validateBatch(VectorSchemaRoot root) throws IOException {
        int rowCount = root.getRowCount();
        validateSchema(root);
        if (rowCount == 0) {
            throw nativeReadException("native output contains a zero-row batch");
        }
        long batchBytes = estimateBatchBytes(root, rowCount);
        if (batchBytes > maxBatchBytes) {
            throw nativeReadException(
                    String.format(
                            "native batch size %s exceeds native-io.max-batch-bytes %s",
                            batchBytes, maxBatchBytes));
        }
        ValueVector rowIndexVector = root.getVector(rowIndexColumn);
        if (!(rowIndexVector instanceof BigIntVector)) {
            throw nativeReadException("native row index column is missing or not BIGINT");
        }
        if (rowIndexVector.getValueCount() != rowCount) {
            throw nativeReadException(
                    String.format(
                            "native row index value count mismatch: expected %s, actual %s",
                            rowCount, rowIndexVector.getValueCount()));
        }

        for (DataField field : readRowType.getFields()) {
            ValueVector vector = root.getVector(field.name());
            if (vector == null) {
                throw nativeReadException("native output is missing field: " + field.name());
            }
            if (vector.getValueCount() != rowCount) {
                throw nativeReadException(
                        String.format(
                                "native field %s value count mismatch: expected %s, actual %s",
                                field.name(), rowCount, vector.getValueCount()));
            }
        }

        BigIntVector indexes = (BigIntVector) rowIndexVector;
        for (int i = 0; i < rowCount; i++) {
            if (indexes.isNull(i)) {
                throw nativeReadException("native row index column contains null at row " + i);
            }
            long rowIndex = indexes.get(i);
            if (rowIndex < 0 || (fileRowCount >= 0 && rowIndex >= fileRowCount)) {
                throw nativeReadException(
                        String.format(
                                "native row index %s is outside file row count %s",
                                rowIndex, fileRowCount));
            }
            if (rowIndex <= lastRowIndex) {
                throw nativeReadException(
                        String.format(
                                "native row index is not ordered: previous %s, current %s",
                                lastRowIndex, rowIndex));
            }
            lastRowIndex = rowIndex;
        }
        return indexes;
    }

    private static long estimateBatchBytes(VectorSchemaRoot root, int rowCount) {
        long bytes = 0;
        for (ValueVector vector : root.getFieldVectors()) {
            bytes = saturatedAdd(bytes, vector.getBufferSizeFor(rowCount));
        }
        return bytes;
    }

    private static long saturatedAdd(long left, int right) {
        long result = left + right;
        return result < left ? Long.MAX_VALUE : result;
    }

    private static NonCorruptFileReadException nativeReadException(String message) {
        return new NonCorruptFileReadException(message);
    }

    private void validateSchema(VectorSchemaRoot root) throws IOException {
        if (root.getFieldVectors().size() != readRowType.getFieldCount() + 1) {
            throw nativeReadException(
                    String.format(
                            "native output schema field count mismatch: expected %s, actual %s",
                            readRowType.getFieldCount() + 1, root.getFieldVectors().size()));
        }
        for (int i = 0; i < readRowType.getFieldCount(); i++) {
            DataField field = readRowType.getFields().get(i);
            ValueVector vector = root.getFieldVectors().get(i);
            if (!field.name().equals(vector.getName())) {
                throw nativeReadException(
                        String.format(
                                "native output field order mismatch at %s: expected %s, actual %s",
                                i, field.name(), vector.getName()));
            }
            if (!matchesPaimonType(vector, field.type())) {
                throw nativeReadException(
                        String.format(
                                "native output field %s has unexpected Arrow vector type %s for %s",
                                field.name(),
                                vector.getClass().getSimpleName(),
                                field.type().getTypeRoot()));
            }
        }
        ValueVector last = root.getFieldVectors().get(readRowType.getFieldCount());
        if (!rowIndexColumn.equals(last.getName()) || !(last instanceof BigIntVector)) {
            throw nativeReadException(
                    "native row index column must be the last BIGINT field named "
                            + rowIndexColumn);
        }
    }

    public static void validateSchema(Schema schema, RowType readRowType, String rowIndexColumn)
            throws IOException {
        List<Field> fields = schema.getFields();
        if (fields.size() != readRowType.getFieldCount() + 1) {
            throw nativeReadException(
                    String.format(
                            "native output schema field count mismatch: expected %s, actual %s",
                            readRowType.getFieldCount() + 1, fields.size()));
        }
        for (int i = 0; i < readRowType.getFieldCount(); i++) {
            DataField expected = readRowType.getFields().get(i);
            Field actual = fields.get(i);
            if (!expected.name().equals(actual.getName())) {
                throw nativeReadException(
                        String.format(
                                "native output field order mismatch at %s: expected %s, actual %s",
                                i, expected.name(), actual.getName()));
            }
            if (!matchesPaimonType(actual.getType(), expected.type())) {
                throw nativeReadException(
                        String.format(
                                "native output field %s has unexpected Arrow schema type %s for %s",
                                expected.name(), actual.getType(), expected.type().getTypeRoot()));
            }
        }
        Field rowIndex = fields.get(readRowType.getFieldCount());
        if (!rowIndexColumn.equals(rowIndex.getName())
                || rowIndex.isNullable()
                || !isSignedInt(rowIndex.getType(), 64)) {
            throw nativeReadException(
                    "native row index column must be the last BIGINT field named "
                            + rowIndexColumn);
        }
    }

    static String rowIndexColumn(RowType readRowType) {
        String candidate = ROW_INDEX_COLUMN;
        int suffix = 1;
        while (readRowType.getFieldNames().contains(candidate)) {
            candidate = ROW_INDEX_COLUMN + "_" + suffix;
            suffix++;
        }
        return candidate;
    }

    private static boolean matchesPaimonType(ValueVector vector, DataType type) {
        switch (type.getTypeRoot()) {
            case CHAR:
            case VARCHAR:
                return vector instanceof VarCharVector;
            case BOOLEAN:
                return vector instanceof BitVector;
            case BINARY:
            case VARBINARY:
                return vector instanceof VarBinaryVector;
            case DECIMAL:
                return vector instanceof DecimalVector
                        && matchesDecimalType(vector.getField().getType(), (DecimalType) type);
            case TINYINT:
                return vector instanceof TinyIntVector;
            case SMALLINT:
                return vector instanceof SmallIntVector;
            case INTEGER:
                return vector instanceof IntVector;
            case BIGINT:
                return vector instanceof BigIntVector;
            case FLOAT:
                return vector instanceof Float4Vector;
            case DOUBLE:
                return vector instanceof Float8Vector;
            case DATE:
                return vector instanceof DateDayVector;
            case TIME_WITHOUT_TIME_ZONE:
                return vector instanceof TimeMilliVector;
            case TIMESTAMP_WITHOUT_TIME_ZONE:
                return vector instanceof TimeStampVector
                        && ((TimestampType) type).getPrecision() <= 6;
            default:
                return false;
        }
    }

    private static boolean matchesPaimonType(ArrowType arrowType, DataType type) {
        switch (type.getTypeRoot()) {
            case CHAR:
            case VARCHAR:
                return arrowType.getTypeID() == ArrowTypeID.Utf8;
            case BOOLEAN:
                return arrowType.getTypeID() == ArrowTypeID.Bool;
            case BINARY:
            case VARBINARY:
                return arrowType.getTypeID() == ArrowTypeID.Binary;
            case DECIMAL:
                return matchesDecimalType(arrowType, (DecimalType) type);
            case TINYINT:
                return isSignedInt(arrowType, 8);
            case SMALLINT:
                return isSignedInt(arrowType, 16);
            case INTEGER:
                return isSignedInt(arrowType, 32);
            case BIGINT:
                return isSignedInt(arrowType, 64);
            case FLOAT:
                return arrowType instanceof ArrowType.FloatingPoint
                        && ((ArrowType.FloatingPoint) arrowType).getPrecision()
                                == FloatingPointPrecision.SINGLE;
            case DOUBLE:
                return arrowType instanceof ArrowType.FloatingPoint
                        && ((ArrowType.FloatingPoint) arrowType).getPrecision()
                                == FloatingPointPrecision.DOUBLE;
            case DATE:
                return arrowType instanceof ArrowType.Date
                        && ((ArrowType.Date) arrowType).getUnit() == DateUnit.DAY;
            case TIME_WITHOUT_TIME_ZONE:
                return arrowType instanceof ArrowType.Time
                        && ((ArrowType.Time) arrowType).getUnit() == TimeUnit.MILLISECOND;
            case TIMESTAMP_WITHOUT_TIME_ZONE:
                return matchesTimestampType(arrowType, (TimestampType) type);
            default:
                return false;
        }
    }

    private static boolean matchesTimestampType(ArrowType arrowType, TimestampType type) {
        if (!(arrowType instanceof ArrowType.Timestamp) || type.getPrecision() > 6) {
            return false;
        }
        ArrowType.Timestamp timestampType = (ArrowType.Timestamp) arrowType;
        String timezone = timestampType.getTimezone();
        TimeUnit unit = timestampType.getUnit();
        return (timezone == null || timezone.isEmpty())
                && (unit == TimeUnit.MILLISECOND || unit == TimeUnit.MICROSECOND);
    }

    private static boolean matchesDecimalType(ArrowType arrowType, DecimalType type) {
        if (!(arrowType instanceof ArrowType.Decimal)) {
            return false;
        }
        ArrowType.Decimal decimalType = (ArrowType.Decimal) arrowType;
        return decimalType.getBitWidth() == 128
                && decimalType.getPrecision() == type.getPrecision()
                && decimalType.getScale() == type.getScale();
    }

    private static boolean isSignedInt(ArrowType arrowType, int bitWidth) {
        return arrowType instanceof ArrowType.Int
                && ((ArrowType.Int) arrowType).getIsSigned()
                && ((ArrowType.Int) arrowType).getBitWidth() == bitWidth;
    }

    interface NativeBatchReader extends AutoCloseable {
        VectorSchemaRoot nextBatch() throws IOException;

        @Override
        void close() throws IOException;
    }

    private static class NativeFileRecordIterator implements FileRecordIterator<InternalRow> {

        private final Iterator<InternalRow> rows;
        private final BigIntVector rowIndexVector;
        private final Path filePath;
        private final VectorSchemaRoot owner;
        private final List<NativeFileRecordIterator> outstandingBatches;
        private int position;
        private long returnedPosition = -1;
        private boolean released;

        private NativeFileRecordIterator(
                Iterator<InternalRow> rows,
                BigIntVector rowIndexVector,
                Path filePath,
                VectorSchemaRoot owner,
                List<NativeFileRecordIterator> outstandingBatches) {
            this.rows = rows;
            this.rowIndexVector = rowIndexVector;
            this.filePath = filePath;
            this.owner = owner;
            this.outstandingBatches = outstandingBatches;
        }

        @Override
        public long returnedPosition() {
            if (returnedPosition < 0) {
                throw new IllegalStateException(
                        "returnedPosition called before next returned a row");
            }
            return returnedPosition;
        }

        @Override
        public Path filePath() {
            return filePath;
        }

        @Override
        public InternalRow next() {
            if (released || !rows.hasNext()) {
                return null;
            }
            InternalRow row = rows.next();
            returnedPosition = rowIndexVector.get(position);
            position++;
            return row;
        }

        @Override
        public void releaseBatch() {
            if (!released) {
                released = true;
                outstandingBatches.remove(this);
                owner.close();
            }
        }
    }
}
