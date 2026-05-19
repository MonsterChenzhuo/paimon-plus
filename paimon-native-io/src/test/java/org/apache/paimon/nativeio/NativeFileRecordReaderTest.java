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

import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.format.FileFormat;
import org.apache.paimon.format.FormatWriter;
import org.apache.paimon.format.FormatWriterFactory;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.local.LocalFileIO;
import org.apache.paimon.io.NonCorruptFileReadException;
import org.apache.paimon.nativeio.jnr.PaimonJnrLoader;
import org.apache.paimon.options.Options;
import org.apache.paimon.reader.FileRecordIterator;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VarBinaryVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.TimeUnit;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.Arrays;
import java.util.Collections;
import java.util.Queue;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class NativeFileRecordReaderTest {

    @TempDir private java.nio.file.Path tempDir;

    @Test
    void readsProjectedLocalParquetWithStableRowIndex() throws Exception {
        PaimonJnrLoader loader = PaimonJnrLoader.current();
        Assumptions.assumeTrue(
                loader.available(),
                () ->
                        "native IO library is not available: "
                                + loader.loadFailure()
                                        .map(Throwable::toString)
                                        .orElse(loader.resourcePath()));

        RowType writeType =
                DataTypes.ROW(
                        DataTypes.FIELD(0, "id", DataTypes.INT()),
                        DataTypes.FIELD(1, "score", DataTypes.BIGINT()));
        Path file = new Path(tempDir.resolve("data.parquet").toUri());
        writeParquet(
                file,
                writeType,
                GenericRow.of(1, 10L),
                GenericRow.of(2, 20L),
                GenericRow.of(3, 30L));

        RowType readType = DataTypes.ROW(DataTypes.FIELD(1, "score", DataTypes.BIGINT()));
        try (NativeFileRecordReader reader =
                new NativeFileRecordReader(file.toString(), readType, 2, Collections.emptyMap())) {
            FileRecordIterator<InternalRow> firstBatch = reader.readBatch();
            assertThat(firstBatch).isNotNull();

            InternalRow first = firstBatch.next();
            assertThat(first.getLong(0)).isEqualTo(10L);
            assertThat(firstBatch.returnedPosition()).isEqualTo(0L);

            InternalRow second = firstBatch.next();
            assertThat(second.getLong(0)).isEqualTo(20L);
            assertThat(firstBatch.returnedPosition()).isEqualTo(1L);
            assertThat(firstBatch.next()).isNull();
            firstBatch.releaseBatch();

            FileRecordIterator<InternalRow> secondBatch = reader.readBatch();
            assertThat(secondBatch).isNotNull();

            InternalRow third = secondBatch.next();
            assertThat(third.getLong(0)).isEqualTo(30L);
            assertThat(secondBatch.returnedPosition()).isEqualTo(2L);
            assertThat(secondBatch.next()).isNull();
            secondBatch.releaseBatch();

            assertThat(reader.readBatch()).isNull();
        }
    }

    @Test
    void keepsOutstandingBatchStableUntilRelease() throws Exception {
        PaimonJnrLoader loader = PaimonJnrLoader.current();
        Assumptions.assumeTrue(
                loader.available(),
                () ->
                        "native IO library is not available: "
                                + loader.loadFailure()
                                        .map(Throwable::toString)
                                        .orElse(loader.resourcePath()));

        RowType writeType =
                DataTypes.ROW(
                        DataTypes.FIELD(0, "id", DataTypes.INT()),
                        DataTypes.FIELD(1, "score", DataTypes.BIGINT()));
        Path file = new Path(tempDir.resolve("stable-batches.parquet").toUri());
        writeParquet(
                file,
                writeType,
                GenericRow.of(1, 10L),
                GenericRow.of(2, 20L),
                GenericRow.of(3, 30L));

        RowType readType = DataTypes.ROW(DataTypes.FIELD(1, "score", DataTypes.BIGINT()));
        try (NativeFileRecordReader reader =
                new NativeFileRecordReader(file.toString(), readType, 2, Collections.emptyMap())) {
            FileRecordIterator<InternalRow> firstBatch = reader.readBatch();
            FileRecordIterator<InternalRow> secondBatch = reader.readBatch();

            assertThat(firstBatch.next().getLong(0)).isEqualTo(10L);
            assertThat(firstBatch.returnedPosition()).isEqualTo(0L);
            assertThat(firstBatch.next().getLong(0)).isEqualTo(20L);
            assertThat(firstBatch.returnedPosition()).isEqualTo(1L);

            assertThat(secondBatch.next().getLong(0)).isEqualTo(30L);
            assertThat(secondBatch.returnedPosition()).isEqualTo(2L);

            firstBatch.releaseBatch();
            secondBatch.releaseBatch();
        }
    }

    @Test
    void supportsEmptyProjectionWithRowIndex() throws Exception {
        RowType readType = RowType.of();
        try (BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE);
                NativeFileRecordReader reader =
                        new NativeFileRecordReader(
                                new FakeNativeBatchReader(rowIndexOnlyRoot(allocator, 10L, 12L)),
                                new Path("file:/tmp/empty-projection.parquet"),
                                readType,
                                20L)) {
            FileRecordIterator<InternalRow> batch = reader.readBatch();

            InternalRow first = batch.next();
            assertThat(first.getFieldCount()).isZero();
            assertThat(batch.returnedPosition()).isEqualTo(10L);

            InternalRow second = batch.next();
            assertThat(second.getFieldCount()).isZero();
            assertThat(batch.returnedPosition()).isEqualTo(12L);

            assertThat(batch.next()).isNull();
            batch.releaseBatch();
            assertThat(reader.readBatch()).isNull();
        }
    }

    @Test
    void avoidsRowIndexColumnNameConflict() throws Exception {
        RowType readType =
                DataTypes.ROW(
                        DataTypes.FIELD(
                                0, NativeFileRecordReader.ROW_INDEX_COLUMN, DataTypes.INT()));
        String rowIndexColumn = NativeFileRecordReader.ROW_INDEX_COLUMN + "_1";
        try (BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE);
                NativeFileRecordReader reader =
                        new NativeFileRecordReader(
                                new FakeNativeBatchReader(
                                        rootWithConflictingBusinessColumn(
                                                allocator, rowIndexColumn)),
                                new Path("file:/tmp/conflicting-row-index-column.parquet"),
                                readType,
                                10L)) {
            FileRecordIterator<InternalRow> batch = reader.readBatch();

            InternalRow row = batch.next();
            assertThat(row.getInt(0)).isEqualTo(7);
            assertThat(batch.returnedPosition()).isEqualTo(3L);

            batch.releaseBatch();
        }
    }

    @Test
    void rejectsInvalidNativeRowIndexes() throws Exception {
        RowType readType = DataTypes.ROW(DataTypes.FIELD(0, "id", DataTypes.INT()));
        try (BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE);
                NativeFileRecordReader reader =
                        new NativeFileRecordReader(
                                new FakeNativeBatchReader(rootWithNullRowIndex(allocator)),
                                new Path("file:/tmp/null-row-index.parquet"),
                                readType,
                                10L)) {
            assertThatThrownBy(reader::readBatch)
                    .isInstanceOf(IOException.class)
                    .hasMessageContaining("contains null");
        }

        try (BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE);
                NativeFileRecordReader reader =
                        new NativeFileRecordReader(
                                new FakeNativeBatchReader(rootWithRowIndexes(allocator, 0L, 2L)),
                                new Path("file:/tmp/out-of-range-row-index.parquet"),
                                readType,
                                2L)) {
            assertThatThrownBy(reader::readBatch)
                    .isInstanceOf(IOException.class)
                    .hasMessageContaining("outside file row count");
        }
    }

    @Test
    void rejectsNativeSchemaMismatch() throws Exception {
        RowType readType = DataTypes.ROW(DataTypes.FIELD(0, "id", DataTypes.INT()));
        try (BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE);
                NativeFileRecordReader reader =
                        new NativeFileRecordReader(
                                new FakeNativeBatchReader(rootWithExtraColumn(allocator)),
                                new Path("file:/tmp/extra-column.parquet"),
                                readType,
                                10L)) {
            assertThatThrownBy(reader::readBatch)
                    .isInstanceOf(IOException.class)
                    .hasMessageContaining("field count mismatch");
        }

        try (BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE);
                NativeFileRecordReader reader =
                        new NativeFileRecordReader(
                                new FakeNativeBatchReader(rootWithWrongFieldOrder(allocator)),
                                new Path("file:/tmp/wrong-order.parquet"),
                                readType,
                                10L)) {
            assertThatThrownBy(reader::readBatch)
                    .isInstanceOf(IOException.class)
                    .hasMessageContaining("field order mismatch");
        }

        try (BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE);
                NativeFileRecordReader reader =
                        new NativeFileRecordReader(
                                new FakeNativeBatchReader(rootWithWrongFieldType(allocator)),
                                new Path("file:/tmp/wrong-type.parquet"),
                                readType,
                                10L)) {
            assertThatThrownBy(reader::readBatch)
                    .isInstanceOf(IOException.class)
                    .hasMessageContaining("unexpected Arrow vector type");
        }

        try (BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE);
                NativeFileRecordReader reader =
                        new NativeFileRecordReader(
                                new FakeNativeBatchReader(
                                        rootWithBusinessValueCountMismatch(allocator)),
                                new Path("file:/tmp/business-value-count-mismatch.parquet"),
                                readType,
                                10L)) {
            assertThatThrownBy(reader::readBatch)
                    .isInstanceOf(IOException.class)
                    .hasMessageContaining("value count mismatch");
        }
    }

    @Test
    void rejectsZeroRowNativeBatch() throws Exception {
        RowType readType = DataTypes.ROW(DataTypes.FIELD(0, "id", DataTypes.INT()));
        try (BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE);
                NativeFileRecordReader reader =
                        new NativeFileRecordReader(
                                new FakeNativeBatchReader(rootWithZeroRows(allocator)),
                                new Path("file:/tmp/zero-row-batch.parquet"),
                                readType,
                                10L)) {
            assertThatThrownBy(reader::readBatch)
                    .isInstanceOf(IOException.class)
                    .hasMessageContaining("zero-row");
        }
    }

    @Test
    void validatesNativeSchemaBeforeReadingFirstBatch() throws Exception {
        RowType readType = DataTypes.ROW(DataTypes.FIELD(0, "id", DataTypes.INT()));

        NativeFileRecordReader.validateSchema(
                new Schema(
                        Arrays.asList(
                                Field.nullable("id", new ArrowType.Int(32, true)),
                                Field.notNullable(
                                        NativeFileRecordReader.ROW_INDEX_COLUMN,
                                        new ArrowType.Int(64, true)))),
                readType,
                NativeFileRecordReader.ROW_INDEX_COLUMN);

        assertThatThrownBy(
                        () ->
                                NativeFileRecordReader.validateSchema(
                                        new Schema(
                                                Arrays.asList(
                                                        Field.nullable(
                                                                "id", new ArrowType.Int(64, true)),
                                                        Field.notNullable(
                                                                NativeFileRecordReader
                                                                        .ROW_INDEX_COLUMN,
                                                                new ArrowType.Int(64, true)))),
                                        readType,
                                        NativeFileRecordReader.ROW_INDEX_COLUMN))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("unexpected Arrow schema type");

        assertThatThrownBy(
                        () ->
                                NativeFileRecordReader.validateSchema(
                                        new Schema(
                                                Arrays.asList(
                                                        Field.nullable(
                                                                "id", new ArrowType.Int(32, true)),
                                                        Field.nullable(
                                                                NativeFileRecordReader
                                                                        .ROW_INDEX_COLUMN,
                                                                new ArrowType.Int(64, true)))),
                                        readType,
                                        NativeFileRecordReader.ROW_INDEX_COLUMN))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("row index column");
    }

    @Test
    void validatesTimestampNativeSchemaBeforeReadingFirstBatch() throws Exception {
        RowType readType = DataTypes.ROW(DataTypes.FIELD(0, "ts", DataTypes.TIMESTAMP(6)));

        NativeFileRecordReader.validateSchema(
                timestampSchema(new ArrowType.Timestamp(TimeUnit.MILLISECOND, null)),
                readType,
                NativeFileRecordReader.ROW_INDEX_COLUMN);
        NativeFileRecordReader.validateSchema(
                timestampSchema(new ArrowType.Timestamp(TimeUnit.MICROSECOND, "")),
                readType,
                NativeFileRecordReader.ROW_INDEX_COLUMN);

        assertThatThrownBy(
                        () ->
                                NativeFileRecordReader.validateSchema(
                                        timestampSchema(
                                                new ArrowType.Timestamp(TimeUnit.NANOSECOND, null)),
                                        readType,
                                        NativeFileRecordReader.ROW_INDEX_COLUMN))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("unexpected Arrow schema type");
        assertThatThrownBy(
                        () ->
                                NativeFileRecordReader.validateSchema(
                                        timestampSchema(
                                                new ArrowType.Timestamp(
                                                        TimeUnit.MICROSECOND, "UTC")),
                                        readType,
                                        NativeFileRecordReader.ROW_INDEX_COLUMN))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("unexpected Arrow schema type");
    }

    @Test
    void validatesCharNativeSchemaBeforeReadingFirstBatch() throws Exception {
        RowType readType = DataTypes.ROW(DataTypes.FIELD(0, "name", DataTypes.CHAR(4)));

        NativeFileRecordReader.validateSchema(
                namedSingleColumnSchema("name", ArrowType.Utf8.INSTANCE),
                readType,
                NativeFileRecordReader.ROW_INDEX_COLUMN);

        assertThatThrownBy(
                        () ->
                                NativeFileRecordReader.validateSchema(
                                        namedSingleColumnSchema(
                                                "name", new ArrowType.FixedSizeBinary(4)),
                                        readType,
                                        NativeFileRecordReader.ROW_INDEX_COLUMN))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("unexpected Arrow schema type");
    }

    @Test
    void validatesDecimalNativeSchemaBeforeReadingFirstBatch() throws Exception {
        RowType readType = DataTypes.ROW(DataTypes.FIELD(0, "amount", DataTypes.DECIMAL(10, 2)));

        NativeFileRecordReader.validateSchema(
                decimalSchema(new ArrowType.Decimal(10, 2, 128)),
                readType,
                NativeFileRecordReader.ROW_INDEX_COLUMN);

        assertThatThrownBy(
                        () ->
                                NativeFileRecordReader.validateSchema(
                                        decimalSchema(new ArrowType.Decimal(10, 3, 128)),
                                        readType,
                                        NativeFileRecordReader.ROW_INDEX_COLUMN))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("unexpected Arrow schema type");
        assertThatThrownBy(
                        () ->
                                NativeFileRecordReader.validateSchema(
                                        decimalSchema(new ArrowType.Decimal(12, 2, 128)),
                                        readType,
                                        NativeFileRecordReader.ROW_INDEX_COLUMN))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("unexpected Arrow schema type");
        assertThatThrownBy(
                        () ->
                                NativeFileRecordReader.validateSchema(
                                        decimalSchema(new ArrowType.Decimal(10, 2, 256)),
                                        readType,
                                        NativeFileRecordReader.ROW_INDEX_COLUMN))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("unexpected Arrow schema type");
    }

    @Test
    void rejectsDecreasingRowIndexesAcrossBatches() throws Exception {
        RowType readType = DataTypes.ROW(DataTypes.FIELD(0, "id", DataTypes.INT()));
        try (BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE);
                NativeFileRecordReader reader =
                        new NativeFileRecordReader(
                                new FakeNativeBatchReader(
                                        rootWithRowIndexes(allocator, 5L),
                                        rootWithRowIndexes(allocator, 4L)),
                                new Path("file:/tmp/decreasing-row-index.parquet"),
                                readType,
                                10L)) {
            FileRecordIterator<InternalRow> first = reader.readBatch();
            first.releaseBatch();

            assertThatThrownBy(reader::readBatch)
                    .isInstanceOf(IOException.class)
                    .hasMessageContaining("not ordered");
        }
    }

    @Test
    void rejectsDuplicateRowIndexes() throws Exception {
        RowType readType = DataTypes.ROW(DataTypes.FIELD(0, "id", DataTypes.INT()));
        try (BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE);
                NativeFileRecordReader reader =
                        new NativeFileRecordReader(
                                new FakeNativeBatchReader(rootWithRowIndexes(allocator, 5L, 5L)),
                                new Path("file:/tmp/duplicate-row-index.parquet"),
                                readType,
                                10L)) {
            assertThatThrownBy(reader::readBatch)
                    .isInstanceOf(IOException.class)
                    .hasMessageContaining("not ordered");
        }
    }

    @Test
    void rejectsBatchExceedingMaxBatchBytes() throws Exception {
        RowType readType = DataTypes.ROW(DataTypes.FIELD(0, "payload", DataTypes.VARBINARY(4096)));
        try (BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE);
                NativeFileRecordReader reader =
                        new NativeFileRecordReader(
                                new FakeNativeBatchReader(rootWithBinaryPayload(allocator, 2048)),
                                new Path("file:/tmp/large-native-batch.parquet"),
                                readType,
                                10L,
                                128L)) {
            assertThatThrownBy(reader::readBatch)
                    .isInstanceOf(NonCorruptFileReadException.class)
                    .hasMessageContaining("exceeds native-io.max-batch-bytes");
        }
    }

    @Test
    void closeIsIdempotentAndClosesNativeReaderOnce() throws Exception {
        RowType readType = DataTypes.ROW(DataTypes.FIELD(0, "id", DataTypes.INT()));
        try (BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE)) {
            CountingNativeBatchReader nativeReader =
                    new CountingNativeBatchReader(rootWithRowIndexes(allocator, 0L));
            NativeFileRecordReader reader =
                    new NativeFileRecordReader(
                            nativeReader,
                            new Path("file:/tmp/idempotent-close.parquet"),
                            readType,
                            10L);

            FileRecordIterator<InternalRow> batch = reader.readBatch();
            assertThat(batch).isNotNull();

            reader.close();
            reader.close();

            assertThat(nativeReader.closeCount).isEqualTo(1);
        }
    }

    @Test
    void outstandingIteratorReturnsNullAfterReaderClose() throws Exception {
        RowType readType = DataTypes.ROW(DataTypes.FIELD(0, "id", DataTypes.INT()));
        try (BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE);
                NativeFileRecordReader reader =
                        new NativeFileRecordReader(
                                new FakeNativeBatchReader(rootWithRowIndexes(allocator, 0L)),
                                new Path("file:/tmp/close-outstanding-iterator.parquet"),
                                readType,
                                10L)) {
            FileRecordIterator<InternalRow> batch = reader.readBatch();

            reader.close();

            assertThat(batch.next()).isNull();
            assertThatThrownBy(batch::returnedPosition)
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessageContaining("before next");
            batch.releaseBatch();
        }
    }

    private static void writeParquet(Path file, RowType rowType, InternalRow... rows)
            throws IOException {
        FileIO fileIO = LocalFileIO.create();
        FileFormat format = FileFormat.fromIdentifier("parquet", new Options());
        FormatWriterFactory writerFactory = format.createWriterFactory(rowType);
        try (FormatWriter writer =
                writerFactory.create(fileIO.newOutputStream(file, false), "none")) {
            for (InternalRow row : rows) {
                writer.addElement(row);
            }
        }
    }

    private static VectorSchemaRoot rowIndexOnlyRoot(
            BufferAllocator allocator, long... rowIndexes) {
        BigIntVector rowIndex =
                new BigIntVector(NativeFileRecordReader.ROW_INDEX_COLUMN, allocator);
        rowIndex.allocateNew(rowIndexes.length);
        for (int i = 0; i < rowIndexes.length; i++) {
            rowIndex.setSafe(i, rowIndexes[i]);
        }
        rowIndex.setValueCount(rowIndexes.length);
        VectorSchemaRoot root = VectorSchemaRoot.of(rowIndex);
        root.setRowCount(rowIndexes.length);
        return root;
    }

    private static VectorSchemaRoot rootWithRowIndexes(
            BufferAllocator allocator, long... rowIndexes) {
        IntVector id = new IntVector("id", allocator);
        id.allocateNew(rowIndexes.length);
        for (int i = 0; i < rowIndexes.length; i++) {
            id.setSafe(i, i);
        }
        id.setValueCount(rowIndexes.length);

        BigIntVector rowIndex =
                new BigIntVector(NativeFileRecordReader.ROW_INDEX_COLUMN, allocator);
        rowIndex.allocateNew(rowIndexes.length);
        for (int i = 0; i < rowIndexes.length; i++) {
            rowIndex.setSafe(i, rowIndexes[i]);
        }
        rowIndex.setValueCount(rowIndexes.length);

        VectorSchemaRoot root = VectorSchemaRoot.of(id, rowIndex);
        root.setRowCount(rowIndexes.length);
        return root;
    }

    private static VectorSchemaRoot rootWithConflictingBusinessColumn(
            BufferAllocator allocator, String rowIndexColumn) {
        IntVector business = intVector(allocator, NativeFileRecordReader.ROW_INDEX_COLUMN, 7);
        BigIntVector rowIndex = rowIndexVector(allocator, rowIndexColumn, 3L);

        VectorSchemaRoot root = VectorSchemaRoot.of(business, rowIndex);
        root.setRowCount(1);
        return root;
    }

    private static VectorSchemaRoot rootWithExtraColumn(BufferAllocator allocator) {
        IntVector id = intVector(allocator, "id", 1);
        IntVector extra = intVector(allocator, "extra", 2);
        BigIntVector rowIndex = rowIndexVector(allocator, 0L);

        VectorSchemaRoot root = VectorSchemaRoot.of(id, extra, rowIndex);
        root.setRowCount(1);
        return root;
    }

    private static VectorSchemaRoot rootWithWrongFieldOrder(BufferAllocator allocator) {
        IntVector id = intVector(allocator, "id", 1);
        BigIntVector rowIndex = rowIndexVector(allocator, 0L);

        VectorSchemaRoot root = VectorSchemaRoot.of(rowIndex, id);
        root.setRowCount(1);
        return root;
    }

    private static VectorSchemaRoot rootWithWrongFieldType(BufferAllocator allocator) {
        BigIntVector id = rowIndexVector(allocator, "id", 1L);
        BigIntVector rowIndex = rowIndexVector(allocator, 0L);

        VectorSchemaRoot root = VectorSchemaRoot.of(id, rowIndex);
        root.setRowCount(1);
        return root;
    }

    private static VectorSchemaRoot rootWithBusinessValueCountMismatch(BufferAllocator allocator) {
        IntVector id = new IntVector("id", allocator);
        id.allocateNew(2);
        id.setSafe(0, 1);
        id.setSafe(1, 2);
        id.setValueCount(2);

        BigIntVector rowIndex = rowIndexVector(allocator, 0L);

        VectorSchemaRoot root = VectorSchemaRoot.of(id, rowIndex);
        root.setRowCount(1);
        id.setValueCount(2);
        return root;
    }

    private static VectorSchemaRoot rootWithZeroRows(BufferAllocator allocator) {
        IntVector id = new IntVector("id", allocator);
        id.allocateNew(0);
        id.setValueCount(0);

        BigIntVector rowIndex =
                new BigIntVector(NativeFileRecordReader.ROW_INDEX_COLUMN, allocator);
        rowIndex.allocateNew(0);
        rowIndex.setValueCount(0);

        VectorSchemaRoot root = VectorSchemaRoot.of(id, rowIndex);
        root.setRowCount(0);
        return root;
    }

    private static IntVector intVector(BufferAllocator allocator, String name, int value) {
        IntVector vector = new IntVector(name, allocator);
        vector.allocateNew(1);
        vector.setSafe(0, value);
        vector.setValueCount(1);
        return vector;
    }

    private static BigIntVector rowIndexVector(BufferAllocator allocator, long value) {
        return rowIndexVector(allocator, NativeFileRecordReader.ROW_INDEX_COLUMN, value);
    }

    private static BigIntVector rowIndexVector(BufferAllocator allocator, String name, long value) {
        BigIntVector vector = new BigIntVector(name, allocator);
        vector.allocateNew(1);
        vector.setSafe(0, value);
        vector.setValueCount(1);
        return vector;
    }

    private static VectorSchemaRoot rootWithBinaryPayload(BufferAllocator allocator, int bytes) {
        VarBinaryVector payload = new VarBinaryVector("payload", allocator);
        payload.allocateNew(bytes, 1);
        payload.setSafe(0, new byte[bytes]);
        payload.setValueCount(1);
        BigIntVector rowIndex = rowIndexVector(allocator, 0L);

        VectorSchemaRoot root = VectorSchemaRoot.of(payload, rowIndex);
        root.setRowCount(1);
        return root;
    }

    private static Schema timestampSchema(ArrowType.Timestamp timestampType) {
        return new Schema(
                Arrays.asList(
                        Field.nullable("ts", timestampType),
                        Field.notNullable(
                                NativeFileRecordReader.ROW_INDEX_COLUMN,
                                new ArrowType.Int(64, true))));
    }

    private static Schema decimalSchema(ArrowType.Decimal decimalType) {
        return namedSingleColumnSchema("amount", decimalType);
    }

    private static Schema namedSingleColumnSchema(String name, ArrowType type) {
        return new Schema(
                Arrays.asList(
                        Field.nullable(name, type),
                        Field.notNullable(
                                NativeFileRecordReader.ROW_INDEX_COLUMN,
                                new ArrowType.Int(64, true))));
    }

    private static VectorSchemaRoot rootWithNullRowIndex(BufferAllocator allocator) {
        IntVector id = new IntVector("id", allocator);
        id.allocateNew(1);
        id.setSafe(0, 1);
        id.setValueCount(1);

        BigIntVector rowIndex =
                new BigIntVector(NativeFileRecordReader.ROW_INDEX_COLUMN, allocator);
        rowIndex.allocateNew(1);
        rowIndex.setNull(0);
        rowIndex.setValueCount(1);

        VectorSchemaRoot root = VectorSchemaRoot.of(id, rowIndex);
        root.setRowCount(1);
        return root;
    }

    private static class FakeNativeBatchReader implements NativeFileRecordReader.NativeBatchReader {

        private final Queue<VectorSchemaRoot> roots = new ArrayDeque<>();

        private FakeNativeBatchReader(VectorSchemaRoot... roots) {
            Collections.addAll(this.roots, roots);
        }

        @Override
        public VectorSchemaRoot nextBatch() {
            return roots.poll();
        }

        @Override
        public void close() {
            while (!roots.isEmpty()) {
                roots.poll().close();
            }
        }
    }

    private static class CountingNativeBatchReader extends FakeNativeBatchReader {

        private int closeCount;

        private CountingNativeBatchReader(VectorSchemaRoot... roots) {
            super(roots);
        }

        @Override
        public void close() {
            closeCount++;
            super.close();
        }
    }
}
