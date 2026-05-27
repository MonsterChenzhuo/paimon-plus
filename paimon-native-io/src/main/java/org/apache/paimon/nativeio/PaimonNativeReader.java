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

import org.apache.paimon.io.NonCorruptFileReadException;
import org.apache.paimon.nativeio.jnr.LibPaimonNativeIO;
import org.apache.paimon.nativeio.jnr.PaimonJnrLoader;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.RowType;

import jnr.ffi.Pointer;
import org.apache.arrow.c.ArrowArray;
import org.apache.arrow.c.ArrowSchema;
import org.apache.arrow.c.CDataDictionaryProvider;
import org.apache.arrow.c.Data;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.Schema;

import java.io.IOException;
import java.util.Map;

/** Thin JNR wrapper around libpaimon_native_io. */
public class PaimonNativeReader implements NativeFileRecordReader.NativeBatchReader {

    private final LibPaimonNativeIO lib;
    private final BufferAllocator allocator;
    private final CDataDictionaryProvider dictionaryProvider;

    private Pointer config;
    private Pointer reader;
    private Schema schema;
    private boolean closed;

    public PaimonNativeReader() throws IOException {
        this.lib =
                PaimonJnrLoader.current()
                        .load()
                        .orElseThrow(
                                () ->
                                        new NonCorruptFileReadException(
                                                "paimon native IO library is not available"));
        this.allocator = new RootAllocator(Long.MAX_VALUE);
        this.dictionaryProvider = new CDataDictionaryProvider();
        this.config = lib.paimon_reader_config_new();
        if (config == null) {
            throw new NonCorruptFileReadException("failed to create native reader config");
        }
    }

    public void addFile(String file) throws IOException {
        ensureConfigOpen();
        checkConfig(lib.paimon_reader_config_add_file(config, file), "add file");
    }

    public void setBatchSize(int batchSize) throws IOException {
        ensureConfigOpen();
        checkConfig(lib.paimon_reader_config_set_batch_size(config, batchSize), "set batch size");
    }

    public void setRowIndexColumn(String columnName) throws IOException {
        ensureConfigOpen();
        checkConfig(
                lib.paimon_reader_config_set_row_index_column(config, columnName),
                "set row index column");
    }

    public void setTargetColumns(RowType rowType) throws IOException {
        ensureConfigOpen();
        checkConfig(
                lib.paimon_reader_config_set_target_schema(config, fieldNamesJson(rowType)),
                "set target columns");
    }

    public void setObjectStoreOptions(Map<String, String> options) throws IOException {
        ensureConfigOpen();
        for (Map.Entry<String, String> entry : options.entrySet()) {
            if (entry.getKey() != null && entry.getValue() != null) {
                checkConfig(
                        lib.paimon_reader_config_set_object_store_option(
                                config, entry.getKey(), entry.getValue()),
                        "set object store option");
            }
        }
    }

    public void addDeletedPosition(String file, long position) throws IOException {
        addDeletedPositions(file, new long[] {position}, 1);
    }

    public void addDeletedPositions(String file, long[] positions, int positionCount)
            throws IOException {
        ensureConfigOpen();
        if (positionCount < 0 || positionCount > positions.length) {
            throw new NonCorruptFileReadException(
                    "invalid deleted position count: " + positionCount);
        }
        checkConfig(
                lib.paimon_reader_config_add_deleted_positions(
                        config, file, positions, positionCount),
                "add deleted positions");
    }

    public void initializeReader() throws IOException {
        ensureConfigOpen();
        reader = lib.paimon_reader_new(config);
        if (reader == null) {
            throw new NonCorruptFileReadException("failed to create native reader");
        }
        String error = lib.paimon_reader_last_error(reader);
        if (error != null) {
            throw new NonCorruptFileReadException("failed to initialize native reader: " + error);
        }
        try (ArrowSchema arrowSchema = ArrowSchema.allocateNew(allocator)) {
            checkReader(
                    lib.paimon_reader_get_schema(reader, arrowSchema.memoryAddress()),
                    "get schema");
            schema = Data.importSchema(allocator, arrowSchema, dictionaryProvider);
        }
    }

    public Schema schema() {
        return schema;
    }

    public VectorSchemaRoot nextBatch() throws IOException {
        ensureInitialized();
        VectorSchemaRoot batchRoot = VectorSchemaRoot.create(schema, allocator);
        try (ArrowArray arrowArray = ArrowArray.allocateNew(allocator)) {
            int rowCount =
                    lib.paimon_reader_next_record_batch_blocked(reader, arrowArray.memoryAddress());
            if (rowCount < 0) {
                throw new NonCorruptFileReadException("native reader failed: " + lastError());
            }
            if (rowCount == 0) {
                batchRoot.close();
                return null;
            }
            Data.importIntoVectorSchemaRoot(allocator, arrowArray, batchRoot, dictionaryProvider);
            if (batchRoot.getRowCount() != rowCount) {
                throw new NonCorruptFileReadException(
                        String.format(
                                "native reader row count mismatch: status %s, Arrow batch %s",
                                rowCount, batchRoot.getRowCount()));
            }
            batchRoot.setRowCount(rowCount);
            return batchRoot;
        } catch (IOException | RuntimeException e) {
            batchRoot.close();
            throw e;
        }
    }

    private void ensureInitialized() throws IOException {
        if (closed) {
            throw new NonCorruptFileReadException("native reader is closed");
        }
        if (reader == null || schema == null) {
            throw new NonCorruptFileReadException("native reader is not initialized");
        }
    }

    private void ensureConfigOpen() throws IOException {
        if (closed || config == null) {
            throw new NonCorruptFileReadException("native reader config is closed");
        }
    }

    private void checkConfig(int status, String action) throws IOException {
        if (status != 0) {
            throw new NonCorruptFileReadException(
                    "failed to " + action + " on native config: " + configError());
        }
    }

    private void checkReader(int status, String action) throws IOException {
        if (status != 0) {
            throw new NonCorruptFileReadException(
                    "failed to " + action + " from native reader: " + lastError());
        }
    }

    private String lastError() {
        if (reader == null) {
            return "unknown";
        }
        String error = lib.paimon_reader_last_error(reader);
        return error == null ? "unknown" : error;
    }

    private String configError() {
        if (config == null) {
            return "unknown";
        }
        try {
            String error = lib.paimon_reader_config_last_error(config);
            return error == null ? "unknown" : error;
        } catch (UnsatisfiedLinkError e) {
            return "unknown";
        }
    }

    private static String fieldNamesJson(RowType rowType) {
        StringBuilder builder = new StringBuilder("[");
        for (int i = 0; i < rowType.getFieldCount(); i++) {
            if (i > 0) {
                builder.append(',');
            }
            DataField field = rowType.getFields().get(i);
            appendJsonString(builder, field.name());
        }
        return builder.append(']').toString();
    }

    private static void appendJsonString(StringBuilder builder, String value) {
        builder.append('"');
        for (int i = 0; i < value.length(); i++) {
            char c = value.charAt(i);
            switch (c) {
                case '"':
                    builder.append("\\\"");
                    break;
                case '\\':
                    builder.append("\\\\");
                    break;
                case '\b':
                    builder.append("\\b");
                    break;
                case '\f':
                    builder.append("\\f");
                    break;
                case '\n':
                    builder.append("\\n");
                    break;
                case '\r':
                    builder.append("\\r");
                    break;
                case '\t':
                    builder.append("\\t");
                    break;
                default:
                    if (c < 0x20) {
                        builder.append(String.format("\\u%04x", (int) c));
                    } else {
                        builder.append(c);
                    }
            }
        }
        builder.append('"');
    }

    @Override
    public void close() throws IOException {
        if (closed) {
            return;
        }
        closed = true;
        IOException failure = null;
        if (reader != null) {
            lib.paimon_reader_free(reader);
            reader = null;
        }
        if (config != null) {
            lib.paimon_reader_config_free(config);
            config = null;
        }
        try {
            dictionaryProvider.close();
            allocator.close();
        } catch (Exception e) {
            failure = new IOException(e);
        }
        if (failure != null) {
            throw failure;
        }
    }
}
