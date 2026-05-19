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

package org.apache.paimon.operation.nativeio;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.format.FileFormatDiscover;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.schema.SchemaManager;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.FileStorePathFactory;

import javax.annotation.Nullable;

/** Reconstructable context passed from core read planning to optional native reader providers. */
public final class NativeSplitReadContext {

    private final FileIO fileIO;
    private final SchemaManager schemaManager;
    private final TableSchema tableSchema;
    private final RowType rowType;
    private final FileFormatDiscover fileFormatDiscover;
    private final FileStorePathFactory pathFactory;
    private final CoreOptions coreOptions;
    private final NativeIOOptions nativeIOOptions;
    private final NativeApplicabilityReporter reporter;
    @Nullable private final String engineName;

    public NativeSplitReadContext(
            FileIO fileIO,
            SchemaManager schemaManager,
            TableSchema tableSchema,
            RowType rowType,
            FileFormatDiscover fileFormatDiscover,
            FileStorePathFactory pathFactory,
            CoreOptions coreOptions,
            NativeIOOptions nativeIOOptions,
            @Nullable String engineName) {
        this(
                fileIO,
                schemaManager,
                tableSchema,
                rowType,
                fileFormatDiscover,
                pathFactory,
                coreOptions,
                nativeIOOptions,
                engineName,
                NativeApplicabilityReporter.NO_OP);
    }

    public NativeSplitReadContext(
            FileIO fileIO,
            SchemaManager schemaManager,
            TableSchema tableSchema,
            RowType rowType,
            FileFormatDiscover fileFormatDiscover,
            FileStorePathFactory pathFactory,
            CoreOptions coreOptions,
            NativeIOOptions nativeIOOptions,
            @Nullable String engineName,
            NativeApplicabilityReporter reporter) {
        this.fileIO = fileIO;
        this.schemaManager = schemaManager;
        this.tableSchema = tableSchema;
        this.rowType = rowType;
        this.fileFormatDiscover = fileFormatDiscover;
        this.pathFactory = pathFactory;
        this.coreOptions = coreOptions;
        this.nativeIOOptions = nativeIOOptions;
        this.engineName = engineName;
        this.reporter = reporter;
    }

    public FileIO fileIO() {
        return fileIO;
    }

    public SchemaManager schemaManager() {
        return schemaManager;
    }

    public TableSchema tableSchema() {
        return tableSchema;
    }

    public RowType rowType() {
        return rowType;
    }

    public FileFormatDiscover fileFormatDiscover() {
        return fileFormatDiscover;
    }

    public FileStorePathFactory pathFactory() {
        return pathFactory;
    }

    public CoreOptions coreOptions() {
        return coreOptions;
    }

    public NativeIOOptions nativeIOOptions() {
        return nativeIOOptions;
    }

    public NativeApplicabilityReporter reporter() {
        return reporter;
    }

    @Nullable
    public String engineName() {
        return engineName;
    }

    public boolean engineSupportsNativeIO() {
        return nativeIOOptions.engineSupportsNativeIO();
    }
}
