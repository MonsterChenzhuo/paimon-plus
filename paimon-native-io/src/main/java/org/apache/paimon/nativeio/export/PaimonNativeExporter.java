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

package org.apache.paimon.nativeio.export;

import org.apache.paimon.nativeio.jnr.LibPaimonNativeIO;
import org.apache.paimon.nativeio.jnr.PaimonJnrLoader;
import org.apache.paimon.operation.nativeio.diagnostics.NativeIODiagnosticsEmitter;
import org.apache.paimon.operation.nativeio.diagnostics.NativeIOEvent;
import org.apache.paimon.operation.nativeio.diagnostics.NativeIOEventType;
import org.apache.paimon.operation.nativeio.diagnostics.NativeIOPhase;

import jnr.ffi.Pointer;
import jnr.ffi.byref.PointerByReference;

import java.io.IOException;
import java.util.UUID;

/** JNR wrapper for native export_parquet FFI. */
public final class PaimonNativeExporter {

    private final LibPaimonNativeIO library;

    public PaimonNativeExporter() throws IOException {
        this(
                PaimonJnrLoader.current()
                        .load()
                        .orElseThrow(
                                () -> new IOException("Paimon native IO library is unavailable.")));
    }

    PaimonNativeExporter(LibPaimonNativeIO library) {
        this.library = library;
    }

    public NativeExportResult exportParquet(NativeExportTask task) throws IOException {
        String operationId = "native-export-parquet-" + UUID.randomUUID();
        emit(task, operationId, NativeIOEventType.OPERATION_START, null, null);
        Pointer exporter = library.paimon_exporter_new();
        if (exporter == null) {
            emitError(task, operationId, "Native exporter creation returned null.", null);
            throw new IOException("Native exporter creation returned null.");
        }

        PointerByReference resultJson = new PointerByReference();
        PointerByReference errorMessage = new PointerByReference();
        try {
            emit(task, operationId, NativeIOEventType.JNI_CALL_START, NativeIOPhase.JNI, null);
            int status =
                    library.paimon_exporter_export_parquet(
                            exporter, NativeExportJson.toJson(task), resultJson, errorMessage);
            emit(task, operationId, NativeIOEventType.JNI_CALL_END, NativeIOPhase.JNI, null);
            if (status == 0) {
                Pointer result = resultJson.getValue();
                if (result == null) {
                    throw new IOException("Native exporter returned null result.");
                }
                try {
                    NativeExportResult exportResult =
                            NativeExportJson.resultFromJson(result.getString(0));
                    emit(
                            task,
                            operationId,
                            NativeIOEventType.OPERATION_END,
                            null,
                            exportResult.rowsOutput());
                    return exportResult;
                } finally {
                    library.paimon_string_free(result);
                }
            }

            Pointer error = errorMessage.getValue();
            String message = error == null ? "unknown native export error" : error.getString(0);
            if (error != null) {
                library.paimon_string_free(error);
            }
            throw new IOException(message);
        } catch (IOException e) {
            emitError(task, operationId, e.getMessage(), e);
            throw e;
        } finally {
            library.paimon_exporter_free(exporter);
        }
    }

    private static void emit(
            NativeExportTask task,
            String operationId,
            NativeIOEventType eventType,
            NativeIOPhase phase,
            Long rows) {
        NativeIOEvent.Builder builder =
                NativeIOEvent.builder(eventType, operationId, "native-export-parquet")
                        .withPhase(phase)
                        .withOutputPath(task.outputPath())
                        .withRuntimeThreads(task.runtimeThreads());
        if (!task.files().isEmpty()) {
            builder.withFilePath(task.files().get(0).path());
        }
        if (rows != null) {
            builder.withRows(rows);
        }
        NativeIODiagnosticsEmitter.emit(builder.build());
    }

    private static void emitError(
            NativeExportTask task, String operationId, String message, Throwable throwable) {
        NativeIOEvent.Builder builder =
                NativeIOEvent.builder(NativeIOEventType.ERROR, operationId, "native-export-parquet")
                        .withOutputPath(task.outputPath())
                        .withErrorMessage(message);
        if (!task.files().isEmpty()) {
            builder.withFilePath(task.files().get(0).path());
        }
        if (throwable != null) {
            builder.withErrorClass(throwable.getClass().getName());
        }
        NativeIODiagnosticsEmitter.emit(builder.build());
    }
}
