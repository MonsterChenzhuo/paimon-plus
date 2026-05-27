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

package org.apache.paimon.nativeio.jnr;

import jnr.ffi.Pointer;
import jnr.ffi.annotations.Delegate;
import jnr.ffi.byref.PointerByReference;

/** JNR ABI binding for the optional paimon native IO library. */
public interface LibPaimonNativeIO {

    Pointer paimon_reader_config_new();

    void paimon_reader_config_free(Pointer config);

    int paimon_reader_config_add_file(Pointer config, String file);

    int paimon_reader_config_set_target_schema(Pointer config, String schemaJson);

    int paimon_reader_config_set_batch_size(Pointer config, int batchSize);

    int paimon_reader_config_set_row_index_column(Pointer config, String columnName);

    int paimon_reader_config_set_object_store_option(Pointer config, String key, String value);

    int paimon_reader_config_add_deleted_position(Pointer config, String file, long position);

    String paimon_reader_config_last_error(Pointer config);

    Pointer paimon_reader_new(Pointer config);

    void paimon_reader_free(Pointer reader);

    int paimon_reader_next_record_batch_blocked(Pointer reader, long arrowArrayAddress);

    int paimon_reader_get_schema(Pointer reader, long arrowSchemaAddress);

    String paimon_reader_last_error(Pointer reader);

    Pointer paimon_exporter_new();

    void paimon_exporter_free(Pointer exporter);

    int paimon_exporter_export_parquet(
            Pointer exporter,
            String requestJson,
            PointerByReference resultJson,
            PointerByReference errorMessage);

    int paimon_exporter_export_parquet_with_diagnostics(
            Pointer exporter,
            String operationId,
            String requestJson,
            NativeIODiagnosticsCallback callback,
            PointerByReference resultJson,
            PointerByReference errorMessage);

    void paimon_string_free(Pointer value);

    /** Callback invoked synchronously by native export code with a NativeIOEvent JSON payload. */
    interface NativeIODiagnosticsCallback {
        @Delegate
        void emit(String eventJson);
    }
}
