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
import org.apache.paimon.operation.nativeio.diagnostics.NativeIOEvent;
import org.apache.paimon.operation.nativeio.diagnostics.NativeIOEventJson;
import org.apache.paimon.operation.nativeio.diagnostics.NativeIOEventType;

import jnr.ffi.Pointer;
import jnr.ffi.Runtime;
import jnr.ffi.byref.PointerByReference;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class PaimonNativeExporterDiagnosticsTest {

    @Test
    void forwardsNativeDiagnosticCallbackEvents() {
        List<NativeIOEvent> events = new ArrayList<>();
        PaimonNativeExporter exporter =
                new PaimonNativeExporter(new CallbackLibrary(), events::add);

        assertThatThrownBy(() -> exporter.exportParquet(sampleTask()))
                .isInstanceOf(java.io.IOException.class)
                .hasMessageContaining("unknown native export error");

        assertThat(events)
                .extracting(NativeIOEvent::eventType)
                .contains(NativeIOEventType.OPERATION_START, NativeIOEventType.REQUEST_PARSED);
        NativeIOEvent nativeEvent =
                events.stream()
                        .filter(event -> event.eventType() == NativeIOEventType.REQUEST_PARSED)
                        .findFirst()
                        .get();
        assertThat(nativeEvent.operationName()).isEqualTo("native-export-parquet");
        assertThat(nativeEvent.operationId()).startsWith("native-export-parquet-");
        assertThat(nativeEvent.outputPath()).isEqualTo("obs://bucket/out");
    }

    private static NativeExportTask sampleTask() {
        return new NativeExportTask(
                "obs://bucket/out",
                "zstd",
                128L * 1024 * 1024,
                8L * 1024 * 1024,
                4,
                30000L,
                10000L,
                8192,
                250000,
                64L * 1024 * 1024,
                512L * 1024 * 1024,
                4,
                true,
                Collections.singletonList("id"),
                "paimon-json-v1",
                "{\"op\":\"true\"}",
                Collections.emptyMap(),
                Collections.singletonList(
                        new NativeExportFile(
                                "obs://bucket/in.parquet",
                                10L,
                                1024L,
                                0L,
                                Collections.emptyMap(),
                                Collections.emptyList())));
    }

    private static class CallbackLibrary implements LibPaimonNativeIO {

        @Override
        public Pointer paimon_reader_config_new() {
            return null;
        }

        @Override
        public void paimon_reader_config_free(Pointer config) {}

        @Override
        public int paimon_reader_config_add_file(Pointer config, String file) {
            return 0;
        }

        @Override
        public int paimon_reader_config_set_target_schema(Pointer config, String schemaJson) {
            return 0;
        }

        @Override
        public int paimon_reader_config_set_batch_size(Pointer config, int batchSize) {
            return 0;
        }

        @Override
        public int paimon_reader_config_set_row_index_column(Pointer config, String columnName) {
            return 0;
        }

        @Override
        public int paimon_reader_config_set_object_store_option(
                Pointer config, String key, String value) {
            return 0;
        }

        @Override
        public String paimon_reader_config_last_error(Pointer config) {
            return null;
        }

        @Override
        public Pointer paimon_reader_new(Pointer config) {
            return null;
        }

        @Override
        public void paimon_reader_free(Pointer reader) {}

        @Override
        public int paimon_reader_next_record_batch_blocked(Pointer reader, long arrowArrayAddress) {
            return 0;
        }

        @Override
        public int paimon_reader_get_schema(Pointer reader, long arrowSchemaAddress) {
            return 0;
        }

        @Override
        public String paimon_reader_last_error(Pointer reader) {
            return null;
        }

        @Override
        public Pointer paimon_exporter_new() {
            return Pointer.wrap(Runtime.getSystemRuntime(), 1L);
        }

        @Override
        public void paimon_exporter_free(Pointer exporter) {}

        @Override
        public int paimon_exporter_export_parquet(
                Pointer exporter,
                String requestJson,
                PointerByReference resultJson,
                PointerByReference errorMessage) {
            throw new AssertionError("diagnostic export entrypoint should be used");
        }

        @Override
        public int paimon_exporter_export_parquet_with_diagnostics(
                Pointer exporter,
                String operationId,
                String requestJson,
                NativeIODiagnosticsCallback callback,
                PointerByReference resultJson,
                PointerByReference errorMessage) {
            NativeIOEvent event =
                    NativeIOEvent.builder(
                                    operationId + "-request-parsed",
                                    1000L,
                                    NativeIOEventType.REQUEST_PARSED,
                                    operationId,
                                    "native-export-parquet")
                            .withOutputPath("obs://bucket/out")
                            .build();
            callback.emit(NativeIOEventJson.toJson(event));
            return -1;
        }

        @Override
        public void paimon_string_free(Pointer value) {}
    }
}
