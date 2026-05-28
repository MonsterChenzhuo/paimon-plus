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

import org.apache.paimon.operation.nativeio.diagnostics.NativeIOEvent;
import org.apache.paimon.operation.nativeio.diagnostics.NativeIOEventType;
import org.apache.paimon.operation.nativeio.diagnostics.NativeIOPhase;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class NativeReadDiagnosticsTest {

    @Test
    void recordsSegmentedPhaseMetricsForCompletedNativeRead() throws Exception {
        List<NativeIOEvent> events = new ArrayList<>();
        NativeReadDiagnostics diagnostics =
                NativeReadDiagnostics.createForTesting(
                        "native-columnar-read", "obs://bucket/table/file.parquet", events::add);

        diagnostics.start();
        diagnostics.recordPhaseForTesting(NativeIOPhase.LOAD_DELETION_VECTOR, 2_000_000L, 0L, 0L);
        diagnostics.recordPhaseForTesting(NativeIOPhase.OPEN_READER, 5_000_000L, 0L, 0L);
        diagnostics.recordPhaseForTesting(NativeIOPhase.READ_BATCH, 11_000_000L, 4096L, 16384L);
        diagnostics.recordPhaseForTesting(
                NativeIOPhase.BUILD_COLUMNAR_BATCH, 3_000_000L, null, null);
        diagnostics.end();

        assertThat(events)
                .extracting(NativeIOEvent::eventType)
                .containsExactly(
                        NativeIOEventType.OPERATION_START,
                        NativeIOEventType.PHASE_END,
                        NativeIOEventType.PHASE_END,
                        NativeIOEventType.PHASE_END,
                        NativeIOEventType.PHASE_END,
                        NativeIOEventType.OPERATION_END);

        NativeIOEvent completed = events.get(events.size() - 1);
        assertThat(completed.operationName()).isEqualTo("native-columnar-read");
        assertThat(completed.filePath()).isEqualTo("obs://bucket/table/file.parquet");
        assertThat(completed.rows()).isEqualTo(4096L);
        assertThat(completed.bytes()).isEqualTo(16384L);
        assertThat(completed.metricsJson())
                .contains("\"load_deletion_vector_ms\":2")
                .contains("\"open_reader_ms\":5")
                .contains("\"read_batch_ms\":11")
                .contains("\"read_batch_count\":1")
                .contains("\"build_columnar_batch_ms\":3")
                .contains("\"build_columnar_batch_count\":1")
                .contains("\"batches\":1");
    }
}
