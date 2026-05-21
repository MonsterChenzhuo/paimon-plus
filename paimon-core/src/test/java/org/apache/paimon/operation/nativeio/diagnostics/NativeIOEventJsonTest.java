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

package org.apache.paimon.operation.nativeio.diagnostics;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class NativeIOEventJsonTest {

    @Test
    void roundTripsActiveFileEvent() {
        NativeIOEvent event =
                NativeIOEvent.builder(
                                "event-1",
                                1000L,
                                NativeIOEventType.FILE_START,
                                "op-1",
                                "export_parquet")
                        .withPhase(NativeIOPhase.OPEN_READER)
                        .withSqlExecutionId(17L)
                        .withStageId(3)
                        .withStageAttemptId(0)
                        .withTaskAttemptId(99L)
                        .withTaskIndex(4)
                        .withAttemptNumber(0)
                        .withExecutorId("2")
                        .withHost("10.0.0.2")
                        .withThreadId(45L)
                        .withFilePath("obs://bucket/table/file.parquet")
                        .withOutputPath("obs://bucket/export")
                        .withRows(123L)
                        .withBytes(456L)
                        .build();

        NativeIOEvent parsed = NativeIOEventJson.fromJson(NativeIOEventJson.toJson(event));

        assertThat(parsed.version()).isEqualTo(1);
        assertThat(parsed.eventType()).isEqualTo(NativeIOEventType.FILE_START);
        assertThat(parsed.operationId()).isEqualTo("op-1");
        assertThat(parsed.phase()).isEqualTo(NativeIOPhase.OPEN_READER);
        assertThat(parsed.sqlExecutionId()).isEqualTo(17L);
        assertThat(parsed.stageId()).isEqualTo(3);
        assertThat(parsed.taskAttemptId()).isEqualTo(99L);
        assertThat(parsed.filePath()).isEqualTo("obs://bucket/table/file.parquet");
        assertThat(parsed.rows()).isEqualTo(123L);
        assertThat(parsed.bytes()).isEqualTo(456L);
    }

    @Test
    void roundTripsCompletedObjectRequestEvent() {
        NativeIOEvent event =
                NativeIOEvent.builder(
                                "event-2",
                                2000L,
                                NativeIOEventType.OBJECT_REQUEST_END,
                                "op-2",
                                "export_parquet")
                        .withPhase(NativeIOPhase.WRITE)
                        .withObjectRequestId("req-7")
                        .withObjectOperation("upload_part")
                        .withFilePath("obs://bucket/export/part.parquet")
                        .withDurationMs(321L)
                        .withBytes(67108864L)
                        .withQueueDepth(5)
                        .withRuntimeThreads(4)
                        .withNativeMemoryBytes(1048576L)
                        .withPeakBufferedBytes(2097152L)
                        .build();

        NativeIOEvent parsed = NativeIOEventJson.fromJson(NativeIOEventJson.toJson(event));

        assertThat(parsed.eventType()).isEqualTo(NativeIOEventType.OBJECT_REQUEST_END);
        assertThat(parsed.operationId()).isEqualTo("op-2");
        assertThat(parsed.phase()).isEqualTo(NativeIOPhase.WRITE);
        assertThat(parsed.objectRequestId()).isEqualTo("req-7");
        assertThat(parsed.objectOperation()).isEqualTo("upload_part");
        assertThat(parsed.durationMs()).isEqualTo(321L);
        assertThat(parsed.bytes()).isEqualTo(67108864L);
        assertThat(parsed.queueDepth()).isEqualTo(5);
        assertThat(parsed.runtimeThreads()).isEqualTo(4);
        assertThat(parsed.nativeMemoryBytes()).isEqualTo(1048576L);
        assertThat(parsed.peakBufferedBytes()).isEqualTo(2097152L);
    }
}
