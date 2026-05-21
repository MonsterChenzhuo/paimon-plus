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

import org.apache.paimon.operation.nativeio.export.NativeExportContext;
import org.apache.paimon.operation.nativeio.export.NativeExportPlanDescriptor;
import org.apache.paimon.operation.nativeio.export.NativeExportPreflightResult;
import org.apache.paimon.operation.nativeio.export.NativeExportSourceFile;
import org.apache.paimon.options.Options;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;

import static org.assertj.core.api.Assertions.assertThat;

class NativeExportPlannerTest {

    @Test
    void acceptsValidNativeExportContext() {
        NativeExportPreflightResult result =
                new NativeExportPlanner()
                        .preflight(
                                new NativeExportContext(
                                        validOptions(),
                                        "obs://bucket/out",
                                        "zstd",
                                        536870912L,
                                        Arrays.asList("id", "dt"),
                                        1,
                                        Collections.singletonList(
                                                new NativeExportSourceFile(
                                                        "obs://bucket/table/file.parquet",
                                                        "parquet",
                                                        1,
                                                        128,
                                                        0,
                                                        Collections.singletonMap(
                                                                "dt", "2026-05-06"),
                                                        Collections.emptyList())),
                                        null));

        assertThat(result.applicable()).isTrue();
    }

    @Test
    void rejectsPlannedSplitsWithoutNativeSourceFiles() {
        NativeExportPreflightResult result =
                new NativeExportPlanner()
                        .preflight(
                                new NativeExportContext(
                                        validOptions(),
                                        "obs://bucket/out",
                                        "zstd",
                                        536870912L,
                                        Arrays.asList("id", "dt"),
                                        1,
                                        Collections.emptyList(),
                                        null));

        assertThat(result.applicable()).isFalse();
        assertThat(result.reason().name()).isEqualTo("NOT_RAW_CONVERTIBLE");
        assertThat(result.detail()).contains("planned splits");
    }

    @Test
    void rejectsVeryWideProjectionSoSparkProcedureFallsBackToJavaStreaming() {
        Options options = validOptions();
        options.setString("native-io.export.max-projected-fields", "3");

        NativeExportPreflightResult result =
                new NativeExportPlanner()
                        .preflight(
                                new NativeExportContext(
                                        options,
                                        "obs://bucket/out",
                                        "zstd",
                                        536870912L,
                                        Arrays.asList("c0", "c1", "c2", "c3"),
                                        1,
                                        Collections.singletonList(
                                                new NativeExportSourceFile(
                                                        "obs://bucket/table/file.parquet",
                                                        "parquet",
                                                        1,
                                                        128,
                                                        0,
                                                        Collections.emptyMap(),
                                                        Collections.emptyList())),
                                        null));

        assertThat(result.applicable()).isFalse();
        assertThat(result.reason().name()).isEqualTo("EXPORT_WIDE_SCHEMA");
        assertThat(result.detail()).contains("projected_fields=4").contains("max=3");
    }

    @Test
    void rejectsObsOutputPathWithQueryOrFragment() {
        NativeExportPreflightResult queryResult =
                new NativeExportPlanner()
                        .preflight(
                                validContext(
                                        "obs://bucket/out?version=1", "obs://bucket/in.parquet"));
        NativeExportPreflightResult fragmentResult =
                new NativeExportPlanner()
                        .preflight(
                                validContext(
                                        "obs://bucket/out#fragment", "obs://bucket/in.parquet"));

        assertThat(queryResult.applicable()).isFalse();
        assertThat(queryResult.reason().name()).isEqualTo("NON_OBS_PATH");
        assertThat(queryResult.detail()).contains("query");
        assertThat(fragmentResult.applicable()).isFalse();
        assertThat(fragmentResult.reason().name()).isEqualTo("NON_OBS_PATH");
        assertThat(fragmentResult.detail()).contains("fragment");
    }

    @Test
    void rejectsObsSourcePathWithQueryOrFragment() {
        NativeExportPreflightResult queryResult =
                new NativeExportPlanner()
                        .preflight(
                                validContext(
                                        "obs://bucket/out", "obs://bucket/in.parquet?version=1"));
        NativeExportPreflightResult fragmentResult =
                new NativeExportPlanner()
                        .preflight(
                                validContext(
                                        "obs://bucket/out", "obs://bucket/in.parquet#fragment"));

        assertThat(queryResult.applicable()).isFalse();
        assertThat(queryResult.reason().name()).isEqualTo("NON_OBS_PATH");
        assertThat(queryResult.detail()).contains("query");
        assertThat(fragmentResult.applicable()).isFalse();
        assertThat(fragmentResult.reason().name()).isEqualTo("NON_OBS_PATH");
        assertThat(fragmentResult.detail()).contains("fragment");
    }

    @Test
    void planCarriesNativeExportTuningOptionsIntoTaskPayload() {
        Options options = validOptions();
        options.setString("native-io.export.obs.read-buffer-size", "16 mb");
        options.setString("native-io.export.obs.read-concurrency", "8");
        options.setString("native-io.export.obs.request-timeout", "45 s");
        options.setString("native-io.export.obs.connect-timeout", "12 s");
        options.setString("native-io.export.writer.batch-size", "4096");
        options.setString("native-io.export.writer.row-group-size", "131072");
        options.setString("native-io.export.writer.multipart-part-size", "32 mb");
        options.setString("native-io.export.memory-limit", "256 mb");
        options.setString("native-io.export.runtime-threads", "6");
        options.setString("native-io.export.metadata-cache.enabled", "false");

        NativeExportPlanDescriptor plan =
                new NativeExportPlanner()
                        .plan(validContext(options, "obs://bucket/out", "obs://bucket/in.parquet"));

        assertThat(plan.taskPayloads()).hasSize(1);
        NativeExportTask task = NativeExportJson.taskFromPayload(plan.taskPayloads().get(0));
        assertThat(task.outputPath()).isEqualTo("obs://bucket/out");
        assertThat(task.readBufferSizeBytes()).isEqualTo(16L * 1024 * 1024);
        assertThat(task.readConcurrency()).isEqualTo(8);
        assertThat(task.obsRequestTimeoutMillis()).isEqualTo(45_000L);
        assertThat(task.obsConnectTimeoutMillis()).isEqualTo(12_000L);
        assertThat(task.writerBatchSize()).isEqualTo(4096);
        assertThat(task.writerRowGroupSize()).isEqualTo(131072);
        assertThat(task.multipartPartSizeBytes()).isEqualTo(32L * 1024 * 1024);
        assertThat(task.memoryLimitBytes()).isEqualTo(256L * 1024 * 1024);
        assertThat(task.runtimeThreads()).isEqualTo(6);
        assertThat(task.metadataCacheEnabled()).isFalse();
        assertThat(task.objectStoreOptions())
                .containsEntry("fs.obs.endpoint", "http://obs.example");
    }

    private static Options validOptions() {
        Options options = new Options();
        options.setString("native-io.enabled", "true");
        options.setString("__paimon.internal.native-io.engine", "spark");
        options.setString("native-io.export.enabled", "true");
        options.setString("fs.obs.endpoint", "http://obs.example");
        options.setString("fs.obs.access.key", "ak");
        options.setString("fs.obs.secret.key", "sk");
        return options;
    }

    private static NativeExportContext validContext(String outputPath, String sourcePath) {
        return validContext(validOptions(), outputPath, sourcePath);
    }

    private static NativeExportContext validContext(
            Options options, String outputPath, String sourcePath) {
        return new NativeExportContext(
                options,
                outputPath,
                "zstd",
                536870912L,
                Arrays.asList("id", "dt"),
                1,
                Collections.singletonList(
                        new NativeExportSourceFile(
                                sourcePath,
                                "parquet",
                                1,
                                128,
                                0,
                                Collections.singletonMap("dt", "2026-05-06"),
                                Collections.emptyList())),
                null);
    }
}
