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
}
