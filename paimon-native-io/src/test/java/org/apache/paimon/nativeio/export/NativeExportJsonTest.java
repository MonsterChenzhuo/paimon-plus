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

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

class NativeExportJsonTest {

    @Test
    void serializesTaskAndRedactsSecrets() {
        Map<String, String> objectStore = new LinkedHashMap<>();
        objectStore.put("fs.obs.endpoint", "http://obs.example");
        objectStore.put("fs.obs.access.key", "ak-value");
        objectStore.put("fs.obs.secret.key", "sk-value");
        objectStore.put("fs.obs.session.token", "token-value");
        NativeExportFile file =
                new NativeExportFile(
                        "obs://bucket/table/file.parquet",
                        10L,
                        1000L,
                        2L,
                        Collections.singletonList(3L));
        NativeExportTask task =
                new NativeExportTask(
                        "obs://bucket/export",
                        "zstd",
                        536870912L,
                        8388608L,
                        4,
                        8192,
                        250000,
                        67108864L,
                        536870912L,
                        4,
                        true,
                        Arrays.asList("id", "score"),
                        "paimon-json-v1",
                        "{\"op\":\"true\"}",
                        objectStore,
                        Collections.singletonList(file));

        String json = NativeExportJson.toJson(task);
        assertThat(json).contains("\"output_path\":\"obs://bucket/export\"");
        assertThat(json).contains("\"runtime_threads\":4");
        assertThat(json).contains("\"metadata_cache_enabled\":true");
        assertThat(json).contains("\"projection\":[\"id\",\"score\"]");
        assertThat(json).contains("\"positions\":[3]");

        String redacted = NativeExportJson.toRedactedJson(task);
        assertThat(redacted).contains("\"fs.obs.access.key\":\"******\"");
        assertThat(redacted).contains("\"fs.obs.secret.key\":\"******\"");
        assertThat(redacted).contains("\"fs.obs.session.token\":\"******\"");
        assertThat(redacted).doesNotContain("ak-value");
        assertThat(redacted).doesNotContain("sk-value");
        assertThat(redacted).doesNotContain("token-value");
    }

    @Test
    void parsesResultAndAggregatesMetrics() {
        String resultJson =
                "{"
                        + "\"rows_output\":2,"
                        + "\"files_written\":[{\"path\":\"obs://bucket/out/part.parquet\",\"rows\":2,\"bytes\":128}],"
                        + "\"metrics\":{"
                        + "\"rows_read\":3,"
                        + "\"rows_output\":2,"
                        + "\"predicate_filtered_rows\":1,"
                        + "\"dv_filtered_rows\":0,"
                        + "\"parquet_row_groups_read\":2,"
                        + "\"parquet_row_groups_pruned\":1,"
                        + "\"peak_buffered_bytes\":1048576,"
                        + "\"writer_rolls\":1,"
                        + "\"obs_read_requests\":4,"
                        + "\"obs_read_retries\":0,"
                        + "\"obs_read_bytes\":4096,"
                        + "\"obs_write_requests\":1,"
                        + "\"obs_write_bytes\":128,"
                        + "\"read_ms\":10,"
                        + "\"decode_ms\":20,"
                        + "\"filter_ms\":5,"
                        + "\"encode_ms\":30,"
                        + "\"obs_write_ms\":40,"
                        + "\"multipart_finish_ms\":3"
                        + "}"
                        + "}";

        NativeExportResult result = NativeExportJson.resultFromJson(resultJson);

        assertThat(result.rowsOutput()).isEqualTo(2L);
        assertThat(result.filesWritten()).hasSize(1);
        assertThat(result.metrics().parquetRowGroupsPruned()).isEqualTo(1L);
        assertThat(result.metrics().peakBufferedBytes()).isEqualTo(1048576L);
        assertThat(result.metrics().writerRolls()).isEqualTo(1L);
        assertThat(result.metrics().obsReadRequests()).isEqualTo(4L);
        assertThat(result.metrics().obsWriteMs()).isEqualTo(40L);
        assertThat(result.metrics().multipartFinishMs()).isEqualTo(3L);

        NativeExportMetrics sum = result.metrics().add(result.metrics());
        assertThat(sum.rowsOutput()).isEqualTo(4L);
        assertThat(sum.peakBufferedBytes()).isEqualTo(1048576L);
    }
}
