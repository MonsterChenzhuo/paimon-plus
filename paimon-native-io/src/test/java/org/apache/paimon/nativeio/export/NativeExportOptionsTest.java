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

import org.apache.paimon.options.Options;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class NativeExportOptionsTest {

    @Test
    void defaultsKeepNativeExportDisabled() {
        NativeExportOptions options = NativeExportOptions.from(new Options());

        assertThat(options.enabled()).isFalse();
        assertThat(options.fallbackEnabled()).isTrue();
        assertThat(options.failOnFallback()).isFalse();
        assertThat(options.metricsEnabled()).isTrue();
        assertThat(options.readBufferSizeBytes()).isEqualTo(8L * 1024 * 1024);
        assertThat(options.readConcurrency()).isEqualTo(4);
        assertThat(options.obsRequestTimeoutMillis()).isEqualTo(30000L);
        assertThat(options.obsConnectTimeoutMillis()).isEqualTo(10000L);
        assertThat(options.writerBatchSize()).isEqualTo(8192);
        assertThat(options.writerRowGroupSize()).isEqualTo(250000);
        assertThat(options.multipartPartSizeBytes()).isEqualTo(64L * 1024 * 1024);
        assertThat(options.memoryLimitBytes()).isEqualTo(512L * 1024 * 1024);
        assertThat(options.runtimeThreads()).isEqualTo(4);
        assertThat(options.metadataCacheEnabled()).isTrue();
    }

    @Test
    void validatesExportTunables() {
        Options raw = new Options();
        raw.setString("native-io.export.enabled", "true");
        raw.setString("native-io.export.obs.read-buffer-size", "0 b");

        assertThat(NativeExportOptions.from(raw).valid().applicable()).isFalse();
        assertThat(NativeExportOptions.from(raw).valid().detail())
                .contains("native-io.export.obs.read-buffer-size");

        raw.setString("native-io.export.obs.read-buffer-size", "8 mb");
        raw.setString("native-io.export.obs.read-concurrency", "0");
        assertThat(NativeExportOptions.from(raw).valid().detail())
                .contains("native-io.export.obs.read-concurrency");

        raw.setString("native-io.export.obs.read-concurrency", "4");
        raw.setString("native-io.export.writer.batch-size", "0");
        assertThat(NativeExportOptions.from(raw).valid().detail())
                .contains("native-io.export.writer.batch-size");

        raw.setString("native-io.export.writer.batch-size", "8192");
        raw.setString("native-io.export.writer.row-group-size", "0");
        assertThat(NativeExportOptions.from(raw).valid().detail())
                .contains("native-io.export.writer.row-group-size");

        raw.setString("native-io.export.writer.row-group-size", "250000");
        raw.setString("native-io.export.writer.multipart-part-size", "1 mb");
        assertThat(NativeExportOptions.from(raw).valid().detail())
                .contains("native-io.export.writer.multipart-part-size");

        raw.setString("native-io.export.writer.multipart-part-size", "64 mb");
        raw.setString("native-io.export.memory-limit", "1 mb");
        assertThat(NativeExportOptions.from(raw).valid().detail())
                .contains("native-io.export.memory-limit");

        raw.setString("native-io.export.memory-limit", "512 mb");
        raw.setString("native-io.export.runtime-threads", "0");
        assertThat(NativeExportOptions.from(raw).valid().detail())
                .contains("native-io.export.runtime-threads");

        raw.setString("native-io.export.runtime-threads", "4");
        raw.setString("native-io.export.obs.request-timeout", "0 ms");
        assertThat(NativeExportOptions.from(raw).valid().detail())
                .contains("native-io.export.obs.request-timeout");

        raw.setString("native-io.export.obs.request-timeout", "30 s");
        raw.setString("native-io.export.obs.connect-timeout", "0 ms");
        assertThat(NativeExportOptions.from(raw).valid().detail())
                .contains("native-io.export.obs.connect-timeout");
    }
}
