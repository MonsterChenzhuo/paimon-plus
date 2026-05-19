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

import org.apache.paimon.options.Options;

import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

class NativeIOOptionsTest {

    @Test
    void defaultsToDisabled() {
        NativeIOOptions options = NativeIOOptions.from(new Options());

        assertThat(options.enabled()).isFalse();
        assertThat(options.batchSize()).isEqualTo(4096);
        assertThat(options.maxBatchBytes().getBytes()).isEqualTo(64L * 1024 * 1024);
        assertThat(options.engineSupportsNativeIO()).isFalse();
    }

    @Test
    void validatesBatchSizeAndMaxBatchBytes() {
        Options raw = new Options();
        raw.setString("native-io.enabled", "true");
        raw.setString("native-io.batch-size", "0");

        NativeApplicability invalidBatch = NativeIOOptions.from(raw).valid();
        assertThat(invalidBatch.applicable()).isFalse();
        assertThat(invalidBatch.reason()).isEqualTo(NativeRejectReason.INVALID_CONFIG);

        raw.setString("native-io.batch-size", "4096");
        raw.setString("native-io.max-batch-bytes", "512 kb");
        NativeApplicability invalidBytes = NativeIOOptions.from(raw).valid();
        assertThat(invalidBytes.applicable()).isFalse();
        assertThat(invalidBytes.reason()).isEqualTo(NativeRejectReason.INVALID_CONFIG);
    }

    @Test
    void recognizesSparkInternalEngineMarker() {
        Options raw = new Options();
        raw.setString("native-io.enabled", "true");
        raw.setString("__paimon.internal.native-io.engine", "spark");

        NativeIOOptions options = NativeIOOptions.from(raw);

        assertThat(options.enabled()).isTrue();
        assertThat(options.engineSupportsNativeIO()).isTrue();
        assertThat(options.valid().applicable()).isTrue();
    }

    @Test
    void acceptsObsConfigFromEnvironmentFallback() {
        NativeIOOptions options = NativeIOOptions.from(new Options());
        Map<String, String> env = new HashMap<>();

        env.put("OBS_ENDPOINT", "obs-endpoint");
        env.put("OBS_ACCESS_KEY_ID", "ak");
        env.put("OBS_SECRET_ACCESS_KEY", "sk");

        assertThat(options.hasRequiredObsConfig(env)).isTrue();
    }

    @Test
    void requiresCompleteObsConfigAcrossOptionsAndEnvironment() {
        Options raw = new Options();
        raw.setString("fs.obs.endpoint", "obs-endpoint");
        NativeIOOptions options = NativeIOOptions.from(raw);
        Map<String, String> env = new HashMap<>();

        env.put("OBS_ACCESS_KEY_ID", "ak");

        assertThat(options.hasRequiredObsConfig(env)).isFalse();

        env.put("OBS_SECRET_ACCESS_KEY", "sk");
        assertThat(options.hasRequiredObsConfig(env)).isTrue();
    }

    @Test
    void objectStoreOptionsSkipEmptyObsValues() {
        Options raw = new Options();
        raw.setString("fs.obs.endpoint", "obs-endpoint");
        raw.setString("fs.obs.access.key", "");

        Map<String, String> objectStoreOptions = NativeIOOptions.from(raw).objectStoreOptions();

        assertThat(objectStoreOptions).containsEntry("fs.obs.endpoint", "obs-endpoint");
        assertThat(objectStoreOptions).doesNotContainKey("fs.obs.access.key");
    }

    @Test
    void objectStoreOptionsSkipBlankObsValues() {
        Options raw = new Options();
        raw.setString("fs.obs.endpoint", "obs-endpoint");
        raw.setString("fs.obs.access.key", "   ");

        Map<String, String> objectStoreOptions = NativeIOOptions.from(raw).objectStoreOptions();

        assertThat(objectStoreOptions).containsEntry("fs.obs.endpoint", "obs-endpoint");
        assertThat(objectStoreOptions).doesNotContainKey("fs.obs.access.key");
    }

    @Test
    void objectStoreOptionsCanonicalizeObsCredentialAliases() {
        Options raw = new Options();
        raw.setString("fs.obs.endpoint", "obs-endpoint");
        raw.setString("fs.obs.accessKey", "ak");
        raw.setString("fs.obs.sk", "sk");

        Map<String, String> objectStoreOptions = NativeIOOptions.from(raw).objectStoreOptions();

        assertThat(objectStoreOptions).containsEntry("fs.obs.endpoint", "obs-endpoint");
        assertThat(objectStoreOptions).containsEntry("fs.obs.access.key", "ak");
        assertThat(objectStoreOptions).containsEntry("fs.obs.secret.key", "sk");
        assertThat(objectStoreOptions).doesNotContainKeys("fs.obs.accessKey", "fs.obs.sk");
    }

    @Test
    void objectStoreOptionsIncludeEnvironmentFallbacks() {
        NativeIOOptions options = NativeIOOptions.from(new Options());
        Map<String, String> env = new HashMap<>();

        env.put("OBS_ENDPOINT", "obs-endpoint");
        env.put("OBS_ACCESS_KEY_ID", "ak");
        env.put("OBS_SECRET_ACCESS_KEY", "sk");

        Map<String, String> objectStoreOptions = options.objectStoreOptions(env);

        assertThat(objectStoreOptions).containsEntry("fs.obs.endpoint", "obs-endpoint");
        assertThat(objectStoreOptions).containsEntry("fs.obs.access.key", "ak");
        assertThat(objectStoreOptions).containsEntry("fs.obs.secret.key", "sk");
    }

    @Test
    void requiresNonBlankObsConfigAcrossOptionsAndEnvironment() {
        Options raw = new Options();
        raw.setString("fs.obs.endpoint", "   ");
        NativeIOOptions options = NativeIOOptions.from(raw);
        Map<String, String> env = new HashMap<>();

        env.put("OBS_ACCESS_KEY_ID", "ak");
        env.put("OBS_SECRET_ACCESS_KEY", "sk");

        assertThat(options.hasRequiredObsConfig(env)).isFalse();

        env.put("OBS_ENDPOINT", "obs-endpoint");
        assertThat(options.hasRequiredObsConfig(env)).isTrue();
    }
}
