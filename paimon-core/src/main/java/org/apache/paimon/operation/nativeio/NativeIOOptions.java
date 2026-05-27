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

import org.apache.paimon.CoreOptions;
import org.apache.paimon.options.MemorySize;
import org.apache.paimon.options.Options;

import java.util.HashMap;
import java.util.Map;

/** Native IO read options. */
public final class NativeIOOptions {

    private static final String SPARK_ENGINE = "spark";
    private static final String OBS_PREFIX = "fs.obs.";
    private static final String OBS_ACCESS_KEY = "fs.obs.access.key";
    private static final String OBS_SECRET_KEY = "fs.obs.secret.key";
    private static final String[] OBS_ACCESS_KEY_ALIASES = {"fs.obs.accessKey", "fs.obs.ak"};
    private static final String[] OBS_SECRET_KEY_ALIASES = {"fs.obs.secretKey", "fs.obs.sk"};
    private static final String[] OBS_ACCESS_KEY_ENV = {
        "OBS_ACCESS_KEY_ID", "OBS_ACCESS_KEY", "HUAWEICLOUD_OBS_ACCESS_KEY_ID", "AWS_ACCESS_KEY_ID"
    };
    private static final String[] OBS_SECRET_KEY_ENV = {
        "OBS_SECRET_ACCESS_KEY",
        "OBS_SECRET_KEY",
        "HUAWEICLOUD_OBS_SECRET_ACCESS_KEY",
        "AWS_SECRET_ACCESS_KEY"
    };
    private static final String[] OBS_ENDPOINT_ENV = {"OBS_ENDPOINT", "HUAWEICLOUD_OBS_ENDPOINT"};
    private static final long MIN_BATCH_BYTES = 1024L * 1024;
    private static final long MAX_BATCH_BYTES = 512L * 1024 * 1024;

    private final Options options;

    private NativeIOOptions(Options options) {
        this.options = options;
    }

    public static NativeIOOptions from(Options options) {
        return new NativeIOOptions(options);
    }

    public boolean enabled() {
        return options.get(CoreOptions.NATIVE_IO_ENABLED);
    }

    public int batchSize() {
        return options.get(CoreOptions.NATIVE_IO_BATCH_SIZE);
    }

    public MemorySize maxBatchBytes() {
        return options.get(CoreOptions.NATIVE_IO_MAX_BATCH_BYTES);
    }

    public boolean columnarEnabled() {
        return options.get(CoreOptions.NATIVE_IO_COLUMNAR_ENABLED);
    }

    public boolean engineSupportsNativeIO() {
        String engine = options.get(CoreOptions.NATIVE_IO_INTERNAL_ENGINE);
        return SPARK_ENGINE.equalsIgnoreCase(engine);
    }

    public Map<String, String> objectStoreOptions() {
        return objectStoreOptions(System.getenv());
    }

    Map<String, String> objectStoreOptions(Map<String, String> env) {
        Map<String, String> objectStoreOptions = new HashMap<>();
        for (Map.Entry<String, String> entry : options.toMap().entrySet()) {
            String key = entry.getKey();
            if (key.startsWith(OBS_PREFIX)
                    && !isObsCredentialAlias(key)
                    && isNonBlank(entry.getValue())) {
                objectStoreOptions.put(key, entry.getValue());
            }
        }
        putObsAliasIfAbsent(objectStoreOptions, OBS_ACCESS_KEY, OBS_ACCESS_KEY_ALIASES);
        putObsAliasIfAbsent(objectStoreOptions, OBS_SECRET_KEY, OBS_SECRET_KEY_ALIASES);
        putEnvIfAbsent(objectStoreOptions, "fs.obs.endpoint", env, OBS_ENDPOINT_ENV);
        putEnvIfAbsent(objectStoreOptions, OBS_ACCESS_KEY, env, OBS_ACCESS_KEY_ENV);
        putEnvIfAbsent(objectStoreOptions, OBS_SECRET_KEY, env, OBS_SECRET_KEY_ENV);
        return objectStoreOptions;
    }

    public boolean hasRequiredObsConfig() {
        return hasRequiredObsConfig(System.getenv());
    }

    boolean hasRequiredObsConfig(Map<String, String> env) {
        boolean hasEndpoint =
                hasNonEmpty("fs.obs.endpoint") || hasAnyNonEmpty(env, OBS_ENDPOINT_ENV);
        boolean hasAccessKey =
                hasAnyNonEmpty("fs.obs.access.key", "fs.obs.accessKey", "fs.obs.ak")
                        || hasAnyNonEmpty(env, OBS_ACCESS_KEY_ENV);
        boolean hasSecretKey =
                hasAnyNonEmpty("fs.obs.secret.key", "fs.obs.secretKey", "fs.obs.sk")
                        || hasAnyNonEmpty(env, OBS_SECRET_KEY_ENV);
        return hasEndpoint && hasAccessKey && hasSecretKey;
    }

    private boolean hasAnyNonEmpty(Map<String, String> values, String... keys) {
        for (String key : keys) {
            String value = values.get(key);
            if (isNonBlank(value)) {
                return true;
            }
        }
        return false;
    }

    private boolean hasAnyNonEmpty(String... keys) {
        for (String key : keys) {
            if (hasNonEmpty(key)) {
                return true;
            }
        }
        return false;
    }

    private boolean hasNonEmpty(String key) {
        String value = options.get(key);
        return isNonBlank(value);
    }

    private void putObsAliasIfAbsent(
            Map<String, String> objectStoreOptions, String canonicalKey, String... aliases) {
        if (objectStoreOptions.containsKey(canonicalKey)) {
            return;
        }
        for (String alias : aliases) {
            String value = options.get(alias);
            if (isNonBlank(value)) {
                objectStoreOptions.put(canonicalKey, value);
                return;
            }
        }
    }

    private static void putEnvIfAbsent(
            Map<String, String> objectStoreOptions,
            String canonicalKey,
            Map<String, String> env,
            String... envKeys) {
        if (objectStoreOptions.containsKey(canonicalKey)) {
            return;
        }
        for (String envKey : envKeys) {
            String value = env.get(envKey);
            if (isNonBlank(value)) {
                objectStoreOptions.put(canonicalKey, value);
                return;
            }
        }
    }

    private static boolean isObsCredentialAlias(String key) {
        for (String alias : OBS_ACCESS_KEY_ALIASES) {
            if (alias.equals(key)) {
                return true;
            }
        }
        for (String alias : OBS_SECRET_KEY_ALIASES) {
            if (alias.equals(key)) {
                return true;
            }
        }
        return false;
    }

    private static boolean isNonBlank(String value) {
        return value != null && !value.trim().isEmpty();
    }

    public NativeApplicability valid() {
        if (!enabled()) {
            return NativeApplicability.rejected(
                    NativeRejectReason.DISABLED, "native IO is disabled");
        }
        int batchSize = batchSize();
        if (batchSize < 1 || batchSize > 65536) {
            return NativeApplicability.rejected(
                    NativeRejectReason.INVALID_CONFIG,
                    "native-io.batch-size must be between 1 and 65536");
        }
        long bytes = maxBatchBytes().getBytes();
        if (bytes < MIN_BATCH_BYTES || bytes > MAX_BATCH_BYTES) {
            return NativeApplicability.rejected(
                    NativeRejectReason.INVALID_CONFIG,
                    "native-io.max-batch-bytes must be between 1 MB and 512 MB");
        }
        if (!engineSupportsNativeIO()) {
            return NativeApplicability.rejected(
                    NativeRejectReason.UNSUPPORTED_ENGINE, "execution engine does not opt in");
        }
        return NativeApplicability.yes();
    }
}
