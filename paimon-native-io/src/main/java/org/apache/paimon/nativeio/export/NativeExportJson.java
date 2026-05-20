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

import org.apache.paimon.utils.JsonSerdeUtil;

import java.nio.charset.StandardCharsets;
import java.util.LinkedHashMap;
import java.util.Locale;
import java.util.Map;

/** JSON serialization helpers for native export requests and results. */
public final class NativeExportJson {

    private NativeExportJson() {}

    public static String toJson(NativeExportTask task) {
        return JsonSerdeUtil.toFlatJson(task);
    }

    public static String toRedactedJson(NativeExportTask task) {
        Map<String, String> redacted = new LinkedHashMap<>();
        for (Map.Entry<String, String> entry : task.objectStoreOptions().entrySet()) {
            redacted.put(entry.getKey(), isSecretKey(entry.getKey()) ? "******" : entry.getValue());
        }
        return toJson(task.withObjectStoreOptions(redacted));
    }

    public static NativeExportResult resultFromJson(String json) {
        return JsonSerdeUtil.fromJson(json, NativeExportResult.class);
    }

    public static byte[] taskToPayload(NativeExportTask task) {
        return toJson(task).getBytes(StandardCharsets.UTF_8);
    }

    public static NativeExportTask taskFromPayload(byte[] payload) {
        return JsonSerdeUtil.fromJson(
                new String(payload, StandardCharsets.UTF_8), NativeExportTask.class);
    }

    public static String metricsToJson(NativeExportMetrics metrics) {
        return JsonSerdeUtil.toFlatJson(metrics);
    }

    private static boolean isSecretKey(String key) {
        String normalized = key.toLowerCase(Locale.ROOT);
        return normalized.contains("access.key")
                || normalized.contains("secret.key")
                || normalized.contains("security.token")
                || normalized.contains("session.token")
                || normalized.endsWith(".ak")
                || normalized.endsWith(".sk");
    }
}
