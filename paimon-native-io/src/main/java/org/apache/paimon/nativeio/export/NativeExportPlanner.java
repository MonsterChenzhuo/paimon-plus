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

import org.apache.paimon.operation.nativeio.NativeApplicability;
import org.apache.paimon.operation.nativeio.NativeIOOptions;
import org.apache.paimon.operation.nativeio.NativeRejectReason;
import org.apache.paimon.operation.nativeio.export.NativeExportContext;
import org.apache.paimon.operation.nativeio.export.NativeExportPlanDescriptor;
import org.apache.paimon.operation.nativeio.export.NativeExportPreflightResult;
import org.apache.paimon.operation.nativeio.export.NativeExportSourceFile;
import org.apache.paimon.options.Options;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Map;

/** Driver-side native export planner. */
public final class NativeExportPlanner {

    public NativeExportPreflightResult preflight(NativeExportContext context) {
        Options options = context.options();
        NativeIOOptions nativeIOOptions = NativeIOOptions.from(options);
        if (!nativeIOOptions.enabled()) {
            return NativeExportPreflightResult.rejected(
                    NativeRejectReason.DISABLED, "native IO is disabled");
        }
        if (!nativeIOOptions.engineSupportsNativeIO()) {
            return NativeExportPreflightResult.rejected(
                    NativeRejectReason.UNSUPPORTED_ENGINE, "native export requires Spark engine");
        }

        NativeExportOptions exportOptions = NativeExportOptions.from(options);
        NativeApplicability config = exportOptions.valid();
        if (!config.applicable()) {
            return NativeExportPreflightResult.rejected(config.reason(), config.detail());
        }
        int projectedFields = context.projectedFieldNames().size();
        int maxProjectedFields = exportOptions.maxProjectedFields();
        if (projectedFields > maxProjectedFields) {
            return NativeExportPreflightResult.rejected(
                    NativeRejectReason.EXPORT_WIDE_SCHEMA,
                    "native export falls back to Java streaming for wide projections: projected_fields="
                            + projectedFields
                            + ", max="
                            + maxProjectedFields);
        }
        if (!"zstd".equalsIgnoreCase(context.compression())) {
            return NativeExportPreflightResult.rejected(
                    NativeRejectReason.EXPORT_UNSUPPORTED_COMPRESSION,
                    "native export currently supports zstd compression only");
        }
        String outputPathError = obsPathError(context.outputPath());
        if (outputPathError != null) {
            return NativeExportPreflightResult.rejected(
                    NativeRejectReason.NON_OBS_PATH,
                    "native export requires an obs:// output path without query or fragment: "
                            + outputPathError);
        }
        if (!nativeIOOptions.hasRequiredObsConfig()) {
            return NativeExportPreflightResult.rejected(
                    NativeRejectReason.MISSING_OBS_CONFIG, "missing required OBS configuration");
        }
        if (context.plannedSplitCount() > 0 && context.sourceFiles().isEmpty()) {
            return NativeExportPreflightResult.rejected(
                    NativeRejectReason.NOT_RAW_CONVERTIBLE,
                    "planned splits cannot be converted to native parquet source files");
        }
        for (NativeExportSourceFile file : context.sourceFiles()) {
            if (!"parquet".equalsIgnoreCase(file.format())) {
                return NativeExportPreflightResult.rejected(
                        NativeRejectReason.NON_PARQUET_FILE,
                        "native export only supports parquet data files: " + file.path());
            }
            String path = file.path();
            String lowerPath = path.toLowerCase(Locale.ROOT);
            if (lowerPath.startsWith("obs://")) {
                String sourcePathError = obsPathError(path);
                if (sourcePathError != null) {
                    return NativeExportPreflightResult.rejected(
                            NativeRejectReason.NON_OBS_PATH,
                            "native export requires obs:// data files without query or fragment: "
                                    + sourcePathError);
                }
            } else if (!lowerPath.startsWith("file:")) {
                return NativeExportPreflightResult.rejected(
                        NativeRejectReason.NON_OBS_PATH,
                        "native export requires obs:// data files: " + path);
            }
        }
        NativeApplicability predicate = NativeExportPredicateJson.validate(context.predicate());
        if (!predicate.applicable()) {
            return NativeExportPreflightResult.rejected(predicate.reason(), predicate.detail());
        }

        return NativeExportPreflightResult.success();
    }

    public NativeExportPlanDescriptor plan(NativeExportContext context) {
        NativeExportPreflightResult result = preflight(context);
        if (!result.applicable()) {
            throw new IllegalStateException(result.reason() + ": " + result.detail());
        }
        if (context.sourceFiles().isEmpty()) {
            return new NativeExportPlanDescriptor(Collections.emptyList());
        }

        NativeExportOptions exportOptions = NativeExportOptions.from(context.options());
        NativeIOOptions nativeIOOptions = NativeIOOptions.from(context.options());
        Map<String, String> objectStoreOptions = nativeIOOptions.objectStoreOptions();
        List<byte[]> payloads = new ArrayList<>();
        for (NativeExportSourceFile sourceFile : context.sourceFiles()) {
            NativeExportFile file =
                    new NativeExportFile(
                            sourceFile.path(),
                            sourceFile.rowCount(),
                            sourceFile.fileSize(),
                            sourceFile.schemaId(),
                            sourceFile.partition(),
                            sourceFile.deletedPositions());
            NativeExportTask task =
                    new NativeExportTask(
                            context.outputPath(),
                            context.compression(),
                            context.targetFileSizeBytes() == null
                                    ? Long.MAX_VALUE
                                    : context.targetFileSizeBytes(),
                            exportOptions.readBufferSizeBytes(),
                            exportOptions.readConcurrency(),
                            exportOptions.obsRequestTimeoutMillis(),
                            exportOptions.obsConnectTimeoutMillis(),
                            exportOptions.writerBatchSize(),
                            exportOptions.writerRowGroupSize(),
                            exportOptions.multipartPartSizeBytes(),
                            exportOptions.memoryLimitBytes(),
                            exportOptions.runtimeThreads(),
                            exportOptions.metadataCacheEnabled(),
                            context.projectedFieldNames(),
                            NativeExportPredicateJson.FORMAT,
                            NativeExportPredicateJson.toJson(context.predicate()),
                            objectStoreOptions,
                            Collections.singletonList(file));
            payloads.add(NativeExportJson.taskToPayload(task));
        }
        return new NativeExportPlanDescriptor(payloads);
    }

    private static String obsPathError(String path) {
        URI uri;
        try {
            uri = new URI(path);
        } catch (URISyntaxException e) {
            return "invalid URI " + path + ": " + e.getMessage();
        }
        if (!"obs".equalsIgnoreCase(uri.getScheme()) || uri.getHost() == null) {
            return "not an obs:// path: " + path;
        }
        if (uri.getQuery() != null) {
            return "query is not supported in OBS path: " + path;
        }
        if (uri.getFragment() != null) {
            return "fragment is not supported in OBS path: " + path;
        }
        return null;
    }
}
