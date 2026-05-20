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
import org.apache.paimon.options.Options;

import java.util.Collections;
import java.util.Locale;

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
        if (!"zstd".equalsIgnoreCase(context.compression())) {
            return NativeExportPreflightResult.rejected(
                    NativeRejectReason.EXPORT_UNSUPPORTED_COMPRESSION,
                    "native export currently supports zstd compression only");
        }
        if (!context.outputPath().toLowerCase(Locale.ROOT).startsWith("obs://")) {
            return NativeExportPreflightResult.rejected(
                    NativeRejectReason.NON_OBS_PATH,
                    "native export requires an obs:// output path");
        }
        if (!nativeIOOptions.hasRequiredObsConfig()) {
            return NativeExportPreflightResult.rejected(
                    NativeRejectReason.MISSING_OBS_CONFIG, "missing required OBS configuration");
        }

        return NativeExportPreflightResult.rejected(
                NativeRejectReason.EXPORT_SPLIT_PLANNING_UNIMPLEMENTED,
                "native export split planning is not implemented in this build");
    }

    public NativeExportPlanDescriptor plan(NativeExportContext context) {
        NativeExportPreflightResult result = preflight(context);
        if (!result.applicable()) {
            throw new IllegalStateException(result.reason() + ": " + result.detail());
        }
        return new NativeExportPlanDescriptor(Collections.emptyList());
    }
}
