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

import org.apache.paimon.fileindex.FileIndexResult;
import org.apache.paimon.fileindex.bitmap.BitmapIndexResult;
import org.apache.paimon.fs.Path;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.table.source.DataSplit;
import org.apache.paimon.table.source.splitread.SplitReadProvider;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.DataTypeRoot;
import org.apache.paimon.types.RowType;
import org.apache.paimon.types.TimestampType;
import org.apache.paimon.utils.FormatReaderMapping;

import javax.annotation.Nullable;

import java.net.URI;
import java.util.Objects;

/** Guard helpers shared by core and optional native IO providers. */
public final class SupportsNativeIO {

    private SupportsNativeIO() {}

    public static NativeApplicability checkEngine(NativeIOOptions options) {
        return options.valid();
    }

    public static NativeApplicability checkNativeSplit(
            DataSplit split,
            NativeIOOptions options,
            SplitReadProvider.Context context,
            boolean providerAvailable) {
        NativeApplicability engine = checkEngine(options);
        if (!engine.applicable()) {
            return engine;
        }
        if (!providerAvailable) {
            return NativeApplicability.rejected(
                    NativeRejectReason.NO_PROVIDER, "no native IO provider was discovered");
        }
        if (context.forceKeepDelete()) {
            return NativeApplicability.rejected(
                    NativeRejectReason.FORCE_KEEP_DELETE, "force keep delete is enabled");
        }
        if (split.isStreaming()) {
            return NativeApplicability.rejected(
                    NativeRejectReason.STREAMING_SPLIT, "streaming splits are not supported");
        }
        if (!split.rawConvertible()) {
            return NativeApplicability.rejected(
                    NativeRejectReason.NOT_RAW_CONVERTIBLE, "split is not raw-convertible");
        }
        for (DataFileMeta file : split.dataFiles()) {
            if (!file.deleteRowCount().isPresent()) {
                return NativeApplicability.rejected(
                        NativeRejectReason.MISSING_DELETE_ROW_COUNT,
                        "data file delete row count is missing");
            }
        }
        return NativeApplicability.yes();
    }

    public static NativeApplicability checkNativeFile(
            DataFileMeta file,
            @Nullable FormatReaderMapping mapping,
            RowType readType,
            boolean rowTrackingEnabled,
            NativeIOOptions options,
            Path actualDataPath) {
        NativeApplicability physicalFileApplicability =
                checkNativePhysicalFile(
                        file,
                        readType,
                        rowTrackingEnabled,
                        hasFilterTopNLimitPushDown(mapping),
                        options,
                        actualDataPath);
        if (!physicalFileApplicability.applicable()) {
            return physicalFileApplicability;
        }
        if (mapping != null
                && (mapping.getPartitionPair() != null || !mapping.getSystemFields().isEmpty())) {
            return NativeApplicability.rejected(
                    NativeRejectReason.PARTITION_OR_SYSTEM_FIELDS,
                    "native IO does not support partition, system, or row tracking fields yet");
        }
        if (mapping != null) {
            if (!mapping.hasIdentityIndexMapping() || !mapping.hasNoCastMapping()) {
                return NativeApplicability.rejected(
                        NativeRejectReason.SCHEMA_CAST_OR_REORDER,
                        "native IO does not support schema cast or field reordering yet");
            }
            RowType actualReadRowType = mapping.getActualReadRowType();
            if (actualReadRowType != null && !Objects.equals(actualReadRowType, readType)) {
                return NativeApplicability.rejected(
                        NativeRejectReason.READ_TYPE_MISMATCH,
                        "native IO physical read type differs from requested read type");
            }
        }
        return NativeApplicability.yes();
    }

    public static NativeApplicability checkNativePhysicalFile(
            DataFileMeta file,
            RowType readType,
            boolean rowTrackingEnabled,
            boolean hasFilterTopNLimitPushDown,
            NativeIOOptions options,
            Path actualDataPath) {
        if (!"parquet".equalsIgnoreCase(file.fileFormat())) {
            return NativeApplicability.rejected(
                    NativeRejectReason.NON_PARQUET_FILE,
                    "native IO only supports Parquet data files");
        }
        URI dataUri = actualDataPath.toUri();
        String scheme = dataUri.getScheme();
        if (!"obs".equalsIgnoreCase(scheme)) {
            return NativeApplicability.rejected(
                    NativeRejectReason.NON_OBS_PATH, "native IO only supports obs:// data paths");
        }
        if (hasQueryOrFragment(dataUri)) {
            return NativeApplicability.rejected(
                    NativeRejectReason.NON_OBS_PATH,
                    "native IO does not support OBS paths with query or fragment");
        }
        if (!options.hasRequiredObsConfig()) {
            return NativeApplicability.rejected(
                    NativeRejectReason.MISSING_OBS_CONFIG,
                    "native IO requires fs.obs endpoint/access/secret options or OBS environment variables");
        }
        if (hasUnsupportedTypes(readType)) {
            return NativeApplicability.rejected(
                    NativeRejectReason.UNSUPPORTED_TYPE,
                    "native IO does not support at least one requested field type");
        }
        if (rowTrackingEnabled) {
            return NativeApplicability.rejected(
                    NativeRejectReason.PARTITION_OR_SYSTEM_FIELDS,
                    "native IO does not support partition, system, or row tracking fields yet");
        }
        if (hasFilterTopNLimitPushDown) {
            return NativeApplicability.rejected(
                    NativeRejectReason.DATA_FILTER_TOPN_LIMIT,
                    "native IO does not support filter, topN, or limit pushdown yet");
        }
        return NativeApplicability.yes();
    }

    private static boolean hasFilterTopNLimitPushDown(@Nullable FormatReaderMapping mapping) {
        return mapping != null
                && (mapping.getDataFilters() != null && !mapping.getDataFilters().isEmpty()
                        || mapping.getTopN() != null
                        || mapping.getLimit() != null);
    }

    private static boolean hasQueryOrFragment(URI uri) {
        String path = uri.getPath();
        return uri.getQuery() != null
                || uri.getFragment() != null
                || (path != null && (path.indexOf('?') >= 0 || path.indexOf('#') >= 0));
    }

    public static NativeApplicability checkFileIndexResult(
            @Nullable FileIndexResult fileIndexResult) {
        if (fileIndexResult instanceof BitmapIndexResult) {
            return NativeApplicability.rejected(
                    NativeRejectReason.BITMAP_INDEX_SELECTION,
                    "native IO does not support bitmap index selections yet");
        }
        return NativeApplicability.yes();
    }

    public static boolean hasUnsupportedTypes(RowType rowType) {
        for (DataType fieldType : rowType.getFieldTypes()) {
            if (hasUnsupportedType(fieldType)) {
                return true;
            }
        }
        return false;
    }

    private static boolean hasUnsupportedType(DataType type) {
        DataTypeRoot root = type.getTypeRoot();
        switch (root) {
            case CHAR:
            case VARCHAR:
            case BOOLEAN:
            case BINARY:
            case VARBINARY:
            case DECIMAL:
            case TINYINT:
            case SMALLINT:
            case INTEGER:
            case BIGINT:
            case FLOAT:
            case DOUBLE:
            case DATE:
            case TIME_WITHOUT_TIME_ZONE:
                return false;
            case TIMESTAMP_WITHOUT_TIME_ZONE:
                return ((TimestampType) type).getPrecision() > 6;
            default:
                return true;
        }
    }
}
