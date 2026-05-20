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

/** Reasons why native IO cannot be used for a read path. */
public enum NativeRejectReason {
    DISABLED,
    UNSUPPORTED_ENGINE,
    NO_PROVIDER,
    NOT_DV_TABLE,
    STREAMING_SPLIT,
    FORCE_KEEP_DELETE,
    NOT_RAW_CONVERTIBLE,
    MISSING_DELETE_ROW_COUNT,
    NON_PARQUET_FILE,
    NON_OBS_PATH,
    MISSING_OBS_CONFIG,
    UNSUPPORTED_TYPE,
    PARQUET_PHYSICAL_TYPE,
    PARQUET_UNSUPPORTED_FEATURE,
    PARQUET_METADATA_MISMATCH,
    SCHEMA_CAST_OR_REORDER,
    READ_TYPE_MISMATCH,
    PARTITION_OR_SYSTEM_FIELDS,
    DATA_FILTER_TOPN_LIMIT,
    BITMAP_INDEX_SELECTION,
    INVALID_CONFIG,
    EXPORT_DISABLED,
    EXPORT_INVALID_CONFIG,
    EXPORT_UNSUPPORTED_COMPRESSION,
    EXPORT_UNSUPPORTED_PREDICATE,
    EXPORT_UNSUPPORTED_SCHEMA_EVOLUTION,
    EXPORT_UNSUPPORTED_DV,
    EXPORT_UNSUPPORTED_OUTPUT_TYPE,
    EXPORT_SPLIT_PLANNING_UNIMPLEMENTED,
    EXPORT_NATIVE_LIBRARY_UNAVAILABLE
}
