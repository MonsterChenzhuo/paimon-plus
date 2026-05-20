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

import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonCreator;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonProperty;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/** Versioned request passed to the native export FFI. */
public final class NativeExportTask implements Serializable {

    private static final long serialVersionUID = 1L;

    private final int requestVersion;
    private final String outputPath;
    private final String compression;
    private final long targetFileSizeBytes;
    private final long readBufferSizeBytes;
    private final int readConcurrency;
    private final int writerBatchSize;
    private final int writerRowGroupSize;
    private final long multipartPartSizeBytes;
    private final long memoryLimitBytes;
    private final int runtimeThreads;
    private final boolean metadataCacheEnabled;
    private final List<String> projection;
    private final String predicateFormat;
    private final String predicateJson;
    private final Map<String, String> objectStoreOptions;
    private final List<NativeExportFile> files;

    public NativeExportTask(
            String outputPath,
            String compression,
            long targetFileSizeBytes,
            long readBufferSizeBytes,
            int readConcurrency,
            int writerBatchSize,
            int writerRowGroupSize,
            long multipartPartSizeBytes,
            long memoryLimitBytes,
            int runtimeThreads,
            boolean metadataCacheEnabled,
            List<String> projection,
            String predicateFormat,
            String predicateJson,
            Map<String, String> objectStoreOptions,
            List<NativeExportFile> files) {
        this(
                1,
                outputPath,
                compression,
                targetFileSizeBytes,
                readBufferSizeBytes,
                readConcurrency,
                writerBatchSize,
                writerRowGroupSize,
                multipartPartSizeBytes,
                memoryLimitBytes,
                runtimeThreads,
                metadataCacheEnabled,
                projection,
                predicateFormat,
                predicateJson,
                objectStoreOptions,
                files);
    }

    @JsonCreator
    public NativeExportTask(
            @JsonProperty("request_version") int requestVersion,
            @JsonProperty("output_path") String outputPath,
            @JsonProperty("compression") String compression,
            @JsonProperty("target_file_size_bytes") long targetFileSizeBytes,
            @JsonProperty("read_buffer_size_bytes") long readBufferSizeBytes,
            @JsonProperty("read_concurrency") int readConcurrency,
            @JsonProperty("writer_batch_size") int writerBatchSize,
            @JsonProperty("writer_row_group_size") int writerRowGroupSize,
            @JsonProperty("multipart_part_size_bytes") long multipartPartSizeBytes,
            @JsonProperty("memory_limit_bytes") long memoryLimitBytes,
            @JsonProperty("runtime_threads") int runtimeThreads,
            @JsonProperty("metadata_cache_enabled") boolean metadataCacheEnabled,
            @JsonProperty("projection") List<String> projection,
            @JsonProperty("predicate_format") String predicateFormat,
            @JsonProperty("predicate_json") String predicateJson,
            @JsonProperty("object_store") Map<String, String> objectStoreOptions,
            @JsonProperty("files") List<NativeExportFile> files) {
        this.requestVersion = requestVersion;
        this.outputPath = outputPath;
        this.compression = compression;
        this.targetFileSizeBytes = targetFileSizeBytes;
        this.readBufferSizeBytes = readBufferSizeBytes;
        this.readConcurrency = readConcurrency;
        this.writerBatchSize = writerBatchSize;
        this.writerRowGroupSize = writerRowGroupSize;
        this.multipartPartSizeBytes = multipartPartSizeBytes;
        this.memoryLimitBytes = memoryLimitBytes;
        this.runtimeThreads = runtimeThreads;
        this.metadataCacheEnabled = metadataCacheEnabled;
        this.projection =
                Collections.unmodifiableList(
                        new ArrayList<>(projection == null ? Collections.emptyList() : projection));
        this.predicateFormat = predicateFormat;
        this.predicateJson = predicateJson;
        this.objectStoreOptions =
                Collections.unmodifiableMap(
                        new LinkedHashMap<>(
                                objectStoreOptions == null
                                        ? Collections.emptyMap()
                                        : objectStoreOptions));
        this.files =
                Collections.unmodifiableList(
                        new ArrayList<>(files == null ? Collections.emptyList() : files));
    }

    @JsonProperty("request_version")
    public int requestVersion() {
        return requestVersion;
    }

    @JsonProperty("output_path")
    public String outputPath() {
        return outputPath;
    }

    @JsonProperty("compression")
    public String compression() {
        return compression;
    }

    @JsonProperty("target_file_size_bytes")
    public long targetFileSizeBytes() {
        return targetFileSizeBytes;
    }

    @JsonProperty("read_buffer_size_bytes")
    public long readBufferSizeBytes() {
        return readBufferSizeBytes;
    }

    @JsonProperty("read_concurrency")
    public int readConcurrency() {
        return readConcurrency;
    }

    @JsonProperty("writer_batch_size")
    public int writerBatchSize() {
        return writerBatchSize;
    }

    @JsonProperty("writer_row_group_size")
    public int writerRowGroupSize() {
        return writerRowGroupSize;
    }

    @JsonProperty("multipart_part_size_bytes")
    public long multipartPartSizeBytes() {
        return multipartPartSizeBytes;
    }

    @JsonProperty("memory_limit_bytes")
    public long memoryLimitBytes() {
        return memoryLimitBytes;
    }

    @JsonProperty("runtime_threads")
    public int runtimeThreads() {
        return runtimeThreads;
    }

    @JsonProperty("metadata_cache_enabled")
    public boolean metadataCacheEnabled() {
        return metadataCacheEnabled;
    }

    @JsonProperty("projection")
    public List<String> projection() {
        return projection;
    }

    @JsonProperty("predicate_format")
    public String predicateFormat() {
        return predicateFormat;
    }

    @JsonProperty("predicate_json")
    public String predicateJson() {
        return predicateJson;
    }

    @JsonProperty("object_store")
    public Map<String, String> objectStoreOptions() {
        return objectStoreOptions;
    }

    @JsonProperty("files")
    public List<NativeExportFile> files() {
        return files;
    }

    NativeExportTask withObjectStoreOptions(Map<String, String> options) {
        return new NativeExportTask(
                requestVersion,
                outputPath,
                compression,
                targetFileSizeBytes,
                readBufferSizeBytes,
                readConcurrency,
                writerBatchSize,
                writerRowGroupSize,
                multipartPartSizeBytes,
                memoryLimitBytes,
                runtimeThreads,
                metadataCacheEnabled,
                projection,
                predicateFormat,
                predicateJson,
                options,
                files);
    }
}
