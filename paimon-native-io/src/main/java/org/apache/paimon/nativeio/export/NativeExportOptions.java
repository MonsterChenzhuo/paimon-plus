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

import org.apache.paimon.CoreOptions;
import org.apache.paimon.operation.nativeio.NativeApplicability;
import org.apache.paimon.operation.nativeio.NativeRejectReason;
import org.apache.paimon.options.Options;

/** Native export_parquet fast path options. */
public final class NativeExportOptions {

    private static final long MIN_READ_BUFFER_BYTES = 1024L * 1024;
    private static final long MAX_READ_BUFFER_BYTES = 128L * 1024 * 1024;
    private static final int MIN_READ_CONCURRENCY = 1;
    private static final int MAX_READ_CONCURRENCY = 64;
    private static final int MIN_WRITER_BATCH_SIZE = 1;
    private static final int MAX_WRITER_BATCH_SIZE = 65536;
    private static final int MIN_WRITER_ROW_GROUP_SIZE = 1024;
    private static final int MAX_WRITER_ROW_GROUP_SIZE = 10000000;
    private static final long MIN_MULTIPART_PART_SIZE_BYTES = 5L * 1024 * 1024;
    private static final long MAX_MULTIPART_PART_SIZE_BYTES = 512L * 1024 * 1024;
    private static final long MIN_MEMORY_LIMIT_BYTES = 64L * 1024 * 1024;
    private static final long MAX_MEMORY_LIMIT_BYTES = 16L * 1024 * 1024 * 1024;
    private static final int MIN_RUNTIME_THREADS = 1;
    private static final int MAX_RUNTIME_THREADS = 64;

    private final Options options;

    private NativeExportOptions(Options options) {
        this.options = options;
    }

    public static NativeExportOptions from(Options options) {
        return new NativeExportOptions(options);
    }

    public boolean enabled() {
        return options.get(CoreOptions.NATIVE_IO_EXPORT_ENABLED);
    }

    public boolean metricsEnabled() {
        return options.get(CoreOptions.NATIVE_IO_EXPORT_METRICS_ENABLED);
    }

    public long readBufferSizeBytes() {
        return options.get(CoreOptions.NATIVE_IO_EXPORT_OBS_READ_BUFFER_SIZE).getBytes();
    }

    public int readConcurrency() {
        return options.get(CoreOptions.NATIVE_IO_EXPORT_OBS_READ_CONCURRENCY);
    }

    public int writerBatchSize() {
        return options.get(CoreOptions.NATIVE_IO_EXPORT_WRITER_BATCH_SIZE);
    }

    public int writerRowGroupSize() {
        return options.get(CoreOptions.NATIVE_IO_EXPORT_WRITER_ROW_GROUP_SIZE);
    }

    public long multipartPartSizeBytes() {
        return options.get(CoreOptions.NATIVE_IO_EXPORT_WRITER_MULTIPART_PART_SIZE).getBytes();
    }

    public long memoryLimitBytes() {
        return options.get(CoreOptions.NATIVE_IO_EXPORT_MEMORY_LIMIT).getBytes();
    }

    public int runtimeThreads() {
        return options.get(CoreOptions.NATIVE_IO_EXPORT_RUNTIME_THREADS);
    }

    public boolean metadataCacheEnabled() {
        return options.get(CoreOptions.NATIVE_IO_EXPORT_METADATA_CACHE_ENABLED);
    }

    public NativeApplicability valid() {
        if (!enabled()) {
            return NativeApplicability.rejected(
                    NativeRejectReason.EXPORT_DISABLED, "native export is disabled");
        }
        long readBufferSizeBytes = readBufferSizeBytes();
        if (readBufferSizeBytes < MIN_READ_BUFFER_BYTES
                || readBufferSizeBytes > MAX_READ_BUFFER_BYTES) {
            return invalid("native-io.export.obs.read-buffer-size must be between 1 MB and 128 MB");
        }
        int readConcurrency = readConcurrency();
        if (readConcurrency < MIN_READ_CONCURRENCY || readConcurrency > MAX_READ_CONCURRENCY) {
            return invalid("native-io.export.obs.read-concurrency must be between 1 and 64");
        }
        int writerBatchSize = writerBatchSize();
        if (writerBatchSize < MIN_WRITER_BATCH_SIZE || writerBatchSize > MAX_WRITER_BATCH_SIZE) {
            return invalid("native-io.export.writer.batch-size must be between 1 and 65536");
        }
        int writerRowGroupSize = writerRowGroupSize();
        if (writerRowGroupSize < MIN_WRITER_ROW_GROUP_SIZE
                || writerRowGroupSize > MAX_WRITER_ROW_GROUP_SIZE) {
            return invalid(
                    "native-io.export.writer.row-group-size must be between 1024 and 10000000");
        }
        long multipartPartSizeBytes = multipartPartSizeBytes();
        if (multipartPartSizeBytes < MIN_MULTIPART_PART_SIZE_BYTES
                || multipartPartSizeBytes > MAX_MULTIPART_PART_SIZE_BYTES) {
            return invalid(
                    "native-io.export.writer.multipart-part-size must be between 5 MB and 512 MB");
        }
        long memoryLimitBytes = memoryLimitBytes();
        if (memoryLimitBytes < MIN_MEMORY_LIMIT_BYTES
                || memoryLimitBytes > MAX_MEMORY_LIMIT_BYTES) {
            return invalid("native-io.export.memory-limit must be between 64 MB and 16 GB");
        }
        int runtimeThreads = runtimeThreads();
        if (runtimeThreads < MIN_RUNTIME_THREADS || runtimeThreads > MAX_RUNTIME_THREADS) {
            return invalid("native-io.export.runtime-threads must be between 1 and 64");
        }
        return NativeApplicability.yes();
    }

    private static NativeApplicability invalid(String detail) {
        return NativeApplicability.rejected(NativeRejectReason.EXPORT_INVALID_CONFIG, detail);
    }
}
