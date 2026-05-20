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

/** Native export metrics returned by Rust. */
public final class NativeExportMetrics implements Serializable {

    private static final long serialVersionUID = 1L;

    private final long rowsRead;
    private final long rowsOutput;
    private final long predicateFilteredRows;
    private final long dvFilteredRows;
    private final long parquetRowGroupsRead;
    private final long parquetRowGroupsPruned;
    private final long peakBufferedBytes;
    private final long writerRolls;
    private final long obsReadRequests;
    private final long obsReadRetries;
    private final long obsReadBytes;
    private final long obsWriteRequests;
    private final long obsWriteBytes;
    private final long readMs;
    private final long decodeMs;
    private final long filterMs;
    private final long encodeMs;
    private final long obsWriteMs;
    private final long multipartFinishMs;

    @JsonCreator
    public NativeExportMetrics(
            @JsonProperty("rows_read") long rowsRead,
            @JsonProperty("rows_output") long rowsOutput,
            @JsonProperty("predicate_filtered_rows") long predicateFilteredRows,
            @JsonProperty("dv_filtered_rows") long dvFilteredRows,
            @JsonProperty("parquet_row_groups_read") long parquetRowGroupsRead,
            @JsonProperty("parquet_row_groups_pruned") long parquetRowGroupsPruned,
            @JsonProperty("peak_buffered_bytes") long peakBufferedBytes,
            @JsonProperty("writer_rolls") long writerRolls,
            @JsonProperty("obs_read_requests") long obsReadRequests,
            @JsonProperty("obs_read_retries") long obsReadRetries,
            @JsonProperty("obs_read_bytes") long obsReadBytes,
            @JsonProperty("obs_write_requests") long obsWriteRequests,
            @JsonProperty("obs_write_bytes") long obsWriteBytes,
            @JsonProperty("read_ms") long readMs,
            @JsonProperty("decode_ms") long decodeMs,
            @JsonProperty("filter_ms") long filterMs,
            @JsonProperty("encode_ms") long encodeMs,
            @JsonProperty("obs_write_ms") long obsWriteMs,
            @JsonProperty("multipart_finish_ms") long multipartFinishMs) {
        this.rowsRead = rowsRead;
        this.rowsOutput = rowsOutput;
        this.predicateFilteredRows = predicateFilteredRows;
        this.dvFilteredRows = dvFilteredRows;
        this.parquetRowGroupsRead = parquetRowGroupsRead;
        this.parquetRowGroupsPruned = parquetRowGroupsPruned;
        this.peakBufferedBytes = peakBufferedBytes;
        this.writerRolls = writerRolls;
        this.obsReadRequests = obsReadRequests;
        this.obsReadRetries = obsReadRetries;
        this.obsReadBytes = obsReadBytes;
        this.obsWriteRequests = obsWriteRequests;
        this.obsWriteBytes = obsWriteBytes;
        this.readMs = readMs;
        this.decodeMs = decodeMs;
        this.filterMs = filterMs;
        this.encodeMs = encodeMs;
        this.obsWriteMs = obsWriteMs;
        this.multipartFinishMs = multipartFinishMs;
    }

    public static NativeExportMetrics empty() {
        return new NativeExportMetrics(0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0);
    }

    public NativeExportMetrics add(NativeExportMetrics other) {
        return new NativeExportMetrics(
                rowsRead + other.rowsRead,
                rowsOutput + other.rowsOutput,
                predicateFilteredRows + other.predicateFilteredRows,
                dvFilteredRows + other.dvFilteredRows,
                parquetRowGroupsRead + other.parquetRowGroupsRead,
                parquetRowGroupsPruned + other.parquetRowGroupsPruned,
                Math.max(peakBufferedBytes, other.peakBufferedBytes),
                writerRolls + other.writerRolls,
                obsReadRequests + other.obsReadRequests,
                obsReadRetries + other.obsReadRetries,
                obsReadBytes + other.obsReadBytes,
                obsWriteRequests + other.obsWriteRequests,
                obsWriteBytes + other.obsWriteBytes,
                readMs + other.readMs,
                decodeMs + other.decodeMs,
                filterMs + other.filterMs,
                encodeMs + other.encodeMs,
                obsWriteMs + other.obsWriteMs,
                multipartFinishMs + other.multipartFinishMs);
    }

    @JsonProperty("rows_read")
    public long rowsRead() {
        return rowsRead;
    }

    @JsonProperty("rows_output")
    public long rowsOutput() {
        return rowsOutput;
    }

    @JsonProperty("predicate_filtered_rows")
    public long predicateFilteredRows() {
        return predicateFilteredRows;
    }

    @JsonProperty("dv_filtered_rows")
    public long dvFilteredRows() {
        return dvFilteredRows;
    }

    @JsonProperty("parquet_row_groups_read")
    public long parquetRowGroupsRead() {
        return parquetRowGroupsRead;
    }

    @JsonProperty("parquet_row_groups_pruned")
    public long parquetRowGroupsPruned() {
        return parquetRowGroupsPruned;
    }

    @JsonProperty("peak_buffered_bytes")
    public long peakBufferedBytes() {
        return peakBufferedBytes;
    }

    @JsonProperty("writer_rolls")
    public long writerRolls() {
        return writerRolls;
    }

    @JsonProperty("obs_read_requests")
    public long obsReadRequests() {
        return obsReadRequests;
    }

    @JsonProperty("obs_read_retries")
    public long obsReadRetries() {
        return obsReadRetries;
    }

    @JsonProperty("obs_read_bytes")
    public long obsReadBytes() {
        return obsReadBytes;
    }

    @JsonProperty("obs_write_requests")
    public long obsWriteRequests() {
        return obsWriteRequests;
    }

    @JsonProperty("obs_write_bytes")
    public long obsWriteBytes() {
        return obsWriteBytes;
    }

    @JsonProperty("read_ms")
    public long readMs() {
        return readMs;
    }

    @JsonProperty("decode_ms")
    public long decodeMs() {
        return decodeMs;
    }

    @JsonProperty("filter_ms")
    public long filterMs() {
        return filterMs;
    }

    @JsonProperty("encode_ms")
    public long encodeMs() {
        return encodeMs;
    }

    @JsonProperty("obs_write_ms")
    public long obsWriteMs() {
        return obsWriteMs;
    }

    @JsonProperty("multipart_finish_ms")
    public long multipartFinishMs() {
        return multipartFinishMs;
    }
}
