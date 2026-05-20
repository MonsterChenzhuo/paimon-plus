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
import java.util.List;

/** Native export result returned by Rust. */
public final class NativeExportResult implements Serializable {

    private static final long serialVersionUID = 1L;

    private final long rowsOutput;
    private final List<NativeExportWrittenFile> filesWritten;
    private final NativeExportMetrics metrics;

    @JsonCreator
    public NativeExportResult(
            @JsonProperty("rows_output") long rowsOutput,
            @JsonProperty("files_written") List<NativeExportWrittenFile> filesWritten,
            @JsonProperty("metrics") NativeExportMetrics metrics) {
        this.rowsOutput = rowsOutput;
        this.filesWritten =
                Collections.unmodifiableList(
                        new ArrayList<>(
                                filesWritten == null ? Collections.emptyList() : filesWritten));
        this.metrics = metrics == null ? NativeExportMetrics.empty() : metrics;
    }

    @JsonProperty("rows_output")
    public long rowsOutput() {
        return rowsOutput;
    }

    @JsonProperty("files_written")
    public List<NativeExportWrittenFile> filesWritten() {
        return filesWritten;
    }

    @JsonProperty("metrics")
    public NativeExportMetrics metrics() {
        return metrics;
    }
}
