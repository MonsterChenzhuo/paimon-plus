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

/** Data file entry for a native export task. */
public final class NativeExportFile implements Serializable {

    private static final long serialVersionUID = 1L;

    private final String path;
    private final long rowCount;
    private final long fileSize;
    private final long schemaId;
    private final List<Long> deletedPositions;

    @JsonCreator
    public NativeExportFile(
            @JsonProperty("path") String path,
            @JsonProperty("row_count") long rowCount,
            @JsonProperty("file_size") long fileSize,
            @JsonProperty("schema_id") long schemaId,
            @JsonProperty("positions") List<Long> deletedPositions) {
        this.path = path;
        this.rowCount = rowCount;
        this.fileSize = fileSize;
        this.schemaId = schemaId;
        this.deletedPositions =
                Collections.unmodifiableList(
                        new ArrayList<>(
                                deletedPositions == null
                                        ? Collections.emptyList()
                                        : deletedPositions));
    }

    @JsonProperty("path")
    public String path() {
        return path;
    }

    @JsonProperty("row_count")
    public long rowCount() {
        return rowCount;
    }

    @JsonProperty("file_size")
    public long fileSize() {
        return fileSize;
    }

    @JsonProperty("schema_id")
    public long schemaId() {
        return schemaId;
    }

    @JsonProperty("positions")
    public List<Long> deletedPositions() {
        return deletedPositions;
    }
}
