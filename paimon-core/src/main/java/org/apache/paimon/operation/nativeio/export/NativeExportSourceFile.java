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

package org.apache.paimon.operation.nativeio.export;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/** Core-visible source data file for native export planning. */
public final class NativeExportSourceFile implements Serializable {

    private static final long serialVersionUID = 1L;

    private final String path;
    private final String format;
    private final long rowCount;
    private final long fileSize;
    private final long schemaId;
    private final Map<String, String> partition;
    private final List<Long> deletedPositions;

    public NativeExportSourceFile(
            String path,
            String format,
            long rowCount,
            long fileSize,
            long schemaId,
            Map<String, String> partition,
            List<Long> deletedPositions) {
        this.path = requireNonEmpty(path, "path");
        this.format = requireNonEmpty(format, "format");
        this.rowCount = rowCount;
        this.fileSize = fileSize;
        this.schemaId = schemaId;
        this.partition =
                Collections.unmodifiableMap(
                        new LinkedHashMap<>(
                                partition == null ? Collections.emptyMap() : partition));
        this.deletedPositions =
                Collections.unmodifiableList(
                        new ArrayList<>(
                                deletedPositions == null
                                        ? Collections.emptyList()
                                        : deletedPositions));
    }

    public String path() {
        return path;
    }

    public String format() {
        return format;
    }

    public long rowCount() {
        return rowCount;
    }

    public long fileSize() {
        return fileSize;
    }

    public long schemaId() {
        return schemaId;
    }

    public Map<String, String> partition() {
        return partition;
    }

    public List<Long> deletedPositions() {
        return deletedPositions;
    }

    private static String requireNonEmpty(String value, String fieldName) {
        if (value == null || value.trim().isEmpty()) {
            throw new IllegalArgumentException(fieldName + " must not be empty");
        }
        return value;
    }
}
