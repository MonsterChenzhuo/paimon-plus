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

import org.apache.paimon.options.Options;

import javax.annotation.Nullable;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/** Driver-side native export planning context with core-visible values only. */
public final class NativeExportContext implements Serializable {

    private static final long serialVersionUID = 1L;

    private final Options options;
    private final String outputPath;
    private final String compression;
    @Nullable private final Long targetFileSizeBytes;
    private final List<String> projectedFieldNames;
    private final int plannedSplitCount;

    public NativeExportContext(
            Options options,
            String outputPath,
            String compression,
            @Nullable Long targetFileSizeBytes,
            List<String> projectedFieldNames,
            int plannedSplitCount) {
        this.options = options == null ? new Options() : new Options(options.toMap());
        this.outputPath = requireNonEmpty(outputPath, "outputPath");
        this.compression = requireNonEmpty(compression, "compression");
        this.targetFileSizeBytes = targetFileSizeBytes;
        this.projectedFieldNames =
                Collections.unmodifiableList(new ArrayList<>(projectedFieldNames));
        this.plannedSplitCount = plannedSplitCount;
    }

    public Options options() {
        return new Options(options.toMap());
    }

    public String outputPath() {
        return outputPath;
    }

    public String compression() {
        return compression;
    }

    @Nullable
    public Long targetFileSizeBytes() {
        return targetFileSizeBytes;
    }

    public List<String> projectedFieldNames() {
        return projectedFieldNames;
    }

    public int plannedSplitCount() {
        return plannedSplitCount;
    }

    private static String requireNonEmpty(String value, String fieldName) {
        if (value == null || value.trim().isEmpty()) {
            throw new IllegalArgumentException(fieldName + " must not be empty");
        }
        return value;
    }
}
