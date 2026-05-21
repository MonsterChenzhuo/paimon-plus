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

import org.apache.paimon.fs.Path;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.types.RowType;

/** Context passed from core file readers to optional native format reader providers. */
public final class NativeFormatReaderContext {

    private final DataFileMeta file;
    private final RowType actualReadRowType;
    private final boolean rowTrackingEnabled;
    private final boolean hasFilterTopNLimitPushDown;
    private final NativeIOOptions nativeIOOptions;
    private final Path actualDataPath;
    private final NativeApplicabilityReporter reporter;

    public NativeFormatReaderContext(
            DataFileMeta file,
            RowType actualReadRowType,
            boolean rowTrackingEnabled,
            boolean hasFilterTopNLimitPushDown,
            NativeIOOptions nativeIOOptions,
            Path actualDataPath,
            NativeApplicabilityReporter reporter) {
        this.file = file;
        this.actualReadRowType = actualReadRowType;
        this.rowTrackingEnabled = rowTrackingEnabled;
        this.hasFilterTopNLimitPushDown = hasFilterTopNLimitPushDown;
        this.nativeIOOptions = nativeIOOptions;
        this.actualDataPath = actualDataPath;
        this.reporter = reporter;
    }

    public DataFileMeta file() {
        return file;
    }

    public RowType actualReadRowType() {
        return actualReadRowType;
    }

    public boolean rowTrackingEnabled() {
        return rowTrackingEnabled;
    }

    public boolean hasFilterTopNLimitPushDown() {
        return hasFilterTopNLimitPushDown;
    }

    public NativeIOOptions nativeIOOptions() {
        return nativeIOOptions;
    }

    public Path actualDataPath() {
        return actualDataPath;
    }

    public NativeApplicabilityReporter reporter() {
        return reporter;
    }
}
