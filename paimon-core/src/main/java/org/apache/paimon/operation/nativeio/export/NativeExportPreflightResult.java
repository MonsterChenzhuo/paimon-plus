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

import org.apache.paimon.operation.nativeio.NativeRejectReason;

import javax.annotation.Nullable;

import java.io.Serializable;

/** Result of checking whether export_parquet can use native export. */
public final class NativeExportPreflightResult implements Serializable {

    private static final long serialVersionUID = 1L;

    private final boolean applicable;
    @Nullable private final NativeRejectReason reason;
    private final String detail;

    private NativeExportPreflightResult(
            boolean applicable, @Nullable NativeRejectReason reason, @Nullable String detail) {
        this.applicable = applicable;
        this.reason = reason;
        this.detail = detail == null ? "" : detail;
    }

    public static NativeExportPreflightResult success() {
        return new NativeExportPreflightResult(true, null, "");
    }

    public static NativeExportPreflightResult rejected(NativeRejectReason reason, String detail) {
        return new NativeExportPreflightResult(false, reason, detail);
    }

    public boolean applicable() {
        return applicable;
    }

    @Nullable
    public NativeRejectReason reason() {
        return reason;
    }

    public String detail() {
        return detail;
    }
}
