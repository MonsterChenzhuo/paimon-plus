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

import javax.annotation.Nullable;

/** Result of checking whether a read can use native IO. */
public final class NativeApplicability {

    private static final NativeApplicability APPLICABLE = new NativeApplicability(true, null, "");

    private final boolean applicable;
    @Nullable private final NativeRejectReason reason;
    private final String detail;

    private NativeApplicability(
            boolean applicable, @Nullable NativeRejectReason reason, @Nullable String detail) {
        this.applicable = applicable;
        this.reason = reason;
        this.detail = detail == null ? "" : detail;
    }

    public static NativeApplicability yes() {
        return APPLICABLE;
    }

    public static NativeApplicability rejected(NativeRejectReason reason, String detail) {
        return new NativeApplicability(false, reason, detail);
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
