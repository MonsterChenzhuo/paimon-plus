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

package org.apache.paimon.operation.nativeio.diagnostics;

import java.lang.reflect.Method;

/** Best-effort bridge from core/native code to Spark-specific Native IO diagnostics. */
public final class NativeIODiagnosticsEmitter {

    private static final String SPARK_DIAGNOSTICS_CLASS =
            "org.apache.paimon.spark.nativeio.diagnostics.NativeIODiagnostics";

    private NativeIODiagnosticsEmitter() {}

    public static void emit(NativeIOEvent event) {
        try {
            Class<?> diagnostics =
                    Class.forName(
                            SPARK_DIAGNOSTICS_CLASS,
                            true,
                            Thread.currentThread().getContextClassLoader());
            Method record = diagnostics.getMethod("record", NativeIOEvent.class);
            record.invoke(null, event);
        } catch (Throwable ignored) {
            // Native IO must never fail because the optional Spark diagnostics bridge is absent.
        }
    }
}
