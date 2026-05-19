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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/** Logs native IO applicability decisions once per reason to keep split planning observable. */
public final class LoggingNativeApplicabilityReporter implements NativeApplicabilityReporter {

    private static final Logger LOG =
            LoggerFactory.getLogger(LoggingNativeApplicabilityReporter.class);

    private final NativeIOOptions options;
    private final Set<String> reported =
            Collections.newSetFromMap(new ConcurrentHashMap<String, Boolean>());

    private LoggingNativeApplicabilityReporter(NativeIOOptions options) {
        this.options = options;
    }

    public static LoggingNativeApplicabilityReporter create(NativeIOOptions options) {
        return new LoggingNativeApplicabilityReporter(options);
    }

    @Override
    public void report(NativeApplicability applicability) {
        String key =
                applicability.applicable() ? "APPLICABLE" : String.valueOf(applicability.reason());
        if (!reported.add(key)) {
            return;
        }

        if (applicability.applicable()) {
            LOG.info("Paimon native IO is enabled for eligible raw Parquet splits.");
        } else if (options.enabled()) {
            LOG.info(
                    "Paimon native IO falls back to Java reader. reason={}, detail={}",
                    applicability.reason(),
                    applicability.detail());
        } else {
            LOG.debug("Paimon native IO is disabled.");
        }
    }
}
