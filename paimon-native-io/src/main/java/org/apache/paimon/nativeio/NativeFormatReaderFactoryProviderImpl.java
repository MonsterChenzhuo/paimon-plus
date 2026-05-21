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

package org.apache.paimon.nativeio;

import org.apache.paimon.format.FormatReaderFactory;
import org.apache.paimon.nativeio.jnr.PaimonJnrLoader;
import org.apache.paimon.operation.nativeio.NativeApplicability;
import org.apache.paimon.operation.nativeio.NativeFormatReaderContext;
import org.apache.paimon.operation.nativeio.NativeFormatReaderFactoryProvider;
import org.apache.paimon.operation.nativeio.NativeRejectReason;
import org.apache.paimon.operation.nativeio.SupportsNativeIO;

import java.util.Optional;

/** Native format reader provider used by key-value merge readers. */
public class NativeFormatReaderFactoryProviderImpl implements NativeFormatReaderFactoryProvider {

    @Override
    public Optional<FormatReaderFactory> create(NativeFormatReaderContext context) {
        NativeApplicability engineApplicability =
                SupportsNativeIO.checkEngine(context.nativeIOOptions());
        if (!engineApplicability.applicable()) {
            report(context, engineApplicability);
            return Optional.empty();
        }

        NativeApplicability fileApplicability =
                SupportsNativeIO.checkNativePhysicalFile(
                        context.file(),
                        context.actualReadRowType(),
                        context.rowTrackingEnabled(),
                        context.hasFilterTopNLimitPushDown(),
                        context.nativeIOOptions(),
                        context.actualDataPath());
        if (!fileApplicability.applicable()) {
            report(context, fileApplicability);
            return Optional.empty();
        }

        if (!PaimonJnrLoader.current().hasNativeResource()) {
            report(
                    context,
                    NativeApplicability.rejected(
                            NativeRejectReason.NO_PROVIDER,
                            "native IO library resource was not found"));
            return Optional.empty();
        }

        report(context, NativeApplicability.yes());
        return Optional.of(
                new NativeFormatReaderFactory(
                        context.actualReadRowType(),
                        context.file().rowCount(),
                        context.nativeIOOptions().batchSize(),
                        context.nativeIOOptions().maxBatchBytes().getBytes(),
                        context.nativeIOOptions().objectStoreOptions()));
    }

    private void report(NativeFormatReaderContext context, NativeApplicability applicability) {
        context.reporter().report(applicability);
    }
}
