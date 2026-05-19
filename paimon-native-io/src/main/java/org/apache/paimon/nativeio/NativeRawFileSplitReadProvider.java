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

import org.apache.paimon.data.InternalRow;
import org.apache.paimon.nativeio.jnr.PaimonJnrLoader;
import org.apache.paimon.operation.SplitRead;
import org.apache.paimon.operation.nativeio.NativeApplicability;
import org.apache.paimon.operation.nativeio.NativeRejectReason;
import org.apache.paimon.operation.nativeio.NativeSplitReadContext;
import org.apache.paimon.operation.nativeio.SupportsNativeIO;
import org.apache.paimon.table.source.DataSplit;
import org.apache.paimon.table.source.Split;
import org.apache.paimon.table.source.splitread.SplitReadProvider;
import org.apache.paimon.utils.LazyField;

import java.util.function.Consumer;

/** Native raw file split provider. It falls back to Java when native resources are absent. */
public class NativeRawFileSplitReadProvider implements SplitReadProvider {

    private final NativeSplitReadContext context;
    private final LazyField<NativeRawFileSplitRead> splitRead;

    public NativeRawFileSplitReadProvider(
            NativeSplitReadContext context, Consumer<SplitRead<InternalRow>> splitReadConfig) {
        this.context = context;
        this.splitRead =
                new LazyField<>(
                        () -> {
                            NativeRawFileSplitRead read = new NativeRawFileSplitRead(context);
                            splitReadConfig.accept(read);
                            return read;
                        });
    }

    @Override
    public boolean match(Split split, Context splitContext) {
        if (!(split instanceof DataSplit)) {
            return false;
        }
        NativeApplicability engineApplicability =
                SupportsNativeIO.checkEngine(context.nativeIOOptions());
        if (!engineApplicability.applicable()) {
            report(engineApplicability);
            return false;
        }
        NativeRawFileSplitRead read = splitRead.get();
        NativeApplicability readConfigApplicability = read.currentReadConfigApplicability();
        if (!readConfigApplicability.applicable()) {
            report(readConfigApplicability);
            return false;
        }
        boolean nativeLibraryAvailable = PaimonJnrLoader.current().hasNativeResource();
        NativeApplicability applicability =
                SupportsNativeIO.checkNativeSplit(
                        (DataSplit) split,
                        context.nativeIOOptions(),
                        splitContext,
                        nativeLibraryAvailable);
        if (!applicability.applicable()) {
            report(applicability);
            return false;
        }
        NativeApplicability fileApplicability = supportsFiles((DataSplit) split);
        report(fileApplicability);
        return fileApplicability.applicable();
    }

    NativeApplicability supportsFiles(DataSplit split) {
        NativeApplicability firstRejection = null;
        for (org.apache.paimon.io.DataFileMeta file : split.dataFiles()) {
            NativeApplicability fileApplicability = supportsFile(split, file);
            if (fileApplicability.applicable()) {
                return NativeApplicability.yes();
            }
            if (firstRejection == null) {
                firstRejection = fileApplicability;
            }
        }
        return firstRejection == null ? NativeApplicability.yes() : firstRejection;
    }

    private NativeApplicability supportsFile(
            DataSplit split, org.apache.paimon.io.DataFileMeta file) {
        if (!"parquet".equalsIgnoreCase(file.fileFormat())) {
            return NativeApplicability.rejected(
                    NativeRejectReason.NON_PARQUET_FILE,
                    "native IO only supports Parquet data files");
        }
        String path = file.externalPath().orElse(split.bucketPath() + "/" + file.fileName());
        if (!path.regionMatches(true, 0, "obs://", 0, "obs://".length())) {
            return NativeApplicability.rejected(
                    NativeRejectReason.NON_OBS_PATH, "native IO only supports obs:// data paths");
        }
        if (hasQueryOrFragment(path)) {
            return NativeApplicability.rejected(
                    NativeRejectReason.NON_OBS_PATH,
                    "native IO does not support OBS paths with query or fragment");
        }
        return NativeApplicability.yes();
    }

    private static boolean hasQueryOrFragment(String path) {
        return path.indexOf('?') >= 0 || path.indexOf('#') >= 0;
    }

    private void report(NativeApplicability applicability) {
        context.reporter().report(applicability);
    }

    @Override
    public LazyField<? extends SplitRead<InternalRow>> get() {
        return splitRead;
    }
}
