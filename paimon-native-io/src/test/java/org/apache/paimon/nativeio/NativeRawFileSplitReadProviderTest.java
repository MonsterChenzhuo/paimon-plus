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

import org.apache.paimon.CoreOptions;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.manifest.FileSource;
import org.apache.paimon.nativeio.jnr.PaimonJnrLoader;
import org.apache.paimon.operation.nativeio.NativeApplicability;
import org.apache.paimon.operation.nativeio.NativeApplicabilityReporter;
import org.apache.paimon.operation.nativeio.NativeIOOptions;
import org.apache.paimon.operation.nativeio.NativeRejectReason;
import org.apache.paimon.operation.nativeio.NativeSplitReadContext;
import org.apache.paimon.options.Options;
import org.apache.paimon.table.source.DataSplit;
import org.apache.paimon.table.source.splitread.SplitReadProvider;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.net.URL;
import java.net.URLClassLoader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.apache.paimon.stats.SimpleStats.EMPTY_STATS;
import static org.assertj.core.api.Assertions.assertThat;

class NativeRawFileSplitReadProviderTest {

    @TempDir private Path tempDir;

    @Test
    void reportsUnsupportedReadType() {
        List<NativeApplicability> reports = new ArrayList<>();
        Options options = new Options();
        options.setString(CoreOptions.NATIVE_IO_ENABLED.key(), "true");
        options.setString(CoreOptions.NATIVE_IO_INTERNAL_ENGINE.key(), "spark");
        options.setString(CoreOptions.DELETION_VECTORS_ENABLED.key(), "true");
        RowType rowType = RowType.of(DataTypes.MAP(DataTypes.STRING(), DataTypes.INT()));
        NativeSplitReadContext context =
                new NativeSplitReadContext(
                        null,
                        null,
                        null,
                        rowType,
                        null,
                        null,
                        new CoreOptions(options),
                        NativeIOOptions.from(options),
                        "spark",
                        reports::add);

        NativeRawFileSplitReadProvider provider =
                new NativeRawFileSplitReadProvider(context, read -> {});

        assertThat(provider.match(new DataSplit(), new SplitReadProvider.Context(false))).isFalse();
        assertThat(reports).hasSize(1);
        assertThat(reports.get(0).reason()).isEqualTo(NativeRejectReason.UNSUPPORTED_TYPE);
    }

    @Test
    void reportsDisabledBeforeUnsupportedReadType() {
        List<NativeApplicability> reports = new ArrayList<>();
        Options options = new Options();
        options.setString(CoreOptions.DELETION_VECTORS_ENABLED.key(), "true");
        RowType rowType = RowType.of(DataTypes.MAP(DataTypes.STRING(), DataTypes.INT()));
        NativeSplitReadContext context =
                new NativeSplitReadContext(
                        null,
                        null,
                        null,
                        rowType,
                        null,
                        null,
                        new CoreOptions(options),
                        NativeIOOptions.from(options),
                        "spark",
                        reports::add);

        NativeRawFileSplitReadProvider provider =
                new NativeRawFileSplitReadProvider(context, read -> {});

        assertThat(provider.match(new DataSplit(), new SplitReadProvider.Context(false))).isFalse();
        assertThat(reports).hasSize(1);
        assertThat(reports.get(0).reason()).isEqualTo(NativeRejectReason.DISABLED);
    }

    @Test
    void allowsNonDeletionVectorTablesForRawConvertibleSplits() {
        Options options = new Options();
        options.setString(CoreOptions.NATIVE_IO_ENABLED.key(), "true");
        options.setString(CoreOptions.NATIVE_IO_INTERNAL_ENGINE.key(), "spark");
        RowType rowType = RowType.of(DataTypes.INT());
        NativeSplitReadContext context =
                new NativeSplitReadContext(
                        null,
                        null,
                        null,
                        rowType,
                        null,
                        null,
                        new CoreOptions(options),
                        NativeIOOptions.from(options),
                        "spark",
                        NativeApplicabilityReporter.NO_OP);

        NativeApplicability applicability =
                new NativeRawFileSplitRead(context).currentReadConfigApplicability();

        assertThat(applicability.applicable()).isTrue();
    }

    @Test
    void forceKeepDeleteReadConfigRejectsNative() {
        NativeRawFileSplitRead read = new NativeRawFileSplitRead(enabledDvContext());
        read.forceKeepDelete();

        NativeApplicability applicability = read.currentReadConfigApplicability();

        assertThat(applicability.applicable()).isFalse();
        assertThat(applicability.reason()).isEqualTo(NativeRejectReason.FORCE_KEEP_DELETE);
    }

    @Test
    void rejectsNonObsFilesAtProviderLevel() {
        NativeApplicability applicability =
                new NativeRawFileSplitReadProvider(null, read -> {})
                        .supportsFiles(dataSplit("file:/tmp/table/bucket-0"));

        assertThat(applicability.applicable()).isFalse();
        assertThat(applicability.reason()).isEqualTo(NativeRejectReason.NON_OBS_PATH);

        NativeApplicability obsApplicability =
                new NativeRawFileSplitReadProvider(null, read -> {})
                        .supportsFiles(dataSplit("obs://bucket/table/bucket-0"));
        assertThat(obsApplicability.applicable()).isTrue();
    }

    @Test
    void rejectsObsPathsWithQueryOrFragmentAtProviderLevel() {
        NativeRawFileSplitReadProvider provider =
                new NativeRawFileSplitReadProvider(null, read -> {});

        NativeApplicability queryApplicability =
                provider.supportsFiles(
                        dataSplitWithExternalPath(
                                "obs://bucket/table/bucket-0/data.parquet?version=1"));
        assertThat(queryApplicability.applicable()).isFalse();
        assertThat(queryApplicability.reason()).isEqualTo(NativeRejectReason.NON_OBS_PATH);

        NativeApplicability fragmentApplicability =
                provider.supportsFiles(
                        dataSplitWithExternalPath(
                                "obs://bucket/table/bucket-0/data.parquet#fragment"));
        assertThat(fragmentApplicability.applicable()).isFalse();
        assertThat(fragmentApplicability.reason()).isEqualTo(NativeRejectReason.NON_OBS_PATH);

        NativeApplicability encodedApplicability =
                provider.supportsFiles(
                        dataSplitWithExternalPath(
                                "obs://bucket/table/bucket-0/data%3Fv%3D1.parquet"));
        assertThat(encodedApplicability.applicable()).isTrue();
    }

    @Test
    void acceptsSplitWithMixedNativeAndJavaFallbackFilesAtProviderLevel() {
        NativeApplicability applicability =
                new NativeRawFileSplitReadProvider(null, read -> {})
                        .supportsFiles(
                                dataSplit(
                                        "obs://bucket/table/bucket-0",
                                        dataFile("java-fallback.parquet", "file:/tmp/table/a"),
                                        dataFile(
                                                "native-candidate.parquet",
                                                "obs://bucket/table/bucket-0/b")));

        assertThat(applicability.applicable()).isTrue();
    }

    @Test
    void matchOnlyProbesNativeResourceWithoutLoadingLibrary() throws Exception {
        PaimonJnrLoader probe = PaimonJnrLoader.create(ClassLoader.getSystemClassLoader());
        Path resource = tempDir.resolve(probe.resourcePath());
        Files.createDirectories(resource.getParent());
        Files.write(resource, new byte[] {0, 1, 2, 3});

        ClassLoader original = Thread.currentThread().getContextClassLoader();
        try (URLClassLoader classLoader =
                new URLClassLoader(new URL[] {tempDir.toUri().toURL()}, null)) {
            Thread.currentThread().setContextClassLoader(classLoader);

            NativeRawFileSplitReadProvider provider =
                    new NativeRawFileSplitReadProvider(enabledDvContext(), read -> {});

            assertThat(
                            provider.match(
                                    dataSplit("obs://bucket/table/bucket-0"),
                                    new SplitReadProvider.Context(false)))
                    .isTrue();
            assertThat(PaimonJnrLoader.current().loadFailure()).isEmpty();
        } finally {
            Thread.currentThread().setContextClassLoader(original);
        }
    }

    private static DataSplit dataSplit(String bucketPath) {
        return dataSplit(bucketPath, (String) null);
    }

    private static DataSplit dataSplitWithExternalPath(String externalPath) {
        return dataSplit("obs://bucket/table/bucket-0", externalPath);
    }

    private static DataSplit dataSplit(String bucketPath, String externalPath) {
        return dataSplit(bucketPath, dataFile("data.parquet", externalPath));
    }

    private static DataSplit dataSplit(String bucketPath, DataFileMeta... files) {
        List<DataFileMeta> dataFiles = new ArrayList<>();
        Collections.addAll(dataFiles, files);
        return DataSplit.builder()
                .withPartition(BinaryRow.EMPTY_ROW)
                .withBucket(0)
                .withBucketPath(bucketPath)
                .withDataFiles(dataFiles)
                .rawConvertible(true)
                .build();
    }

    private static DataFileMeta dataFile(String fileName, String externalPath) {
        DataFileMeta file =
                DataFileMeta.create(
                        fileName,
                        1,
                        1,
                        DataFileMeta.EMPTY_MIN_KEY,
                        DataFileMeta.EMPTY_MAX_KEY,
                        EMPTY_STATS,
                        EMPTY_STATS,
                        0,
                        1,
                        0,
                        0,
                        Collections.emptyList(),
                        0L,
                        null,
                        FileSource.APPEND,
                        null,
                        externalPath,
                        null,
                        null);
        return file;
    }

    private static NativeSplitReadContext enabledDvContext() {
        Options options = new Options();
        options.setString(CoreOptions.NATIVE_IO_ENABLED.key(), "true");
        options.setString(CoreOptions.NATIVE_IO_INTERNAL_ENGINE.key(), "spark");
        options.setString(CoreOptions.DELETION_VECTORS_ENABLED.key(), "true");
        return new NativeSplitReadContext(
                null,
                null,
                null,
                RowType.of(DataTypes.INT()),
                null,
                null,
                new CoreOptions(options),
                NativeIOOptions.from(options),
                "spark",
                NativeApplicabilityReporter.NO_OP);
    }
}
