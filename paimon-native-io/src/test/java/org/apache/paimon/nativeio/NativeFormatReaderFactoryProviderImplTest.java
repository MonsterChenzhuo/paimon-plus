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
import org.apache.paimon.fs.Path;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.manifest.FileSource;
import org.apache.paimon.nativeio.jnr.PaimonJnrLoader;
import org.apache.paimon.operation.nativeio.NativeApplicability;
import org.apache.paimon.operation.nativeio.NativeFormatReaderContext;
import org.apache.paimon.operation.nativeio.NativeFormatReaderFactoryProviderLoader;
import org.apache.paimon.operation.nativeio.NativeIOOptions;
import org.apache.paimon.operation.nativeio.NativeRejectReason;
import org.apache.paimon.options.Options;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.net.URL;
import java.net.URLClassLoader;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.apache.paimon.stats.SimpleStats.EMPTY_STATS;
import static org.assertj.core.api.Assertions.assertThat;

class NativeFormatReaderFactoryProviderImplTest {

    @TempDir private java.nio.file.Path tempDir;

    @Test
    void reportsDisabledBeforeCheckingFileShape() {
        List<NativeApplicability> reports = new ArrayList<>();
        Options options = new Options();
        RowType unsupportedType = RowType.of(DataTypes.MAP(DataTypes.STRING(), DataTypes.INT()));

        assertThat(
                        new NativeFormatReaderFactoryProviderImpl()
                                .create(context(options, unsupportedType, false, reports)))
                .isEmpty();

        assertThat(reports).hasSize(1);
        assertThat(reports.get(0).reason()).isEqualTo(NativeRejectReason.DISABLED);
    }

    @Test
    void rejectsFilterTopNLimitPushDownBeforeProbingNativeResource() {
        List<NativeApplicability> reports = new ArrayList<>();
        Options options = enabledSparkObsOptions();

        assertThat(
                        new NativeFormatReaderFactoryProviderImpl()
                                .create(
                                        context(
                                                options,
                                                RowType.of(DataTypes.INT()),
                                                true,
                                                reports)))
                .isEmpty();

        assertThat(reports).hasSize(1);
        assertThat(reports.get(0).reason()).isEqualTo(NativeRejectReason.DATA_FILTER_TOPN_LIMIT);
    }

    @Test
    void createsFactoryWhenResourceAndFileAreSupportedWithoutLoadingLibrary() throws Exception {
        PaimonJnrLoader probe = PaimonJnrLoader.create(ClassLoader.getSystemClassLoader());
        java.nio.file.Path resource = tempDir.resolve(probe.resourcePath());
        Files.createDirectories(resource.getParent());
        Files.write(resource, new byte[] {0, 1, 2, 3});

        ClassLoader original = Thread.currentThread().getContextClassLoader();
        try (URLClassLoader classLoader =
                new URLClassLoader(new URL[] {tempDir.toUri().toURL()}, null)) {
            Thread.currentThread().setContextClassLoader(classLoader);

            assertThat(
                            new NativeFormatReaderFactoryProviderImpl()
                                    .create(
                                            context(
                                                    enabledSparkObsOptions(),
                                                    RowType.of(DataTypes.INT()),
                                                    false,
                                                    new ArrayList<>())))
                    .hasValueSatisfying(
                            factory ->
                                    assertThat(factory)
                                            .isInstanceOf(NativeFormatReaderFactory.class));
            assertThat(PaimonJnrLoader.current().loadFailure()).isEmpty();
        } finally {
            Thread.currentThread().setContextClassLoader(original);
        }
    }

    @Test
    void discoversProviderThroughServiceLoader() throws Exception {
        PaimonJnrLoader probe = PaimonJnrLoader.create(ClassLoader.getSystemClassLoader());
        java.nio.file.Path resource = tempDir.resolve(probe.resourcePath());
        Files.createDirectories(resource.getParent());
        Files.write(resource, new byte[] {0, 1, 2, 3});

        ClassLoader original = Thread.currentThread().getContextClassLoader();
        try (URLClassLoader classLoader =
                new URLClassLoader(new URL[] {tempDir.toUri().toURL()}, null)) {
            Thread.currentThread().setContextClassLoader(classLoader);

            assertThat(
                            NativeFormatReaderFactoryProviderLoader.tryCreate(
                                    context(
                                            enabledSparkObsOptions(),
                                            RowType.of(DataTypes.INT()),
                                            false,
                                            new ArrayList<>())))
                    .hasValueSatisfying(
                            factory ->
                                    assertThat(factory)
                                            .isInstanceOf(NativeFormatReaderFactory.class));
        } finally {
            Thread.currentThread().setContextClassLoader(original);
        }
    }

    private static NativeFormatReaderContext context(
            Options options,
            RowType rowType,
            boolean hasFilterTopNLimitPushDown,
            List<NativeApplicability> reports) {
        return new NativeFormatReaderContext(
                dataFile("data.parquet"),
                rowType,
                false,
                hasFilterTopNLimitPushDown,
                NativeIOOptions.from(options),
                new Path("obs://bucket/table/data.parquet"),
                reports::add);
    }

    private static DataFileMeta dataFile(String fileName) {
        return DataFileMeta.create(
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
                null,
                null,
                null);
    }

    private static Options enabledSparkObsOptions() {
        Options options = new Options();
        options.setString(CoreOptions.NATIVE_IO_ENABLED.key(), "true");
        options.setString(CoreOptions.NATIVE_IO_INTERNAL_ENGINE.key(), "spark");
        options.setString("fs.obs.endpoint", "obs.example.com");
        options.setString("fs.obs.access.key", "ak");
        options.setString("fs.obs.secret.key", "sk");
        return options;
    }
}
