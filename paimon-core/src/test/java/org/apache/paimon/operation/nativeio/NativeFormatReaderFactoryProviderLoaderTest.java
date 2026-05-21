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
import org.apache.paimon.io.DataFileTestUtils;
import org.apache.paimon.options.Options;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;

import org.junit.jupiter.api.Test;

import java.net.URL;
import java.net.URLClassLoader;

import static org.assertj.core.api.Assertions.assertThat;

class NativeFormatReaderFactoryProviderLoaderTest {

    @Test
    void missingContextReturnsEmpty() {
        assertThat(NativeFormatReaderFactoryProviderLoader.tryCreate(null)).isEmpty();
    }

    @Test
    void fallsBackToOwnClassLoaderWhenContextClassLoaderHasNoProvider() throws Exception {
        ClassLoader original = Thread.currentThread().getContextClassLoader();
        try (URLClassLoader emptyClassLoader = new URLClassLoader(new URL[0], null)) {
            Thread.currentThread().setContextClassLoader(emptyClassLoader);

            assertThat(NativeFormatReaderFactoryProviderLoader.tryCreate(testContext()))
                    .isPresent();
        } finally {
            Thread.currentThread().setContextClassLoader(original);
        }
    }

    private static NativeFormatReaderContext testContext() {
        return new NativeFormatReaderContext(
                DataFileTestUtils.newFile("data.parquet", 0, 1, 1, 1, 0L),
                RowType.of(DataTypes.INT()),
                false,
                false,
                NativeIOOptions.from(new Options()),
                new Path("obs://bucket/data.parquet"),
                NativeApplicabilityReporter.NO_OP);
    }
}
