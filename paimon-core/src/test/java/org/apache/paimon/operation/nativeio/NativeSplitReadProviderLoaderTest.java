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

import org.junit.jupiter.api.Test;

import java.net.URL;
import java.net.URLClassLoader;

import static org.assertj.core.api.Assertions.assertThat;

class NativeSplitReadProviderLoaderTest {

    @Test
    void missingProviderReturnsEmpty() {
        assertThat(NativeSplitReadProviderLoader.tryCreate(null, read -> {})).isEmpty();
    }

    @Test
    void fallsBackToOwnClassLoaderWhenContextClassLoaderHasNoProvider() throws Exception {
        ClassLoader original = Thread.currentThread().getContextClassLoader();
        try (URLClassLoader emptyClassLoader = new URLClassLoader(new URL[0], null)) {
            Thread.currentThread().setContextClassLoader(emptyClassLoader);

            assertThat(NativeSplitReadProviderLoader.tryCreate(testContext(), read -> {}))
                    .isPresent();
        } finally {
            Thread.currentThread().setContextClassLoader(original);
        }
    }

    private static NativeSplitReadContext testContext() {
        return new NativeSplitReadContext(null, null, null, null, null, null, null, null, "spark");
    }
}
