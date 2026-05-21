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
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.Collections;
import java.util.Enumeration;

import static org.assertj.core.api.Assertions.assertThat;

class NativeSplitReadProviderLoaderTest {

    @TempDir private java.nio.file.Path tempDir;

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

    @Test
    void triesNextFactoryWhenFirstFactoryReturnsNull() {
        assertThat(NativeSplitReadProviderLoader.tryCreate(testContext(), read -> {})).isPresent();
    }

    @Test
    void fallsBackToOwnClassLoaderWhenContextFactoriesReturnNull() throws Exception {
        java.nio.file.Path serviceFile =
                tempDir.resolve(
                        "META-INF/services/" + NativeSplitReadProviderFactory.class.getName());
        Files.createDirectories(serviceFile.getParent());
        Files.write(
                serviceFile,
                Collections.singletonList(EmptyNativeSplitReadProviderFactory.class.getName()),
                StandardCharsets.UTF_8);

        ClassLoader original = Thread.currentThread().getContextClassLoader();
        ClassLoader classLoader =
                new ServiceOnlyClassLoader(
                        NativeSplitReadProviderLoader.class.getClassLoader(),
                        serviceFile.toUri().toURL());
        try {
            Thread.currentThread().setContextClassLoader(classLoader);

            assertThat(NativeSplitReadProviderLoader.tryCreate(testContext(), read -> {}))
                    .isPresent();
        } finally {
            Thread.currentThread().setContextClassLoader(original);
        }
    }

    @Test
    void testFactoryDoesNotMatchRegularContexts() {
        assertThat(NativeSplitReadProviderLoader.tryCreate(regularContext(), read -> {})).isEmpty();
    }

    private static NativeSplitReadContext testContext() {
        return new NativeSplitReadContext(
                null, null, null, null, null, null, null, null, "__native_split_provider_test__");
    }

    private static NativeSplitReadContext regularContext() {
        return new NativeSplitReadContext(null, null, null, null, null, null, null, null, "spark");
    }

    private static class ServiceOnlyClassLoader extends ClassLoader {

        private static final String SERVICE_NAME =
                "META-INF/services/" + NativeSplitReadProviderFactory.class.getName();

        private final URL serviceUrl;

        private ServiceOnlyClassLoader(ClassLoader parent, URL serviceUrl) {
            super(parent);
            this.serviceUrl = serviceUrl;
        }

        @Override
        public Enumeration<URL> getResources(String name) throws IOException {
            if (SERVICE_NAME.equals(name)) {
                return Collections.enumeration(Collections.singleton(serviceUrl));
            }
            return super.getResources(name);
        }
    }
}
