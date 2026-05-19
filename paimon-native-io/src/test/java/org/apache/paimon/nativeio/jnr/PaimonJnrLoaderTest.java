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

package org.apache.paimon.nativeio.jnr;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.net.URL;
import java.net.URLClassLoader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import static org.assertj.core.api.Assertions.assertThat;

class PaimonJnrLoaderTest {

    @TempDir private Path tempDir;

    @Test
    void availableLoadsNativeLibraryBeforeReportingUsable() throws Exception {
        PaimonJnrLoader probe = PaimonJnrLoader.create(ClassLoader.getSystemClassLoader());
        Path resource = tempDir.resolve(probe.resourcePath());
        Files.createDirectories(resource.getParent());
        Files.write(resource, new byte[] {0, 1, 2, 3});

        PaimonJnrLoader loader =
                PaimonJnrLoader.create(
                        new URLClassLoader(new URL[] {tempDir.toUri().toURL()}, null));

        assertThat(loader.nativeResource()).isPresent();
        assertThat(loader.hasNativeResource()).isTrue();
        assertThat(loader.loadFailure()).isEmpty();
        assertThat(loader.available()).isFalse();
        assertThat(loader.load()).isEmpty();
    }

    @Test
    void fallsBackToRootMappedLibraryResourceForLocalDevelopment() throws Exception {
        Path resource = tempDir.resolve(System.mapLibraryName("paimon_native_io"));
        Files.write(resource, new byte[] {0, 1, 2, 3});

        PaimonJnrLoader loader =
                PaimonJnrLoader.create(
                        new URLClassLoader(new URL[] {tempDir.toUri().toURL()}, null));

        assertThat(loader.nativeResource()).isPresent();
        assertThat(loader.hasNativeResource()).isTrue();
    }

    @Test
    void currentFallsBackToOwnClassLoaderWhenContextClassLoaderHasNoResource() throws Exception {
        PaimonJnrLoader probe = PaimonJnrLoader.create(PaimonJnrLoader.class.getClassLoader());
        Path classpathRoot =
                Paths.get(
                        PaimonJnrLoader.class
                                .getProtectionDomain()
                                .getCodeSource()
                                .getLocation()
                                .toURI());
        Path resource = classpathRoot.resolve(probe.resourcePath());
        Files.createDirectories(resource.getParent());
        Files.write(resource, new byte[] {0, 1, 2, 3});

        ClassLoader original = Thread.currentThread().getContextClassLoader();
        try (URLClassLoader emptyClassLoader = new URLClassLoader(new URL[0], null)) {
            Thread.currentThread().setContextClassLoader(emptyClassLoader);

            assertThat(PaimonJnrLoader.current().nativeResource()).isPresent();
            assertThat(PaimonJnrLoader.current().hasNativeResource()).isTrue();
        } finally {
            Thread.currentThread().setContextClassLoader(original);
            Files.deleteIfExists(resource);
        }
    }
}
