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

import jnr.ffi.LibraryLoader;
import jnr.ffi.LibraryOption;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.InputStream;
import java.net.URL;
import java.net.URLConnection;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.WeakHashMap;

/** ClassLoader-local native library discovery. Loading is lazy and optional. */
public final class PaimonJnrLoader {

    private static final String RESOURCE_PREFIX = "paimon-native-io";
    private static final Map<ClassLoader, PaimonJnrLoader> CACHE = new WeakHashMap<>();

    private final ClassLoader classLoader;
    private volatile boolean loaded;
    private volatile LibPaimonNativeIO library;
    private volatile Throwable loadFailure;

    private PaimonJnrLoader(ClassLoader classLoader) {
        this.classLoader = classLoader;
    }

    public static PaimonJnrLoader create(ClassLoader classLoader) {
        return new PaimonJnrLoader(classLoader);
    }

    public static PaimonJnrLoader current() {
        ClassLoader loader = currentClassLoader();
        synchronized (CACHE) {
            return CACHE.computeIfAbsent(loader, PaimonJnrLoader::create);
        }
    }

    private static ClassLoader currentClassLoader() {
        ClassLoader contextClassLoader = Thread.currentThread().getContextClassLoader();
        ClassLoader ownClassLoader = PaimonJnrLoader.class.getClassLoader();
        if (contextClassLoader == null || contextClassLoader == ownClassLoader) {
            return ownClassLoader;
        }
        PaimonJnrLoader contextLoader = create(contextClassLoader);
        return contextLoader.hasNativeResource() ? contextClassLoader : ownClassLoader;
    }

    public boolean available() {
        return load().isPresent();
    }

    public boolean hasNativeResource() {
        return nativeResource().isPresent();
    }

    public Optional<Throwable> loadFailure() {
        return Optional.ofNullable(loadFailure);
    }

    public synchronized Optional<LibPaimonNativeIO> load() {
        if (loaded) {
            return Optional.ofNullable(library);
        }
        loaded = true;
        try {
            URL resource =
                    nativeResource().orElseThrow(() -> new FileNotFoundException(resourcePath()));
            File extracted = extract(resource);
            Map<LibraryOption, Object> libraryOptions = new HashMap<>();
            libraryOptions.put(LibraryOption.LoadNow, true);
            libraryOptions.put(LibraryOption.IgnoreError, true);
            library =
                    LibraryLoader.loadLibrary(
                            LibPaimonNativeIO.class, libraryOptions, extracted.getAbsolutePath());
            return Optional.ofNullable(library);
        } catch (Throwable e) {
            loadFailure = e;
            return Optional.empty();
        }
    }

    public Optional<URL> nativeResource() {
        URL resource = classLoader.getResource(resourcePath());
        if (resource == null) {
            resource = classLoader.getResource(System.mapLibraryName("paimon_native_io"));
        }
        return Optional.ofNullable(resource);
    }

    public String resourcePath() {
        return RESOURCE_PREFIX
                + "/"
                + normalizeOs(osName())
                + "/"
                + normalizeArch(osArch())
                + "/"
                + libraryName();
    }

    private File extract(URL resource) throws Exception {
        String tmpdir =
                System.getProperty(
                        "paimon.native-io.tmpdir",
                        System.getenv()
                                .getOrDefault(
                                        "PAIMON_NATIVE_IO_TMPDIR",
                                        System.getProperty("java.io.tmpdir")));
        File target =
                File.createTempFile("paimon-native-io-", "-" + libraryName(), new File(tmpdir));
        target.deleteOnExit();
        URLConnection connection = resource.openConnection();
        connection.setUseCaches(false);
        try (InputStream in = connection.getInputStream()) {
            Files.copy(in, target.toPath(), StandardCopyOption.REPLACE_EXISTING);
        }
        return target;
    }

    static String normalizeArch(String arch) {
        String normalized = arch.toLowerCase(Locale.ROOT);
        if ("amd64".equals(normalized) || "x86_64".equals(normalized)) {
            return "x86_64";
        }
        if ("arm64".equals(normalized) || "aarch64".equals(normalized)) {
            return "aarch64";
        }
        return normalized;
    }

    static String normalizeOs(String os) {
        String normalized = os.toLowerCase(Locale.ROOT);
        if (normalized.contains("mac") || normalized.contains("darwin")) {
            return "darwin";
        }
        if (normalized.contains("linux")) {
            return "linux";
        }
        return normalized.replaceAll("[^a-z0-9]+", "_");
    }

    private static String libraryName() {
        return "darwin".equals(normalizeOs(osName()))
                ? "libpaimon_native_io.dylib"
                : "libpaimon_native_io.so";
    }

    private static String osName() {
        return System.getProperty("os.name", "");
    }

    private static String osArch() {
        return System.getProperty("os.arch", "");
    }
}
