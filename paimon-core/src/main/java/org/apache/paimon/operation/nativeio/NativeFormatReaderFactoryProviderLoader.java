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

import org.apache.paimon.format.FormatReaderFactory;

import javax.annotation.Nullable;

import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import java.util.ServiceConfigurationError;
import java.util.ServiceLoader;
import java.util.WeakHashMap;

/** ClassLoader-safe loader for optional native format reader providers. */
public final class NativeFormatReaderFactoryProviderLoader {

    private static final Map<ClassLoader, Optional<NativeFormatReaderFactoryProvider>> CACHE =
            Collections.synchronizedMap(new WeakHashMap<>());

    private NativeFormatReaderFactoryProviderLoader() {}

    public static Optional<FormatReaderFactory> tryCreate(
            @Nullable NativeFormatReaderContext context) {
        if (context == null) {
            return Optional.empty();
        }
        Optional<NativeFormatReaderFactoryProvider> provider = loadProvider();
        if (!provider.isPresent()) {
            return Optional.empty();
        }
        try {
            Optional<FormatReaderFactory> readerFactory = provider.get().create(context);
            return readerFactory == null ? Optional.empty() : readerFactory;
        } catch (LinkageError | RuntimeException e) {
            return Optional.empty();
        }
    }

    private static Optional<NativeFormatReaderFactoryProvider> loadProvider() {
        ClassLoader contextClassLoader = Thread.currentThread().getContextClassLoader();
        ClassLoader ownClassLoader = NativeFormatReaderFactoryProviderLoader.class.getClassLoader();

        if (contextClassLoader != null) {
            Optional<NativeFormatReaderFactoryProvider> provider = loadProvider(contextClassLoader);
            if (provider.isPresent() || contextClassLoader == ownClassLoader) {
                return provider;
            }
        }
        return loadProvider(ownClassLoader);
    }

    private static Optional<NativeFormatReaderFactoryProvider> loadProvider(
            ClassLoader classLoader) {
        Optional<NativeFormatReaderFactoryProvider> cached = CACHE.get(classLoader);
        if (cached != null) {
            return cached;
        }
        Optional<NativeFormatReaderFactoryProvider> loaded = discoverProvider(classLoader);
        CACHE.put(classLoader, loaded);
        return loaded;
    }

    private static Optional<NativeFormatReaderFactoryProvider> discoverProvider(
            ClassLoader classLoader) {
        try {
            ServiceLoader<NativeFormatReaderFactoryProvider> loader =
                    ServiceLoader.load(NativeFormatReaderFactoryProvider.class, classLoader);
            for (NativeFormatReaderFactoryProvider provider : loader) {
                return Optional.of(provider);
            }
            return Optional.empty();
        } catch (ServiceConfigurationError | LinkageError | RuntimeException e) {
            return Optional.empty();
        }
    }
}
