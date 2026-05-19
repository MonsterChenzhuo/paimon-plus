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

import org.apache.paimon.data.InternalRow;
import org.apache.paimon.operation.SplitRead;
import org.apache.paimon.table.source.splitread.SplitReadProvider;

import javax.annotation.Nullable;

import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import java.util.ServiceConfigurationError;
import java.util.ServiceLoader;
import java.util.WeakHashMap;
import java.util.function.Consumer;

/** ClassLoader-safe loader for optional native split read providers. */
public final class NativeSplitReadProviderLoader {

    private static final Map<ClassLoader, Optional<NativeSplitReadProviderFactory>> CACHE =
            Collections.synchronizedMap(new WeakHashMap<>());

    private NativeSplitReadProviderLoader() {}

    public static Optional<SplitReadProvider> tryCreate(
            @Nullable NativeSplitReadContext context, Consumer<SplitRead<InternalRow>> config) {
        if (context == null) {
            return Optional.empty();
        }
        Optional<NativeSplitReadProviderFactory> factory = loadFactory();
        if (!factory.isPresent()) {
            return Optional.empty();
        }
        try {
            return Optional.ofNullable(factory.get().create(context, config));
        } catch (LinkageError | RuntimeException e) {
            return Optional.empty();
        }
    }

    private static Optional<NativeSplitReadProviderFactory> loadFactory() {
        ClassLoader contextClassLoader = Thread.currentThread().getContextClassLoader();
        ClassLoader ownClassLoader = NativeSplitReadProviderLoader.class.getClassLoader();

        if (contextClassLoader != null) {
            Optional<NativeSplitReadProviderFactory> factory = loadFactory(contextClassLoader);
            if (factory.isPresent() || contextClassLoader == ownClassLoader) {
                return factory;
            }
        }
        return loadFactory(ownClassLoader);
    }

    private static Optional<NativeSplitReadProviderFactory> loadFactory(ClassLoader classLoader) {
        Optional<NativeSplitReadProviderFactory> cached = CACHE.get(classLoader);
        if (cached != null) {
            return cached;
        }
        Optional<NativeSplitReadProviderFactory> loaded = discoverFactory(classLoader);
        CACHE.put(classLoader, loaded);
        return loaded;
    }

    private static Optional<NativeSplitReadProviderFactory> discoverFactory(
            ClassLoader classLoader) {
        try {
            ServiceLoader<NativeSplitReadProviderFactory> loader =
                    ServiceLoader.load(NativeSplitReadProviderFactory.class, classLoader);
            for (NativeSplitReadProviderFactory factory : loader) {
                return Optional.of(factory);
            }
            return Optional.empty();
        } catch (ServiceConfigurationError | LinkageError | RuntimeException e) {
            return Optional.empty();
        }
    }
}
