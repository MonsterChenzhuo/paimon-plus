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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.ServiceConfigurationError;
import java.util.ServiceLoader;
import java.util.WeakHashMap;
import java.util.function.Consumer;

/** ClassLoader-safe loader for optional native split read providers. */
public final class NativeSplitReadProviderLoader {

    private static final Map<ClassLoader, List<NativeSplitReadProviderFactory>> CACHE =
            Collections.synchronizedMap(new WeakHashMap<>());

    private NativeSplitReadProviderLoader() {}

    public static Optional<SplitReadProvider> tryCreate(
            @Nullable NativeSplitReadContext context, Consumer<SplitRead<InternalRow>> config) {
        if (context == null) {
            return Optional.empty();
        }
        for (NativeSplitReadProviderFactory factory : loadFactories()) {
            try {
                SplitReadProvider provider = factory.create(context, config);
                if (provider != null) {
                    return Optional.of(provider);
                }
            } catch (LinkageError | RuntimeException e) {
                // Try the next factory. Native IO is optional and must not break Java reads.
            }
        }
        return Optional.empty();
    }

    private static List<NativeSplitReadProviderFactory> loadFactories() {
        ClassLoader contextClassLoader = Thread.currentThread().getContextClassLoader();
        ClassLoader ownClassLoader = NativeSplitReadProviderLoader.class.getClassLoader();

        if (contextClassLoader == null || contextClassLoader == ownClassLoader) {
            return loadFactories(ownClassLoader);
        }

        List<NativeSplitReadProviderFactory> factories =
                new ArrayList<>(loadFactories(contextClassLoader));
        factories.addAll(loadFactories(ownClassLoader));
        return factories;
    }

    private static List<NativeSplitReadProviderFactory> loadFactories(ClassLoader classLoader) {
        List<NativeSplitReadProviderFactory> cached = CACHE.get(classLoader);
        if (cached != null) {
            return cached;
        }
        List<NativeSplitReadProviderFactory> loaded = discoverFactories(classLoader);
        CACHE.put(classLoader, loaded);
        return loaded;
    }

    private static List<NativeSplitReadProviderFactory> discoverFactories(ClassLoader classLoader) {
        try {
            ServiceLoader<NativeSplitReadProviderFactory> loader =
                    ServiceLoader.load(NativeSplitReadProviderFactory.class, classLoader);
            List<NativeSplitReadProviderFactory> factories = new ArrayList<>();
            for (NativeSplitReadProviderFactory factory : loader) {
                factories.add(factory);
            }
            return factories;
        } catch (ServiceConfigurationError | LinkageError | RuntimeException e) {
            return Collections.emptyList();
        }
    }
}
