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

package org.apache.paimon.operation.nativeio.export;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.ServiceConfigurationError;
import java.util.ServiceLoader;

/** Factory discovered by {@link ServiceLoader} for optional native export support. */
public interface NativeExportProviderFactory {

    NativeExportProvider create();

    static List<NativeExportProviderFactory> discover(ClassLoader classLoader) {
        List<NativeExportProviderFactory> factories = discoverWith(classLoader);
        ClassLoader ownClassLoader = NativeExportProviderFactory.class.getClassLoader();
        if (!factories.isEmpty() || classLoader == ownClassLoader) {
            return factories;
        }
        return discoverWith(ownClassLoader);
    }

    static List<NativeExportProviderFactory> discoverWith(ClassLoader classLoader) {
        if (classLoader == null) {
            return Collections.emptyList();
        }
        try {
            List<NativeExportProviderFactory> factories = new ArrayList<>();
            ServiceLoader<NativeExportProviderFactory> loader =
                    ServiceLoader.load(NativeExportProviderFactory.class, classLoader);
            for (NativeExportProviderFactory factory : loader) {
                factories.add(factory);
            }
            return factories;
        } catch (ServiceConfigurationError | LinkageError | RuntimeException e) {
            return Collections.emptyList();
        }
    }
}
