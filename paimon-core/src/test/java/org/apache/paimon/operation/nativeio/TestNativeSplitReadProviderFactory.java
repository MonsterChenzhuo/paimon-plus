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
import org.apache.paimon.table.source.Split;
import org.apache.paimon.table.source.splitread.SplitReadProvider;
import org.apache.paimon.utils.LazyField;

import java.util.function.Consumer;

/** Test ServiceLoader provider for {@link NativeSplitReadProviderLoaderTest}. */
public class TestNativeSplitReadProviderFactory implements NativeSplitReadProviderFactory {

    @Override
    public SplitReadProvider create(
            NativeSplitReadContext context, Consumer<SplitRead<InternalRow>> splitReadConfig) {
        return new TestSplitReadProvider();
    }

    private static class TestSplitReadProvider implements SplitReadProvider {

        @Override
        public boolean match(Split split, Context context) {
            return false;
        }

        @Override
        public LazyField<? extends SplitRead<InternalRow>> get() {
            return new LazyField<>(() -> null);
        }
    }
}
