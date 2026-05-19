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

package org.apache.paimon.nativeio;

import org.apache.paimon.data.InternalRow;
import org.apache.paimon.operation.SplitRead;
import org.apache.paimon.operation.nativeio.NativeSplitReadContext;
import org.apache.paimon.operation.nativeio.NativeSplitReadProviderFactory;
import org.apache.paimon.table.source.splitread.SplitReadProvider;

import java.util.function.Consumer;

/** ServiceLoader entry point for paimon-native-io. */
public class PaimonNativeSplitReadProviderFactory implements NativeSplitReadProviderFactory {

    @Override
    public SplitReadProvider create(
            NativeSplitReadContext context, Consumer<SplitRead<InternalRow>> splitReadConfig) {
        return new NativeRawFileSplitReadProvider(context, splitReadConfig);
    }
}
