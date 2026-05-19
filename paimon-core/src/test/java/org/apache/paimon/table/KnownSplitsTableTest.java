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

package org.apache.paimon.table;

import org.apache.paimon.table.source.Split;

import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class KnownSplitsTableTest {

    @Test
    void copyPreservesKnownSplitsAndCopiesOriginOptions() {
        InnerTable origin = mock(InnerTable.class);
        InnerTable copiedOrigin = mock(InnerTable.class);
        Split[] splits = new Split[0];
        Map<String, String> dynamicOptions = new HashMap<>();
        dynamicOptions.put("__paimon.internal.native-io.engine", "spark");
        Map<String, String> copiedOptions = new HashMap<>(dynamicOptions);

        when(origin.copy(dynamicOptions)).thenReturn(copiedOrigin);
        when(copiedOrigin.options()).thenReturn(copiedOptions);

        KnownSplitsTable copied =
                (KnownSplitsTable) KnownSplitsTable.create(origin, splits).copy(dynamicOptions);

        assertThat(copied.splits()).isSameAs(splits);
        assertThat(copied.options()).containsEntry("__paimon.internal.native-io.engine", "spark");
    }
}
