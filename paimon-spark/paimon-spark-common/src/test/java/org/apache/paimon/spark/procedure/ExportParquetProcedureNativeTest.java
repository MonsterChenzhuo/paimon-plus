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

package org.apache.paimon.spark.procedure;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class ExportParquetProcedureNativeTest {

    @Test
    void nativeTaskCountDoesNotApplyTargetFileSizeCoalescing() {
        assertThat(ExportParquetProcedure.nativeTaskCount(32, 11)).isEqualTo(11);
        assertThat(ExportParquetProcedure.nativeTaskCount(32, 100)).isEqualTo(32);
        assertThat(ExportParquetProcedure.nativeTaskCount(1, 100)).isEqualTo(1);
    }

    @Test
    void defaultPartitionJobParallelismUsesAllPartitions() {
        assertThat(ExportParquetProcedure.partitionJobParallelism(null, 2)).isEqualTo(2);
        assertThat(ExportParquetProcedure.partitionJobParallelism(null, 8)).isEqualTo(8);
        assertThat(ExportParquetProcedure.partitionJobParallelism(4, 8)).isEqualTo(4);
        assertThat(ExportParquetProcedure.partitionJobParallelism(16, 8)).isEqualTo(8);
        assertThat(ExportParquetProcedure.partitionJobParallelism(0, 8)).isEqualTo(1);
    }
}
