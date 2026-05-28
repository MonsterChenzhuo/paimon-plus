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

package org.apache.paimon.nativeio.spark;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.TimeStampMicroVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.vectorized.ColumnVector;
import org.apache.spark.sql.vectorized.ColumnarBatch;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

class ArrowSparkColumnVectorTest {

    @Test
    void readsPrimitiveValuesAndClosesSharedRootOnce() {
        try (BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE);
                IntVector id = new IntVector("id", allocator);
                VarCharVector name = new VarCharVector("name", allocator);
                TimeStampMicroVector ts = new TimeStampMicroVector("ts", allocator)) {
            id.allocateNew(2);
            id.setSafe(0, 7);
            id.setNull(1);
            id.setValueCount(2);

            name.allocateNew();
            name.setSafe(0, "paimon".getBytes(StandardCharsets.UTF_8));
            name.setSafe(1, "native".getBytes(StandardCharsets.UTF_8));
            name.setValueCount(2);

            ts.allocateNew(2);
            ts.setSafe(0, 1_234_567L);
            ts.setSafe(1, 2_345_678L);
            ts.setValueCount(2);

            AtomicInteger closeCount = new AtomicInteger();
            AtomicBoolean closed = new AtomicBoolean();
            Runnable closeAction =
                    () -> {
                        if (closed.compareAndSet(false, true)) {
                            closeCount.incrementAndGet();
                        }
                    };

            ColumnVector idColumn =
                    new ArrowSparkColumnVector(DataTypes.IntegerType, id, closeAction);
            ColumnVector nameColumn =
                    new ArrowSparkColumnVector(DataTypes.StringType, name, closeAction);
            ColumnVector tsColumn =
                    new ArrowSparkColumnVector(DataTypes.TimestampType, ts, closeAction);
            ColumnarBatch batch =
                    new ColumnarBatch(new ColumnVector[] {idColumn, nameColumn, tsColumn}, 2);

            assertThat(idColumn.hasNull()).isTrue();
            assertThat(idColumn.numNulls()).isEqualTo(1);
            assertThat(idColumn.getInt(0)).isEqualTo(7);
            assertThat(idColumn.isNullAt(1)).isTrue();
            assertThat(nameColumn.getUTF8String(0).toString()).isEqualTo("paimon");
            assertThat(tsColumn.getLong(1)).isEqualTo(2_345_678L);

            batch.close();
            assertThat(closeCount).hasValue(1);
        }
    }
}
