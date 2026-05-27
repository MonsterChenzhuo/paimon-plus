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

package org.apache.paimon.spark;

import org.apache.paimon.spark.schema.PaimonMetadataColumn;
import org.apache.paimon.table.source.ReadBuilder;

import org.apache.spark.sql.connector.read.PartitionReader;
import org.apache.spark.sql.vectorized.ColumnarBatch;

import java.io.Serializable;
import java.util.List;

/** Optional provider for native readers that can return Spark {@link ColumnarBatch} directly. */
public interface NativeColumnarBatchReadProvider extends Serializable {

    boolean supportColumnarReads(
            ReadBuilder readBuilder,
            PaimonInputPartition partition,
            List<PaimonMetadataColumn> metadataColumns);

    PartitionReader<ColumnarBatch> createColumnarReader(
            ReadBuilder readBuilder,
            PaimonInputPartition partition,
            List<PaimonMetadataColumn> metadataColumns);
}
