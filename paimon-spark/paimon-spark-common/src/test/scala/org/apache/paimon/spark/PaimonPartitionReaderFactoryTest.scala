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

package org.apache.paimon.spark

import org.apache.paimon.partition.PartitionPredicate
import org.apache.paimon.predicate.{Predicate, TopN}
import org.apache.paimon.spark.schema.PaimonMetadataColumn
import org.apache.paimon.table.source.{ReadBuilder, StreamTableScan, TableRead, TableScan}
import org.apache.paimon.types.RowType
import org.apache.paimon.utils.{Filter, Range, RowRangeIndex}

import org.scalatest.FunSuite

import java.util.{List => JList, Map => JMap, Objects}

class PaimonPartitionReaderFactoryTest extends FunSuite {

  test("reader factory equality includes blob descriptor setting") {
    val readBuilder = new StubReadBuilder("t")

    val normal =
      PaimonPartitionReaderFactory(readBuilder, Seq.empty, blobAsDescriptor = false)
    val blobAsDescriptor =
      PaimonPartitionReaderFactory(readBuilder, Seq.empty, blobAsDescriptor = true)

    assert(normal != blobAsDescriptor)
    assert(normal.hashCode() != blobAsDescriptor.hashCode())
  }

  test("reader factory hash includes metadata columns") {
    val readBuilder = new StubReadBuilder("t")

    val withoutMetadata =
      PaimonPartitionReaderFactory(readBuilder, Seq.empty, blobAsDescriptor = false)
    val withMetadata =
      PaimonPartitionReaderFactory(
        readBuilder,
        Seq(PaimonMetadataColumn.ROW_INDEX),
        blobAsDescriptor = false)

    assert(withoutMetadata != withMetadata)
    assert(withoutMetadata.hashCode() != withMetadata.hashCode())
  }

  private class StubReadBuilder(private val id: String) extends ReadBuilder {

    override def tableName(): String = id

    override def readType(): RowType = null

    override def withFilter(predicate: Predicate): ReadBuilder = this

    override def withPartitionFilter(partitionSpec: JMap[String, String]): ReadBuilder = this

    override def withPartitionFilter(partitionPredicate: PartitionPredicate): ReadBuilder = this

    override def withBucket(bucket: Int): ReadBuilder = this

    override def withBucketFilter(bucketFilter: Filter[Integer]): ReadBuilder = this

    override def withReadType(readType: RowType): ReadBuilder = this

    override def withProjection(projection: Array[Int]): ReadBuilder = this

    override def withLimit(limit: Int): ReadBuilder = this

    override def withTopN(topN: TopN): ReadBuilder = this

    override def withShard(indexOfThisSubtask: Int, numberOfParallelSubtasks: Int): ReadBuilder =
      this

    override def withRowRanges(rowRanges: JList[Range]): ReadBuilder = this

    override def withRowRangeIndex(rowRangeIndex: RowRangeIndex): ReadBuilder = this

    override def dropStats(): ReadBuilder = this

    override def newScan(): TableScan = null

    override def newStreamScan(): StreamTableScan = null

    override def newRead(): TableRead = null

    override def equals(obj: Any): Boolean = {
      obj match {
        case other: StubReadBuilder => id == other.id
        case _ => false
      }
    }

    override def hashCode(): Int = Objects.hashCode(id)
  }
}
