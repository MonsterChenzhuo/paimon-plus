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

import org.apache.paimon.spark.schema.PaimonMetadataColumn
import org.apache.paimon.table.source.ReadBuilder

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.read.{InputPartition, PartitionReader, PartitionReaderFactory}
import org.apache.spark.sql.vectorized.ColumnarBatch

import java.util.{Objects, ServiceLoader}

import scala.collection.JavaConverters._

case class PaimonPartitionReaderFactory(
    readBuilder: ReadBuilder,
    metadataColumns: Seq[PaimonMetadataColumn] = Seq.empty,
    blobAsDescriptor: Boolean,
    nativeColumnarBatchReadProviders: Seq[NativeColumnarBatchReadProvider] =
      PaimonPartitionReaderFactory.loadNativeColumnarBatchReadProviders())
  extends PartitionReaderFactory {

  override def createReader(partition: InputPartition): PartitionReader[InternalRow] = {
    partition match {
      case paimonInputPartition: PaimonInputPartition =>
        PaimonPartitionReader(readBuilder, paimonInputPartition, metadataColumns, blobAsDescriptor)
      case _ =>
        throw new RuntimeException(s"It's not a Paimon input partition, $partition")
    }
  }

  override def supportColumnarReads(partition: InputPartition): Boolean = {
    partition match {
      case paimonInputPartition: PaimonInputPartition =>
        nativeColumnarBatchReadProviders.exists(
          _.supportColumnarReads(readBuilder, paimonInputPartition, metadataColumns.asJava))
      case _ => false
    }
  }

  override def createColumnarReader(
      partition: InputPartition): PartitionReader[ColumnarBatch] = {
    partition match {
      case paimonInputPartition: PaimonInputPartition =>
        nativeColumnarBatchReadProviders
          .find(
            _.supportColumnarReads(readBuilder, paimonInputPartition, metadataColumns.asJava))
          .map(_.createColumnarReader(readBuilder, paimonInputPartition, metadataColumns.asJava))
          .getOrElse(
            throw new RuntimeException(
              s"No native columnar reader provider supports Paimon input partition $partition"))
      case _ =>
        throw new RuntimeException(s"It's not a Paimon input partition, $partition")
    }
  }

  override def equals(obj: Any): Boolean = {
    obj match {
      case other: PaimonPartitionReaderFactory =>
        this.readBuilder.equals(other.readBuilder) &&
        this.metadataColumns == other.metadataColumns &&
        this.blobAsDescriptor == other.blobAsDescriptor

      case _ => false
    }
  }

  override def hashCode(): Int = {
    Objects.hash(readBuilder, metadataColumns, java.lang.Boolean.valueOf(blobAsDescriptor))
  }
}

object PaimonPartitionReaderFactory {

  def loadNativeColumnarBatchReadProviders(): Seq[NativeColumnarBatchReadProvider] = {
    ServiceLoader
      .load(classOf[NativeColumnarBatchReadProvider])
      .iterator()
      .asScala
      .toSeq
  }
}
