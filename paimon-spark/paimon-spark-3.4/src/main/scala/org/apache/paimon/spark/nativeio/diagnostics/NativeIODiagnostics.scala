/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.paimon.spark.nativeio.diagnostics

import org.apache.paimon.operation.nativeio.diagnostics.NativeIOEvent

import org.apache.spark.{NativeIORpcSupport, TaskContext}
import org.apache.spark.sql.SparkSession
import org.apache.spark.ui.NativeIOTabSupport

import java.net.InetAddress

object NativeIODiagnostics {

  private val SqlExecutionIdKey = "spark.sql.execution.id"
  private val DefaultMaxEvents = 10000
  private val DefaultStuckThresholdMs = 30000L

  @volatile private var installedStore: Option[NativeIOStore] = None

  def install(spark: SparkSession): NativeIOStore = synchronized {
    installedStore match {
      case Some(store) => store
      case None =>
        val store = new NativeIOStore(DefaultMaxEvents, DefaultStuckThresholdMs)
        NativeIORpcSupport.setupDriver(spark.sparkContext, store)
        sparkUi(spark).foreach(NativeIOTabSupport.attach(_, store))
        installedStore = Some(store)
        store
    }
  }

  def record(event: NativeIOEvent): Unit = {
    val enriched = enrichFromSparkTask(event)
    installedStore match {
      case Some(store) => store.record(enriched)
      case None => sendToDriver(enriched)
    }
  }

  def store: Option[NativeIOStore] = installedStore

  private def sparkUi(spark: SparkSession): Option[Any] = {
    val method = spark.sparkContext.getClass.getMethod("ui")
    method.invoke(spark.sparkContext).asInstanceOf[Option[Any]]
  }

  private def sendToDriver(event: NativeIOEvent): Unit = {
    NativeIORpcSupport.sendToDriver(event)
  }

  private def enrichFromSparkTask(event: NativeIOEvent): NativeIOEvent = {
    val builder = event.toBuilder()
    val context = TaskContext.get()
    if (context != null) {
      if (event.stageId() == null) {
        builder.withStageId(context.stageId())
      }
      if (event.stageAttemptId() == null) {
        builder.withStageAttemptId(context.stageAttemptNumber())
      }
      if (event.taskAttemptId() == null) {
        builder.withTaskAttemptId(context.taskAttemptId())
      }
      if (event.taskIndex() == null) {
        builder.withTaskIndex(context.partitionId())
      }
      if (event.attemptNumber() == null) {
        builder.withAttemptNumber(context.attemptNumber())
      }
      if (event.sqlExecutionId() == null) {
        val sqlExecutionId = context.getLocalProperty(SqlExecutionIdKey)
        if (sqlExecutionId != null) {
          builder.withSqlExecutionId(sqlExecutionId.toLong)
        }
      }
    }

    if (event.executorId() == null) {
      NativeIORpcSupport.executorId.foreach(builder.withExecutorId)
    }
    if (event.host() == null) {
      builder.withHost(InetAddress.getLocalHost.getHostName)
    }
    builder.build()
  }
}
