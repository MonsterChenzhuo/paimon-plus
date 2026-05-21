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

package org.apache.spark.scheduler

import org.apache.paimon.operation.nativeio.diagnostics.{
  NativeIOEvent,
  NativeIOEventJson,
  NativeIOEventType
}
import org.apache.spark.status.{ElementTrackingStore, NativeIOEventData}

class NativeIOHistoryListener(store: ElementTrackingStore) extends SparkListener {

  override def onTaskStart(taskStart: SparkListenerTaskStart): Unit = {
    write(baseEvent(taskStart.stageId, taskStart.stageAttemptId, taskStart.taskInfo,
      NativeIOEventType.TASK_RECEIVED).build())
  }

  override def onTaskEnd(taskEnd: SparkListenerTaskEnd): Unit = {
    val info = taskEnd.taskInfo
    write(
      baseEvent(taskEnd.stageId, taskEnd.stageAttemptId, info, NativeIOEventType.OPERATION_END)
        .withErrorMessage(if (info.failed || info.killed) taskEnd.reason.toString else null)
        .build())
  }

  override def onOtherEvent(event: SparkListenerEvent): Unit = event match {
    case nativeEvent: SparkListenerNativeIOEvent =>
      write(NativeIOEventJson.fromJson(nativeEvent.eventJson))
    case _ =>
  }

  private def baseEvent(
      stageId: Int,
      stageAttemptId: Int,
      info: TaskInfo,
      eventType: NativeIOEventType): NativeIOEvent.Builder = {
    val operationId = "spark-task-" + stageId + "." + stageAttemptId + "-" + info.taskId
    NativeIOEvent
      .builder(operationId + "-" + eventType.name(), System.currentTimeMillis(), eventType,
        operationId, "spark-task")
      .withStageId(stageId)
      .withStageAttemptId(stageAttemptId)
      .withTaskAttemptId(info.taskId)
      .withTaskIndex(info.index)
      .withAttemptNumber(info.attemptNumber)
      .withExecutorId(info.executorId)
      .withHost(info.host)
  }

  private def write(event: NativeIOEvent): Unit = {
    store.write(new NativeIOEventData(
      event.eventId(),
      event.eventTime(),
      NativeIOEventJson.toJson(event)))
  }
}
