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

import org.apache.paimon.operation.nativeio.diagnostics.{
  NativeIOEvent,
  NativeIOEventType,
  NativeIOPhase
}

import org.scalatest.FunSuite

class NativeIOStoreSuite extends FunSuite {

  test("keeps active operation phase so stuck native task can be diagnosed") {
    val store = new NativeIOStore(maxEvents = 16, stuckThresholdMs = 30000L)

    store.record(
      event("op-1", 1000L, NativeIOEventType.OPERATION_START)
        .withSqlExecutionId(7L)
        .withStageId(11)
        .withStageAttemptId(0)
        .withTaskAttemptId(42L)
        .withTaskIndex(3)
        .withAttemptNumber(0)
        .withExecutorId("5")
        .withHost("worker-5")
        .build())
    store.record(
      event("op-1", 1100L, NativeIOEventType.JNI_CALL_START)
        .withPhase(NativeIOPhase.JNI)
        .build())
    store.record(
      event("op-1", 1200L, NativeIOEventType.FILE_START)
        .withPhase(NativeIOPhase.OPEN_READER)
        .withFilePath("s3://warehouse/t/dt=1/file.parquet")
        .withObjectOperation("GetObject")
        .withObjectRequestId("req-1")
        .build())

    val active = store.activeOperations(41200L)
    assert(active.size == 1)

    val operation = active.head
    assert(operation.operationId == "op-1")
    assert(operation.sqlExecutionId.contains(7L))
    assert(operation.stageId.contains(11))
    assert(operation.taskAttemptId.contains(42L))
    assert(operation.taskIndex.contains(3))
    assert(operation.executorId.contains("5"))
    assert(operation.host.contains("worker-5"))
    assert(operation.currentPhase.contains(NativeIOPhase.OPEN_READER))
    assert(operation.phaseElapsedMs(41200L) == 40000L)
    assert(operation.filePath.contains("s3://warehouse/t/dt=1/file.parquet"))
    assert(operation.objectOperation.contains("GetObject"))
    assert(operation.objectRequestId.contains("req-1"))

    val stuck = store.stuckOperations(41200L)
    assert(stuck.map(_.operationId) == Seq("op-1"))
  }

  test("marks completed operation and keeps bounded timeline") {
    val store = new NativeIOStore(maxEvents = 2, stuckThresholdMs = 30000L)

    store.record(event("op-1", 1000L, NativeIOEventType.OPERATION_START).build())
    store.record(event("op-1", 2000L, NativeIOEventType.PHASE_START).withPhase(NativeIOPhase.READ).build())
    store.record(event("op-1", 3000L, NativeIOEventType.OPERATION_END).withRows(10L).withBytes(2048L).build())

    assert(store.activeOperations(60000L).isEmpty)
    assert(store.completedOperations.size == 1)
    assert(store.completedOperations.head.rows.contains(10L))
    assert(store.completedOperations.head.bytes.contains(2048L))
    assert(store.timeline.map(_.eventTime()) == Seq(2000L, 3000L))
  }

  test("marks active spark task without native phase as stuck") {
    val store = new NativeIOStore(maxEvents = 16, stuckThresholdMs = 30000L)

    store.record(
      event("task-1", 1000L, NativeIOEventType.TASK_RECEIVED, "spark-task")
        .withStageId(2)
        .withTaskAttemptId(9L)
        .withExecutorId("3")
        .build())

    val active = store.activeOperations(32000L)
    assert(active.size == 1)
    assert(active.head.phaseOrStatus == "NO_NATIVE_PHASE")
    assert(active.head.stuckElapsedMs(32000L) == 31000L)
    assert(active.head.diagnosis.contains("no native phase event"))

    val stuck = store.stuckOperations(32000L)
    assert(stuck.map(_.operationId) == Seq("task-1"))
  }

  private def event(
      operationId: String,
      eventTime: Long,
      eventType: NativeIOEventType,
      operationName: String = "native-export-parquet"): NativeIOEvent.Builder = {
    NativeIOEvent
      .builder(operationId + "-" + eventTime, eventTime, eventType, operationId, operationName)
  }
}
