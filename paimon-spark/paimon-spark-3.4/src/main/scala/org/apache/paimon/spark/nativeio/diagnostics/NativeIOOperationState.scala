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

import org.apache.paimon.operation.nativeio.diagnostics.NativeIOPhase

case class NativeIOOperationState(
    operationId: String,
    operationName: String,
    startTime: Long,
    lastEventTime: Long,
    currentPhase: Option[NativeIOPhase] = None,
    phaseStartTime: Option[Long] = None,
    sqlExecutionId: Option[Long] = None,
    stageId: Option[Int] = None,
    stageAttemptId: Option[Int] = None,
    taskAttemptId: Option[Long] = None,
    taskIndex: Option[Int] = None,
    attemptNumber: Option[Int] = None,
    executorId: Option[String] = None,
    host: Option[String] = None,
    threadId: Option[Long] = None,
    filePath: Option[String] = None,
    outputPath: Option[String] = None,
    objectRequestId: Option[String] = None,
    objectOperation: Option[String] = None,
    rows: Option[Long] = None,
    bytes: Option[Long] = None,
    queueDepth: Option[Int] = None,
    runtimeThreads: Option[Int] = None,
    nativeMemoryBytes: Option[Long] = None,
    peakBufferedBytes: Option[Long] = None,
    metricsJson: Option[String] = None,
    errorClass: Option[String] = None,
    errorMessage: Option[String] = None,
    stackTrace: Option[String] = None,
    completed: Boolean = false,
    eventCount: Int = 0) {

  def phaseElapsedMs(now: Long): Long = {
    phaseStartTime.map(start => math.max(0L, now - start)).getOrElse(0L)
  }

  def elapsedMs(now: Long): Long = math.max(0L, now - startTime)

  def displayElapsedMs(now: Long): Long = {
    if (completed) {
      elapsedMs(lastEventTime)
    } else {
      stuckElapsedMs(now)
    }
  }

  def stuckElapsedMs(now: Long): Long = {
    if (currentPhase.isDefined) {
      phaseElapsedMs(now)
    } else {
      elapsedMs(now)
    }
  }

  def phaseOrStatus: String = {
    if (completed) {
      "COMPLETED"
    } else {
      currentPhase.map(_.name()).getOrElse("NO_NATIVE_PHASE")
    }
  }

  def diagnosis: String = {
    if (currentPhase.isDefined) {
      "Native phase is active and has not completed."
    } else if (operationName == "spark-task") {
      "Spark task is active, but no native phase event has been received yet. " +
        "The task may be blocked before diagnostics are emitted, or the deployed " +
        "paimon-native-io jar/native library may still be using a legacy entrypoint " +
        "without diagnostics callbacks. Check the executor stack for the exact FFI symbol."
    } else {
      "Native operation is active, but no phase event has been received yet. " +
        "Check whether diagnostics callbacks are registered and emitted by the deployed native library."
    }
  }
}
