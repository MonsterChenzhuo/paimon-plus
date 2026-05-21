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

import scala.collection.mutable

class NativeIOStore(maxEvents: Int, stuckThresholdMs: Long) {

  private val timelineBuffer = new mutable.Queue[NativeIOEvent]()
  private val active = new mutable.LinkedHashMap[String, NativeIOOperationState]()
  private val completed = new mutable.Queue[NativeIOOperationState]()

  def record(event: NativeIOEvent): Unit = synchronized {
    appendTimeline(event)

    val oldState = active.getOrElse(
      event.operationId(),
      NativeIOOperationState(
        operationId = event.operationId(),
        operationName = event.operationName(),
        startTime = event.eventTime(),
        lastEventTime = event.eventTime()))

    val newState = merge(oldState, event)
    if (isFinished(event)) {
      active.remove(event.operationId())
      completed.enqueue(newState.copy(completed = true))
    } else {
      active.put(event.operationId(), newState)
    }
  }

  def activeOperations(now: Long): Seq[NativeIOOperationState] = synchronized {
    active.values.toSeq.sortBy(state => -state.elapsedMs(now))
  }

  def stuckOperations(now: Long): Seq[NativeIOOperationState] = synchronized {
    activeOperations(now)
      .filter(state => state.stuckElapsedMs(now) >= stuckThresholdMs)
  }

  def completedOperations: Seq[NativeIOOperationState] = synchronized {
    completed.toSeq.reverse
  }

  def timeline: Seq[NativeIOEvent] = synchronized {
    timelineBuffer.toSeq
  }

  private def appendTimeline(event: NativeIOEvent): Unit = {
    timelineBuffer.enqueue(event)
    while (timelineBuffer.size > maxEvents) {
      timelineBuffer.dequeue()
    }
  }

  private def merge(
      state: NativeIOOperationState,
      event: NativeIOEvent): NativeIOOperationState = {
    val nextPhase = phaseFrom(event)
    val nextPhaseStartTime = nextPhase match {
      case Some(phase) if state.currentPhase.contains(phase) => state.phaseStartTime
      case Some(_) => Some(event.eventTime())
      case None => state.phaseStartTime
    }

    state.copy(
      operationName = event.operationName(),
      lastEventTime = event.eventTime(),
      currentPhase = nextPhase.orElse(state.currentPhase),
      phaseStartTime = nextPhaseStartTime,
      sqlExecutionId = option(event.sqlExecutionId()).map(_.longValue()).orElse(state.sqlExecutionId),
      stageId = option(event.stageId()).map(_.intValue()).orElse(state.stageId),
      stageAttemptId = option(event.stageAttemptId()).map(_.intValue()).orElse(state.stageAttemptId),
      taskAttemptId = option(event.taskAttemptId()).map(_.longValue()).orElse(state.taskAttemptId),
      taskIndex = option(event.taskIndex()).map(_.intValue()).orElse(state.taskIndex),
      attemptNumber = option(event.attemptNumber()).map(_.intValue()).orElse(state.attemptNumber),
      executorId = option(event.executorId()).orElse(state.executorId),
      host = option(event.host()).orElse(state.host),
      threadId = option(event.threadId()).map(_.longValue()).orElse(state.threadId),
      filePath = option(event.filePath()).orElse(state.filePath),
      outputPath = option(event.outputPath()).orElse(state.outputPath),
      objectRequestId = option(event.objectRequestId()).orElse(state.objectRequestId),
      objectOperation = option(event.objectOperation()).orElse(state.objectOperation),
      rows = option(event.rows()).map(_.longValue()).orElse(state.rows),
      bytes = option(event.bytes()).map(_.longValue()).orElse(state.bytes),
      queueDepth = option(event.queueDepth()).map(_.intValue()).orElse(state.queueDepth),
      runtimeThreads = option(event.runtimeThreads()).map(_.intValue()).orElse(state.runtimeThreads),
      nativeMemoryBytes =
        option(event.nativeMemoryBytes()).map(_.longValue()).orElse(state.nativeMemoryBytes),
      peakBufferedBytes =
        option(event.peakBufferedBytes()).map(_.longValue()).orElse(state.peakBufferedBytes),
      metricsJson = option(event.metricsJson()).orElse(state.metricsJson),
      errorClass = option(event.errorClass()).orElse(state.errorClass),
      errorMessage = option(event.errorMessage()).orElse(state.errorMessage),
      stackTrace = option(event.stackTrace()).orElse(state.stackTrace),
      completed = isFinished(event),
      eventCount = state.eventCount + 1)
  }

  private def phaseFrom(event: NativeIOEvent): Option[NativeIOPhase] = {
    option(event.phase()).orElse {
      event.eventType() match {
        case NativeIOEventType.JNI_CALL_START => Some(NativeIOPhase.JNI)
        case NativeIOEventType.FFI_ENTERED => Some(NativeIOPhase.PARSE_REQUEST)
        case NativeIOEventType.REQUEST_PARSED => Some(NativeIOPhase.PLAN)
        case NativeIOEventType.FILE_START => Some(NativeIOPhase.OPEN_READER)
        case NativeIOEventType.READER_READY => Some(NativeIOPhase.READ)
        case _ => None
      }
    }
  }

  private def isFinished(event: NativeIOEvent): Boolean = {
    event.eventType() == NativeIOEventType.OPERATION_END ||
    event.eventType() == NativeIOEventType.ERROR
  }

  private def option[T](value: T): Option[T] = {
    if (value == null) {
      None
    } else {
      Some(value)
    }
  }
}
