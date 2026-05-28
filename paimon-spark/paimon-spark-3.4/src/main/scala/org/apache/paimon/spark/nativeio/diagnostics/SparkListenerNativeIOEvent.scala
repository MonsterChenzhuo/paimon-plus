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

import org.apache.paimon.operation.nativeio.diagnostics.{NativeIOEvent, NativeIOEventJson}
import org.apache.paimon.utils.JsonSerdeUtil

import org.apache.spark.annotation.DeveloperApi

import java.lang.{Integer => JInteger, Long => JLong}
import java.util.{Collections, Map => JMap}
import scala.util.control.NonFatal

@DeveloperApi
case class SparkListenerNativeIOEvent(
    eventJson: String,
    native_io_schema_version: JInteger,
    native_io_event_id: String,
    native_io_event_time: JLong,
    native_io_ai_kind: String,
    native_io_ai_summary: String,
    native_io_event_type: String,
    native_io_operation_id: String,
    native_io_operation_name: String,
    native_io_phase: String,
    native_io_sql_execution_id: JLong,
    native_io_stage_id: JInteger,
    native_io_stage_attempt_id: JInteger,
    native_io_task_attempt_id: JLong,
    native_io_task_index: JInteger,
    native_io_attempt_number: JInteger,
    native_io_executor_id: String,
    native_io_host: String,
    native_io_thread_id: JLong,
    native_io_file_path: String,
    native_io_output_path: String,
    native_io_object_request_id: String,
    native_io_object_operation: String,
    native_io_duration_ms: JLong,
    native_io_rows: JLong,
    native_io_bytes: JLong,
    native_io_queue_depth: JInteger,
    native_io_runtime_threads: JInteger,
    native_io_native_memory_bytes: JLong,
    native_io_peak_buffered_bytes: JLong,
    native_io_metrics: JMap[String, Object],
    native_io_error_class: String,
    native_io_error_message: String,
    native_io_stack_trace: String)
  extends SparkListenerEvent {

  private def this(event: SparkListenerNativeIOEvent) = this(
    event.eventJson,
    event.native_io_schema_version,
    event.native_io_event_id,
    event.native_io_event_time,
    event.native_io_ai_kind,
    event.native_io_ai_summary,
    event.native_io_event_type,
    event.native_io_operation_id,
    event.native_io_operation_name,
    event.native_io_phase,
    event.native_io_sql_execution_id,
    event.native_io_stage_id,
    event.native_io_stage_attempt_id,
    event.native_io_task_attempt_id,
    event.native_io_task_index,
    event.native_io_attempt_number,
    event.native_io_executor_id,
    event.native_io_host,
    event.native_io_thread_id,
    event.native_io_file_path,
    event.native_io_output_path,
    event.native_io_object_request_id,
    event.native_io_object_operation,
    event.native_io_duration_ms,
    event.native_io_rows,
    event.native_io_bytes,
    event.native_io_queue_depth,
    event.native_io_runtime_threads,
    event.native_io_native_memory_bytes,
    event.native_io_peak_buffered_bytes,
    event.native_io_metrics,
    event.native_io_error_class,
    event.native_io_error_message,
    event.native_io_stack_trace)

  def this(eventJson: String) = this(SparkListenerNativeIOEvent.fromEventJson(eventJson))
}

object SparkListenerNativeIOEvent {

  private val SchemaVersion = JInteger.valueOf(1)

  def apply(eventJson: String): SparkListenerNativeIOEvent = {
    fromEventJson(eventJson)
  }

  private[scheduler] def fromEventJson(eventJson: String): SparkListenerNativeIOEvent = {
    fromNativeEvent(NativeIOEventJson.fromJson(eventJson))
  }

  def fromNativeEvent(event: NativeIOEvent): SparkListenerNativeIOEvent = {
    new SparkListenerNativeIOEvent(
      NativeIOEventJson.toJson(event),
      SchemaVersion,
      event.eventId(),
      event.eventTime(),
      aiKind(event),
      aiSummary(event),
      event.eventType().name(),
      event.operationId(),
      event.operationName(),
      optionName(event.phase()),
      event.sqlExecutionId(),
      event.stageId(),
      event.stageAttemptId(),
      event.taskAttemptId(),
      event.taskIndex(),
      event.attemptNumber(),
      event.executorId(),
      event.host(),
      event.threadId(),
      event.filePath(),
      event.outputPath(),
      event.objectRequestId(),
      event.objectOperation(),
      event.durationMs(),
      event.rows(),
      event.bytes(),
      event.queueDepth(),
      event.runtimeThreads(),
      event.nativeMemoryBytes(),
      event.peakBufferedBytes(),
      metrics(event.metricsJson()),
      event.errorClass(),
      event.errorMessage(),
      event.stackTrace())
  }

  private def aiKind(event: NativeIOEvent): String = {
    val operation = Option(event.operationName()).getOrElse("")
    if (operation.contains("read")) {
      "paimon_native_io_reader"
    } else if (operation.contains("export")) {
      "paimon_native_io_export"
    } else {
      "paimon_native_io"
    }
  }

  private def aiSummary(event: NativeIOEvent): String = {
    val phase = Option(optionName(event.phase())).getOrElse("-")
    val duration = Option(event.durationMs()).map(v => s" duration_ms=$v").getOrElse("")
    val rows = Option(event.rows()).map(v => s" rows=$v").getOrElse("")
    val bytes = Option(event.bytes()).map(v => s" bytes=$v").getOrElse("")
    s"${event.operationName()} ${event.eventType().name()} phase=$phase$duration$rows$bytes"
  }

  private def metrics(metricsJson: String): JMap[String, Object] = {
    if (metricsJson == null || metricsJson.trim.isEmpty) {
      Collections.emptyMap[String, Object]()
    } else {
      try {
        JsonSerdeUtil.parseJsonMap(metricsJson, classOf[Object])
      } catch {
        case NonFatal(_) => Collections.emptyMap[String, Object]()
      }
    }
  }

  private def optionName(value: AnyRef): String = {
    if (value == null) {
      null
    } else {
      value.toString
    }
  }
}
