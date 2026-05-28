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
  NativeIOEventType,
  NativeIOPhase
}
import org.apache.spark.util.JsonProtocol
import org.json4s.DefaultFormats
import org.json4s.jackson.JsonMethods.parse
import org.scalatest.FunSuite

class NativeIOEventLogSuite extends FunSuite {

  private implicit val formats: DefaultFormats.type = DefaultFormats

  test("serializes native io event through spark event log protocol") {
    val nativeEvent =
      NativeIOEvent
        .builder("op-1-1000", 1000L, NativeIOEventType.JNI_CALL_START, "op-1",
          "native-export-parquet")
        .withPhase(NativeIOPhase.JNI)
        .build()
    val sparkEvent = SparkListenerNativeIOEvent(NativeIOEventJson.toJson(nativeEvent))

    val json = JsonProtocol.sparkEventToJsonString(sparkEvent)
    val replayed = JsonProtocol.sparkEventFromJson(json).asInstanceOf[SparkListenerNativeIOEvent]
    val replayedNativeEvent = NativeIOEventJson.fromJson(replayed.eventJson)

    assert(replayedNativeEvent.operationId() == "op-1")
    assert(replayedNativeEvent.eventType() == NativeIOEventType.JNI_CALL_START)
    assert(replayedNativeEvent.phase() == NativeIOPhase.JNI)
  }

  test("serializes ai friendly native io fields through spark event log protocol") {
    val nativeEvent =
      NativeIOEvent
        .builder("op-2-3000", 3000L, NativeIOEventType.OPERATION_END, "op-2",
          "native-columnar-read")
        .withPhase(NativeIOPhase.READ_BATCH)
        .withSqlExecutionId(17L)
        .withStageId(3)
        .withStageAttemptId(0)
        .withTaskAttemptId(99L)
        .withTaskIndex(4)
        .withAttemptNumber(1)
        .withExecutorId("5")
        .withHost("worker-5")
        .withFilePath("obs://bucket/table/file.parquet")
        .withDurationMs(500L)
        .withRows(4096L)
        .withBytes(16384L)
        .withMetricsJson("""{"read_batch_ms":500,"read_batch_count":1,"rows":4096,"bytes":16384}""")
        .build()
    val sparkEvent = SparkListenerNativeIOEvent(NativeIOEventJson.toJson(nativeEvent))

    val json = JsonProtocol.sparkEventToJsonString(sparkEvent)
    val eventLog = parse(json)
    val metrics = eventLog \ "native_io_metrics"

    assert((eventLog \ "native_io_schema_version").extract[Int] == 1)
    assert((eventLog \ "native_io_event_id").extract[String] == "op-2-3000")
    assert((eventLog \ "native_io_event_time").extract[Long] == 3000L)
    assert((eventLog \ "native_io_ai_kind").extract[String] == "paimon_native_io_reader")
    assert((eventLog \ "native_io_event_type").extract[String] == "OPERATION_END")
    assert((eventLog \ "native_io_operation_id").extract[String] == "op-2")
    assert((eventLog \ "native_io_operation_name").extract[String] == "native-columnar-read")
    assert((eventLog \ "native_io_phase").extract[String] == "READ_BATCH")
    assert((eventLog \ "native_io_sql_execution_id").extract[Long] == 17L)
    assert((eventLog \ "native_io_stage_id").extract[Int] == 3)
    assert((eventLog \ "native_io_task_attempt_id").extract[Long] == 99L)
    assert((eventLog \ "native_io_executor_id").extract[String] == "5")
    assert((eventLog \ "native_io_file_path").extract[String] == "obs://bucket/table/file.parquet")
    assert((eventLog \ "native_io_duration_ms").extract[Long] == 500L)
    assert((eventLog \ "native_io_rows").extract[Long] == 4096L)
    assert((eventLog \ "native_io_bytes").extract[Long] == 16384L)
    assert((metrics \ "read_batch_ms").extract[Long] == 500L)
    assert((metrics \ "read_batch_count").extract[Long] == 1L)
    assert((metrics \ "rows").extract[Long] == 4096L)
    assert((metrics \ "bytes").extract[Long] == 16384L)
  }

  test("keeps native io event log record when metrics json is malformed") {
    val nativeEvent =
      NativeIOEvent
        .builder("op-3-3000", 3000L, NativeIOEventType.OPERATION_END, "op-3",
          "native-columnar-read")
        .withMetricsJson("{bad-json")
        .withStackTrace("java.lang.RuntimeException: bad metrics")
        .build()
    val sparkEvent = SparkListenerNativeIOEvent(NativeIOEventJson.toJson(nativeEvent))

    val json = JsonProtocol.sparkEventToJsonString(sparkEvent)
    val eventLog = parse(json)

    assert((eventLog \ "native_io_operation_id").extract[String] == "op-3")
    assert((eventLog \ "native_io_metrics").extract[Map[String, Any]].isEmpty)
    assert((eventLog \ "native_io_stack_trace").extract[String] ==
      "java.lang.RuntimeException: bad metrics")
    assert(NativeIOEventJson.fromJson((eventLog \ "eventJson").extract[String]).operationId() == "op-3")
  }

  test("replays legacy native io event log record with only eventJson") {
    val nativeEvent =
      NativeIOEvent
        .builder("op-4-1000", 1000L, NativeIOEventType.JNI_CALL_START, "op-4",
          "native-export-parquet")
        .withPhase(NativeIOPhase.JNI)
        .build()
    val nativeJson = NativeIOEventJson.toJson(nativeEvent)
    val legacyJson =
      "{\"Event\":\"org.apache.spark.scheduler.SparkListenerNativeIOEvent\",\"eventJson\":\"" +
        escapeJson(nativeJson) + "\"}"

    val replayed =
      JsonProtocol.sparkEventFromJson(legacyJson).asInstanceOf[SparkListenerNativeIOEvent]

    assert(NativeIOEventJson.fromJson(replayed.eventJson).operationId() == "op-4")
  }

  private def escapeJson(value: String): String = {
    value.replace("\\", "\\\\").replace("\"", "\\\"")
  }
}
