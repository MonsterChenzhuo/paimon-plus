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
import org.scalatest.FunSuite

class NativeIOEventLogSuite extends FunSuite {

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
}
