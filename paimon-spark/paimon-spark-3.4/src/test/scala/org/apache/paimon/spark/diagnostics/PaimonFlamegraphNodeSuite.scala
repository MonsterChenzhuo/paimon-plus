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

package org.apache.paimon.spark.diagnostics

import org.apache.spark.status.api.v1.{StackTrace, ThreadStackTrace}
import org.scalatest.FunSuite

class PaimonFlamegraphNodeSuite extends FunSuite {

  test("folds executor thread dump frames into flamegraph JSON") {
    val json =
      PaimonFlamegraphNode
        .fromThreadDump(
          Array(
            thread(1L, Seq("reader-a", "shared-parent")),
            thread(2L, Seq("reader-b", "shared-parent"))))
        .toJsonString

    assert(json.contains("\"name\":\"root\",\"value\":2"))
    assert(json.contains("\"name\":\"shared-parent\",\"value\":2"))
    assert(json.contains("\"name\":\"reader-a\",\"value\":1"))
    assert(json.contains("\"name\":\"reader-b\",\"value\":1"))
  }

  test("escapes frame names when rendering JSON") {
    val json =
      PaimonFlamegraphNode
        .fromThreadDump(Array(thread(1L, Seq("reader-\"quoted\"", "shared-parent"))))
        .toJsonString

    assert(json.contains("reader-\\\"quoted\\\""))
  }

  private def thread(threadId: Long, frames: Seq[String]): ThreadStackTrace = {
    ThreadStackTrace(
      threadId,
      "executor-task-" + threadId,
      Thread.State.RUNNABLE,
      StackTrace(frames),
      None,
      "",
      Seq.empty)
  }
}
