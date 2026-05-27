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

package org.apache.spark.ui

import org.apache.paimon.spark.diagnostics.profiler.PaimonProfilerConf

import org.apache.spark.SparkConf
import org.apache.spark.status.api.v1.{StackTrace, ThreadStackTrace}
import org.json4s.JsonAST.{JArray, JBool, JInt, JString}
import org.scalatest.FunSuite

class PaimonDiagnosticsJsonSuite extends FunSuite {

  test("thread dump snapshot exposes AI-readable stack facts and flamegraph") {
    val json =
      PaimonDiagnosticsJson.threadDumpSnapshot(
        Some("application_1"),
        "3.4.4",
        "7",
        Array(
          thread(
            1L,
            "Executor task launch worker for task 1",
            Thread.State.RUNNABLE,
            Seq(
              "org.apache.paimon.table.sink.TableWriteImpl.write(TableWriteImpl.java:120)",
              "org.apache.parquet.hadoop.InternalParquetRecordWriter.write(InternalParquetRecordWriter.java:130)",
              "org.apache.spark.sql.execution.datasources.v2.WriteToDataSourceV2Exec.run(WriteToDataSourceV2Exec.scala:90)")),
          thread(
            2L,
            "dispatcher-Executor",
            Thread.State.WAITING,
            Seq(
              "java.util.concurrent.LinkedBlockingQueue.take(LinkedBlockingQueue.java:442)",
              "sun.misc.Unsafe.park(Native Method)"))),
        42L,
        flamegraphEnabled = true)

    assert((json \ "schema_version") == JInt(1))
    assert((json \ "app_id") == JString("application_1"))
    assert((json \ "executor_id") == JString("7"))
    assert((json \ "diagnosis" \ "category") == JString("executor_paimon_or_parquet_io"))
    assert((json \ "diagnosis" \ "severity") == JString("info"))
    assert((json \ "facts" \ "thread_count") == JInt(2))
    assert((json \ "facts" \ "state_counts" \ "RUNNABLE") == JInt(1))
    assert((json \ "facts" \ "state_counts" \ "WAITING") == JInt(1))
    assert((json \ "facts" \ "flamegraph_enabled") == JBool(true))
    assert((json \ "facts" \ "flamegraph" \ "name") == JString("root"))

    val stacks = (json \ "facts" \ "top_stacks").asInstanceOf[JArray].arr
    assert(stacks.nonEmpty)
    assert((stacks.head \ "tags").asInstanceOf[JArray].arr.contains(JString("paimon_io")))
    assert((stacks.head \ "tags").asInstanceOf[JArray].arr.contains(JString("parquet_io")))
  }

  test("profiler snapshot exposes configuration and artifacts for CLI consumers") {
    val conf = new SparkConf(false)
      .set("spark.plugins", PaimonProfilerConf.PluginClass)
      .set(PaimonProfilerConf.ExecutorEnabledKey, "true")
      .set(PaimonProfilerConf.ExecutorFractionKey, "1.0")
      .set(PaimonProfilerConf.OutputSuffixKey, "html")
      .set(PaimonProfilerConf.AsyncProfilerArgsKey, "event=cpu,interval=10ms")

    val json =
      PaimonDiagnosticsJson.profilerSnapshot(
        conf,
        Some("application_2"),
        Some("1"),
        Some("obs://bucket/tmp/profiles/application_2_1"),
        Right(
          Seq(
            PaimonProfilerArtifacts.Artifact(
              "profile-exec-7.html",
              "obs://bucket/tmp/profiles/application_2_1/profile-exec-7.html",
              "html",
              1024L,
              1000L))),
        2000L)

    assert((json \ "schema_version") == JInt(1))
    assert((json \ "app_id") == JString("application_2"))
    assert((json \ "facts" \ "config" \ "executor_profiling") == JBool(true))
    assert((json \ "facts" \ "config" \ "output_suffix") == JString("html"))
    assert((json \ "diagnosis" \ "category") == JString("profiler_artifacts_available"))

    val artifacts = (json \ "facts" \ "artifacts").asInstanceOf[JArray].arr
    assert(artifacts.size == 1)
    assert((artifacts.head \ "executor_id") == JString("7"))
    assert((artifacts.head \ "open_url") == JString(
      "/paimon-diagnostics/profiler/?profile=profile-exec-7.html"))
  }

  private def thread(
      threadId: Long,
      name: String,
      state: Thread.State,
      frames: Seq[String]): ThreadStackTrace = {
    ThreadStackTrace(threadId, name, state, StackTrace(frames), None, "", Seq.empty)
  }
}
