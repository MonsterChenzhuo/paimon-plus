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

import org.apache.paimon.spark.diagnostics.{PaimonDiagnostics, PaimonFlamegraphNode}
import org.apache.paimon.spark.diagnostics.profiler.PaimonProfilerConf

import org.apache.spark.SPARK_VERSION
import org.apache.spark.status.api.v1.{ExecutorSummary, ThreadStackTrace}
import org.json4s.JsonAST.{JArray, JBool, JDouble, JInt, JNull, JObject, JString, JValue}
import org.json4s.jackson.JsonMethods.parse

import java.net.URLEncoder
import java.util.Locale

import scala.collection.mutable
import scala.util.control.NonFatal

private[ui] object PaimonDiagnosticsJson {

  private val SchemaVersion = 1
  private val Source = "paimon-diagnostics"
  private val MaxTopStacks = 10
  private val MaxFramesPerStack = 40
  private val MaxSampleThreadNames = 8

  def overview(sparkUI: SparkUI): JValue = {
    val executors = sparkUI.sc.map(_.statusStore.executorList(activeOnly = false)).getOrElse(Nil)
    overviewSnapshot(Option(sparkUI.appId), SPARK_VERSION, executors, System.currentTimeMillis())
  }

  def overviewSnapshot(
      appId: Option[String],
      sparkVersion: String,
      executors: Seq[ExecutorSummary],
      generatedAtMs: Long): JValue = {
    extend(
      base(appId, sparkVersion, generatedAtMs),
      obj(
        "diagnosis" -> diagnosis(
          "diagnostics_overview",
          "info",
          1.0D,
          "Paimon diagnostics overview is available.",
          Seq("executor_count=" + executors.size),
          Seq(
            "Use /paimon-diagnostics/threadDump/json?executorId=<id> for stack aggregation.",
            "Use /paimon-diagnostics/profiler/json for async-profiler artifacts.")),
        "facts" -> obj(
          "executor_count" -> jint(executors.size),
          "executors" -> JArray(executors.sortBy(executorSortKey).map(executorJson).toList))))
  }

  def threadDump(sparkUI: SparkUI, executorId: String): JValue = {
    sparkUI.sc match {
      case Some(sc) =>
        val flamegraphEnabled = PaimonDiagnostics.flamegraphEnabled(sc.getConf)
        sc.getExecutorThreadDump(executorId) match {
          case Some(threadDump) =>
            threadDumpSnapshot(
              Option(sparkUI.appId),
              SPARK_VERSION,
              executorId,
              threadDump,
              System.currentTimeMillis(),
              flamegraphEnabled)
          case None =>
            errorSnapshot(
              Option(sparkUI.appId),
              SPARK_VERSION,
              System.currentTimeMillis(),
              "thread_dump_unavailable",
              "Error fetching thread dump from executor " + executorId)
        }
      case None =>
        errorSnapshot(
          Option(sparkUI.appId),
          SPARK_VERSION,
          System.currentTimeMillis(),
          "live_spark_context_missing",
          "Thread dump is only available in a live Spark UI.")
    }
  }

  def threadDumpSnapshot(
      appId: Option[String],
      sparkVersion: String,
      executorId: String,
      threadDump: Array[ThreadStackTrace],
      generatedAtMs: Long,
      flamegraphEnabled: Boolean): JValue = {
    val groups = topStackGroups(threadDump)
    extend(
      base(appId, sparkVersion, generatedAtMs),
      obj(
        "executor_id" -> JString(executorId),
        "diagnosis" -> diagnoseThreadDump(executorId, groups, threadDump.length),
        "facts" -> obj(
          "thread_count" -> jint(threadDump.length),
          "state_counts" -> stringIntMap(threadDump.groupBy(_.threadState.toString).mapValues(_.length)),
          "top_frames" -> topFrames(threadDump),
          "top_stacks" -> JArray(groups.map(stackGroupJson).toList),
          "flamegraph_enabled" -> JBool(flamegraphEnabled),
          "flamegraph" -> flamegraphJson(threadDump, flamegraphEnabled))))
  }

  def profiler(sparkUI: SparkUI): JValue = {
    val conf = sparkUI.conf
    val appId = Option(sparkUI.appId)
    val attemptId = conf.getOption("spark.app.attempt.id")
    val outputDirOption = PaimonProfilerConf.dfsDir(conf)
      .map(PaimonProfilerConf.appAttemptDir(_, appId.getOrElse("<application-id>"), attemptId))
    val artifacts = outputDirOption match {
      case Some(outputDir) => PaimonProfilerArtifacts.list(conf, outputDir)
      case None => Right(Nil)
    }
    profilerSnapshot(
      conf,
      appId,
      attemptId,
      outputDirOption,
      artifacts,
      System.currentTimeMillis())
  }

  def profilerSnapshot(
      conf: org.apache.spark.SparkConf,
      appId: Option[String],
      attemptId: Option[String],
      outputDirOption: Option[String],
      artifactsResult: Either[String, Seq[PaimonProfilerArtifacts.Artifact]],
      generatedAtMs: Long): JValue = {
    val sparkVersion = SPARK_VERSION
    val suffix = PaimonProfilerConf.outputSuffix(conf)
    val artifacts = artifactsResult.right.getOrElse(Nil)
    extend(
      base(appId, sparkVersion, generatedAtMs),
      obj(
        "attempt_id" -> stringOrNull(attemptId),
        "diagnosis" -> diagnoseProfiler(outputDirOption, artifactsResult),
        "facts" -> obj(
          "config" -> profilerConfigJson(conf, outputDirOption, suffix),
          "artifact_count" -> jint(artifacts.size),
          "artifacts" -> JArray(artifacts.map(artifactJson).toList),
          "list_error" -> artifactsResult.left.toOption.map(JString).getOrElse(JNull),
          "warnings" -> JArray(profilerWarnings(outputDirOption, artifactsResult).map(JString).toList))))
  }

  private def base(appId: Option[String], sparkVersion: String, generatedAtMs: Long): JObject = {
    obj(
      "schema_version" -> jint(SchemaVersion),
      "source" -> JString(Source),
      "generated_at_ms" -> jint(generatedAtMs),
      "app_id" -> stringOrNull(appId),
      "spark_version" -> JString(sparkVersion))
  }

  private def errorSnapshot(
      appId: Option[String],
      sparkVersion: String,
      generatedAtMs: Long,
      category: String,
      message: String): JValue = {
    extend(
      base(appId, sparkVersion, generatedAtMs),
      obj(
        "diagnosis" -> diagnosis(category, "warning", 1.0D, message, Seq(message), Nil),
        "facts" -> obj("error" -> JString(message))))
  }

  private def executorJson(executor: ExecutorSummary): JValue = {
    obj(
      "id" -> JString(executor.id),
      "host_port" -> JString(executor.hostPort),
      "active_tasks" -> jint(executor.activeTasks),
      "total_tasks" -> jint(executor.totalTasks),
      "failed_tasks" -> jint(executor.failedTasks),
      "total_duration_ms" -> jint(executor.totalDuration),
      "total_gc_time_ms" -> jint(executor.totalGCTime),
      "thread_dump_json_url" -> JString(
        "/paimon-diagnostics/threadDump/json?executorId=" + encode(executor.id)))
  }

  private def executorSortKey(executor: ExecutorSummary): (Int, String) = {
    if (executor.id == "driver") {
      (0, executor.id)
    } else {
      (1, executor.id)
    }
  }

  private def topFrames(threadDump: Array[ThreadStackTrace]): JArray = {
    val counts = new mutable.HashMap[String, Int]()
    threadDump.foreach { thread =>
      normalizedFrames(thread).headOption.foreach { frame =>
        counts.put(frame, counts.getOrElse(frame, 0) + 1)
      }
    }
    JArray(
      counts.toSeq
        .sortBy { case (frame, count) => (-count, frame) }
        .take(MaxTopStacks)
        .map { case (frame, count) =>
          obj("frame" -> JString(frame), "count" -> jint(count))
        }
        .toList)
  }

  private def topStackGroups(threadDump: Array[ThreadStackTrace]): Seq[StackGroup] = {
    val byStack = new mutable.LinkedHashMap[String, mutable.ArrayBuffer[ThreadStackTrace]]()
    threadDump.foreach { thread =>
      val frames = normalizedFrames(thread)
      val signature = frames.mkString("\n")
      val threads = byStack.getOrElseUpdate(signature, mutable.ArrayBuffer.empty[ThreadStackTrace])
      threads += thread
    }
    byStack.toSeq.map {
      case (_, threads) =>
        val sample = threads.head
        val frames = normalizedFrames(sample)
        StackGroup(
          count = threads.size,
          topFrame = frames.headOption.getOrElse("<empty>"),
          sampleThreadNames = threads.map(_.threadName).distinct.take(MaxSampleThreadNames),
          threadStates = threads.groupBy(_.threadState.toString).mapValues(_.size).toMap,
          stackTrace = frames.take(MaxFramesPerStack),
          tags = tags(sample.threadName, frames))
    }.sortBy(group => (-group.count, -group.tags.size, group.topFrame)).take(MaxTopStacks)
  }

  private def stackGroupJson(group: StackGroup): JValue = {
    obj(
      "count" -> jint(group.count),
      "top_frame" -> JString(group.topFrame),
      "sample_thread_names" -> JArray(group.sampleThreadNames.map(JString).toList),
      "thread_states" -> stringIntMap(group.threadStates),
      "tags" -> JArray(group.tags.map(JString).toList),
      "stack_trace" -> JArray(group.stackTrace.map(JString).toList))
  }

  private def normalizedFrames(thread: ThreadStackTrace): Seq[String] = {
    thread.stackTrace.elems.map { frame =>
      frame.split("\n").headOption.getOrElse(frame).trim
    }.filter(_.nonEmpty)
  }

  private def tags(threadName: String, frames: Seq[String]): Seq[String] = {
    val text = (threadName + "\n" + frames.mkString("\n")).toLowerCase(Locale.ROOT)
    val result = mutable.ArrayBuffer[String]()
    addTag(result, text.contains("executor task launch worker"), "executor_task")
    addTag(result, text.contains("org.apache.paimon"), "paimon_io")
    addTag(result, text.contains("org.apache.parquet"), "parquet_io")
    addTag(result, text.contains("waitforsparkjob") || text.contains("sparkcontext.runjob"),
      "driver_waiting_for_spark_job")
    addTag(result, text.contains("schema") && text.contains("validation"), "paimon_schema_validation")
    addTag(result, text.contains("inferfiltersfromconstraints"), "spark_constraint_inference")
    addTag(result, text.contains("collapseproject"), "spark_collapse_project")
    addTag(result, text.contains("prunefilesourcepartitions"), "spark_file_pruning")
    addTag(result, text.contains("v2scanrelationpushdown"), "spark_v2_pushdown")
    addTag(result, text.contains("queryexecution") || text.contains("sql.execution"),
      "spark_sql")
    addTag(result, text.contains("codegen") || text.contains("generatediterator"), "spark_codegen")
    addTag(result, text.contains("unsafeprojection") || text.contains("projection"), "spark_projection")
    addTag(result, text.contains("shuffle"), "shuffle")
    addTag(result, text.contains("linkedblockingqueue.take") || text.contains("unsafe.park"),
      "idle_or_waiting")
    result.distinct
  }

  private def diagnoseThreadDump(
      executorId: String,
      groups: Seq[StackGroup],
      threadCount: Int): JValue = {
    val allTags = groups.flatMap(_.tags).toSet
    if (allTags.contains("driver_waiting_for_spark_job")) {
      diagnosis(
        "driver_waiting_for_spark_job",
        "info",
        0.85D,
        "Driver thread dump shows Spark job waiting or synchronous execution frames.",
        groups.flatMap(_.tags).distinct.take(6),
        Seq(
          "Inspect SQL/stage progress and executor thread dumps for the blocked work.",
          "Compare this stack with async-profiler wall-clock artifacts."))
    } else if (allTags.contains("paimon_io") || allTags.contains("parquet_io")) {
      diagnosis(
        "executor_paimon_or_parquet_io",
        "info",
        0.85D,
        "Executor stacks include Paimon or Parquet read/write frames.",
        groups.flatMap(_.tags).distinct.take(6),
        Seq(
          "Open profiler artifacts for CPU or wall-clock samples from the same executor.",
          "Check whether runnable task threads stay in Paimon, Parquet, or object-store calls."))
    } else if (
      allTags.exists(tag => tag.startsWith("spark_")) && executorId == "driver") {
      diagnosis(
        "driver_spark_sql_planning",
        "info",
        0.75D,
        "Driver stacks include Spark SQL planning or optimizer frames.",
        groups.flatMap(_.tags).distinct.take(6),
        Seq("Compare optimizer rule exclusions with the stack tags and SQL plan."))
    } else {
      diagnosis(
        "thread_dump_collected",
        "info",
        0.5D,
        "Thread dump was collected and aggregated.",
        Seq("thread_count=" + threadCount),
        Seq("Inspect top_stacks and state_counts for runnable or blocked thread clusters."))
    }
  }

  private def flamegraphJson(threadDump: Array[ThreadStackTrace], enabled: Boolean): JValue = {
    if (!enabled) {
      JNull
    } else {
      try {
        parse(PaimonFlamegraphNode.fromThreadDump(threadDump).toJsonString)
      } catch {
        case NonFatal(e) =>
          obj("error" -> JString(Option(e.getMessage).getOrElse(e.getClass.getName)))
      }
    }
  }

  private def profilerConfigJson(
      conf: org.apache.spark.SparkConf,
      outputDirOption: Option[String],
      suffix: String): JValue = {
    obj(
      "plugin_class" -> JString(PaimonProfilerConf.PluginClass),
      "spark_plugins_contains_plugin" -> JBool(pluginConfigured(conf)),
      "driver_profiling" -> JBool(PaimonProfilerConf.driverEnabled(conf)),
      "executor_profiling" -> JBool(PaimonProfilerConf.executorEnabled(conf)),
      "executor_fraction" -> JDouble(PaimonProfilerConf.executorFraction(conf)),
      "local_directory" -> JString(PaimonProfilerConf.localDir(conf)),
      "dfs_directory" -> stringOrNull(outputDirOption),
      "output_suffix" -> JString(suffix),
      "async_profiler_args" -> JString(PaimonProfilerConf.asyncProfilerArgs(conf)),
      "dfs_write_interval_seconds" -> jint(PaimonProfilerConf.dfsWriteIntervalSeconds(conf)),
      "output_file_pattern" -> JString(
        PaimonProfilerConf.profileFile("driver", suffix) +
          " / profile-exec-<executorId>." + suffix))
  }

  private def artifactJson(artifact: PaimonProfilerArtifacts.Artifact): JValue = {
    obj(
      "name" -> JString(artifact.name),
      "path" -> JString(artifact.path),
      "format" -> JString(artifact.format),
      "size_bytes" -> jint(artifact.length),
      "updated_at_ms" -> jint(artifact.modificationTime),
      "executor_id" -> JString(executorIdFromProfile(artifact.name)),
      "open_url" -> JString(
        "/paimon-diagnostics/profiler/?profile=" + encode(artifact.name)))
  }

  private def diagnoseProfiler(
      outputDirOption: Option[String],
      artifactsResult: Either[String, Seq[PaimonProfilerArtifacts.Artifact]]): JValue = {
    outputDirOption match {
      case None =>
        diagnosis(
          "profiler_dfs_dir_missing",
          "warning",
          1.0D,
          "Async-profiler DFS output directory is not configured.",
          Seq(PaimonProfilerConf.DfsDirKey + " is empty"),
          Seq("Set " + PaimonProfilerConf.DfsDirKey + " to a shared filesystem path."))
      case Some(_) =>
        artifactsResult match {
          case Left(error) =>
            diagnosis(
              "profiler_artifact_list_failed",
              "warning",
              1.0D,
              "Failed to list async-profiler output files.",
              Seq(error),
              Seq("Check filesystem credentials and the configured DFS directory."))
          case Right(artifacts) if artifacts.nonEmpty =>
            diagnosis(
              "profiler_artifacts_available",
              "info",
              1.0D,
              "Async-profiler output files are available.",
              Seq("artifact_count=" + artifacts.size),
              Seq("Open HTML artifacts directly or fetch this JSON from spark-cli for triage."))
          case Right(_) =>
            diagnosis(
              "profiler_waiting_for_artifacts",
              "info",
              0.8D,
              "No async-profiler output files are available yet.",
              Seq("artifact_count=0"),
              Seq("Run tasks long enough for sampled executors to upload profiler outputs."))
        }
    }
  }

  private def profilerWarnings(
      outputDirOption: Option[String],
      artifactsResult: Either[String, Seq[PaimonProfilerArtifacts.Artifact]]): Seq[String] = {
    val warnings = mutable.ArrayBuffer[String]()
    if (outputDirOption.isEmpty) {
      warnings += "No DFS directory is configured."
    }
    artifactsResult.left.foreach(warnings += _)
    warnings
  }

  private def pluginConfigured(conf: org.apache.spark.SparkConf): Boolean = {
    conf.get("spark.plugins", "")
      .split(",")
      .map(_.trim)
      .contains(PaimonProfilerConf.PluginClass)
  }

  private def executorIdFromProfile(profileName: String): String = {
    val lower = profileName.toLowerCase(Locale.ROOT)
    if (lower.startsWith("profile-driver.")) {
      "driver"
    } else if (lower.startsWith("profile-exec-")) {
      val afterPrefix = profileName.substring("profile-exec-".length)
      val dot = afterPrefix.lastIndexOf('.')
      if (dot >= 0) {
        afterPrefix.substring(0, dot)
      } else {
        afterPrefix
      }
    } else {
      ""
    }
  }

  private def diagnosis(
      category: String,
      severity: String,
      confidence: Double,
      summary: String,
      evidence: Seq[String],
      nextActions: Seq[String]): JValue = {
    obj(
      "category" -> JString(category),
      "severity" -> JString(severity),
      "confidence" -> JDouble(confidence),
      "summary" -> JString(summary),
      "evidence" -> JArray(evidence.map(JString).toList),
      "next_actions" -> JArray(nextActions.map(JString).toList))
  }

  private def stringIntMap(values: Map[String, Int]): JValue = {
    JObject(values.toSeq.sortBy(_._1).map { case (key, value) => key -> jint(value) }.toList)
  }

  private def stringOrNull(value: Option[String]): JValue = {
    value.map(JString).getOrElse(JNull)
  }

  private def jint(value: Long): JInt = {
    JInt(BigInt(value))
  }

  private def addTag(tags: mutable.ArrayBuffer[String], condition: Boolean, tag: String): Unit = {
    if (condition) {
      tags += tag
    }
  }

  private def encode(value: String): String = {
    URLEncoder.encode(value, "UTF-8")
  }

  private def obj(fields: (String, JValue)*): JObject = {
    JObject(fields.toList)
  }

  private def extend(base: JObject, rest: JObject): JObject = {
    JObject(base.obj ++ rest.obj)
  }

  private case class StackGroup(
      count: Int,
      topFrame: String,
      sampleThreadNames: Seq[String],
      threadStates: Map[String, Int],
      stackTrace: Seq[String],
      tags: Seq[String])
}
