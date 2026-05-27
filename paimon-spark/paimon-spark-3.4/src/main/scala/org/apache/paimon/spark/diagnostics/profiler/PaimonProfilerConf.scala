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

package org.apache.paimon.spark.diagnostics.profiler

import org.apache.spark.SparkConf

object PaimonProfilerConf {

  val PluginClass = "org.apache.paimon.spark.diagnostics.profiler.PaimonProfilerPlugin"

  val DriverEnabledKey = "spark.paimon.profiler.driver.enabled"
  val ExecutorEnabledKey = "spark.paimon.profiler.executor.enabled"
  val ExecutorFractionKey = "spark.paimon.profiler.executor.fraction"
  val DfsDirKey = "spark.paimon.profiler.dfsDir"
  val LocalDirKey = "spark.paimon.profiler.localDir"
  val AsyncProfilerArgsKey = "spark.paimon.profiler.asyncProfiler.args"
  val DfsWriteIntervalKey = "spark.paimon.profiler.dfsWriteInterval"
  val OutputSuffixKey = "spark.paimon.profiler.outputSuffix"

  private val DefaultAsyncProfilerArgs = "event=wall,interval=10ms,alloc=2m,lock=10ms,chunktime=300s"
  private val DefaultOutputSuffix = "html"

  def driverEnabled(conf: SparkConf): Boolean = {
    conf.getBoolean(DriverEnabledKey, defaultValue = false)
  }

  def executorEnabled(conf: SparkConf): Boolean = {
    conf.getBoolean(ExecutorEnabledKey, defaultValue = false)
  }

  def executorFraction(conf: SparkConf): Double = {
    val configured = conf.getDouble(ExecutorFractionKey, defaultValue = 0.1D)
    math.max(0.0D, math.min(1.0D, configured))
  }

  def dfsDir(conf: SparkConf): Option[String] = {
    conf.getOption(DfsDirKey).map(_.stripSuffix("/")).filter(_.nonEmpty)
  }

  def localDir(conf: SparkConf): String = {
    conf.get(LocalDirKey, ".")
  }

  def asyncProfilerArgs(conf: SparkConf): String = {
    conf.get(AsyncProfilerArgsKey, DefaultAsyncProfilerArgs)
  }

  def dfsWriteIntervalSeconds(conf: SparkConf): Long = {
    math.max(1L, conf.getTimeAsSeconds(DfsWriteIntervalKey, "30s"))
  }

  def outputSuffix(conf: SparkConf): String = {
    val suffix = conf.get(OutputSuffixKey, DefaultOutputSuffix).trim.stripPrefix(".")
    if (suffix.isEmpty) {
      DefaultOutputSuffix
    } else {
      suffix
    }
  }

  def profileFile(executorId: String, suffix: String): String = {
    if (executorId == "driver") {
      "profile-driver." + suffix
    } else {
      "profile-exec-" + executorId + "." + suffix
    }
  }

  def appAttemptDir(baseDir: String, appId: String, attemptId: Option[String]): String = {
    baseDir.stripSuffix("/") + "/" + appId + attemptId.map("_" + _).getOrElse("")
  }
}
