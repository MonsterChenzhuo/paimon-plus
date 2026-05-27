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

import org.apache.paimon.spark.diagnostics.PaimonDiagnostics

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.api.plugin.{DriverPlugin, ExecutorPlugin, PluginContext, SparkPlugin}
import org.apache.spark.internal.Logging
import org.apache.spark.ui.PaimonDiagnosticsTabSupport

import java.util.{Collections, Map => JMap}

import scala.util.Random
import scala.util.control.NonFatal

class PaimonProfilerPlugin extends SparkPlugin {

  override def driverPlugin(): DriverPlugin = new PaimonProfilerDriverPlugin

  override def executorPlugin(): ExecutorPlugin = new PaimonProfilerExecutorPlugin
}

private class PaimonProfilerDriverPlugin extends DriverPlugin with Logging {

  private var profiler: PaimonSparkAsyncProfiler = _

  override def init(sc: SparkContext, ctx: PluginContext): JMap[String, String] = {
    val conf = ctx.conf()
    if (conf.getBoolean(PaimonDiagnostics.UiEnabledKey, defaultValue = true)) {
      installDiagnosticsTab(sc)
    }
    if (PaimonProfilerConf.driverEnabled(conf)) {
      profiler = new PaimonSparkAsyncProfiler(conf, "driver")
      profiler.start()
    }
    Collections.emptyMap[String, String]()
  }

  override def shutdown(): Unit = {
    if (profiler != null) {
      profiler.stop()
    }
  }

  private def installDiagnosticsTab(sc: SparkContext): Unit = {
    try {
      val uiMethod = sc.getClass.getMethod("ui")
      val ui = uiMethod.invoke(sc).asInstanceOf[Option[Any]]
      ui.foreach(PaimonDiagnosticsTabSupport.attach)
    } catch {
      case NonFatal(e) =>
        logWarning("Failed to attach Paimon diagnostics tab from profiler plugin.", e)
    }
  }
}

private class PaimonProfilerExecutorPlugin extends ExecutorPlugin with Logging {

  private var profiler: PaimonSparkAsyncProfiler = _
  private val random = new Random(System.currentTimeMillis())

  override def init(ctx: PluginContext, extraConf: JMap[String, String]): Unit = {
    val conf: SparkConf = ctx.conf()
    if (PaimonProfilerConf.executorEnabled(conf) &&
        random.nextDouble() < PaimonProfilerConf.executorFraction(conf)) {
      logInfo("Executor " + ctx.executorID() + " selected for Paimon async profiling.")
      profiler = new PaimonSparkAsyncProfiler(conf, ctx.executorID())
      profiler.start()
    }
  }

  override def shutdown(): Unit = {
    if (profiler != null) {
      profiler.stop()
    }
  }
}
