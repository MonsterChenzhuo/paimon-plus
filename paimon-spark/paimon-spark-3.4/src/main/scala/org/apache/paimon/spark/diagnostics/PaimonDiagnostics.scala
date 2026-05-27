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

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.ui.PaimonDiagnosticsTabSupport

object PaimonDiagnostics {

  val UiEnabledKey = "spark.paimon.diagnostics.ui.enabled"
  val ThreadDumpFlamegraphEnabledKey =
    "spark.paimon.diagnostics.thread-dump.flamegraph.enabled"

  def install(spark: SparkSession): Unit = {
    sparkUi(spark).foreach(PaimonDiagnosticsTabSupport.attach)
  }

  def flamegraphEnabled(conf: SparkConf): Boolean = {
    conf.getBoolean(ThreadDumpFlamegraphEnabledKey, defaultValue = true)
  }

  private def sparkUi(spark: SparkSession): Option[Any] = {
    val method = spark.sparkContext.getClass.getMethod("ui")
    method.invoke(spark.sparkContext).asInstanceOf[Option[Any]]
  }
}
