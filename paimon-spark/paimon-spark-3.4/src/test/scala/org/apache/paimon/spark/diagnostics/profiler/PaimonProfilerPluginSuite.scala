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

import com.codahale.metrics.MetricRegistry

import org.apache.spark.SparkConf
import org.apache.spark.api.plugin.PluginContext
import org.apache.spark.resource.ResourceInformation
import org.apache.spark.sql.SparkSession
import org.scalatest.FunSuite

import java.util.{Collections, Map => JMap}

class PaimonProfilerPluginSuite extends FunSuite {

  test("driver plugin installs diagnostics tab without waiting for SQL extension rules") {
    val spark =
      SparkSession
        .builder()
        .master("local[1]")
        .appName("paimon-profiler-plugin-ui")
        .config("spark.ui.enabled", "true")
        .config("spark.ui.port", "0")
        .getOrCreate()

    try {
      new PaimonProfilerPlugin()
        .driverPlugin()
        .init(spark.sparkContext, new TestPluginContext(spark.sparkContext.getConf))

      assert(tabPrefixes(spark).contains("paimon-diagnostics"))
    } finally {
      spark.stop()
      SparkSession.clearActiveSession()
      SparkSession.clearDefaultSession()
    }
  }

  private def tabPrefixes(spark: SparkSession): Seq[String] = {
    val uiMethod = spark.sparkContext.getClass.getMethod("ui")
    val ui = uiMethod.invoke(spark.sparkContext).asInstanceOf[Option[Any]]
    ui.map { sparkUI =>
      val getTabs = sparkUI.getClass.getMethod("getTabs")
      getTabs.invoke(sparkUI).asInstanceOf[Seq[Any]].map { tab =>
        tab.getClass.getMethod("prefix").invoke(tab).asInstanceOf[String]
      }
    }.getOrElse(Seq.empty)
  }

  private class TestPluginContext(confValue: SparkConf) extends PluginContext {

    override def metricRegistry(): MetricRegistry = new MetricRegistry()

    override def conf(): SparkConf = confValue

    override def executorID(): String = "driver"

    override def hostname(): String = "localhost"

    override def resources(): JMap[String, ResourceInformation] = {
      Collections.emptyMap[String, ResourceInformation]()
    }

    override def send(message: Any): Unit = {}

    override def ask(message: Any): AnyRef = null
  }
}
