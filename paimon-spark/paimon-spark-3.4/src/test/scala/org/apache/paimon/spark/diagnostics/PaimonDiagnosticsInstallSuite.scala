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

import org.apache.paimon.spark.extensions.PaimonSparkSessionExtensions

import org.apache.spark.sql.SparkSession
import org.scalatest.FunSuite

class PaimonDiagnosticsInstallSuite extends FunSuite {

  test("installs Paimon diagnostics tab when native io is disabled") {
    val spark =
      SparkSession
        .builder()
        .master("local[1]")
        .appName("paimon-diagnostics-install")
        .config("spark.ui.enabled", "true")
        .config("spark.ui.port", "0")
        .config("spark.sql.extensions", classOf[PaimonSparkSessionExtensions].getName)
        .config("spark.paimon.native-io.enabled", "false")
        .config("spark.paimon.diagnostics.ui.enabled", "true")
        .getOrCreate()

    try {
      spark.sql("SELECT 1").collect()

      assert(tabPrefixes(spark).contains("paimon-diagnostics"))
      assert(!tabPrefixes(spark).contains("native-io"))
    } finally {
      spark.stop()
      SparkSession.clearActiveSession()
      SparkSession.clearDefaultSession()
    }
  }

  test("does not install Paimon diagnostics tab when disabled") {
    val spark =
      SparkSession
        .builder()
        .master("local[1]")
        .appName("paimon-diagnostics-disabled")
        .config("spark.ui.enabled", "true")
        .config("spark.ui.port", "0")
        .config("spark.sql.extensions", classOf[PaimonSparkSessionExtensions].getName)
        .config("spark.paimon.diagnostics.ui.enabled", "false")
        .getOrCreate()

    try {
      spark.sql("SELECT 1").collect()

      assert(!tabPrefixes(spark).contains("paimon-diagnostics"))
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
}
