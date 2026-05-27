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

import org.apache.spark.{SecurityManager, SparkConf}
import org.apache.spark.status.AppStatusStore
import org.scalatest.FunSuite
import org.scalatest.concurrent.Eventually
import org.scalatest.time.{Millis, Seconds, Span}

class PaimonDiagnosticsTabSupportSuite extends FunSuite with Eventually {

  test("defers tab attachment while Spark UI startup handlers are not attached") {
    val conf =
      new SparkConf(false)
        .set("spark.app.name", "paimon-diagnostics-ui-startup")
        .set("spark.driver.host", "localhost")
        .set("spark.ui.port", "0")
    val store = AppStatusStore.createLiveStore(conf)
    val ui =
      SparkUI.create(
        None,
        store,
        conf,
        new SecurityManager(conf),
        "paimon-diagnostics-ui-startup",
        "",
        System.currentTimeMillis())
    ui.bind()

    try {
      PaimonDiagnosticsTabSupport.attach(ui)
      ui.attachAllHandler()

      eventually(timeout(Span(5, Seconds)), interval(Span(50, Millis))) {
        assert(ui.getTabs.exists(_.prefix == PaimonDiagnosticsTabSupport.TabPrefix))
      }
    } finally {
      ui.stop()
      store.close()
    }
  }
}
