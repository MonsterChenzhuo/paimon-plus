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

package org.apache.spark.status

import org.apache.paimon.operation.nativeio.diagnostics.NativeIOEventJson
import org.apache.paimon.spark.nativeio.diagnostics.{
  NativeIOStore
}
import org.apache.spark.SparkConf
import org.apache.spark.scheduler.{NativeIOHistoryListener, SparkListener}
import org.apache.spark.ui.{NativeIOTabSupport, SparkUI}

class NativeIOHistoryServerPlugin extends AppHistoryServerPlugin {

  override def createListeners(conf: SparkConf, store: ElementTrackingStore): Seq[SparkListener] = {
    Seq(new NativeIOHistoryListener(store))
  }

  override def setupUI(ui: SparkUI): Unit = {
    val store = new NativeIOStore(MaxEvents, StuckThresholdMs)
    val kvStore = ui.store.store
    val events =
      KVUtils.viewToSeq(kvStore.view(classOf[NativeIOEventData]).index("eventTime"))

    events.foreach { data =>
      store.record(NativeIOEventJson.fromJson(data.eventJson))
    }

    if (events.nonEmpty) {
      NativeIOTabSupport.attach(ui, store)
    }
  }

  override def displayOrder: Int = 1

  private val MaxEvents = 10000
  private val StuckThresholdMs = 30000L
}
