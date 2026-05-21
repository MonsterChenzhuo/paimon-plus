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

package org.apache.spark

import org.apache.paimon.operation.nativeio.diagnostics.NativeIOEvent
import org.apache.paimon.spark.nativeio.diagnostics.{NativeIOEventEnvelope, NativeIOStore}
import org.apache.spark.rpc.{RpcAddress, RpcEndpointRef}
import org.apache.spark.scheduler.NativeIOLifecycleListener

object NativeIORpcSupport {

  private val EndpointName = "paimon-native-io-diagnostics"

  @volatile private var driverEndpointRef: Option[RpcEndpointRef] = None
  @volatile private var listenerInstalled = false

  def setupDriver(sparkContext: SparkContext, store: NativeIOStore): Unit = synchronized {
    val env = SparkEnv.get
    if (env != null && driverEndpointRef.isEmpty) {
      driverEndpointRef = Some(env.rpcEnv.setupEndpoint(EndpointName, new NativeIOEndpoint(env.rpcEnv, store)))
    }
    if (!listenerInstalled) {
      sparkContext.addSparkListener(new NativeIOLifecycleListener(store))
      listenerInstalled = true
    }
  }

  def sendToDriver(event: NativeIOEvent): Unit = {
    try {
      resolveDriverEndpoint().foreach(_.send(NativeIOEventEnvelope(event)))
    } catch {
      case _: Throwable =>
    }
  }

  def executorId: Option[String] = {
    val env = SparkEnv.get
    if (env == null) {
      None
    } else {
      Option(env.executorId)
    }
  }

  private def resolveDriverEndpoint(): Option[RpcEndpointRef] = synchronized {
    driverEndpointRef match {
      case some @ Some(_) => some
      case None =>
        val env = SparkEnv.get
        if (env == null) {
          None
        } else {
          val host = env.conf.get("spark.driver.host", null)
          val port = env.conf.getInt("spark.driver.port", -1)
          if (host == null || port < 0) {
            None
          } else {
            driverEndpointRef =
              Some(env.rpcEnv.setupEndpointRef(RpcAddress(host, port), EndpointName))
            driverEndpointRef
          }
        }
    }
  }
}
