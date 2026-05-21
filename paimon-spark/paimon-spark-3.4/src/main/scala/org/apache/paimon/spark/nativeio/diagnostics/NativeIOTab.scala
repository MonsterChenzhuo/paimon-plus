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

import org.apache.paimon.spark.nativeio.diagnostics.NativeIOStore

class NativeIOTab(parent: SparkUI, store: NativeIOStore) extends SparkUITab(parent, "native-io") {
  attachPage(new NativeIOPage(this, store, "summary", "Summary"))
  attachPage(new NativeIOPage(this, store, "sql", "Per SQL"))
  attachPage(new NativeIOPage(this, store, "stage", "Per Stage"))
  attachPage(new NativeIOPage(this, store, "task", "Per Task"))
  attachPage(new NativeIOPage(this, store, "file", "Per File"))
  attachPage(new NativeIOPage(this, store, "timeline", "Timeline"))
  attachPage(new NativeIOPage(this, store, "slow", "Slow Operations"))
  attachPage(new NativeIOPage(this, store, "stuck", "Stuck Operations"))
}
