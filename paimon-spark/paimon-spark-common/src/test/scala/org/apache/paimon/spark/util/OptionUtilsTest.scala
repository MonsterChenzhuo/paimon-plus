/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.paimon.spark.util

import org.apache.paimon.CoreOptions
import org.apache.paimon.fs.FileIO
import org.apache.paimon.table.{ReadonlyTable, Table}
import org.apache.paimon.table.source.{InnerTableRead, InnerTableScan}
import org.apache.paimon.types.{DataTypes, RowType}

import org.apache.spark.sql.internal.SQLConf
import org.scalatest.FunSuite

import java.util.{Collections, HashMap => JHashMap, List => JList, Map => JMap}

class OptionUtilsTest extends FunSuite {

  test("native IO read options include spark marker and OBS Hadoop fallback") {
    val sqlConf = new SQLConf
    SQLConf.withExistingConf(sqlConf) {
      sqlConf.setConfString("spark.hadoop.fs.obs.endpoint", "obs-endpoint")
      sqlConf.setConfString("spark.hadoop.fs.obs.access.key", "obs-ak")

      val readOptions = OptionUtils.withNativeIOReadOptions(new JHashMap[String, String]())

      assert(readOptions.get(CoreOptions.NATIVE_IO_INTERNAL_ENGINE.key()) == "spark")
      assert(readOptions.get("fs.obs.endpoint") == "obs-endpoint")
      assert(readOptions.get("fs.obs.access.key") == "obs-ak")
    }
  }

  test("explicit OBS options override Spark Hadoop OBS fallback") {
    val sqlConf = new SQLConf
    SQLConf.withExistingConf(sqlConf) {
      sqlConf.setConfString("spark.hadoop.fs.obs.endpoint", "cluster-endpoint")

      val explicitOptions = new JHashMap[String, String]()
      explicitOptions.put("fs.obs.endpoint", "explicit-endpoint")

      val readOptions = OptionUtils.withNativeIOReadOptions(explicitOptions)

      assert(readOptions.get("fs.obs.endpoint") == "explicit-endpoint")
    }
  }

  test("catalog table copy includes table-specific native SQL conf") {
    val sqlConf = new SQLConf
    SQLConf.withExistingConf(sqlConf) {
      sqlConf.setConfString("spark.paimon.my_catalog.my_db.my_table.native-io.enabled", "true")

      val copied = OptionUtils.copyWithSQLConf(
        new RecordingTable(Collections.emptyMap()),
        "my_catalog",
        "my_db",
        "my_table",
        new JHashMap[String, String]())

      assert(copied.options().get("native-io.enabled") == "true")
      assert(copied.options().get(CoreOptions.NATIVE_IO_INTERNAL_ENGINE.key()) == "spark")
    }
  }

  private class RecordingTable(private val tableOptions: JMap[String, String])
    extends ReadonlyTable {

    override def name(): String = "my_table"

    override def rowType(): RowType = RowType.of(DataTypes.INT())

    override def primaryKeys(): JList[String] = Collections.emptyList()

    override def options(): JMap[String, String] = tableOptions

    override def fileIO(): FileIO = null

    override def newRead(): InnerTableRead = null

    override def newScan(): InnerTableScan = null

    override def copy(dynamicOptions: JMap[String, String]): Table = {
      val copiedOptions = new JHashMap[String, String](tableOptions)
      copiedOptions.putAll(dynamicOptions)
      new RecordingTable(copiedOptions)
    }
  }
}
