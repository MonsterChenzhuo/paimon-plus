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

package org.apache.paimon.spark.procedure

import org.apache.paimon.spark.PaimonSparkTestBase

import org.apache.spark.sql.Row
import org.assertj.core.api.Assertions.assertThat

import java.io.File
import java.util.concurrent.ThreadLocalRandom

class NativeIOBenchmarkProcedureTest extends PaimonSparkTestBase {

  test("Paimon native IO benchmark procedure: synthetic table writes reports") {
    val random = ThreadLocalRandom.current().nextInt(100000)
    val tableName = s"native_io_bench_$random"

    withTempDir {
      warehouseDir =>
        withTempDir {
          resultDir =>
            val warehouse = warehouseDir.getAbsolutePath.replace("\\", "/")
            val resultPath = resultDir.getAbsolutePath.replace("\\", "/")

            val result = spark.sql(
              s"""
                 |CALL sys.native_io_benchmark(
                 |  warehouse => '$warehouse',
                 |  result_path => '$resultPath',
                 |  table_name => '$tableName',
                 |  rows => 200L,
                 |  repeat => 1,
                 |  warmup => 0,
                 |  dv_delete_ratio => 0.10D,
                 |  target_file_size => '1 mb',
                 |  fail_if_native_unavailable => false)
                 |""".stripMargin).collect().toSeq

            assert(result == Seq(Row(true, resultPath, "ok")))
            assertThat(new File(resultDir, "env.json")).exists()
            assertThat(new File(resultDir, "dataset.json")).exists()
            assertThat(new File(resultDir, "result.json")).exists()
            assertThat(new File(resultDir, "result.csv")).exists()
            assertThat(new File(resultDir, "summary.json")).exists()
            assertThat(new File(resultDir, "duration_bar.svg")).exists()
            assertThat(new File(resultDir, "speedup_bar.svg")).exists()
            assertThat(new File(resultDir, "summary.html")).exists()
        }
    }
  }
}
