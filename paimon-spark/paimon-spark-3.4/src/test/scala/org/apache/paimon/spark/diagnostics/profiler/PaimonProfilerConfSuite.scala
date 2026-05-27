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

import org.apache.spark.SparkConf
import org.scalatest.FunSuite

class PaimonProfilerConfSuite extends FunSuite {

  test("uses safe defaults when profiler is not configured") {
    val conf = new SparkConf(false)

    assert(!PaimonProfilerConf.driverEnabled(conf))
    assert(!PaimonProfilerConf.executorEnabled(conf))
    assert(PaimonProfilerConf.executorFraction(conf) == 0.1D)
    assert(PaimonProfilerConf.localDir(conf) == ".")
    assert(PaimonProfilerConf.outputSuffix(conf) == "html")
    assert(PaimonProfilerConf.asyncProfilerArgs(conf).contains("event=wall"))
  }

  test("builds stable profile file names for driver and executors") {
    assert(PaimonProfilerConf.profileFile("driver", "jfr") == "profile-driver.jfr")
    assert(PaimonProfilerConf.profileFile("5", "html") == "profile-exec-5.html")
  }

  test("builds application attempt output directory") {
    assert(
      PaimonProfilerConf.appAttemptDir("hdfs:///profiles", "application_1", None) ==
        "hdfs:///profiles/application_1")
    assert(
      PaimonProfilerConf.appAttemptDir("hdfs:///profiles", "application_1", Some("attempt_2")) ==
        "hdfs:///profiles/application_1_attempt_2")
  }
}
