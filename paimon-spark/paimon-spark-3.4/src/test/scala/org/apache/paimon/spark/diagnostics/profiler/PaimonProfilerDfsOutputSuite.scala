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

import org.apache.hadoop.conf.Configuration
import org.scalatest.FunSuite

import java.nio.charset.StandardCharsets
import java.nio.file.Files

class PaimonProfilerDfsOutputSuite extends FunSuite {

  test("copies complete html profiler snapshot to DFS output path") {
    val localFile = Files.createTempFile("paimon-profiler", ".html")
    val outputDir = Files.createTempDirectory("paimon-profiler-output")
    val outputFile = outputDir.resolve("profile-exec-1.html")
    val html = "<html><body>flame graph</body></html>"
    Files.write(localFile, html.getBytes(StandardCharsets.UTF_8))

    val copied =
      PaimonProfilerDfsOutput.copySnapshot(
        localFile.toString,
        outputFile.toUri.toString,
        new Configuration())

    assert(copied)
    assert(new String(Files.readAllBytes(outputFile), StandardCharsets.UTF_8) == html)
  }

  test("skips missing profiler snapshot without creating DFS output") {
    val localFile = Files.createTempDirectory("paimon-profiler-missing").resolve("missing.html")
    val outputDir = Files.createTempDirectory("paimon-profiler-output-missing")
    val outputFile = outputDir.resolve("profile-exec-1.html")

    val copied =
      PaimonProfilerDfsOutput.copySnapshot(
        localFile.toString,
        outputFile.toUri.toString,
        new Configuration())

    assert(!copied)
    assert(!Files.exists(outputFile))
  }
}
