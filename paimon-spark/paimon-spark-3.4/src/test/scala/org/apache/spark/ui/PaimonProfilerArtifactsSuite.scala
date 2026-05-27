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

import org.apache.spark.SparkConf
import org.scalatest.FunSuite

import java.nio.charset.StandardCharsets
import java.nio.file.Files

class PaimonProfilerArtifactsSuite extends FunSuite {

  test("lists supported profiler artifacts from DFS output directory") {
    val outputDir = Files.createTempDirectory("paimon-profiler-artifacts")
    Files.write(outputDir.resolve("profile-driver.jfr"), Array[Byte](1, 2, 3))
    Files.write(outputDir.resolve("profile-exec-2.html"), "<html>profile</html>".getBytes(StandardCharsets.UTF_8))
    Files.write(outputDir.resolve("profile-exec-3.txt"), Array[Byte](1))

    val artifacts = PaimonProfilerArtifacts.list(new SparkConf(false), outputDir.toUri.toString).right.get

    assert(artifacts.map(_.name) == Seq("profile-driver.jfr", "profile-exec-2.html"))
    assert(artifacts.find(_.name == "profile-driver.jfr").exists(_.format == "jfr"))
    assert(artifacts.find(_.name == "profile-exec-2.html").exists(_.format == "html"))
  }

  test("resolves only safe profiler artifact names") {
    val outputDir = Files.createTempDirectory("paimon-profiler-safe")

    assert(PaimonProfilerArtifacts.resolve(outputDir.toUri.toString, "profile-driver.jfr").isDefined)
    assert(PaimonProfilerArtifacts.resolve(outputDir.toUri.toString, "../profile-driver.jfr").isEmpty)
    assert(PaimonProfilerArtifacts.resolve(outputDir.toUri.toString, "profile-driver.txt").isEmpty)
    assert(PaimonProfilerArtifacts.resolve(outputDir.toUri.toString, "not-profile-driver.jfr").isEmpty)
  }

  test("loads html profiler artifact for embedding") {
    val outputDir = Files.createTempDirectory("paimon-profiler-html")
    val html = "<html><body>flame graph</body></html>"
    val file = outputDir.resolve("profile-exec-1.html")
    Files.write(file, html.getBytes(StandardCharsets.UTF_8))

    val loaded =
      PaimonProfilerArtifacts
        .loadFlameGraphHtml(new SparkConf(false), file.toUri.toString)
        .right
        .get

    assert(loaded == html)
  }

  test("returns guidance instead of throwing when jfr conversion is unavailable on java 8") {
    val outputDir = Files.createTempDirectory("paimon-profiler-jfr-java8")
    val file = outputDir.resolve("profile-exec-1.jfr")
    Files.write(file, Array[Byte](1, 2, 3))

    val loaded =
      PaimonProfilerArtifacts
        .loadFlameGraphHtml(new SparkConf(false), file.toUri.toString, "1.8")

    assert(loaded.isLeft)
    assert(loaded.left.get.contains("Java 9+"))
    assert(loaded.left.get.contains("spark.paimon.profiler.outputSuffix=html"))
  }
}
