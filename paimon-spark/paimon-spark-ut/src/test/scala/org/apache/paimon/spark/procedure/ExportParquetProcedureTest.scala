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
import org.apache.paimon.utils.JsonSerdeUtil

import org.apache.spark.sql.Row
import org.assertj.core.api.Assertions.{assertThat, assertThatThrownBy}

import java.io.File
import java.nio.charset.StandardCharsets
import java.nio.file.Files
import java.util.concurrent.ThreadLocalRandom

import scala.collection.JavaConverters._

class ExportParquetProcedureTest extends PaimonSparkTestBase {

  test("Paimon export parquet procedure: project columns and filter rows") {
    val random = ThreadLocalRandom.current().nextInt(100000)
    withTable(s"tbl$random") {
      sql(s"""
             |CREATE TABLE tbl$random (
             |  id INT,
             |  name STRING,
             |  score INT,
             |  dt STRING
             |)
             |PARTITIONED BY (dt)
             |""".stripMargin)

      sql(s"""
             |INSERT INTO tbl$random VALUES
             |  (1, 'a', 10, '2026-05-13'),
             |  (2, 'b', 20, '2026-05-14'),
             |  (3, 'c', 30, '2026-05-14')
             |""".stripMargin)

      withTempDir {
        dir =>
          val output = new File(dir, "export").getAbsolutePath

          checkAnswer(
            spark.sql(s"""
                         |CALL sys.export_parquet(
                         |  table => 'tbl$random',
                         |  columns => 'id,name',
                         |  output_path => '$output',
                         |  where => "dt = '2026-05-14' and score >= 20",
                         |  parallelism => 2)
                         |""".stripMargin),
            Row(true, 2L) :: Nil
          )

          assertThat(new File(output, "_SUCCESS")).exists()
          val parquetFiles = new File(output).listFiles().filter(_.getName.endsWith(".parquet"))
          assertThat(parquetFiles.length)
            .isGreaterThan(0)
          val manifest = readManifest(output)
          assertThat(manifest.get("base_path").asText()).isEqualTo(output)
          val manifestPaths = manifestPathsFrom(manifest)
          assertThat(manifestPaths.toSet == parquetFiles.map(_.getName).toSet).isTrue()

          val exported = spark.read.parquet(output)
          assertThat(exported.schema.fieldNames).containsExactly("id", "name")
          checkAnswer(exported.orderBy("id"), Row(2, "b") :: Row(3, "c") :: Nil)
      }
    }
  }

  test("Paimon export parquet procedure: export all columns without filter") {
    val random = ThreadLocalRandom.current().nextInt(100000)
    withTable(s"tbl_all$random") {
      sql(s"""
             |CREATE TABLE tbl_all$random (
             |  id INT,
             |  name STRING,
             |  score INT
             |)
             |""".stripMargin)

      sql(s"""
             |INSERT INTO tbl_all$random VALUES
             |  (1, 'a', 10),
             |  (2, 'b', 20)
             |""".stripMargin)

      withTempDir {
        dir =>
          val output = new File(dir, "export-all").getAbsolutePath

          checkAnswer(
            spark.sql(s"""
                         |CALL sys.export_parquet(
                         |  table => 'tbl_all$random',
                         |  columns => '*',
                         |  output_path => '$output')
                         |""".stripMargin),
            Row(true, 2L) :: Nil
          )

          assertThat(new File(output, "_SUCCESS")).exists()
          val exported = spark.read.parquet(output)
          assertThat(exported.schema.fieldNames).containsExactly("id", "name", "score")
          checkAnswer(exported.orderBy("id"), Row(1, "a", 10) :: Row(2, "b", 20) :: Nil)
      }
    }
  }

  test("Paimon export parquet procedure: roll files by target file size") {
    val random = ThreadLocalRandom.current().nextInt(100000)
    withTable(s"tbl_roll$random") {
      sql(s"""
             |CREATE TABLE tbl_roll$random (
             |  id INT,
             |  name STRING
             |)
             |""".stripMargin)

      sql(s"""
             |INSERT INTO tbl_roll$random VALUES
             |  (1, 'a'), (2, 'b'), (3, 'c'), (4, 'd'), (5, 'e')
             |""".stripMargin)

      withTempDir {
        dir =>
          val output = new File(dir, "export-roll").getAbsolutePath

          checkAnswer(
            spark.sql(s"""
                         |CALL sys.export_parquet(
                         |  table => 'tbl_roll$random',
                         |  columns => '*',
                         |  output_path => '$output',
                         |  target_file_size => '1 b',
                         |  parallelism => 1)
                         |""".stripMargin),
            Row(true, 5L) :: Nil
          )

          assertThat(new File(output).listFiles().filter(_.getName.endsWith(".parquet")).length)
            .isGreaterThan(1)

          val exported = spark.read.parquet(output)
          checkAnswer(
            exported.orderBy("id"),
            Row(1, "a") :: Row(2, "b") :: Row(3, "c") :: Row(4, "d") :: Row(5, "e") :: Nil)
      }
    }
  }

  test("Paimon export parquet procedure: partitioned output writes each partition") {
    val random = ThreadLocalRandom.current().nextInt(100000)
    withTable(s"tbl_part$random") {
      sql(s"""
             |CREATE TABLE tbl_part$random (
             |  id INT,
             |  name STRING,
             |  dt STRING
             |)
             |PARTITIONED BY (dt)
             |""".stripMargin)

      sql(s"""
             |INSERT INTO tbl_part$random VALUES
             |  (1, 'a', '2026-05-01'),
             |  (2, 'b', '2026-05-01'),
             |  (3, 'c', '2026-05-03'),
             |  (4, 'd', '2026-05-06'),
             |  (5, 'e', '2026-05-07')
             |""".stripMargin)

      withTempDir {
        dir =>
          val output = new File(dir, "export-partitioned").getAbsolutePath

          checkAnswer(
            spark.sql(s"""
                         |CALL sys.export_parquet(
                         |  table => 'tbl_part$random',
                         |  columns => 'id,name',
                         |  output_path => '$output',
                         |  where => "dt >= '2026-05-01' and dt <= '2026-05-06'",
                         |  parallelism => 2,
                         |  overwrite => true,
                         |  partitioned_output => true,
                         |  partition_job_parallelism => 2)
                         |""".stripMargin),
            Row(true, 4L) :: Nil
          )

          assertThat(new File(output, "_SUCCESS")).exists()
          assertThat(new File(output, "dt=2026-05-01")).isDirectory()
          assertThat(new File(output, "dt=2026-05-03")).isDirectory()
          assertThat(new File(output, "dt=2026-05-06")).isDirectory()
          assertThat(new File(output, "dt=2026-05-07")).doesNotExist()

          Seq("2026-05-01", "2026-05-03", "2026-05-06").foreach {
            dt =>
              val partitionDir = new File(output, s"dt=$dt")
              assertThat(partitionDir.listFiles().filter(_.getName.endsWith(".parquet")).length)
                .isGreaterThan(0)
              assertThat(new File(partitionDir, "_SUCCESS")).exists()
          }
          val manifest = readManifest(output)
          assertThat(manifest.get("base_path").asText()).isEqualTo(output)
          val manifestPaths = manifestPathsFrom(manifest)
          assertThat(manifestPaths.size).isEqualTo(3)
          Seq("2026-05-01", "2026-05-03", "2026-05-06").foreach {
            dt =>
              assertThat(manifestPaths.exists(_.startsWith(s"dt=$dt/part-"))).isTrue()
          }
          manifestPaths.foreach {
            path =>
              assertThat(path).endsWith(".parquet")
              assertThat(path).doesNotStartWith(output)
          }

          val exported = spark.read
            .option("basePath", output)
            .parquet(output)
            .selectExpr("id", "name", "cast(dt as string) as dt")
          assertThat(exported.schema.fieldNames).containsExactly("id", "name", "dt")
          checkAnswer(
            exported.orderBy("id"),
            Row(1, "a", "2026-05-01") ::
              Row(2, "b", "2026-05-01") ::
              Row(3, "c", "2026-05-03") ::
              Row(4, "d", "2026-05-06") :: Nil)
      }
    }
  }

  test("Paimon export parquet procedure: compact_output argument is not supported") {
    val random = ThreadLocalRandom.current().nextInt(100000)
    withTable(s"tbl_no_compact$random") {
      sql(s"""
             |CREATE TABLE tbl_no_compact$random (
             |  id INT,
             |  name STRING
             |)
             |""".stripMargin)

      sql(s"""
             |INSERT INTO tbl_no_compact$random VALUES
             |  (1, 'a'), (2, 'b')
             |""".stripMargin)

      withTempDir {
        dir =>
          val output = new File(dir, "export-no-compact").getAbsolutePath

          assertThatThrownBy(() =>
            spark.sql(s"""
                         |CALL sys.export_parquet(
                         |  table => 'tbl_no_compact$random',
                         |  columns => '*',
                         |  output_path => '$output',
                         |  compact_output => true)
                         |""".stripMargin).collect())
            .hasMessageContaining("compact_output")
      }
    }
  }

  test("Paimon export parquet procedure: removed native export configs do not affect Java export") {
    val random = ThreadLocalRandom.current().nextInt(100000)
    withTable(s"tbl_native_config$random") {
      sql(s"""
             |CREATE TABLE tbl_native_config$random (
             |  id INT,
             |  name STRING
             |)
             |""".stripMargin)

      sql(s"""
             |INSERT INTO tbl_native_config$random VALUES
             |  (1, 'a'), (2, 'b')
             |""".stripMargin)

      withTempDir {
        dir =>
          val output = new File(dir, "export-native-config").getAbsolutePath
          withSparkSQLConf(
            "spark.paimon.native-io.export.enabled" -> "true",
            "spark.paimon.native-io.export.fail-on-fallback" -> "true") {
            checkAnswer(
              spark.sql(s"""
                           |CALL sys.export_parquet(
                           |  table => 'tbl_native_config$random',
                           |  columns => '*',
                           |  output_path => '$output')
                           |""".stripMargin),
              Row(true, 2L) :: Nil
            )
          }

          val exported = spark.read.parquet(output)
          checkAnswer(exported.orderBy("id"), Row(1, "a") :: Row(2, "b") :: Nil)
      }
    }
  }

  private def readManifest(output: String) = {
    JsonSerdeUtil.OBJECT_MAPPER_INSTANCE.readTree(
      new String(
        Files.readAllBytes(new File(output, "_manifest.json").toPath),
        StandardCharsets.UTF_8))
  }

  private def manifestPathsFrom(
      manifest: org.apache.paimon.shade.jackson2.com.fasterxml.jackson.databind.JsonNode)
      : Seq[String] = {
    manifest.get("files").elements().asScala.map(_.get("path").asText()).toSeq
  }
}
