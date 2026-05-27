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

import one.profiler.{AsyncProfiler, AsyncProfilerLoader}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FSDataOutputStream, FileSystem, Path}

import org.apache.spark.SparkConf
import org.apache.spark.internal.Logging

import java.io.{BufferedInputStream, FileInputStream, IOException, InputStream}
import java.net.URI
import java.nio.file.{Files, Paths}
import java.util.Locale
import java.util.concurrent.{Executors, ScheduledExecutorService, ThreadFactory, TimeUnit}

class PaimonSparkAsyncProfiler(conf: SparkConf, executorId: String) extends Logging {

  private val profilerArgs = PaimonProfilerConf.asyncProfilerArgs(conf)
  private val profilerDfsDir = PaimonProfilerConf.dfsDir(conf)
  private val profilerLocalDir = PaimonProfilerConf.localDir(conf)
  private val outputSuffix = PaimonProfilerConf.outputSuffix(conf)
  private val profileFile =
    PaimonProfilerConf.profileFile(executorId, outputSuffix)
  private val profilePath = profilerLocalDir + "/" + profileFile
  private val writeInterval = PaimonProfilerConf.dfsWriteIntervalSeconds(conf)
  private val appendDfsOutput = outputSuffix.toLowerCase(Locale.ROOT) == "jfr"

  private val startCommand = "start," + profilerArgs + ",file=" + profilePath
  private val stopCommand = "stop," + profilerArgs + ",file=" + profilePath
  private val dumpCommand = "dump," + profilerArgs + ",file=" + profilePath
  private val resumeCommand = "resume," + profilerArgs + ",file=" + profilePath

  private val UploadSize = 8 * 1024 * 1024
  private val dataBuffer = new Array[Byte](UploadSize)

  @volatile private var running = false
  @volatile private var writing = false
  @volatile private var outputStream: FSDataOutputStream = _
  private var inputStream: InputStream = _
  private var threadPool: ScheduledExecutorService = _

  private lazy val extractionDir = Files.createTempDirectory("paimonAsyncProfiler")

  private val profiler: Option[AsyncProfiler] = {
    Option(
      if (AsyncProfilerLoader.isSupported) {
        AsyncProfilerLoader.setExtractionDirectory(extractionDir)
        AsyncProfilerLoader.load()
      } else {
        null
      })
  }

  def start(): Unit = {
    if (!running) {
      try {
        profiler.foreach { profiler =>
          Files.createDirectories(Paths.get(profilerLocalDir))
          profiler.execute(startCommand)
          running = true
          logInfo("Paimon async profiler started for " + executorId + ".")
          startWriting()
        }
      } catch {
        case e @ (_: IllegalArgumentException | _: IllegalStateException | _: IOException) =>
          logError("Paimon async profiler aborted in native profiler code.", e)
        case e: Exception =>
          logWarning("Paimon async profiler aborted.", e)
      }
    }
  }

  def stop(): Unit = {
    if (running) {
      try {
        profiler.foreach(_.execute(stopCommand))
        running = false
        finishWriting()
        logInfo("Paimon async profiler stopped for " + executorId + ".")
      } catch {
        case e @ (_: IllegalArgumentException | _: IllegalStateException | _: IOException) =>
          logError("Paimon async profiler failed to stop cleanly.", e)
      }
    }
  }

  private def startWriting(): Unit = {
    profilerDfsDir.foreach { _ =>
      try {
        threadPool = Executors.newSingleThreadScheduledExecutor(daemonThreadFactory)
        threadPool.scheduleWithFixedDelay(
          new Runnable {
            override def run(): Unit = writeChunk(lastChunk = false)
          },
          writeInterval,
          writeInterval,
          TimeUnit.SECONDS)
        writing = true
      } catch {
        case e: Exception =>
          logError("Failed to start Paimon profiler DFS writer.", e)
          closeWriter()
      }
    }
  }

  private def writeChunk(lastChunk: Boolean): Unit = {
    if (!writing) {
      return
    }

    try {
      profiler.foreach(_.execute(stopCommand))
      profiler.foreach(_.execute(dumpCommand))
      if (appendDfsOutput) {
        ensureInputStream()
        val remaining = inputStream.available()
        if (!lastChunk) {
          profiler.foreach(_.execute(resumeCommand))
        }
        writeAppendedBytes(remaining)
      } else {
        writeSnapshot()
        if (!lastChunk) {
          profiler.foreach(_.execute(resumeCommand))
        }
      }
    } catch {
      case e: IOException =>
        logError("Exception while writing Paimon profiler output.", e)
      case e @ (_: IllegalArgumentException | _: IllegalStateException) =>
        logError("Exception in async-profiler while writing Paimon profiler output.", e)
      case e: Exception =>
        logError("Unexpected exception while writing Paimon profiler output.", e)
    }
  }

  private def ensureInputStream(): Unit = {
    if (inputStream == null) {
      inputStream = new BufferedInputStream(new FileInputStream(profilePath))
    }
  }

  private def writeAppendedBytes(bytes: Int): Unit = {
    ensureOutputStream()
    var remaining = bytes
    while (remaining > 0) {
      val read = inputStream.read(dataBuffer, 0, math.min(remaining, UploadSize))
      if (read < 0) {
        return
      }
      outputStream.write(dataBuffer, 0, read)
      remaining -= read
    }
    outputStream.hflush()
  }

  private def writeSnapshot(): Unit = {
    val copied =
      PaimonProfilerDfsOutput.copySnapshot(
        profilePath,
        dfsOutputFile,
        newHadoopConfiguration())
    if (!copied) {
      logWarning("Paimon profiler output file is not available yet: " + profilePath)
    }
  }

  private def ensureOutputStream(): Unit = {
    if (outputStream == null) {
      val hadoopConf = newHadoopConfiguration()
      val outputFile = dfsOutputFile
      val fs = FileSystem.get(new URI(outputFile), hadoopConf)
      fs.mkdirs(new Path(outputFile).getParent)
      outputStream = fs.create(new Path(outputFile), true)
      logInfo("Copying Paimon profiler output to " + outputFile)
    }
  }

  private def dfsOutputFile: String = {
    while (conf.getOption("spark.app.id").isEmpty) {
      Thread.sleep(1000L)
    }
    val appId = conf.getAppId
    val attemptId = conf.getOption("spark.app.attempt.id")
    PaimonProfilerConf.appAttemptDir(profilerDfsDir.get, appId, attemptId) + "/" + profileFile
  }

  private def finishWriting(): Unit = {
    if (profilerDfsDir.isDefined && writing) {
      try {
        if (threadPool != null) {
          threadPool.shutdown()
          threadPool.awaitTermination(30L, TimeUnit.SECONDS)
        }
        writeChunk(lastChunk = true)
      } catch {
        case _: InterruptedException =>
          Thread.currentThread().interrupt()
        case e: IOException =>
          logWarning("Exception while completing Paimon profiler output.", e)
      } finally {
        closeWriter()
        writing = false
      }
    }
  }

  private def closeWriter(): Unit = {
    if (threadPool != null) {
      threadPool.shutdownNow()
    }
    if (inputStream != null) {
      try {
        inputStream.close()
      } catch {
        case e: IOException => logWarning("Failed to close Paimon profiler input stream.", e)
      }
    }
    if (outputStream != null) {
      try {
        outputStream.close()
      } catch {
        case e: IOException => logWarning("Failed to close Paimon profiler output stream.", e)
      }
    }
  }

  private def newHadoopConfiguration(): Configuration = {
    val hadoopConf = new Configuration()
    conf.getAll.foreach {
      case (key, value) if key.startsWith("spark.hadoop.") =>
        hadoopConf.set(key.stripPrefix("spark.hadoop."), value)
      case _ =>
    }
    hadoopConf
  }

  private def daemonThreadFactory: ThreadFactory = {
    new ThreadFactory {
      override def newThread(runnable: Runnable): Thread = {
        val thread = new Thread(runnable, "paimon-profiler-writer")
        thread.setDaemon(true)
        thread
      }
    }
  }
}

private[profiler] object PaimonProfilerDfsOutput {

  private val CopyBufferSize = 8 * 1024 * 1024

  def copySnapshot(localFile: String, outputFile: String, hadoopConf: Configuration): Boolean = {
    val localPath = Paths.get(localFile)
    if (!Files.exists(localPath)) {
      return false
    }

    val fs = FileSystem.get(new URI(outputFile), hadoopConf)
    val outputPath = new Path(outputFile)
    fs.mkdirs(outputPath.getParent)

    val input = new BufferedInputStream(new FileInputStream(localFile))
    val output = fs.create(outputPath, true)
    try {
      val buffer = new Array[Byte](CopyBufferSize)
      var read = input.read(buffer)
      while (read >= 0) {
        output.write(buffer, 0, read)
        read = input.read(buffer)
      }
      output.hflush()
      true
    } finally {
      input.close()
      output.close()
    }
  }
}
