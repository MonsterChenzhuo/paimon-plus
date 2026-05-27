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

import one.converter.jfr2flame
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileStatus, Path}
import org.apache.paimon.spark.diagnostics.{PaimonDiagnostics, PaimonFlamegraphNode}
import org.apache.paimon.spark.diagnostics.profiler.PaimonProfilerConf

import org.apache.spark.SparkContext
import org.apache.spark.internal.Logging
import org.apache.spark.status.api.v1.{ExecutorSummary, ThreadStackTrace}

import java.io.{ByteArrayOutputStream, InputStream}
import java.net.URLEncoder
import java.nio.charset.StandardCharsets
import java.nio.file.{Files, StandardCopyOption}
import java.util.Locale
import java.util.{Collections, WeakHashMap}
import javax.servlet.http.HttpServletRequest

import scala.xml.{Node, Text, Unparsed}
import scala.util.control.NonFatal

import PaimonDiagnosticsNav._

class PaimonDiagnosticsTab(parent: SparkUI)
    extends SparkUITab(parent, PaimonDiagnosticsTabSupport.TabPrefix) {

  attachPage(new PaimonDiagnosticsPage(this, parent))
  attachPage(new PaimonThreadDumpFlamegraphPage(this, parent))
  attachPage(new PaimonProfilerPage(this, parent))
}

object PaimonDiagnosticsTabSupport extends Logging {

  val TabPrefix = "paimon-diagnostics"

  private val StartupAttachTimeoutMs = 10 * 60 * 1000L
  private val StartupAttachPollMs = 100L
  private val PendingAttachUis =
    Collections.newSetFromMap(new WeakHashMap[SparkUI, java.lang.Boolean]())

  def attach(ui: Any): Unit = {
    val sparkUI = ui.asInstanceOf[SparkUI]
    if (readyToAttach(sparkUI)) {
      attachNow(sparkUI)
    } else {
      deferAttach(sparkUI)
    }
  }

  private def attachNow(sparkUI: SparkUI): Unit = sparkUI.synchronized {
    if (!sparkUI.getTabs.exists(_.prefix == TabPrefix)) {
      sparkUI.attachTab(new PaimonDiagnosticsTab(sparkUI))
    }
  }

  private def readyToAttach(sparkUI: SparkUI): Boolean = {
    if (sparkUI.boundPort < 0) {
      true
    } else {
      val handlers = uiHandlers(sparkUI)
      handlers.nonEmpty && handlers.forall(_.isStarted)
    }
  }

  private def deferAttach(sparkUI: SparkUI): Unit = {
    val startupHandlers = uiHandlers(sparkUI).toList
    if (!markPending(sparkUI)) {
      return
    }

    val thread = new Thread("paimon-diagnostics-ui-attach") {
      override def run(): Unit = {
        try {
          val deadline = System.currentTimeMillis() + StartupAttachTimeoutMs
          while (
            !sparkUI.getTabs.exists(_.prefix == TabPrefix) &&
            !startupHandlersReady(startupHandlers) &&
            System.currentTimeMillis() < deadline
          ) {
            Thread.sleep(StartupAttachPollMs)
          }

          if (!sparkUI.getTabs.exists(_.prefix == TabPrefix) &&
              startupHandlersReady(startupHandlers)) {
            Thread.sleep(StartupAttachPollMs)
            attachNow(sparkUI)
          } else if (!sparkUI.getTabs.exists(_.prefix == TabPrefix)) {
            logWarning("Timed out waiting for Spark UI startup before attaching Paimon diagnostics tab.")
          }
        } catch {
          case _: InterruptedException =>
            Thread.currentThread().interrupt()
          case NonFatal(e) =>
            logWarning("Failed to attach Paimon diagnostics tab after Spark UI startup.", e)
        } finally {
          unmarkPending(sparkUI)
        }
      }
    }
    thread.setDaemon(true)
    thread.start()
  }

  private def startupHandlersReady(handlers: Seq[UiHandler]): Boolean = {
    handlers.nonEmpty && handlers.forall(_.isStarted)
  }

  private def uiHandlers(sparkUI: SparkUI): Seq[UiHandler] = {
    val method = sparkUI.getClass.getMethod("getHandlers")
    method.invoke(sparkUI).asInstanceOf[Seq[AnyRef]].map(UiHandler)
  }

  private def markPending(sparkUI: SparkUI): Boolean = PendingAttachUis.synchronized {
    if (PendingAttachUis.contains(sparkUI) || sparkUI.getTabs.exists(_.prefix == TabPrefix)) {
      false
    } else {
      PendingAttachUis.add(sparkUI)
      true
    }
  }

  private def unmarkPending(sparkUI: SparkUI): Unit = PendingAttachUis.synchronized {
    PendingAttachUis.remove(sparkUI)
  }

  private case class UiHandler(handler: AnyRef) {

    def isStarted: Boolean = {
      handler.getClass.getMethod("isStarted").invoke(handler).asInstanceOf[Boolean]
    }
  }
}

private class PaimonDiagnosticsPage(tab: PaimonDiagnosticsTab, sparkUI: SparkUI)
    extends WebUIPage("") {

  override def render(request: HttpServletRequest): Seq[Node] = {
    UIUtils.headerSparkPage(
      request,
      "Paimon Diagnostics",
      nav(request) ++ executorTable(request),
      tab,
      None,
      false,
      false)
  }

  private def executorTable(request: HttpServletRequest): Seq[Node] = {
    val executors = sparkUI.sc.map(_.statusStore.executorList(activeOnly = false)).getOrElse(Nil)
    Seq(
      <div>
        <h4>Executors</h4>
        <table class={UIUtils.TABLE_CLASS_STRIPED + " sortable"}>
          <thead>
            <tr>
              <th>Executor</th>
              <th>Host</th>
              <th>Active Tasks</th>
              <th>Total Tasks</th>
              <th>Failed Tasks</th>
              <th>Total Duration</th>
              <th>GC Time</th>
              <th>Thread Dump Flame Graph</th>
            </tr>
          </thead>
          <tbody>
            {if (executors.isEmpty) {
              <tr><td colspan="8">No executor data is available.</td></tr>
            } else {
              executors.sortBy(sortKey).map(executorRow(request, _))
            }}
          </tbody>
        </table>
      </div>)
  }

  private def executorRow(request: HttpServletRequest, executor: ExecutorSummary): Node = {
    <tr>
      <td>{executor.id}</td>
      <td>{executor.hostPort}</td>
      <td>{executor.activeTasks}</td>
      <td>{executor.totalTasks}</td>
      <td>{executor.failedTasks}</td>
      <td>{formatDuration(executor.totalDuration)}</td>
      <td>{formatDuration(executor.totalGCTime)}</td>
      <td>
        <a href={threadDumpUrl(request, executor.id)}>Open</a>
      </td>
    </tr>
  }

  private def threadDumpUrl(request: HttpServletRequest, executorId: String): String = {
    val encoded = URLEncoder.encode(executorId, "UTF-8")
    UIUtils.prependBaseUri(request, "/" + PaimonDiagnosticsTabSupport.TabPrefix +
      "/threadDump/?executorId=" + encoded)
  }

  private def sortKey(executor: ExecutorSummary): (Int, String) = {
    if (executor.id == "driver") {
      (0, executor.id)
    } else {
      (1, executor.id)
    }
  }
}

private class PaimonThreadDumpFlamegraphPage(tab: PaimonDiagnosticsTab, sparkUI: SparkUI)
    extends WebUIPage("threadDump") {

  override def render(request: HttpServletRequest): Seq[Node] = {
    val executorId = Option(request.getParameter("executorId")).map(UIUtils.decodeURLParameter)
      .getOrElse(throw new IllegalArgumentException("Missing executorId parameter"))
    val title = "Paimon Thread Dump - " + executorId
    val content = sparkUI.sc match {
      case Some(sc) => renderThreadDump(request, sc, executorId)
      case None => Seq(Text("Thread dump flame graph is only available in a live Spark UI."))
    }
    UIUtils.headerSparkPage(request, title, nav(request) ++ content, tab, None, false, false)
  }

  private def renderThreadDump(
      request: HttpServletRequest,
      sc: SparkContext,
      executorId: String): Seq[Node] = {
    val now = System.currentTimeMillis()
    sc.getExecutorThreadDump(executorId).map { threadDump =>
      Seq(
        <div>
          <h4>Executor {executorId}</h4>
          <p>Updated at {UIUtils.formatDate(now)}. Samples: {threadDump.length} threads.</p>
          {if (PaimonDiagnostics.flamegraphEnabled(sc.getConf)) {
            flamegraph(request, threadDump)
          } else {
            Seq(<p>Flame graph rendering is disabled.</p>)
          }}
          {threadTable(threadDump)}
        </div>)
    }.getOrElse(Seq(Text("Error fetching thread dump from executor " + executorId)))
  }

  private def flamegraph(request: HttpServletRequest, threadDump: Array[ThreadStackTrace]): Seq[Node] = {
    val js = "paimonDrawFlamegraph('paimon-flamegraph-data', 'paimon-flamegraph-chart');"
    Seq(
      <div>
        <h4>Flame Graph</h4>
        <div id="paimon-flamegraph-data" style="display: none;">
          {PaimonFlamegraphNode.fromThreadDump(threadDump).toJsonString}
        </div>
        <div id="paimon-flamegraph-chart" class="paimon-flamegraph"></div>
        <style type="text/css">{Unparsed(PaimonFlamegraphAssets.Css)}</style>
        <script>{Unparsed(PaimonFlamegraphAssets.Js)}</script>
        <script>{Unparsed(js)}</script>
      </div>)
  }

  private def threadTable(threadDump: Array[ThreadStackTrace]): Seq[Node] = {
    Seq(
      <div>
        <h4>Thread Stack Trace</h4>
        <table class={UIUtils.TABLE_CLASS_STRIPED + " sortable"}>
          <thead>
            <tr>
              <th>Thread ID</th>
              <th>Thread Name</th>
              <th>State</th>
              <th>Locks</th>
              <th>Stack Trace</th>
            </tr>
          </thead>
          <tbody>
            {threadDump.map { thread =>
              <tr>
                <td>{thread.threadId}</td>
                <td>{thread.threadName}</td>
                <td>{thread.threadState.toString}</td>
                <td>{thread.holdingLocks.mkString(", ")}</td>
                <td><pre>{thread.stackTrace.mkString("", "\n", "")}</pre></td>
              </tr>
            }}
          </tbody>
        </table>
      </div>)
  }
}

private class PaimonProfilerPage(tab: PaimonDiagnosticsTab, sparkUI: SparkUI)
    extends WebUIPage("profiler") {

  override def render(request: HttpServletRequest): Seq[Node] = {
    val conf = sparkUI.conf
    val appId = Option(sparkUI.appId).getOrElse("<application-id>")
    val attemptId = conf.getOption("spark.app.attempt.id")
    val outputDirOption = PaimonProfilerConf.dfsDir(conf)
      .map(PaimonProfilerConf.appAttemptDir(_, appId, attemptId))
    val outputDir = outputDirOption.getOrElse("-")
    val suffix = PaimonProfilerConf.outputSuffix(conf)
    val content =
      nav(request) ++ Seq(
        <div>
          <h4>Async Profiler</h4>
          <table class="table table-bordered table-sm">
            <tbody>
              <tr><th>Plugin Class</th><td>{PaimonProfilerConf.PluginClass}</td></tr>
              <tr><th>spark.plugins Contains Plugin</th><td>{pluginConfigured(conf)}</td></tr>
              <tr><th>Driver Profiling</th><td>{PaimonProfilerConf.driverEnabled(conf).toString}</td></tr>
              <tr><th>Executor Profiling</th><td>{PaimonProfilerConf.executorEnabled(conf).toString}</td></tr>
              <tr><th>Executor Fraction</th><td>{PaimonProfilerConf.executorFraction(conf).toString}</td></tr>
              <tr><th>Local Directory</th><td>{PaimonProfilerConf.localDir(conf)}</td></tr>
              <tr><th>DFS Directory</th><td>{outputDir}</td></tr>
              <tr><th>Output Suffix</th><td>{suffix}</td></tr>
              <tr><th>Async Profiler Args</th><td>{PaimonProfilerConf.asyncProfilerArgs(conf)}</td></tr>
              <tr>
                <th>Output File Pattern</th>
                <td>{PaimonProfilerConf.profileFile("driver", suffix)} / profile-exec-&lt;executorId&gt;.{suffix}</td>
              </tr>
            </tbody>
          </table>
        </div>) ++ profilerArtifacts(request, conf, outputDirOption)
    UIUtils.headerSparkPage(request, "Paimon Profiler", content, tab, None, false, false)
  }

  private def pluginConfigured(conf: org.apache.spark.SparkConf): String = {
    conf.get("spark.plugins", "")
      .split(",")
      .map(_.trim)
      .contains(PaimonProfilerConf.PluginClass)
      .toString
  }

  private def profilerArtifacts(
      request: HttpServletRequest,
      conf: org.apache.spark.SparkConf,
      outputDirOption: Option[String]): Seq[Node] = {
    outputDirOption match {
      case Some(outputDir) =>
        val artifactTable = PaimonProfilerArtifacts.list(conf, outputDir) match {
          case Right(artifacts) => profilerArtifactTable(request, conf, artifacts)
          case Left(error) =>
            Seq(<p class="text-danger">Failed to list profiler output files: {error}</p>)
        }
        artifactTable ++ selectedProfilerArtifact(request, conf, outputDir)
      case None =>
        Seq(
          <div>
            <h4>Profiler Output Files</h4>
            <p>No DFS directory is configured.</p>
          </div>)
    }
  }

  private def profilerArtifactTable(
      request: HttpServletRequest,
      conf: org.apache.spark.SparkConf,
      artifacts: Seq[PaimonProfilerArtifacts.Artifact]): Seq[Node] = {
    Seq(
      <div>
        <h4>Profiler Output Files</h4>
        <table class={UIUtils.TABLE_CLASS_STRIPED + " sortable"}>
          <thead>
            <tr>
              <th>File</th>
              <th>Format</th>
              <th>Size</th>
              <th>Updated</th>
              <th>Flame Graph</th>
            </tr>
          </thead>
          <tbody>
            {if (artifacts.isEmpty) {
              <tr>
                <td colspan="5">
                  No profiler output files are available yet. Executor profiles are uploaded every {PaimonProfilerConf.dfsWriteIntervalSeconds(conf)} seconds from sampled executors.
                </td>
              </tr>
            } else {
              artifacts.map { artifact =>
                <tr>
                  <td><code>{artifact.name}</code></td>
                  <td>{artifact.format}</td>
                  <td>{formatBytes(artifact.length)}</td>
                  <td>{UIUtils.formatDate(artifact.modificationTime)}</td>
                  <td><a href={profilerArtifactUrl(request, artifact.name)}>Open</a></td>
                </tr>
              }
            }}
          </tbody>
        </table>
      </div>)
  }

  private def selectedProfilerArtifact(
      request: HttpServletRequest,
      conf: org.apache.spark.SparkConf,
      outputDir: String): Seq[Node] = {
    Option(request.getParameter("profile")).map(UIUtils.decodeURLParameter) match {
      case Some(profileName) =>
        PaimonProfilerArtifacts.resolve(outputDir, profileName) match {
          case Some(path) =>
            PaimonProfilerArtifacts.loadFlameGraphHtml(conf, path.toString) match {
              case Right(html) =>
                Seq(
                  <div>
                    <h4>Async Profiler Flame Graph - {profileName}</h4>
                    <iframe
                      sandbox="allow-scripts"
                      style="width: 100%; min-height: 720px; border: 1px solid #d6d6d6;"
                      srcdoc={html}></iframe>
                  </div>)
              case Left(error) =>
                Seq(<p class="text-danger">Failed to open profiler flame graph: {error}</p>)
            }
          case None =>
            Seq(<p class="text-danger">Unsupported profiler output file: {profileName}</p>)
        }
      case None => Nil
    }
  }

  private def profilerArtifactUrl(request: HttpServletRequest, profileName: String): String = {
    UIUtils.prependBaseUri(
      request,
      "/" + PaimonDiagnosticsTabSupport.TabPrefix + "/profiler/?profile=" +
        URLEncoder.encode(profileName, "UTF-8"))
  }

  private def formatBytes(bytes: Long): String = {
    if (bytes < 1024L) {
      bytes + " B"
    } else if (bytes < 1024L * 1024L) {
      "%.1f KiB".format(bytes.toDouble / 1024.0D)
    } else {
      "%.1f MiB".format(bytes.toDouble / 1024.0D / 1024.0D)
    }
  }
}

private[ui] object PaimonProfilerArtifacts extends Logging {

  case class Artifact(name: String, path: String, format: String, length: Long, modificationTime: Long)

  private val SupportedProfileFile = "^profile-(driver|exec-[A-Za-z0-9_.-]+)\\.(jfr|html)$".r
  private val ReadBufferSize = 64 * 1024

  def list(conf: org.apache.spark.SparkConf, outputDir: String): Either[String, Seq[Artifact]] = {
    try {
      val dir = new Path(outputDir)
      val fs = dir.getFileSystem(newHadoopConfiguration(conf))
      if (!fs.exists(dir)) {
        Right(Nil)
      } else {
        Right(
          fs.listStatus(dir)
            .filter(_.isFile)
            .flatMap(toArtifact)
            .sortBy(_.name))
      }
    } catch {
      case NonFatal(e) => Left(errorMessage(e))
    }
  }

  def resolve(outputDir: String, profileName: String): Option[Path] = {
    if (profileName.contains("/") || profileName.contains("\\") || profileName.contains("..")) {
      None
    } else {
      format(profileName).map { _ =>
        new Path(outputDir.stripSuffix("/") + "/" + profileName)
      }
    }
  }

  def loadFlameGraphHtml(
      conf: org.apache.spark.SparkConf,
      profilePath: String): Either[String, String] = {
    val path = new Path(profilePath)
    format(path.getName) match {
      case Some("html") => readUtf8(conf, path)
      case Some("jfr") => convertJfrToHtml(conf, path)
      case _ => Left("Unsupported profiler output format for " + path.getName)
    }
  }

  private def toArtifact(status: FileStatus): Option[Artifact] = {
    val name = status.getPath.getName
    format(name).map { profileFormat =>
      Artifact(name, status.getPath.toString, profileFormat, status.getLen, status.getModificationTime)
    }
  }

  private def format(profileName: String): Option[String] = {
    profileName.toLowerCase(Locale.ROOT) match {
      case SupportedProfileFile(_, profileFormat) => Some(profileFormat)
      case _ => None
    }
  }

  private def readUtf8(conf: org.apache.spark.SparkConf, path: Path): Either[String, String] = {
    try {
      val fs = path.getFileSystem(newHadoopConfiguration(conf))
      Right(readUtf8(fs.open(path)))
    } catch {
      case NonFatal(e) => Left(errorMessage(e))
    }
  }

  private def convertJfrToHtml(
      conf: org.apache.spark.SparkConf,
      path: Path): Either[String, String] = {
    var tempDir: java.nio.file.Path = null
    var localJfr: java.nio.file.Path = null
    var localHtml: java.nio.file.Path = null

    try {
      val fs = path.getFileSystem(newHadoopConfiguration(conf))
      tempDir = Files.createTempDirectory("paimon-profiler-view")
      localJfr = tempDir.resolve(path.getName)
      localHtml = tempDir.resolve(path.getName.stripSuffix(".jfr") + ".html")

      val input = fs.open(path)
      try {
        Files.copy(input, localJfr, StandardCopyOption.REPLACE_EXISTING)
      } finally {
        input.close()
      }

      jfr2flame.main(
        Array(
          "--title",
          "Paimon Async Profiler - " + path.getName,
          localJfr.toString,
          localHtml.toString))
      Right(new String(Files.readAllBytes(localHtml), StandardCharsets.UTF_8))
    } catch {
      case NonFatal(e) => Left(errorMessage(e))
    } finally {
      deleteIfExists(localHtml)
      deleteIfExists(localJfr)
      deleteIfExists(tempDir)
    }
  }

  private def readUtf8(input: InputStream): String = {
    try {
      val output = new ByteArrayOutputStream()
      val buffer = new Array[Byte](ReadBufferSize)
      var read = input.read(buffer)
      while (read >= 0) {
        output.write(buffer, 0, read)
        read = input.read(buffer)
      }
      new String(output.toByteArray, StandardCharsets.UTF_8)
    } finally {
      input.close()
    }
  }

  private def deleteIfExists(path: java.nio.file.Path): Unit = {
    if (path != null) {
      try {
        Files.deleteIfExists(path)
      } catch {
        case NonFatal(e) => logWarning("Failed to delete temporary profiler file " + path, e)
      }
    }
  }

  private def newHadoopConfiguration(conf: org.apache.spark.SparkConf): Configuration = {
    val hadoopConf = new Configuration()
    conf.getAll.foreach {
      case (key, value) if key.startsWith("spark.hadoop.") =>
        hadoopConf.set(key.stripPrefix("spark.hadoop."), value)
      case _ =>
    }
    hadoopConf
  }

  private def errorMessage(e: Throwable): String = {
    Option(e.getMessage).getOrElse(e.getClass.getName)
  }
}

private object PaimonFlamegraphAssets {

  val Css: String =
    """
      |.paimon-flamegraph {
      |  border: 1px solid #d6d6d6;
      |  margin: 12px 0 24px 0;
      |  overflow-x: auto;
      |  overflow-y: hidden;
      |  position: relative;
      |  width: 100%;
      |}
      |
      |.paimon-flamegraph-frame {
      |  border: 1px solid rgba(255, 255, 255, 0.85);
      |  box-sizing: border-box;
      |  color: #1f1f1f;
      |  cursor: pointer;
      |  font-family: Verdana, Arial, sans-serif;
      |  font-size: 12px;
      |  height: 22px;
      |  line-height: 20px;
      |  overflow: hidden;
      |  padding: 0 4px;
      |  position: absolute;
      |  text-overflow: ellipsis;
      |  white-space: nowrap;
      |}
      |
      |.paimon-flamegraph-frame:hover {
      |  border-color: #222;
      |}
      |""".stripMargin

  val Js: String =
    """
      |(function () {
      |  function depth(node) {
      |    if (!node.children || node.children.length === 0) {
      |      return 1;
      |    }
      |    return 1 + Math.max.apply(null, node.children.map(depth));
      |  }
      |
      |  function color(name) {
      |    var hash = 0;
      |    for (var i = 0; i < name.length; i++) {
      |      hash = ((hash << 5) - hash) + name.charCodeAt(i);
      |      hash = hash & hash;
      |    }
      |    var hue = Math.abs(hash) % 360;
      |    return "hsl(" + hue + ", 70%, 72%)";
      |  }
      |
      |  function title(node, rootValue) {
      |    var pct = rootValue === 0 ? 0 : (node.value * 100.0 / rootValue);
      |    return node.name + " (" + node.value + " samples, " + pct.toFixed(2) + "%)";
      |  }
      |
      |  function addFrame(chart, node, rootValue, level, left, width) {
      |    var frame = document.createElement("div");
      |    frame.className = "paimon-flamegraph-frame";
      |    frame.style.backgroundColor = color(node.name);
      |    frame.style.left = left + "%";
      |    frame.style.top = (level * 22) + "px";
      |    frame.style.width = width + "%";
      |    frame.title = title(node, rootValue);
      |    frame.textContent = node.name;
      |    chart.appendChild(frame);
      |
      |    if (!node.children || node.children.length === 0 || node.value === 0) {
      |      return;
      |    }
      |
      |    var childLeft = left;
      |    node.children.forEach(function (child) {
      |      var childWidth = width * child.value / node.value;
      |      addFrame(chart, child, rootValue, level + 1, childLeft, childWidth);
      |      childLeft += childWidth;
      |    });
      |  }
      |
      |  window.paimonDrawFlamegraph = function (dataId, chartId) {
      |    var dataElement = document.getElementById(dataId);
      |    var chart = document.getElementById(chartId);
      |    if (!dataElement || !chart) {
      |      return;
      |    }
      |
      |    var data = JSON.parse(dataElement.textContent.trim());
      |    chart.innerHTML = "";
      |    chart.style.height = (depth(data) * 22) + "px";
      |    addFrame(chart, data, data.value || 0, 0, 0, 100);
      |  };
      |})();
      |""".stripMargin
}

private object PaimonDiagnosticsNav {

  def nav(request: HttpServletRequest): Seq[Node] = {
    val prefix = "/" + PaimonDiagnosticsTabSupport.TabPrefix
    Seq(
      <div>
        <ul class="unstyled">
          <li>
            <a href={UIUtils.prependBaseUri(request, prefix + "/")}>Executors</a> |
            <a href={UIUtils.prependBaseUri(request, prefix + "/profiler/")}>Profiler</a>
          </li>
        </ul>
      </div>)
  }

  def formatDuration(ms: Long): String = {
    if (ms < 1000L) {
      ms + " ms"
    } else {
      "%.1f s".format(ms.toDouble / 1000.0D)
    }
  }
}
