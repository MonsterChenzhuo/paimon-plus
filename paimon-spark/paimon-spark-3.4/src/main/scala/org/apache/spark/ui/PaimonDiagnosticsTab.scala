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

import org.apache.paimon.spark.diagnostics.{PaimonDiagnostics, PaimonFlamegraphNode}
import org.apache.paimon.spark.diagnostics.profiler.PaimonProfilerConf

import org.apache.spark.SparkContext
import org.apache.spark.status.api.v1.{ExecutorSummary, ThreadStackTrace}

import java.net.URLEncoder
import javax.servlet.http.HttpServletRequest

import scala.xml.{Node, Text, Unparsed}

import PaimonDiagnosticsNav._

class PaimonDiagnosticsTab(parent: SparkUI)
    extends SparkUITab(parent, PaimonDiagnosticsTabSupport.TabPrefix) {

  attachPage(new PaimonDiagnosticsPage(this, parent))
  attachPage(new PaimonThreadDumpFlamegraphPage(this, parent))
  attachPage(new PaimonProfilerPage(this, parent))
}

object PaimonDiagnosticsTabSupport {

  val TabPrefix = "paimon-diagnostics"

  def attach(ui: Any): Unit = {
    val sparkUI = ui.asInstanceOf[SparkUI]
    if (!sparkUI.getTabs.exists(_.prefix == TabPrefix)) {
      sparkUI.attachTab(new PaimonDiagnosticsTab(sparkUI))
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
    val outputDir = PaimonProfilerConf.dfsDir(conf)
      .map(PaimonProfilerConf.appAttemptDir(_, appId, attemptId))
      .getOrElse("-")
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
        </div>)
    UIUtils.headerSparkPage(request, "Paimon Profiler", content, tab, None, false, false)
  }

  private def pluginConfigured(conf: org.apache.spark.SparkConf): String = {
    conf.get("spark.plugins", "")
      .split(",")
      .map(_.trim)
      .contains(PaimonProfilerConf.PluginClass)
      .toString
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
