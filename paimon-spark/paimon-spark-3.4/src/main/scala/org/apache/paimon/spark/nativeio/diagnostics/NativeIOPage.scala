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

import org.apache.paimon.operation.nativeio.diagnostics.NativeIOEvent
import org.apache.paimon.spark.nativeio.diagnostics.{NativeIOOperationState, NativeIOStore}

import javax.servlet.http.HttpServletRequest

import scala.xml.{Node, NodeSeq, Text}

class NativeIOPage(tab: NativeIOTab, store: NativeIOStore, pagePrefix: String, pageTitle: String)
    extends WebUIPage(pagePrefix) {

  override def render(request: HttpServletRequest): Seq[Node] = {
    UIUtils.headerSparkPage(
      request,
      "Native IO - " + pageTitle,
      content(pagePrefix),
      tab,
      None,
      false,
      false)
  }

  private def content(page: String): Seq[Node] = {
    val now = System.currentTimeMillis()
    val active = store.activeOperations(now)
    val stuck = store.stuckOperations(now)
    val completed = store.completedOperations

    nav ++ {
      page match {
        case "summary" => summary(now, active, stuck, completed)
        case "sql" => grouped("SQL Execution", active.groupBy(_.sqlExecutionId.map(_.toString).getOrElse("-")), now)
        case "stage" => grouped("Stage", active.groupBy(stageName), now)
        case "task" => operationsTable("Active Native IO Tasks", active, now)
        case "file" => grouped("File", active.groupBy(_.filePath.getOrElse("-")), now)
        case "timeline" => timelineTable(store.timeline)
        case "slow" => operationsTable("Slow Native IO Operations", stuck, now)
        case "stuck" => operationsTable("Stuck Native IO Operations", stuck, now)
        case _ => summary(now, active, stuck, completed)
      }
    }
  }

  private def nav: Seq[Node] = {
    Seq(
    <div>
      <ul class="unstyled">
        <li>
          <a href="summary">Summary</a> |
          <a href="sql">Per SQL</a> |
          <a href="stage">Per Stage</a> |
          <a href="task">Per Task</a> |
          <a href="file">Per File</a> |
          <a href="timeline">Timeline</a> |
          <a href="slow">Slow Operations</a> |
          <a href="stuck">Stuck Operations</a>
        </li>
      </ul>
    </div>
    )
  }

  private def summary(
      now: Long,
      active: Seq[NativeIOOperationState],
      stuck: Seq[NativeIOOperationState],
      completed: Seq[NativeIOOperationState]): Seq[Node] = {
    Seq(<div>
      <h4>Summary</h4>
      <p>
        Native IO diagnostics are installed for this Spark application.
        Native read and export events appear below when eligible work is planned or executed.
      </p>
      <table class="table table-bordered table-condensed">
        <tbody>
          <tr><th>Active Operations</th><td>{active.size}</td></tr>
          <tr><th>Stuck Operations</th><td>{stuck.size}</td></tr>
          <tr><th>Completed Operations</th><td>{completed.size}</td></tr>
          <tr><th>Timeline Events</th><td>{store.timeline.size}</td></tr>
        </tbody>
      </table>
    </div>) ++
      operationsTable("Stuck Operations", stuck, now) ++
      operationsTable("Active Operations", active, now)
  }

  private def grouped(
      title: String,
      groups: Map[String, Seq[NativeIOOperationState]],
      now: Long): Seq[Node] = {
    Seq(<div>
      <h4>{title}</h4>
      <table class="table table-striped table-condensed">
        <thead>
          <tr>
            <th>{title}</th>
            <th>Active</th>
            <th>Max Elapsed</th>
            <th>Max Last Event Age</th>
            <th>Executors</th>
            <th>Current Phase / Status</th>
          </tr>
        </thead>
        <tbody>
          {groups.toSeq.sortBy(_._1).map {
            case (key, states) =>
              <tr>
                <td>{key}</td>
                <td>{states.size}</td>
                <td>{formatDuration(states.map(_.stuckElapsedMs(now)).foldLeft(0L)(math.max))}</td>
                <td>{formatDuration(states.map(state => math.max(0L, now - state.lastEventTime)).foldLeft(0L)(math.max))}</td>
                <td>{states.flatMap(_.executorId).distinct.sorted.mkString(", ")}</td>
                <td>{states.map(_.phaseOrStatus).distinct.sorted.mkString(", ")}</td>
              </tr>
          }}
        </tbody>
      </table>
    </div>)
  }

  private def operationsTable(
      title: String,
      operations: Seq[NativeIOOperationState],
      now: Long): Seq[Node] = {
    Seq(<div>
      <h4>{title}</h4>
      <table class="table table-striped table-condensed">
        <thead>
          <tr>
            <th>Operation</th>
            <th>SQL</th>
            <th>Stage</th>
            <th>Task</th>
            <th>Executor</th>
            <th>Phase / Status</th>
            <th>Elapsed</th>
            <th>Last Event Age</th>
            <th>Diagnosis</th>
            <th>Runtime</th>
            <th>Native Memory</th>
            <th>File</th>
            <th>Object Request</th>
            <th>Rows</th>
            <th>Bytes</th>
            <th>Details</th>
            <th>Error</th>
          </tr>
        </thead>
        <tbody>
          {if (operations.isEmpty) {
            <tr><td colspan="17">No data</td></tr>
          } else {
            operations.map(operationRow(_, now))
          }}
        </tbody>
      </table>
    </div>)
  }

  private def operationRow(state: NativeIOOperationState, now: Long): Node = {
    <tr>
      <td>{state.operationName}<br/><small>{state.operationId}</small></td>
      <td>{state.sqlExecutionId.map(_.toString).getOrElse("-")}</td>
      <td>{stageName(state)}</td>
      <td>{taskName(state)}</td>
      <td>{state.executorId.getOrElse("-")}<br/><small>{state.host.getOrElse("-")}</small></td>
      <td>{state.phaseOrStatus}</td>
      <td>{formatDuration(state.stuckElapsedMs(now))}</td>
      <td>{formatDuration(math.max(0L, now - state.lastEventTime))}</td>
      <td>{state.diagnosis}</td>
      <td>{runtimeDetails(state)}</td>
      <td>{memoryDetails(state)}</td>
      <td>{state.filePath.getOrElse(state.outputPath.getOrElse("-"))}</td>
      <td>{state.objectOperation.getOrElse("-")}<br/><small>{state.objectRequestId.getOrElse("-")}</small></td>
      <td>{state.rows.map(_.toString).getOrElse("-")}</td>
      <td>{state.bytes.map(formatBytes).getOrElse("-")}</td>
      <td>{state.metricsJson.map(truncate).getOrElse("-")}</td>
      <td>{state.errorMessage.getOrElse("-")}</td>
    </tr>
  }

  private def timelineTable(events: Seq[NativeIOEvent]): Seq[Node] = {
    Seq(<div>
      <h4>Timeline</h4>
      <table class="table table-striped table-condensed">
        <thead>
          <tr>
            <th>Time</th>
            <th>Event</th>
            <th>Operation</th>
            <th>Phase</th>
            <th>Task</th>
            <th>Executor</th>
            <th>File</th>
            <th>Details</th>
          </tr>
        </thead>
        <tbody>
          {events.reverse.map { event =>
            <tr>
              <td>{UIUtils.formatDate(event.eventTime())}</td>
              <td>{event.eventType().name()}</td>
              <td>{event.operationName()}<br/><small>{event.operationId()}</small></td>
              <td>{option(event.phase()).map(_.name()).getOrElse("-")}</td>
              <td>{taskName(event)}</td>
              <td>{option(event.executorId()).getOrElse("-")}</td>
              <td>{option(event.filePath()).orElse(option(event.outputPath())).getOrElse("-")}</td>
              <td>{details(event)}</td>
            </tr>
          }}
        </tbody>
      </table>
    </div>)
  }

  private def details(event: NativeIOEvent): NodeSeq = {
    val values = Seq(
      "object" -> option(event.objectOperation()),
      "request" -> option(event.objectRequestId()),
      "duration" -> option(event.durationMs()).map(value => formatDuration(value.longValue())),
      "rows" -> option(event.rows()).map(_.toString),
      "bytes" -> option(event.bytes()).map(value => formatBytes(value.longValue())),
      "queueDepth" -> option(event.queueDepth()).map(_.toString),
      "runtimeThreads" -> option(event.runtimeThreads()).map(_.toString),
      "nativeMemory" -> option(event.nativeMemoryBytes()).map(value => formatBytes(value.longValue())),
      "peakBuffered" -> option(event.peakBufferedBytes()).map(value => formatBytes(value.longValue())),
      "metrics" -> option(event.metricsJson()).map(truncate),
      "error" -> option(event.errorMessage()))
      .flatMap {
        case (name, Some(value)) => Some(name + "=" + value)
        case _ => None
      }
    if (values.isEmpty) {
      Text("-")
    } else {
      Text(values.mkString(", "))
    }
  }

  private def stageName(state: NativeIOOperationState): String = {
    state.stageId.map { stageId =>
      stageId + "." + state.stageAttemptId.getOrElse(0)
    }.getOrElse("-")
  }

  private def taskName(state: NativeIOOperationState): String = {
    state.taskAttemptId.map { taskAttemptId =>
      state.taskIndex.map(_ + " / ").getOrElse("") + taskAttemptId
    }.getOrElse("-")
  }

  private def taskName(event: NativeIOEvent): String = {
    option(event.taskAttemptId()).map { taskAttemptId =>
      option(event.taskIndex()).map(_.toString + " / ").getOrElse("") + taskAttemptId
    }.getOrElse("-")
  }

  private def formatDuration(ms: Long): String = UIUtils.formatDuration(ms)

  private def formatBytes(bytes: Long): String = {
    if (bytes < 1024L) {
      bytes + " B"
    } else if (bytes < 1024L * 1024L) {
      "%.1f KiB".format(bytes.toDouble / 1024L)
    } else if (bytes < 1024L * 1024L * 1024L) {
      "%.1f MiB".format(bytes.toDouble / (1024L * 1024L))
    } else {
      "%.1f GiB".format(bytes.toDouble / (1024L * 1024L * 1024L))
    }
  }

  private def option[T](value: T): Option[T] = {
    if (value == null) {
      None
    } else {
      Some(value)
    }
  }

  private def runtimeDetails(state: NativeIOOperationState): NodeSeq = {
    val values = Seq(
      "threads" -> state.runtimeThreads.map(_.toString),
      "queueDepth" -> state.queueDepth.map(_.toString))
      .flatMap {
        case (name, Some(value)) => Some(name + "=" + value)
        case _ => None
      }
    if (values.isEmpty) {
      Text("-")
    } else {
      Text(values.mkString(", "))
    }
  }

  private def memoryDetails(state: NativeIOOperationState): NodeSeq = {
    val values = Seq(
      "native" -> state.nativeMemoryBytes.map(formatBytes),
      "peakBuffered" -> state.peakBufferedBytes.map(formatBytes))
      .flatMap {
        case (name, Some(value)) => Some(name + "=" + value)
        case _ => None
      }
    if (values.isEmpty) {
      Text("-")
    } else {
      Text(values.mkString(", "))
    }
  }

  private def truncate(value: String): String = {
    if (value.length <= 240) {
      value
    } else {
      value.substring(0, 240) + "...(truncated)"
    }
  }
}
