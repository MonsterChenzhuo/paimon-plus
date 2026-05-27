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

package org.apache.paimon.spark.diagnostics

import org.apache.spark.status.api.v1.ThreadStackTrace

import scala.collection.mutable

case class PaimonFlamegraphNode(name: String) {

  private val children = new mutable.LinkedHashMap[String, PaimonFlamegraphNode]()
  private var value: Int = 0

  def toJsonString: String = {
    val childJson = children.values.map(_.toJsonString).mkString(",")
    "{\"name\":\"" + name + "\",\"value\":" + value + ",\"children\":[" + childJson + "]}"
  }

  private def increment(): Unit = {
    value += 1
  }

  private def child(frame: String): PaimonFlamegraphNode = {
    children.getOrElseUpdate(frame, PaimonFlamegraphNode(frame))
  }
}

object PaimonFlamegraphNode {

  def fromThreadDump(stacks: Array[ThreadStackTrace]): PaimonFlamegraphNode = {
    val root = PaimonFlamegraphNode("root")
    stacks.foreach { stack =>
      root.increment()
      var current = root
      stack.stackTrace.elems.reverse.foreach { frame =>
        val name = escapeJson(frame.split("\n").headOption.getOrElse(frame))
        if (name.nonEmpty) {
          current = current.child(name)
          current.increment()
        }
      }
    }
    root
  }

  private def escapeJson(value: String): String = {
    val builder = new StringBuilder(value.length + 16)
    value.foreach {
      case '"' => builder.append("\\\"")
      case '\\' => builder.append("\\\\")
      case '\b' => builder.append("\\b")
      case '\f' => builder.append("\\f")
      case '\n' => builder.append("\\n")
      case '\r' => builder.append("\\r")
      case '\t' => builder.append("\\t")
      case c if c < ' ' => builder.append("\\u%04x".format(c.toInt))
      case c => builder.append(c)
    }
    builder.toString()
  }
}
