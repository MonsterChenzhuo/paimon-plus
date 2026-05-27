/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

(function () {
  function depth(node) {
    if (!node.children || node.children.length === 0) {
      return 1;
    }
    return 1 + Math.max.apply(null, node.children.map(depth));
  }

  function color(name) {
    var hash = 0;
    for (var i = 0; i < name.length; i++) {
      hash = ((hash << 5) - hash) + name.charCodeAt(i);
      hash = hash & hash;
    }
    var hue = Math.abs(hash) % 360;
    return "hsl(" + hue + ", 70%, 72%)";
  }

  function title(node, rootValue) {
    var pct = rootValue === 0 ? 0 : (node.value * 100.0 / rootValue);
    return node.name + " (" + node.value + " samples, " + pct.toFixed(2) + "%)";
  }

  function addFrame(chart, node, rootValue, level, left, width) {
    var frame = document.createElement("div");
    frame.className = "paimon-flamegraph-frame";
    frame.style.backgroundColor = color(node.name);
    frame.style.left = left + "%";
    frame.style.top = (level * 22) + "px";
    frame.style.width = width + "%";
    frame.title = title(node, rootValue);
    frame.textContent = node.name;
    chart.appendChild(frame);

    if (!node.children || node.children.length === 0 || node.value === 0) {
      return;
    }

    var childLeft = left;
    node.children.forEach(function (child) {
      var childWidth = width * child.value / node.value;
      addFrame(chart, child, rootValue, level + 1, childLeft, childWidth);
      childLeft += childWidth;
    });
  }

  window.paimonDrawFlamegraph = function (dataId, chartId) {
    var dataElement = document.getElementById(dataId);
    var chart = document.getElementById(chartId);
    if (!dataElement || !chart) {
      return;
    }

    var data = JSON.parse(dataElement.textContent.trim());
    chart.innerHTML = "";
    chart.style.height = (depth(data) * 22) + "px";
    addFrame(chart, data, data.value || 0, 0, 0, 100);
  };
})();
