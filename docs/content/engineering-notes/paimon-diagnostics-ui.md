---
title: Paimon Diagnostics UI
weight: 10
bookToc: false
plainPage: true
---
<!--
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied.  See the License for the
specific language governing permissions and limitations
under the License.
-->

<article class="pp-blog-page">
<header class="pp-blog-hero">
<nav class="pp-blog-nav" aria-label="Article navigation">
<a href="../../">Paimon Plus</a>
<a href="../">Engineering Notes</a>
<a href="https://paimon.apache.org/docs/1.4/">Official Paimon Docs</a>
</nav>
<div class="pp-eyebrow">Engineering Note</div>
<h1>Paimon Diagnostics UI</h1>
<p class="pp-blog-lead">
A Spark UI diagnostics layer for running Paimon jobs: executor stack aggregation,
thread-dump flame graphs, async-profiler artifacts, and JSON evidence that AI agents can
read without scraping screenshots.
</p>
<div class="pp-blog-meta">
<span>Published as a Paimon Plus case study</span>
<span>Spark 3.4</span>
<span>Diagnostics</span>
</div>
</header>

<section class="pp-blog-section pp-blog-summary">
<div>
<span class="pp-blog-label">Why it exists</span>
<h2>When Spark UI progress is not enough</h2>
</div>
<p>
Some Paimon read or export jobs look stuck while the usual Spark Jobs and Stages pages
do not explain the delay. Operators need lower-level runtime evidence before the
application exits: which executor is still alive, what its Java threads are doing, and
whether profiler output already points at CPU, wall-clock, lock, native, or object-storage
time.
</p>
</section>

<section class="pp-blog-section">
<h2>The incident loop this removes</h2>
<div class="pp-blog-grid">
<div class="pp-blog-card">
<span>Before</span>
<p>Open Spark UI, grep logs, ask for thread dumps, find the right executor, copy profiler files, then manually correlate stack traces with task progress.</p>
</div>
<div class="pp-blog-card">
<span>After</span>
<p>Open the Paimon diagnostics tab, pick an executor, inspect the aggregated thread-dump flame graph, then open async-profiler HTML artifacts from the same Spark UI surface.</p>
</div>
<div class="pp-blog-card">
<span>For AI agents</span>
<p>Read the JSON endpoints for executor state, top stack groups, flame graph data, profiler artifacts, and warnings. The agent no longer has to parse Spark UI HTML.</p>
</div>
</div>
</section>

<section class="pp-blog-media">
<figure>
  <img src="../../img/engineering/paimon-diagnostics-executors.svg" alt="Paimon Diagnostics executor overview in Spark UI">
  <figcaption>The diagnostics tab starts with executor context: ids, host, task counters, GC time, and one-click links to thread-dump flame graphs.</figcaption>
</figure>
</section>

<section class="pp-blog-section">
<h2>The page family</h2>
<div class="pp-blog-steps">
<div>
<span>01</span>
<h3>Executor overview</h3>
<p>Lists live executors and provides a stable path to inspect driver or executor thread dumps.</p>
</div>
<div>
<span>02</span>
<h3>Thread-dump flame graph</h3>
<p>Groups stack traces into a compact flame graph so blocked Spark RPC, scheduler, Netty, object-storage, or user-code paths are visible quickly.</p>
</div>
<div>
<span>03</span>
<h3>Profiler artifacts</h3>
<p>Lists async-profiler driver and executor output uploaded to DFS, then opens selected HTML flame graphs inside the same diagnostics tab.</p>
</div>
<div>
<span>04</span>
<h3>AI-readable JSON</h3>
<p>Exposes overview, thread dump, and profiler payloads for spark-cli, Codex, Claude Code, and other tools that need structured evidence.</p>
</div>
</div>
</section>

<section class="pp-blog-media">
<figure>
  <img src="../../img/engineering/paimon-thread-dump-flamegraph.svg" alt="Paimon thread dump flame graph page">
  <figcaption>The thread-dump view turns live stack traces into an aggregate shape, making repeated waits or hot call paths easier to see than a raw thread list.</figcaption>
</figure>
</section>

<section class="pp-blog-section">
<h2>How to enable it</h2>
<p>
The tab is installed by the Paimon Spark profiler plugin. For Java 8 clusters, prefer
HTML profiler output so the Spark UI can open async-profiler flame graphs directly.
</p>

<div class="pp-code-panel">
<pre><code>spark.plugins=org.apache.paimon.spark.diagnostics.profiler.PaimonProfilerPlugin
spark.paimon.diagnostics.ui.enabled=true
spark.paimon.profiler.executor.enabled=true
spark.paimon.profiler.executor.fraction=0.2
spark.paimon.profiler.dfsDir=obs://bucket/paimon-profiler
spark.paimon.profiler.outputSuffix=html
spark.paimon.profiler.asyncProfiler.args=event=wall,interval=10ms</code></pre>
</div>
</section>

<section class="pp-blog-media">
<figure>
  <img src="../../img/engineering/paimon-diagnostics-profiler-output.svg" alt="Paimon async-profiler output file list and flame graph">
  <figcaption>The profiler page shows uploaded artifacts and lets operators open the selected flame graph without logging into executor machines.</figcaption>
</figure>
</section>

<section class="pp-blog-section">
<h2>The JSON contract for spark-cli</h2>
<p>
The same evidence is available through Spark UI JSON routes, so tools can collect diagnostics
while the application is still running.
</p>
<div class="pp-code-panel">
<pre><code>/paimon-diagnostics/json
/paimon-diagnostics/threadDump/json?executorId=&lt;driver|executorId&gt;
/paimon-diagnostics/profiler/json</code></pre>
</div>
<p>
AI agents should start with <code>thread_dump.facts.state_counts</code>,
<code>thread_dump.facts.top_stacks</code>, <code>thread_dump.facts.flamegraph</code>,
and <code>profiler.facts.artifacts</code>. Missing endpoints or empty profiler output are
reported as warnings, not as ambiguous blank pages.
</p>
</section>

<section class="pp-blog-section">
<h2>spark-cli workflow</h2>
<p>
The companion command reads the Paimon diagnostics JSON through the YARN tracking or proxy
URL. It does not need EventLog data and is useful while the job is still running.
</p>

<div class="pp-code-panel">
<pre><code>spark-cli paimon-diagnostics application_1772605260987_20765 \
  --yarn-base-urls http://rm-or-gateway/yarn \
  --executor-id 7</code></pre>
</div>
</section>

<section class="pp-blog-section pp-blog-result">
<span class="pp-blog-label">Result</span>
<h2>A shorter path from stall to evidence</h2>
<p>
Paimon Diagnostics UI does not replace the Spark SQL, Stage, or Executor pages. It adds
the missing Paimon-specific evidence layer: stack shape, profiler artifacts, and structured
payloads that a human or AI agent can use immediately.
</p>
<a class="pp-button pp-button-primary" href="../">More Engineering Notes</a>
</section>
</article>
