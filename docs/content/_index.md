---
title: Paimon Plus
type: docs
bookToc: false
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

<div class="paimon-plus-home">
<section class="pp-hero">
<div class="pp-hero-copy">
<div class="pp-eyebrow">Spark-native lakehouse execution layer</div>
<h1><span>Paimon</span> <span>Plus</span></h1>
<p class="pp-hero-lead">
Native acceleration, wide-table export, and AI-ready diagnostics for Apache Paimon based
production workloads.
</p>
<p class="pp-hero-body">
Paimon Plus keeps the Apache Paimon table format as the foundation, then adds the missing
operational layer for Spark: Rust powered Native IO, feature-table Parquet export, Spark UI
observability, structured diagnostics APIs, and a spark-cli workflow built for AI agents.
</p>
<div class="pp-actions">
<a class="pp-button pp-button-primary" href="engineering-notes/">Start with Spark</a>
<a class="pp-button pp-button-secondary" href="https://github.com/MonsterChenzhuo/paimon-plus">View GitHub</a>
</div>
<div class="pp-hero-tags" aria-label="Paimon Plus focus areas">
<span>Native IO</span>
<span>Feature Export</span>
<span>Spark UI</span>
<span>Diagnostics API</span>
<span>AI Ops</span>
</div>
</div>

<div class="pp-hero-visual" aria-label="Paimon Plus execution cockpit">
<div class="pp-console">
<div class="pp-console-top">
<span class="pp-dot pp-dot-red"></span>
<span class="pp-dot pp-dot-yellow"></span>
<span class="pp-dot pp-dot-green"></span>
<span class="pp-console-title">Native IO Control Plane</span>
</div>
<div class="pp-console-grid">
<div class="pp-metric">
<span>fast path</span>
<strong>native</strong>
</div>
<div class="pp-metric">
<span>schema width</span>
<strong>5000+</strong>
</div>
<div class="pp-metric">
<span>signal</span>
<strong>API</strong>
</div>
</div>
<div class="pp-flow">
<span>Paimon table</span>
<i></i>
<span>export_parquet</span>
<i></i>
<span>Rust Native IO</span>
<i></i>
<span>AI diagnosis</span>
</div>
<pre><code>CALL sys.export_parquet(
table =&gt; 'feature.user_profile',
columns =&gt; '*',
output_path =&gt; 'obs://lake/export/user_profile',
parallelism =&gt; 256,
target_file_size =&gt; '128 MB'
);</code></pre>
<div class="pp-status-board">
<div>
<span>OBS read</span>
<b style="width: 78%"></b>
</div>
<div>
<span>Parquet decode</span>
<b style="width: 52%"></b>
</div>
<div>
<span>Native write</span>
<b style="width: 64%"></b>
</div>
</div>
</div>
</div>
</section>

<section class="pp-proof-grid" aria-label="Paimon Plus proof points">
<div class="pp-proof">
<span>01</span>
<strong>Native execution where it matters</strong>
<p>Move hot Spark read and export paths across the JVM boundary into a Rust native library.</p>
</div>
<div class="pp-proof">
<span>02</span>
<strong>Feature-table export at width</strong>
<p>Export thousands of columns directly to Parquet for feature loading and offline training.</p>
</div>
<div class="pp-proof">
<span>03</span>
<strong>Every slow task leaves evidence</strong>
<p>Expose phase, file, object request, fallback reason, and bottleneck verdict in Spark UI.</p>
</div>
<div class="pp-proof">
<span>04</span>
<strong>AI can operate the lakehouse</strong>
<p>Structured APIs and spark-cli turn Spark jobs into inspectable, machine-readable cases.</p>
</div>
</section>

<section class="pp-section">
<div class="pp-section-head">
<span class="pp-eyebrow">Major capabilities</span>
<h2>Built for the hard Spark workloads around Paimon</h2>
<p>
Paimon Plus focuses on the production gaps that show up after the table format works:
moving bytes faster, exporting very wide tables safely, and explaining what happened when
a Spark job stalls inside native or object-storage code.
</p>
</div>

<div class="pp-feature-grid">
<article class="pp-feature-card pp-feature-wide">
<div class="pp-feature-kicker">Native IO</div>
<h3>Rust powered read and export acceleration</h3>
<p>
The <code>paimon-native-io</code> module packages a Rust native library behind JNR, returns Arrow
batches to Spark, and keeps OBS options flowing from Spark, Paimon options, or environment
variables.
</p>
</article>
<article class="pp-feature-card">
<div class="pp-feature-kicker">export_parquet</div>
<h3>Feature wide-table export</h3>
<p>
<code>CALL sys.export_parquet</code> writes Paimon tables into external Parquet directories with
projection, filtering, partitioned output, compact output, manifests, and target file
sizing for downstream feature loading.
</p>
</article>
<article class="pp-feature-card">
<div class="pp-feature-kicker">Spark UI</div>
<h3>Native IO runtime cockpit</h3>
<p>
A Native IO tab surfaces active operations, stuck phases, slow files, executor context,
object-storage requests, and History Server replay for post-run investigation.
</p>
</article>
<article class="pp-feature-card">
<div class="pp-feature-kicker">Diagnostics</div>
<h3>AI-friendly Paimon diagnostics</h3>
<p>
REST endpoints expose session state, SQL summaries, detail breakdowns, timelines, reject
reasons, and bottleneck verdicts so tools can diagnose without parsing screenshots or logs.
</p>
</article>
<article class="pp-feature-card pp-feature-wide">
<div class="pp-feature-kicker">spark-cli</div>
<h3>A CLI surface designed for AI Spark analysis</h3>
<p>
<a href="https://github.com/MonsterChenzhuo/spark-cli">spark-cli</a> is the companion
command-line tool for AI agents that inspect Paimon Spark jobs, collect execution evidence,
and turn diagnostics APIs into concrete tuning actions.
</p>
</article>
</div>
</section>

<section class="pp-pipeline">
<div class="pp-section-head pp-section-head-light">
<span class="pp-eyebrow">Execution path</span>
<h2>From Paimon table to AI-readable root cause</h2>
</div>
<div class="pp-pipeline-grid">
<div class="pp-pipeline-step">
<span>1</span>
<h3>Plan from Spark</h3>
<p>Spark procedures and readers build Paimon scans with projection, filters, and split metadata.</p>
</div>
<div class="pp-pipeline-step">
<span>2</span>
<h3>Preflight native</h3>
<p>Driver checks raw Parquet convertibility, compression, OBS configuration, memory limits, and fallback policy.</p>
</div>
<div class="pp-pipeline-step">
<span>3</span>
<h3>Execute in Rust</h3>
<p>Native workers read object storage, decode Parquet, apply delete vectors and filters, then write optimized output.</p>
</div>
<div class="pp-pipeline-step">
<span>4</span>
<h3>Explain the job</h3>
<p>Spark UI, History Server, diagnostics APIs, and spark-cli expose the exact phase, evidence, and action list.</p>
</div>
</div>
</section>

<section class="pp-section pp-capabilities">
<div class="pp-section-head">
<span class="pp-eyebrow">More Plus work</span>
<h2>Large features get the spotlight, small fixes remove production drag</h2>
<p>
The big items above are the product surface. Paimon Plus also carries focused fixes that make
wide schemas and operational exports less fragile in real Spark deployments.
</p>
</div>
<div class="pp-capability-layout">
<div class="pp-capability-panel">
<h3>Big themes</h3>
<ul>
<li>Native IO for Spark read and export hot paths.</li>
<li>Wide feature-table export for downstream feature loading scenarios.</li>
<li>Native IO UI and diagnostics APIs for live Spark UI and Spark History Server.</li>
<li>Paimon diagnostics that can be consumed directly by spark-cli and AI agents.</li>
</ul>
</div>
<div class="pp-capability-panel pp-chip-panel">
<h3>Smaller but important</h3>
<div class="pp-chip-list">
<span>wide schema validation cache</span>
<span>wide-row page-size checks</span>
<span>OBS config propagation</span>
<span>native fallback reasons</span>
<span>export manifests</span>
<span>partitioned output</span>
<span>compact output</span>
<span>target file sizing</span>
<span>delete-vector pushdown</span>
<span>predicate JSON bridge</span>
</div>
</div>
</div>
</section>

<section class="pp-section pp-notes">
<div class="pp-section-head">
<span class="pp-eyebrow">Engineering Notes</span>
<h2>Scenario, design, implementation, and commits</h2>
<p>
The product surface is only the entry point. Engineering Notes go deeper into the production
scenario, the technical route, the concrete implementation, the related commits, and the problems
each feature actually removes.
</p>
</div>
<div class="pp-notes-grid">
<a class="pp-note-card pp-note-card-primary" href="engineering-notes/paimon-diagnostics-ui/">
<span>Case study</span>
<h3>Paimon Diagnostics UI</h3>
<p>How Paimon Plus adds Spark UI pages for executors, thread dump flame graphs, and async-profiler outputs.</p>
<strong>Read the note</strong>
</a>
<article class="pp-note-card">
<span>Coming next</span>
<h3>Native IO Export</h3>
<p>Why wide feature-table export needs driver preflight, raw Parquet split planning, DV pushdown, and native fallback evidence.</p>
</article>
<article class="pp-note-card">
<span>Coming next</span>
<h3>spark-cli Analysis</h3>
<p>How diagnostics APIs and Spark History Server evidence can become an AI-readable root-cause workflow.</p>
</article>
</div>
</section>
</div>
