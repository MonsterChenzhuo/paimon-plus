---
title: export_parquet Java Export
weight: 15
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
<a href="../../spark/procedures/#export_parquet">Spark Procedures</a>
</nav>
<div class="pp-eyebrow">Engineering Note</div>
<h1>export_parquet Java Export</h1>
<p class="pp-blog-lead">
A standard Java implementation for exporting Paimon tables into external Parquet
directories. It is built for wide feature tables, deterministic manifests, controlled
file sizing, and downstream feature-loading pipelines.
</p>
<div class="pp-blog-meta">
<span>Feature Export</span>
<span>Spark Procedure</span>
<span>Java Path</span>
</div>
</header>

<section class="pp-blog-section pp-blog-summary">
<div>
<span class="pp-blog-label">Scenario</span>
<h2>Feature loading wants Parquet, not another table job</h2>
</div>
<p>
Large offline feature tables often live in Paimon, while model training, feature loading,
and ad-hoc delivery systems want a stable Parquet directory. The slow path is to build a
huge Spark SQL projection, materialize rows through a wide query plan, and then write
the result as a separate job. <code>CALL sys.export_parquet</code> gives operators a direct export
surface: select the columns, push the simple filters into the Paimon scan, and write the
result into an external Parquet layout.
</p>
</section>

<section class="pp-blog-media">
<figure>
  <img src="../../img/engineering/export-parquet-java-flow.svg" alt="export_parquet standard Java implementation flow">
  <figcaption>The procedure keeps Spark responsible for planning and scheduling, while Java tasks read projected Paimon rows and write external Parquet files.</figcaption>
</figure>
</section>

<section class="pp-blog-section">
<h2>What problem it solves</h2>
<div class="pp-blog-grid">
<div class="pp-blog-card">
<span>Wide projection</span>
<p>Export only the required feature columns without constructing a separate Spark SQL wide projection for every downstream load.</p>
</div>
<div class="pp-blog-card">
<span>Object-store layout</span>
<p>Write to an external directory with <code>part-*.parquet</code>, <code>_SUCCESS</code>, and <code>_manifest.json</code> so later loaders can treat the export as an immutable artifact.</p>
</div>
<div class="pp-blog-card">
<span>Operational control</span>
<p>Use <code>parallelism</code>, <code>target_file_size</code>, and <code>partitioned_output</code> to keep output count and partition layout predictable.</p>
</div>
</div>
</section>

<section class="pp-blog-section">
<h2>Procedure shape</h2>
<p>
The public interface is intentionally small. The required arguments are the source table,
the output columns, and the target directory. Everything else controls filtering,
parallelism, file size, partition layout, or overwrite behavior.
</p>

<div class="pp-code-panel">
<pre><code>CALL sys.export_parquet(
  table => 'default.feature_events',
  columns => 'user_id,item_id,ctr_score,cvr_score,dt',
  output_path => 's3://feature-bucket/export/feature_events/dt=2026-05-28',
  where => "dt = '2026-05-28' and scene in ('feed', 'search')",
  parallelism => 128,
  compression => 'zstd',
  target_file_size => '256 MB',
  overwrite => true
);</code></pre>
</div>
</section>

<section class="pp-blog-section">
<h2>The Java execution path</h2>
<div class="pp-blog-steps">
<div>
<span>01</span>
<h3>Parse and validate</h3>
<p>The driver loads the table, resolves projected columns, validates the output path, and rejects unsafe overwrite cases unless explicitly enabled.</p>
</div>
<div>
<span>02</span>
<h3>Build a Paimon scan</h3>
<p>The procedure creates a Paimon predicate from simple <code>AND</code> conditions and reads the union of output columns plus filter columns.</p>
</div>
<div>
<span>03</span>
<h3>Run Spark tasks</h3>
<p>Spark parallelizes planned Paimon splits. Each task reads rows through <code>TableRead</code>, applies the projected predicate, and writes Parquet.</p>
</div>
<div>
<span>04</span>
<h3>Publish the artifact</h3>
<p>The driver writes <code>_manifest.json</code> with relative Parquet file paths, then writes <code>_SUCCESS</code> for downstream consumers.</p>
</div>
</div>
</section>

<section class="pp-blog-section">
<h2>Partitioned export</h2>
<p>
For partitioned Paimon tables, <code>partitioned_output => true</code> writes each planned
partition into its own directory under the export root. This is useful when a feature
loader expects Hive-style partition paths and wants to load a date range incrementally.
</p>

<div class="pp-code-panel">
<pre><code>CALL sys.export_parquet(
  table => 'default.feature_events',
  columns => 'user_id,item_id,ctr_score,cvr_score',
  output_path => 's3://feature-bucket/export/feature_events_range',
  where => "dt >= '2026-05-01' and dt <= '2026-05-07'",
  partitioned_output => true,
  partition_job_parallelism => 4,
  target_file_size => '256 MB',
  overwrite => true
);</code></pre>
</div>

<p>
Only partitions that appear in the planned splits are created. Empty partitions do not
produce placeholder directories, which keeps the manifest aligned with the actual data.
</p>
</section>

<section class="pp-blog-section">
<h2>File sizing</h2>
<p>
Without <code>target_file_size</code>, a non-empty Paimon split usually produces one Parquet file.
With <code>target_file_size</code>, each Spark partition uses a rolling writer and opens a new
<code>part-*.parquet</code> file when the current writer reaches the target. The value is a target,
not a strict promise, because row groups, compression ratio, and split distribution still
shape the final size.
</p>
</section>

<section class="pp-blog-section">
<h2>Output contract</h2>
<p>
The export directory is a standalone artifact. Consumers do not need to understand Paimon
metadata; they only need to read Parquet files listed by the manifest or discovered under
the output root.
</p>

<div class="pp-code-panel">
<pre><code>s3://feature-bucket/export/feature_events_range/
  dt=2026-05-01/
    part-7d1c.parquet
    _SUCCESS
  dt=2026-05-02/
    part-a4b9.parquet
    _SUCCESS
  _manifest.json
  _SUCCESS</code></pre>
</div>

<table>
<thead>
<tr><th>File</th><th>Purpose</th></tr>
</thead>
<tbody>
<tr><td><code>part-*.parquet</code></td><td>The exported feature rows.</td></tr>
<tr><td><code>_manifest.json</code></td><td>Relative Parquet paths under the export root, stable for downstream loaders.</td></tr>
<tr><td><code>_SUCCESS</code></td><td>A completion marker written after the export and optional compaction finish.</td></tr>
</tbody>
</table>
</section>

<section class="pp-blog-section">
<h2>What it deliberately does not do</h2>
<div class="pp-blog-grid">
<div class="pp-blog-card">
<span>No table commit</span>
<p>The procedure exports to an external directory. It does not create, overwrite, or commit a target Paimon table.</p>
</div>
<div class="pp-blog-card">
<span>No SQL expression engine</span>
<p>Use it for direct row and column subsets. For joins, aggregations, or derived expressions, materialize a query result first.</p>
</div>
<div class="pp-blog-card">
<span>No hidden overwrite</span>
<p>If the output directory already exists, the call fails unless <code>overwrite => true</code> is provided.</p>
</div>
</div>
</section>

<section class="pp-blog-section">
<h2>Operational diagnosis</h2>
<p>
During a long export, the driver may wait in <code>ExecutorCompletionService.take</code> while
executors are still inside <code>ExportParquetProcedure.exportSplits</code>,
<code>RollingParquetWriter.addElement</code>, or <code>ParquetWriter.write</code>. That is not enough
evidence for a driver deadlock. It usually means Spark tasks are actively encoding or
writing Parquet.
</p>
<p>
The practical check is to watch active-stage runtime, output records, output bytes, and
executor thread dumps over time. If output metrics keep moving, the export is slow but
making progress. If metrics stop and the same writer stack repeats, collect executor logs
and profiler artifacts before changing Spark or Paimon settings.
</p>
</section>

<section class="pp-blog-section">
<h2>Implementation map</h2>
<p>
The current page describes the standard Java export implementation. The useful code and
test entry points are:
</p>

| Area | Source |
| --- | --- |
| Procedure registration | [`SparkProcedures.java`](https://github.com/MonsterChenzhuo/paimon-plus/blob/main/paimon-spark/paimon-spark-common/src/main/java/org/apache/paimon/spark/SparkProcedures.java) |
| Export procedure | [`ExportParquetProcedure.java`](https://github.com/MonsterChenzhuo/paimon-plus/blob/main/paimon-spark/paimon-spark-common/src/main/java/org/apache/paimon/spark/procedure/ExportParquetProcedure.java) |
| Spark SQL behavior tests | [`ExportParquetProcedureTest.scala`](https://github.com/MonsterChenzhuo/paimon-plus/blob/main/paimon-spark/paimon-spark-ut/src/test/scala/org/apache/paimon/spark/procedure/ExportParquetProcedureTest.scala) |
| User-facing procedure docs | [`spark/procedures.md`](https://github.com/MonsterChenzhuo/paimon-plus/blob/main/docs/content/spark/procedures.md) |
</section>

<section class="pp-blog-section">
<h2>Change trail</h2>
<p>
The branch history exposes the export work as commits rather than stable public PR
numbers. These are the main Java export changes to inspect when reviewing the feature.
</p>

| Change | Why it matters |
| --- | --- |
| [`d6b9bbb`](https://github.com/MonsterChenzhuo/paimon-plus/commit/d6b9bbb) Add export parquet manifest | Adds the manifest contract used by downstream loaders. |
| [`1e7f360`](https://github.com/MonsterChenzhuo/paimon-plus/commit/1e7f360) Support partitioned parquet export output | Adds partition-aware export behavior. |
| [`673fed1`](https://github.com/MonsterChenzhuo/paimon-plus/commit/673fed1) Auto parallelize partitioned parquet export | Lets partitioned output submit independent partition jobs concurrently. |
</section>

<section class="pp-blog-section pp-blog-result">
<span class="pp-blog-label">Result</span>
<h2>A direct export surface for feature artifacts</h2>
<p>
<code>export_parquet</code> turns a Paimon table into a controlled Parquet artifact: projected
columns, pushed filters, optional partition directories, target file sizing, a manifest,
and a success marker. For feature-loading pipelines, that is the
important contract.
</p>
<a class="pp-button pp-button-primary" href="../">More Engineering Notes</a>
</section>
</article>
