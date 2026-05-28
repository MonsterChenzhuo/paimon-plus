---
title: Engineering Notes
weight: 1
bookCollapseSection: true
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

<div class="pp-blog-index">
<section class="pp-blog-hero pp-blog-index-hero">
<nav class="pp-blog-nav" aria-label="Engineering Notes navigation">
<a href="../">Paimon Plus</a>
<a href="https://github.com/MonsterChenzhuo/paimon-plus">GitHub</a>
<a href="https://paimon.apache.org/docs/1.4/">Official Paimon Docs</a>
</nav>
<div class="pp-eyebrow">Engineering Notes</div>
<h1>Production notes from Paimon Plus</h1>
<p class="pp-blog-lead">
Long-form articles about the Spark runtime work around Paimon Plus: what problem
triggered the feature, how the design landed, and how operators or AI agents can use it.
</p>
</section>

<section class="pp-blog-list">
<a class="pp-blog-list-card" href="paimon-diagnostics-ui/">
<span>Diagnostics</span>
<h2>Paimon Diagnostics UI</h2>
<p>
Executor stack aggregation, thread-dump flame graphs, async-profiler artifacts, and
AI-readable JSON for Spark jobs that are still running but no longer explain themselves
through stage metrics.
</p>
<strong>Read article</strong>
</a>

<a class="pp-blog-list-card" href="spark-cli-codex/">
<span>AI Ops</span>
<h2>spark-cli with Codex</h2>
<p>
One-click install, cluster profile initialization, Spark History Server and YARN gateway
configuration, and the command sequence Codex can use to analyze online Spark jobs.
</p>
<strong>Read article</strong>
</a>

<a class="pp-blog-list-card" href="export-parquet/">
<span>Feature Export</span>
<h2>export_parquet Java Export</h2>
<p>
Standard Java export implementation, driver preflight, Paimon split planning,
partitioned output, copy compaction, and manifests for wide feature-table export.
</p>
<strong>Read article</strong>
</a>
</section>
</div>
