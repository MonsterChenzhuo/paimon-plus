---
title: spark-cli with Codex
weight: 20
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
<a href="https://github.com/MonsterChenzhuo/spark-cli">spark-cli</a>
</nav>
<div class="pp-eyebrow">Engineering Note</div>
<h1>spark-cli with Codex</h1>
<p class="pp-blog-lead">
Use Codex as the reasoning layer and spark-cli as the evidence layer for live Spark and
Paimon production incidents. Codex asks questions; spark-cli pulls EventLogs, YARN state,
Spark UI thread dumps, and Paimon diagnostics JSON.
</p>
<div class="pp-blog-meta">
<span>AI Ops workflow</span>
<span>Spark EventLog</span>
<span>Codex</span>
</div>
</header>

<section class="pp-blog-section pp-blog-summary">
<div>
<span class="pp-blog-label">Scenario</span>
<h2>When a Spark job looks stuck, Codex needs data instead of screenshots</h2>
</div>
<p>
A Paimon export or read job may look frozen in Spark UI while it is still doing useful
work. The driver might be waiting in <code>ExportParquetProcedure</code>, executors may still be
RUNNABLE inside Parquet writers, and the EventLog may be incomplete because the
application is still running. Codex can explain that situation only when it can collect
the same signals an operator would inspect manually.
</p>
</section>

<section class="pp-blog-media">
<figure>
  <img src="../../img/engineering/spark-cli-codex-loop.svg" alt="Codex and spark-cli diagnostic workflow">
  <figcaption>Codex delegates evidence collection to spark-cli, then combines live Spark UI state, YARN metadata, EventLog aggregates, and Paimon diagnostics into one root-cause answer.</figcaption>
</figure>
</section>

<section class="pp-blog-section">
<h2>Install once on the Codex machine</h2>
<p>
The installer downloads the latest release binary and puts <code>spark-cli</code> on the local
PATH. Re-running the same command upgrades the binary.
</p>

```
curl -fsSL https://raw.githubusercontent.com/MonsterChenzhuo/spark-cli/main/scripts/install.sh | bash
spark-cli version
```

<p>
For a non-sudo Codex workspace, install into <code>$HOME/.local/bin</code> and make sure that
directory is on PATH.
</p>

```
curl -fsSL https://raw.githubusercontent.com/MonsterChenzhuo/spark-cli/main/scripts/install.sh \
  | PREFIX="$HOME/.local/bin" NO_SKILL=1 bash

export PATH="$HOME/.local/bin:$PATH"
spark-cli self-update --dry-run
```
</section>

<section class="pp-blog-section">
<h2>Initialize the environment</h2>
<p>
Codex needs three production coordinates for a Spark cluster: the Spark History Server
address, the YARN ResourceManager or gateway address, and the EventLog root used by that
History Server. In current spark-cli releases, EventLogs are read from
<code>file://</code>, <code>hdfs://</code>, or <code>shs://</code>. For object-storage EventLogs, the recommended
path is through Spark History Server: SHS owns the object-store connector and spark-cli
fetches the EventLog zip through the SHS REST API.
</p>

| Coordinate | Example | How spark-cli uses it |
| --- | --- | --- |
| Spark History Server Web UI / REST endpoint | `http://history.example.com:18081` | Stored as `shs://history.example.com:18081` in `log_dirs`; spark-cli calls `/api/v1/applications/<app>/logs`. |
| YARN Web UI / RM gateway | `http://gateway.example.com/gateway/prod/yarn` | Stored in `yarn.base_urls`; spark-cli reads RM state, tracking URLs, thread dumps, and container log links. |
| EventLog object-storage root | `obs://lake/spark-history` or `s3://lake/spark-history` | Configured in Spark History Server or mounted as `file://` / `hdfs://`; Codex does not need cloud credentials when SHS is used. |

```
spark-cli config init

spark-cli config cluster add prod \
  --log-dirs shs://history.example.com:18081 \
  --yarn-base-urls http://gateway.example.com/gateway/prod/yarn \
  --shs-timeout 10m \
  --activate

spark-cli config show --format json
```

<p>
The generated config lives at <code>~/.config/spark-cli/config.yaml</code>. A production profile
usually looks like this:
</p>

```
active_cluster: prod
clusters:
  prod:
    log_dirs:
      - shs://history.example.com:18081
    yarn:
      base_urls:
        - http://gateway.example.com/gateway/prod/yarn
    shs:
      timeout: 10m
timeout: 30s

# The Spark History Server behind shs:// is configured with the real EventLog
# root, for example obs://lake/spark-history or s3://lake/spark-history.
# That keeps cloud credentials out of the Codex runtime.
```
</section>

<section class="pp-blog-section">
<h2>The Codex runbook</h2>
<p>
Once the cluster profile exists, Codex can diagnose an application by running a small,
repeatable command sequence. The first command confirms configuration. The second command
checks live driver state before EventLog metrics are trusted. The remaining commands parse
history data and fetch deeper evidence only when needed.
</p>

<p><strong>First pass.</strong> Confirm config, inspect the live driver, then parse EventLog evidence.</p>

```
APP=application_1779838558973_1527

spark-cli config show --format json

spark-cli --cluster prod driver-thread-dump "$APP" \
  --executor-id driver \
  --thread-summary-only

spark-cli --cluster prod diagnose "$APP"
spark-cli --cluster prod app-summary "$APP"
spark-cli --cluster prod slow-stages "$APP" --top 5
```

<p><strong>Deep dive.</strong> Fetch executor logs or Paimon runtime JSON only when the first pass points there.</p>

```
spark-cli --cluster prod yarn-logs "$APP" \
  --executor-id 7 \
  --yarn-log-types stderr,gc \
  --yarn-log-bytes 131072

spark-cli --cluster prod paimon-diagnostics "$APP" \
  --executor-id 7
```
</section>

<section class="pp-blog-section">
<h2>What Codex should infer</h2>
<div class="pp-blog-grid">
<div class="pp-blog-card">
<span>Live state first</span>
<p>If the EventLog is incomplete or has zero finished tasks, Codex should not conclude the job is idle. The driver or executor thread dump is the source of truth while the application is still running.</p>
</div>
<div class="pp-blog-card">
<span>Impact over severity</span>
<p><code>diagnose.summary.top_findings_by_impact</code> ranks findings by wall share. A warning that owns half the wall time is more important than a critical finding on a tiny stage.</p>
</div>
<div class="pp-blog-card">
<span>Paimon context</span>
<p>When Paimon diagnostics is enabled, Codex should read <code>thread_dump.facts.top_stacks</code>, <code>thread_dump.facts.flamegraph</code>, and <code>profiler.facts.artifacts</code> before parsing UI HTML.</p>
</div>
</div>
</section>

<section class="pp-blog-section">
<h2>Prompt template for Codex</h2>
<p>
Keep the prompt operational. Give Codex the application id and cluster name, then tell it
to use spark-cli before making a judgment.
</p>

```
Use spark-cli to diagnose Spark application application_1779838558973_1527
on cluster prod.

First run:
1. spark-cli config show --format json
2. spark-cli --cluster prod driver-thread-dump <app> --executor-id driver --thread-summary-only
3. spark-cli --cluster prod diagnose <app>
4. spark-cli --cluster prod app-summary <app>

If the app is still running or Paimon export looks stuck, also run:
- spark-cli --cluster prod paimon-diagnostics <app> --executor-id <executorId>
- spark-cli --cluster prod yarn-logs <app> --executor-id <executorId> --yarn-log-types stderr,gc

Return:
- current application state
- strongest evidence
- likely root cause
- what is not proven yet
- next command or Spark UI check
```
</section>

<section class="pp-blog-section">
<h2>A Paimon export example</h2>
<p>
For a long-running <code>ExportParquetProcedure</code> job, Codex may see a driver stack waiting in
<code>ExecutorCompletionService.take</code>, executor stacks inside
<code>RollingParquetWriter.addElement</code> or <code>ParquetWriter.write</code>, and an incomplete
EventLog with no clear skew, GC, spill, or failed-task finding. That does not look like a
driver deadlock. It looks like active executor-side Parquet writing or encoding work.
</p>
<p>
The useful next checks are concrete: watch active-stage running time, output records,
output bytes, and executor thread dumps for the same writer stack over time. If output
metrics keep increasing, the job is slow but making progress. If they stop while the same
executor stack repeats, collect executor logs and profiler artifacts before changing Spark
or Paimon settings.
</p>
</section>

<section class="pp-blog-section">
<h2>Implementation map</h2>
<p>
The workflow is intentionally small: Codex does not need a browser plugin, SSH access to
executors, or ad-hoc log scraping. It needs a stable CLI that returns structured JSON.
</p>

| Area | spark-cli source |
| --- | --- |
| One-line installer | [`scripts/install.sh`](https://github.com/MonsterChenzhuo/spark-cli/blob/main/scripts/install.sh) |
| Interactive config initialization | [`cmd/configcmd/init.go`](https://github.com/MonsterChenzhuo/spark-cli/blob/main/cmd/configcmd/init.go) |
| Named cluster profiles | [`cmd/configcmd/cluster.go`](https://github.com/MonsterChenzhuo/spark-cli/blob/main/cmd/configcmd/cluster.go) |
| SHS EventLog fetching | [`internal/fs/shs.go`](https://github.com/MonsterChenzhuo/spark-cli/blob/main/internal/fs/shs.go) |
| YARN and thread-dump probes | [`internal/yarn`](https://github.com/MonsterChenzhuo/spark-cli/tree/main/internal/yarn) |
| Scenario commands | [`cmd/scenarios/register.go`](https://github.com/MonsterChenzhuo/spark-cli/blob/main/cmd/scenarios/register.go) |
</section>

<section class="pp-blog-section pp-blog-result">
<span class="pp-blog-label">Result</span>
<h2>Codex becomes a Spark incident operator</h2>
<p>
With spark-cli configured, Codex can move from a vague request like "this Spark job looks
stuck" to a grounded answer: live state, EventLog findings, YARN evidence, Paimon-specific
thread stacks, profiler artifacts, and a next action that names the exact signal to check.
</p>
<a class="pp-button pp-button-primary" href="../">More Engineering Notes</a>
</section>
</article>
