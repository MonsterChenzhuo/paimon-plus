---
title: Paimon Diagnostics UI
weight: 10
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

# Paimon Diagnostics UI

Paimon Diagnostics UI is a Spark UI extension for production Paimon jobs. It starts from a simple
question: when a Spark application is still running but task metrics do not explain the stall, can an
operator see the executor, the thread stack shape, and the profiler output without logging into every
machine?

## Scenario

Long-running Spark jobs that read or export large Paimon tables often fail slowly. The Spark SQL page
shows stages and tasks, but it does not answer the operational questions that matter during an
incident:

- Which executor is still active?
- Is the executor blocked in Spark scheduling, object storage, Netty, or user code?
- Can we get a flame graph from the driver or executors before the evidence disappears?
- Can an AI agent use the same evidence without scraping driver logs?

The screenshots below show the workflow that Paimon Plus exposes directly inside Spark UI.

<figure class="engineering-figure">
  <img src="../../img/engineering/paimon-diagnostics-executors.svg" alt="Paimon Diagnostics executor overview in Spark UI">
  <figcaption>Paimon Diagnostics adds a Spark UI tab that lists executors, task counters, GC time, and one-click thread dump flame graph entry points.</figcaption>
</figure>

## Problem

The default Spark UI is excellent for stage-level progress, but Paimon incidents frequently require
lower-level evidence:

- Executor thread dumps are usually collected manually and too late.
- Async-profiler output may exist on driver or executor disks, but users need a stable UI path to
discover and open it.
- Spark History Server cannot help when evidence only lives in ephemeral executor local files.
- Java 8 deployments cannot rely on JFR-to-flame conversion in-process.

The result is a noisy loop: check Spark UI, grep logs, ask for thread dumps, find the right executor,
copy profiler files, then manually correlate everything.

## Technical Design

Paimon Plus keeps the feature inside Spark instead of building a separate diagnostics service.

The design has three surfaces:

- **Executors page**: enumerate Spark executors and link each executor to a live thread dump flame graph.
- **Thread dump flame graph page**: render sampled thread stacks as a flame graph using inline UI assets.
- **Profiler page**: list async-profiler artifacts from configured DFS output and embed selected HTML flame graphs.

This keeps the security and lifecycle model aligned with Spark UI, while still giving operators a
Paimon-specific diagnostic path.

<figure class="engineering-figure">
  <img src="../../img/engineering/paimon-thread-dump-flamegraph.svg" alt="Paimon thread dump flame graph page">
  <figcaption>The per-executor thread dump page turns stack traces into a compact flame graph, making blocked Netty, Spark RPC, scheduler, and user-code paths visible at a glance.</figcaption>
</figure>

## Implementation

The implementation lives in the Spark 3.4 module:

- `org.apache.spark.ui.PaimonDiagnosticsTabSupport` attaches the tab to Spark UI, including delayed
  attachment when Spark UI is not ready at plugin startup.
- `PaimonDiagnosticsPage` renders the executor overview and links to executor flame graphs.
- `PaimonThreadDumpFlamegraphPage` collects live executor thread dumps and renders the flame graph.
- `PaimonProfilerPage` lists async-profiler outputs and embeds selected HTML flame graphs.
- `PaimonSparkAsyncProfiler` starts async-profiler from the Spark plugin and periodically uploads
  snapshots to DFS.
- `PaimonProfilerArtifacts` resolves safe profiler artifact names, lists output files, and loads HTML
  or JFR artifacts for display.

The profiler path is configurable through Spark conf:

```properties
spark.plugins=org.apache.paimon.spark.diagnostics.profiler.PaimonProfilerPlugin
spark.paimon.diagnostics.ui.enabled=true
spark.paimon.profiler.executor.enabled=true
spark.paimon.profiler.executor.fraction=0.2
spark.paimon.profiler.dfsDir=obs://bucket/paimon-profiler
spark.paimon.profiler.outputSuffix=html
spark.paimon.profiler.asyncProfiler.args=event=wall,interval=10ms
```

<figure class="engineering-figure">
  <img src="../../img/engineering/paimon-diagnostics-profiler-output.svg" alt="Paimon async-profiler output file list and flame graph">
  <figcaption>The profiler page lists uploaded driver and executor artifacts, then embeds the selected async-profiler HTML flame graph in the same Spark UI tab.</figcaption>
</figure>

## PRs / Commits

The diagnostics UI work landed as a series of focused commits:

| Commit | What changed |
| --- | --- |
| [`de1cb99`](https://github.com/MonsterChenzhuo/paimon-plus/commit/de1cb99) | Add the initial Paimon diagnostics profiler UI. |
| [`f32b45e`](https://github.com/MonsterChenzhuo/paimon-plus/commit/f32b45e) | Attach the diagnostics UI from the Spark profiler plugin. |
| [`8184cc9`](https://github.com/MonsterChenzhuo/paimon-plus/commit/8184cc9) | Inline diagnostics UI assets so the flame graph view works inside Spark UI. |
| [`c87ef70`](https://github.com/MonsterChenzhuo/paimon-plus/commit/c87ef70) | Defer diagnostics tab attachment until Spark UI startup. |
| [`8ec8248`](https://github.com/MonsterChenzhuo/paimon-plus/commit/8ec8248) | Show async-profiler artifacts in the diagnostics UI. |
| [`f59c4e9`](https://github.com/MonsterChenzhuo/paimon-plus/commit/f59c4e9) | Upload HTML profiler snapshots after async-profiler dump. |
| [`f6a8d8a`](https://github.com/MonsterChenzhuo/paimon-plus/commit/f6a8d8a) | Fix async-profiler snapshot commands. |

## Result

The incident workflow becomes shorter and more deterministic:

- Open Spark UI.
- Click **Paimon-diagnostics**.
- Identify active executors and open thread dump flame graphs.
- Open profiler artifacts for driver or sampled executors.
- Hand the same evidence to a human or AI agent without copying files from cluster machines.

This does not replace Spark SQL, Stage, or Executor pages. It adds the missing Paimon-specific
evidence layer on top of them.

## Next

The next diagnostics step is to connect this UI with structured APIs:

- expose executor and profiler evidence as JSON;
- connect thread dump and profiler artifacts to `spark-cli`;
- let AI agents produce a root-cause summary from Spark UI and Paimon Diagnostics evidence;
- fold Native IO phase events into the same page family when native export is enabled.
