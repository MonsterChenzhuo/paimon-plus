# spark-cli Codex Blog Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add an English Engineering Notes article that explains how Codex uses spark-cli to diagnose online Spark and Paimon jobs.

**Architecture:** Keep the content inside the existing Hugo Engineering Notes section. Reuse the current blog page classes, add one static SVG workflow diagram, and update the homepage plus Engineering Notes index to link the new article.

**Tech Stack:** Hugo Markdown content, static SVG asset, existing Paimon Plus SCSS.

---

### Task 1: Verify Missing Content Contract

**Files:**
- Check: `docs/content/engineering-notes/spark-cli-codex.md`
- Check: `docs/content/_index.md`

- [x] **Step 1: Run the failing content check**

```bash
test -f docs/content/engineering-notes/spark-cli-codex.md && \
  grep -q "Codex" docs/content/engineering-notes/spark-cli-codex.md && \
  grep -q "spark-cli config cluster add" docs/content/engineering-notes/spark-cli-codex.md && \
  grep -q "Spark History Server" docs/content/engineering-notes/spark-cli-codex.md && \
  grep -q "YARN" docs/content/engineering-notes/spark-cli-codex.md && \
  grep -q "EventLog" docs/content/engineering-notes/spark-cli-codex.md && \
  grep -q "spark-cli-codex" docs/content/_index.md
```

Expected: FAIL because the article and homepage link do not exist yet.

### Task 2: Add Article and Workflow Asset

**Files:**
- Create: `docs/content/engineering-notes/spark-cli-codex.md`
- Create: `docs/static/img/engineering/spark-cli-codex-loop.svg`

- [x] **Step 1: Add the article**

Write an English article with these sections:

- scenario: why Codex needs evidence rather than screenshots;
- install: one-line installer, non-sudo install, self-update;
- initialization: `spark-cli config init`, `spark-cli config cluster add`, History Server, YARN gateway, and EventLog object-storage context;
- runbook: `config show`, `driver-thread-dump`, `diagnose`, `app-summary`, `slow-stages`, `yarn-logs`, and `paimon-diagnostics`;
- Codex prompt template;
- Paimon export example;
- implementation map.

- [x] **Step 2: Add the SVG workflow**

Create a static SVG showing Codex, spark-cli, Spark History Server, YARN, EventLog storage, and Paimon diagnostics.

### Task 3: Wire Navigation

**Files:**
- Modify: `docs/content/_index.md`
- Modify: `docs/content/engineering-notes/_index.md`

- [x] **Step 1: Link the new article from the homepage**

Change the `spark-cli Analysis` Engineering Notes card from a muted placeholder into a clickable link to `engineering-notes/spark-cli-codex/`.

- [x] **Step 2: Link the new article from Engineering Notes index**

Add a second article card titled `spark-cli with Codex`, and keep `Native IO Export` as the remaining coming-next item.

### Task 4: Verify

**Files:**
- Check: all modified docs files

- [x] **Step 1: Run content contract**

```bash
test -f docs/content/engineering-notes/spark-cli-codex.md && \
  grep -q "Codex" docs/content/engineering-notes/spark-cli-codex.md && \
  grep -q "spark-cli config cluster add" docs/content/engineering-notes/spark-cli-codex.md && \
  grep -q "Spark History Server" docs/content/engineering-notes/spark-cli-codex.md && \
  grep -q "YARN" docs/content/engineering-notes/spark-cli-codex.md && \
  grep -q "EventLog" docs/content/engineering-notes/spark-cli-codex.md && \
  grep -q "spark-cli-codex" docs/content/_index.md
```

Expected: PASS.

- [x] **Step 2: Run whitespace check**

```bash
git diff --check -- docs/content/_index.md docs/content/engineering-notes docs/static/img/engineering docs/superpowers/plans/2026-05-28-spark-cli-codex-blog.md
```

Expected: PASS.

- [x] **Step 3: Build Hugo site**

```bash
cd docs && /tmp/paimon-plus-hugo/hugo --gc --minify --baseURL "http://localhost:1313/paimon-plus/"
```

Expected: PASS. The existing Google Analytics async template warning may remain.
