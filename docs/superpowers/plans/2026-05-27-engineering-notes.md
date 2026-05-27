# Engineering Notes Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add an Engineering Notes blog-style documentation section and seed it with a Paimon Diagnostics UI case study that uses the provided Spark UI screenshots as visual evidence.

**Architecture:** Keep the Hugo Book theme and the existing product homepage. Add a new `docs/content/engineering-notes/` section for article-style pages, link it from the homepage, and store visual assets under `docs/static/img/engineering/`.

**Tech Stack:** Hugo Markdown, raw HTML-enabled homepage, SVG static assets, scoped SCSS in `docs/assets/_custom.scss`.

---

### Task 1: Red Content Contract

**Files:**
- Read: `docs/content/_index.md`
- Read: `docs/content/engineering-notes/paimon-diagnostics-ui.md`

- [ ] **Step 1: Verify the Engineering Notes article is absent**

Run:

```bash
test -f docs/content/engineering-notes/paimon-diagnostics-ui.md && \
  grep -q "Scenario" docs/content/engineering-notes/paimon-diagnostics-ui.md && \
  grep -q "Paimon Diagnostics" docs/content/_index.md
```

Expected: FAIL because the section and homepage entry do not exist yet.

### Task 2: Add Article Section

**Files:**
- Create: `docs/content/engineering-notes/_index.md`
- Create: `docs/content/engineering-notes/paimon-diagnostics-ui.md`

- [ ] **Step 1: Add a section page and diagnostics article**

The article must follow this structure:

- Scenario
- Problem
- Technical Design
- Implementation
- PRs / Commits
- Result
- Next

### Task 3: Add Visual Assets

**Files:**
- Create: `docs/static/img/engineering/paimon-diagnostics-executors.svg`
- Create: `docs/static/img/engineering/paimon-thread-dump-flamegraph.svg`
- Create: `docs/static/img/engineering/paimon-diagnostics-profiler-output.svg`

- [ ] **Step 1: Add SVG illustrations based on the provided screenshots**

Use sanitized, documentation-ready SVGs that match the three screenshot roles:

- Paimon Diagnostics executor overview
- Per-executor thread dump flame graph
- Async-profiler output file list and embedded flame graph

### Task 4: Homepage Entry

**Files:**
- Modify: `docs/content/_index.md`
- Modify: `docs/assets/_custom.scss`

- [ ] **Step 1: Add an Engineering Notes section to the homepage**

Add cards for:

- Paimon Diagnostics UI
- Native IO Export
- spark-cli Analysis

### Task 5: Verification

**Files:**
- Verify: `docs/public/engineering-notes/paimon-diagnostics-ui/index.html`

- [ ] **Step 1: Re-run content contract**

Run:

```bash
test -f docs/content/engineering-notes/paimon-diagnostics-ui.md && \
  grep -q "Scenario" docs/content/engineering-notes/paimon-diagnostics-ui.md && \
  grep -q "Paimon Diagnostics" docs/content/_index.md
```

Expected: PASS.

- [ ] **Step 2: Build docs**

Run:

```bash
cd docs && /tmp/paimon-plus-hugo/hugo --gc --minify --baseURL "http://localhost:1313/paimon-plus/"
```

Expected: PASS.
