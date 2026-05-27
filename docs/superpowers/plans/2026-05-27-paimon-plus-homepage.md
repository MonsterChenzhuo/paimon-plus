# Paimon Plus Homepage Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace the legacy Apache Paimon-style homepage with an English Paimon Plus product homepage focused on Native IO, wide-table export, Spark UI diagnostics, and AI-oriented spark-cli workflows.

**Architecture:** Keep the existing Hugo Book theme and GitHub Pages workflow. Implement the redesign as a scoped homepage Markdown rewrite plus homepage-specific SCSS classes, avoiding Java/Scala/Rust code and unrelated documentation churn.

**Tech Stack:** Hugo 0.124.1 extended, Markdown with unsafe HTML enabled, SCSS in `docs/assets/_custom.scss`.

---

### Task 1: Content Contract

**Files:**
- Read: `docs/content/_index.md`

- [ ] **Step 1: Run the failing homepage content assertion**

Run:

```bash
test "$(grep -c 'Paimon Plus' docs/content/_index.md)" -ge 3 && \
  grep -q 'Native IO' docs/content/_index.md && \
  grep -q 'spark-cli' docs/content/_index.md && \
  ! grep -q '^# Apache Paimon$' docs/content/_index.md
```

Expected: FAIL, because the current homepage is still centered on `# Apache Paimon` and does not yet contain the new Plus product narrative.

### Task 2: Homepage Rewrite

**Files:**
- Modify: `docs/content/_index.md`

- [ ] **Step 1: Replace the homepage body**

Rewrite the homepage in English with these sections:

- Hero: `Paimon Plus`, Spark-native lakehouse operations, Native IO, AI diagnostics.
- Proof points: native acceleration, wide-table export, observability, AI workflow.
- Feature grid: Native IO, `export_parquet`, Native IO UI, Paimon diagnostics, `spark-cli`.
- Data path: table scan, procedure, native Rust fast path, Spark UI / History Server, AI analysis.
- Additional capabilities: wide schema validation, wide-row page-size checks, OBS config propagation, manifest output, partitioned export, compact output, fallback reject reasons.

### Task 3: Visual Styling

**Files:**
- Modify: `docs/assets/_custom.scss`

- [ ] **Step 1: Add homepage-specific styles**

Add scoped classes under `.paimon-plus-home` for a dark hero, compact product badges, feature cards, pipeline blocks, and responsive layout. Preserve existing documentation styles outside the homepage.

### Task 4: Site Metadata

**Files:**
- Modify: `docs/config.toml`

- [ ] **Step 1: Update Paimon Plus metadata**

Set the site title and repository links to Paimon Plus / `MonsterChenzhuo/paimon-plus` while keeping the existing Hugo theme and docs build path.

### Task 5: Verification

**Files:**
- Verify: `docs/content/_index.md`
- Verify: `docs/assets/_custom.scss`
- Verify: `docs/public/index.html`

- [ ] **Step 1: Re-run the content assertion**

Run:

```bash
test "$(grep -c 'Paimon Plus' docs/content/_index.md)" -ge 3 && \
  grep -q 'Native IO' docs/content/_index.md && \
  grep -q 'spark-cli' docs/content/_index.md && \
  ! grep -q '^# Apache Paimon$' docs/content/_index.md
```

Expected: PASS.

- [ ] **Step 2: Build the Hugo site**

Run:

```bash
cd docs && hugo --gc --minify --baseURL "http://localhost:1313/paimon-plus/"
```

Expected: PASS and `docs/public/index.html` is generated.

- [ ] **Step 3: Inspect the homepage in the browser**

Run a local Hugo server or serve `docs/public`, open the homepage in the in-app browser, and verify desktop and mobile layouts have no obvious overlap, blank hero, or clipped controls.
