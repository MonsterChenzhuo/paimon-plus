# Paimon Native IO Compliance Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make the Native IO PoC auditable for Apache licensing, NOTICE, RAT, Rust dependency inventory, and binary jar contents before any implementation is proposed for merge.

**Architecture:** Treat compliance as a separate deliverable that runs after Rust/Java packaging exists but before PR or internal release. The plan inspects migrated LakeSoul and OBS files, generated/native artifacts, Maven shade contents, Rust crate licenses, and release-source boundaries, then records decisions in a compliance report.

**Tech Stack:** Maven RAT, Maven dependency plugin, shade plugin metadata, Cargo metadata, cargo-deny or equivalent license inventory, shell source scans.

---

### File Structure

- Create: `docs/superpowers/compliance/2026-05-18-paimon-native-io-compliance.md`
- Modify: `pom.xml` and/or `paimon-native-io/pom.xml` RAT/shade configuration if needed.
- Modify: `LICENSE` and `NOTICE` if migrated or bundled dependencies require it.
- Create: `paimon-native-io/rust/deny.toml` if `cargo-deny` is selected.

### Task 1: Source License Inventory

**Files:**
- Create: `docs/superpowers/compliance/2026-05-18-paimon-native-io-compliance.md`

- [ ] **Step 1: List migrated source files**

Run:

```bash
PAIMON_ROOT=/Users/opay-20240095/IdeaProjects/nativeio/paimon
find "$PAIMON_ROOT/paimon-native-io" -path '*/target/*' -prune -o -type f -print | sort
```

Expected: list includes Java wrappers, Rust crates, OBS migrated files, service file, and no generated cargo target files.

- [ ] **Step 2: Check Apache headers**

Run:

```bash
cd $PAIMON_ROOT
find paimon-native-io -path '*/target/*' -prune -o \
  \( -name '*.java' -o -name '*.rs' -o -name 'Cargo.toml' -o -name 'pom.xml' \) -type f -print \
  | xargs grep -L "Licensed to the Apache Software Foundation" || true
```

Expected: every source file either has the ASF header or is listed in the compliance report with a reason and RAT exclusion.

- [ ] **Step 3: Record LakeSoul and OBS provenance**

In the compliance report, add:

```markdown
## Source Provenance

- LakeSoul migrated files: <list>
- LakeSoul license header status: pass/fail
- obs-rust-sdk migrated files: <list>
- obs-rust-sdk source licenses observed: Apache-2.0/MIT or exact values
- Decision: compatible / blocked
```

- [ ] **Step 4: Commit**

```bash
git add docs/superpowers/compliance/2026-05-18-paimon-native-io-compliance.md
git commit -m "docs(native-io): record source license inventory"
```

### Task 2: Rust Dependency License Inventory

**Files:**
- Create: `paimon-native-io/rust/deny.toml`
- Modify: `docs/superpowers/compliance/2026-05-18-paimon-native-io-compliance.md`

- [ ] **Step 1: Add `cargo-deny` policy**

Use a minimal policy that allows Apache-2.0, MIT, BSD-2-Clause, BSD-3-Clause, ISC, Unicode-DFS-2016, and other licenses already accepted by the Paimon project. Any unknown license must fail review.

- [ ] **Step 2: Generate license inventory**

Run:

```bash
cd $PAIMON_ROOT/paimon-native-io/rust
cargo metadata --format-version 1 > /tmp/paimon-native-io-cargo-metadata.json
cargo deny check licenses | tee /tmp/paimon-native-io-cargo-deny.txt
```

Expected: PASS or an explicit blocked dependency list copied into the compliance report.

- [ ] **Step 3: Decide `Cargo.lock` policy**

Record one decision:

```markdown
## Cargo.lock

Decision: commit Cargo.lock for internal PoC reproducibility / do not commit for ASF library release.
Benchmark env requirement: record cargo lock hash or cargo metadata summary.
```

- [ ] **Step 4: Commit**

```bash
git add paimon-native-io/rust/deny.toml docs/superpowers/compliance/2026-05-18-paimon-native-io-compliance.md
git commit -m "build(native-io): add rust license inventory policy"
```

### Task 3: RAT and Source Release Boundary

**Files:**
- Modify: `pom.xml`
- Modify: `paimon-native-io/pom.xml`
- Modify: `docs/superpowers/compliance/2026-05-18-paimon-native-io-compliance.md`

- [ ] **Step 1: Run RAT**

Run:

```bash
cd $PAIMON_ROOT
mvn -pl paimon-native-io -am -Drat.skip=false apache-rat:check
```

Expected: PASS, or failures are limited to binary/generated files that are explicitly excluded with rationale.

- [ ] **Step 2: Verify source release excludes native binaries**

Run:

```bash
cd $PAIMON_ROOT
find paimon-native-io -path '*/target/*' -prune -o \( -name '*.so' -o -name '*.dylib' -o -name '*.dll' \) -print
```

Expected: no native binaries under source tree.

- [ ] **Step 3: Record release boundary**

Add to compliance report:

```markdown
## Source/Binary Boundary

- Source release contains Rust/Java source only.
- Prebuilt `.so/.dylib` appear only in internal binary artifacts or classifier/profile outputs.
- Cargo build outputs remain under `target/`.
```

- [ ] **Step 4: Commit**

```bash
git add pom.xml paimon-native-io/pom.xml docs/superpowers/compliance/2026-05-18-paimon-native-io-compliance.md
git commit -m "build(native-io): define rat and source release boundary"
```

### Task 4: Binary Jar NOTICE and Dependency Contents

**Files:**
- Modify: `paimon-native-io/pom.xml`
- Modify: `LICENSE`
- Modify: `NOTICE`
- Modify: `docs/superpowers/compliance/2026-05-18-paimon-native-io-compliance.md`

- [ ] **Step 1: Inspect with-dependencies jar**

Run:

```bash
cd $PAIMON_ROOT
mvn -pl paimon-native-io -am -Pnative-io -DskipTests package
jar tf paimon-native-io/target/*with-dependencies*.jar | sort > /tmp/paimon-native-io-jar-contents.txt
grep -E 'jnr|jffi|com/kenai/jffi|META-INF/services|libpaimon_native_io' /tmp/paimon-native-io-jar-contents.txt
```

Expected: JNR/JFFI runtime resources, service files, and native resource are present.

- [ ] **Step 2: Verify excluded main dependencies**

Run:

```bash
grep -E 'org/apache/spark/|org/apache/arrow/|org/apache/paimon/CoreOptions.class' /tmp/paimon-native-io-jar-contents.txt || true
```

Expected: no Spark classes, no Arrow Java classes, and no Paimon main classes shaded into the native with-dependencies jar.

- [ ] **Step 3: Merge NOTICE/LICENSE if required**

If `jnr-ffi`, `jffi`, copied LakeSoul source, copied OBS source, or Rust binary distribution requires additional entries, update `LICENSE` and `NOTICE`. Record the exact entries in the compliance report.

- [ ] **Step 4: Commit**

```bash
git add paimon-native-io/pom.xml LICENSE NOTICE docs/superpowers/compliance/2026-05-18-paimon-native-io-compliance.md
git commit -m "build(native-io): verify binary jar license contents"
```

### Final Verification

- [ ] **Step 1: Run compliance command set**

Run:

```bash
cd $PAIMON_ROOT
mvn -pl paimon-native-io -am -Drat.skip=false apache-rat:check
cd paimon-native-io/rust && cargo deny check licenses
```

Expected: PASS.

- [ ] **Step 2: Confirm report has no unresolved blockers**

Run:

```bash
grep -n "blocked\\|unknown\\|unresolved" "$PAIMON_ROOT/docs/superpowers/compliance/2026-05-18-paimon-native-io-compliance.md" || true
```

Expected: no unresolved blocker remains. If a blocker remains, do not publish or merge the native IO PoC.

### Self-Review Checklist

- [ ] Migrated LakeSoul and OBS files have compatible license provenance.
- [ ] Rust dependencies have a generated license inventory.
- [ ] RAT passes or exclusions are explicit and justified.
- [ ] Source tree contains no compiled native binary.
- [ ] with-dependencies jar includes JNR/JFFI resources but does not shade Paimon/Spark/Arrow main classes.
