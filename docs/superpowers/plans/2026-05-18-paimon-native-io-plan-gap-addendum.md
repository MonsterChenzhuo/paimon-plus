# Paimon Native IO Plan Gap Addendum

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to fold these gaps back into the implementation plans before coding. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Close coverage gaps found after comparing the implementation plans with `docs/superpowers/specs/2026-05-16-paimon-native-io-poc-design.md`.

**Architecture:** Treat this document as a patch queue for the five existing plans. P0 items must be merged into the corresponding plan before execution; P1 items can be executed as follow-up tasks in the same phase; P2 items are hygiene and release-readiness tasks.

**Tech Stack:** Maven, Rust, Spark, Arrow Java, JNR, DataFusion, OBS, ASF compliance tooling.

---

## Fold-In Status

As of this update, the gaps below have been folded into the executable plans:

- Gaps 1, 4, 14, and release/license checks: `2026-05-18-paimon-native-io-compliance.md`
- Gaps 2, 3, 5, 8, 9, 12, 20, 22, and 23: `2026-05-18-paimon-native-io-java-reader.md`
- Gaps 6, 16, 17, 18, 20, and Spark source-path corrections: `2026-05-18-paimon-native-io-core-spark.md`
- Gaps 7, 8, 18, 21, 25, and 26: `2026-05-18-paimon-native-io-rust-ffi.md`
- Gaps 10, 11, 14, 15, 17, 19, 24, 25, 26, and 27: `2026-05-18-paimon-native-io-correctness-benchmark.md`
- Gap 13: `2026-05-18-paimon-native-io-spikes.md`

Keep this addendum as an audit trail. During execution, follow the concrete tasks in the target plan files rather than using this addendum as the primary task list.

## P0: Must Add Before Implementation

### Gap 1: Compliance / NOTICE / RAT Is Not Planned

**Why:** The spec has explicit Apache compliance requirements, but the current plans only mention packaging. There is no task for license inventory, NOTICE merge, RAT exclusions, `Cargo.lock` decision, or `cargo-deny`.

**Add to:** `2026-05-18-paimon-native-io-java-reader.md` or create a separate compliance plan.

- [ ] Add a `Compliance` task that checks migrated LakeSoul files preserve Apache headers.
- [ ] Add a `Compliance` task that checks migrated `obs-rust-sdk` license compatibility and records Apache/MIT dual-license handling.
- [ ] Add `cargo-deny` or equivalent license inventory generation for Rust crates.
- [ ] Add Maven RAT configuration for `rust/`, `Cargo.toml`, generated files, lockfiles, and native resources.
- [ ] Add a binary artifact NOTICE check for `jnr-ffi`, `jffi`, and bundled native resources.
- [ ] Decide whether `Cargo.lock` is committed for internal PoC reproducibility; if not, require benchmark `env.json` dependency metadata.

### Gap 2: Arrow Runtime Compatibility Is Underspecified

**Why:** The spec requires Arrow Java 15 primary validation, Arrow 18 profile smoke testing, and allocator-manager checks. Current plans only say to manage allocator lifecycle.

**Add to:** `2026-05-18-paimon-native-io-java-reader.md`

- [ ] Add tests that record Arrow Java version and allocator manager before native reader initialization.
- [ ] Add startup/deployment check that rejects incompatible allocator state instead of trying to switch after Arrow classes are initialized.
- [ ] Add a profile smoke test for Arrow Java 18.1.0 if Spark 3.5 profile is used.
- [ ] Explicitly avoid introducing `arrow-memory-netty`; continue using Paimon `arrow-memory-unsafe`.
- [ ] Add benchmark `env.json` fields for Arrow version, allocator manager, `arrow.enable_unsafe_memory_access`, and `arrow.enable_null_check_for_get`.

### Gap 3: Native Loader Must Not Reuse `JNIUtils`

**Why:** The spec warns that Paimon `JNIUtils` has a global static init semantic unsuitable for multiple native libs. Current plan does not explicitly test this.

**Add to:** `2026-05-18-paimon-native-io-java-reader.md`

- [ ] Add a loader test proving `PaimonJnrLoader` does not call `org.apache.paimon.utils.JNIUtils.load(...)`.
- [ ] Add a test for two classloaders loading the same native resource without type-incompatible JNR bindings.
- [ ] Add a test that a prior unrelated native load cannot mark `paimon_native_io` as loaded.

### Gap 4: Maven Shade / Service Merge Details Are Too Thin

**Why:** The spec requires service-resource merge and preserving `jffi` native resources. Current plan says with-dependencies jar, but not exact failure checks.

**Add to:** `2026-05-18-paimon-native-io-java-reader.md`

- [ ] Add a jar inspection test that `META-INF/services/org.apache.paimon.operation.nativeio.NativeSplitReadProviderFactory` exists after shading.
- [ ] Add a jar inspection test that `com/kenai/jffi/**` resources exist.
- [ ] Add a shade config step using `ServicesResourceTransformer` or equivalent.
- [ ] Add a test that `paimon-native-io-*-with-dependencies.jar` excludes Paimon/Spark/Arrow main artifacts but includes JNR/JFFI runtime dependencies.

### Gap 5: Runtime Cancellation / Interrupt Behavior Is Missing

**Why:** The spec requires Java checks around Spark task interrupt and bounded native blocking. Current reader plan has no interrupt/cancel tests.

**Add to:** `2026-05-18-paimon-native-io-java-reader.md`

- [ ] Add test where thread is interrupted before `nextBatchBlocked`; expect fast close and `InterruptedIOException` or `IOException`.
- [ ] Add test where interrupt happens after native returns; no half-batch should be emitted.
- [ ] Add OBS timeout fields to native options and benchmark `env.json`.
- [ ] Ensure external timeout/cancel errors use `NonCorruptFileReadException` and are not swallowed by `ignoreCorruptFiles`.

### Gap 6: `max-batch-bytes` Is Missing

**Why:** The spec defines `native-io.max-batch-bytes` as an internal/benchmark guard. Current plans only cover row batch size.

**Add to:** `2026-05-18-paimon-native-io-core-spark.md` and `2026-05-18-paimon-native-io-java-reader.md`

- [ ] Add `native-io.max-batch-bytes` parsing with default `64MB` and allowed range `[1MB, 512MB]`.
- [ ] Add Java imported-batch buffer-size estimation after `VectorSchemaRoot` import.
- [ ] Add test that a too-large string/binary batch is rejected, resources are released, and error is non-corrupt.
- [ ] Add benchmark `env.json` field for effective max batch bytes.

## P1: Important Coverage Gaps

### Gap 7: LakeSoul/DataFusion Session Details Are Incomplete

**Why:** Current Rust plan mentions `target_partitions`, round-robin, pruning, dictionary, but misses metadata cache, size `< 8` file filtering, field metadata cleanup, `schema_force_view_types=false`, and timezone session checks.

**Add to:** `2026-05-18-paimon-native-io-rust-ffi.md`

- [ ] Add `RuntimeEnv` metadata cache setup and env var `PAIMON_NATIVE_IO_FILE_META_CACHE_LIMIT`.
- [ ] Add file metadata fetch concurrency and size `< 8` filtering.
- [ ] Add schema merge helper that clears field metadata before merge.
- [ ] Add explicit `schema_force_view_types=false` session option if exposed by DataFusion 51.
- [ ] Add timestamp NTZ test under different Spark session timezones and Rust session timezone settings.

### Gap 8: OBS TOCTOU and Short Read Checks Need Stronger Tests

**Why:** Current plans mention eTag/version and range length in places, but do not require a preflight/native object consistency test or short-read classification in the main reader.

**Add to:** `2026-05-18-paimon-native-io-rust-ffi.md` and `2026-05-18-paimon-native-io-java-reader.md`

- [ ] Add native config fields for validated file size, footer row count, optional eTag/version.
- [ ] Add test where preflight succeeds then native `head` returns a different size/eTag/version; expect external consistency error.
- [ ] Add OBS `206/200` status and returned length validation for range reads.
- [ ] Add test that short read is not retried as corrupt and does not silently succeed.

### Gap 9: Arrow Type Rejection Matrix Is Not Explicit Enough

**Why:** Spec requires rejection tests for `LargeUtf8`, `LargeBinary`, dictionary vectors, `FixedSizeBinary`, row_index `UInt64/Int32/nullable`, and decimal256. Current plan only says validate Arrow type.

**Add to:** `2026-05-18-paimon-native-io-java-reader.md`

- [ ] Add tests for `LargeUtf8`, `LargeBinary`, dictionary vector, and `FixedSizeBinary`.
- [ ] Add tests for row index as `UInt64`, `Int32`, and nullable `BigIntVector`.
- [ ] Add decimal256 and decimal precision/scale mismatch tests.
- [ ] Add `TIMESTAMP_WITH_LOCAL_TIME_ZONE` explicit rejection test.

### Gap 10: Spark Dynamic Allocation Benchmark Case Missing

**Why:** Spec says benchmark must handle dynamic allocation by rechecking new executors or pinning executor set. Current benchmark plan only says executor check before benchmark.

**Add to:** `2026-05-18-paimon-native-io-correctness-benchmark.md`

- [ ] Add benchmark mode that fails fast if dynamic allocation is enabled and executor set changes without revalidation.
- [ ] Record executor set before and after every query repeat.
- [ ] Re-run native loader check for newly observed executors or mark benchmark failed.

### Gap 11: Standard Dataset Modes Are Declared But Not Planned

**Why:** Procedure API includes `tpcds` and `ssb`, but the implementation plan only builds synthetic datasets.

**Add to:** `2026-05-18-paimon-native-io-correctness-benchmark.md`

- [ ] Either add tasks for derived PK+DV TPC-DS/SSB conversion, or explicitly reject `tpcds/ssb` in phase 1 with `UNSUPPORTED_MODE`.
- [ ] Ensure reports label derived datasets and do not claim standard benchmark comparability.

### Gap 12: Parquet Footer Helper Boundary Is Risky

**Why:** The spec recommends exposing a small shaded `paimon-format` helper to avoid unshaded Parquet class conflicts. Current plan puts validator in native module but does not create the neutral helper.

**Add to:** `2026-05-18-paimon-native-io-java-reader.md`

- [ ] Add a `paimon-format` neutral footer/schema inspection helper returning Paimon POJOs/enums only.
- [ ] Add test that public/native module APIs do not expose `ParquetMetadata`, `MessageType`, or `ColumnDescriptor`.
- [ ] Add runtime classpath test with Paimon shaded parquet package to avoid unshaded references.

## P2: Hygiene / Execution Quality Gaps

### Gap 13: Plans Contain Spike Tasks With Intentional Compile Failures

**Why:** The spike plan intentionally writes failing harnesses, but it is not clearly isolated from normal implementation execution. This can confuse subagents.

**Add to:** `2026-05-18-paimon-native-io-spikes.md`

- [ ] Mark intentional-failure steps as red-phase only.
- [ ] Add a final task requiring all intentional failures to be replaced by real checks before the spike plan is considered complete.

### Gap 14: Dependency Version Drift Is Not Captured in Benchmark Output

**Why:** Spec requires benchmark `env.json` to record native resource hash and dependency lock metadata when no lockfile is committed. Current benchmark plan does not mention cargo metadata/lock hash.

**Add to:** `2026-05-18-paimon-native-io-correctness-benchmark.md`

- [ ] Add `cargo.lock.hash` or `cargo.metadata.summary` to `env.json`.
- [ ] Add native jar manifest version and Maven artifact coordinates to `env.json`.
- [ ] Fail benchmark if driver/executor native artifact versions differ.

### Gap 15: No Explicit “Do Not Compare” Enforcement

**Why:** Spec says not to compare against LakeSoul, Comet, or Spark native Parquet reader. Current benchmark plan does not prevent accidental report language drift.

**Add to:** `2026-05-18-paimon-native-io-correctness-benchmark.md`

- [ ] Add report metadata field `comparison_scope = "paimon_java_reader_vs_paimon_native_io"`.
- [ ] Add tests that generated summary text does not mention LakeSoul, Comet, ClickHouse, Doris, or Spark native Parquet as a benchmark competitor.

## Recommended Patch Order

1. Patch `java-reader` first: loader/JNR/resource packaging, Arrow runtime, interrupt, max batch bytes, Parquet helper boundary.
2. Patch `rust-ffi` next: metadata cache, file metadata filtering, TOCTOU, short reads.
3. Patch `core-spark`: `max-batch-bytes` option and any extra non-corrupt error classification.
4. Patch `correctness-benchmark`: dynamic allocation, dataset modes, dependency metadata, comparison scope.
5. Add or fold a compliance plan before any code is proposed for merge.

---

## Second-Pass Gaps: Execution Readiness

### Gap 16: Spark Source Paths Are Wrong for Key Files

**Why:** Current plans say to modify Spark option/read helpers under `src/main/java`, but the actual files are Scala:

- `paimon-spark/paimon-spark-common/src/main/scala/org/apache/paimon/spark/util/OptionUtils.scala`
- `paimon-spark/paimon-spark-common/src/main/scala/org/apache/paimon/spark/SparkSource.scala`
- `paimon-spark/paimon-spark-common/src/main/scala/org/apache/paimon/spark/PaimonSparkTableBase.scala`
- `paimon-spark/paimon-spark-common/src/main/scala/org/apache/paimon/spark/PaimonPartitionReaderFactory.scala`

**Add to:** `2026-05-18-paimon-native-io-core-spark.md`

- [ ] Replace Spark file paths in the plan from `src/main/java` to the actual `src/main/scala` paths for `OptionUtils`, `SparkSource`, `PaimonSparkTableBase`, and `PaimonPartitionReaderFactory`.
- [ ] Add Scala test locations or explicitly create Java tests only for Java-owned Spark classes.
- [ ] Add a compile step for `paimon-spark-common` that includes Scala compilation, not only Java tests.

### Gap 17: Spark 3.4.4 Target Is Not Locked in Verification

**Why:** The spec targets Spark 3.4.4 + JDK 8, while current commands mainly run `paimon-spark-common`, whose configured Spark version in the root POM may default to a newer Spark line. The plans do not require running the Spark 3.4 module.

**Add to:** `2026-05-18-paimon-native-io-core-spark.md` and `2026-05-18-paimon-native-io-correctness-benchmark.md`

- [ ] Add verification against `paimon-spark/paimon-spark-3.4` for Spark 3.4.x compatibility.
- [ ] Add benchmark env output for Spark runtime version and Scala binary version.
- [ ] Add a guard that Spark 3.5/4.0 runs are optional compatibility smoke tests, not the PoC acceptance baseline.
- [ ] Add an explicit failure if the benchmark procedure is run on a Spark version outside the recorded target without marking the run as non-baseline.

### Gap 18: Maven Commands Need `-am` and Existing Test Targets

**Why:** Several commands use `-pl paimon-spark/paimon-spark-common` or `-pl paimon-native-io` without `-am`. Fresh checkouts may fail because dependent modules are not built. Some referenced tests do not exist yet and the plan does not always say to create them.

**Add to:** all implementation plans.

- [ ] Add `-am` to Maven commands where the target module depends on newly changed modules.
- [ ] For every command that names a test class, ensure the plan creates that exact test file in the same task.
- [ ] Replace fallback commands like `-Dtest=KeyValueTableReadTest` if the test does not exist, or add a task that creates it.
- [ ] Add one full-module compile/test command per plan after narrow test commands.

### Gap 19: Spark Procedure Registration Details Need Codebase-Specific Steps

**Why:** The benchmark plan says “modify procedure registration” but does not name `SparkProcedures` / `ProcedureCatalog` / existing procedure builder patterns. Execution agents may invent a registration style.

**Add to:** `2026-05-18-paimon-native-io-correctness-benchmark.md`

- [ ] Inspect `paimon-spark-common/src/main/java/org/apache/paimon/spark/SparkProcedures.java` and follow the existing builder registration pattern.
- [ ] Add `NativeIOBenchmarkProcedure` to the same registry path used by existing procedures.
- [ ] Add a procedure discovery test that resolves `sys.native_io_benchmark` through Spark SQL, not only direct Java construction.
- [ ] Match existing procedure parameter parsing conventions (`ProcedureParameter`, `ProcedureBuilder`, `BaseProcedure`) before adding custom parsing helpers.

### Gap 20: Java 8 API Compatibility Is Not Explicitly Enforced

**Why:** Paimon compiles with release 8. New native Java module and tests must avoid Java 9+ APIs. The plans do not include a Java 8 compatibility check beyond mentioning JDK 8.

**Add to:** `2026-05-18-paimon-native-io-java-reader.md` and `2026-05-18-paimon-native-io-core-spark.md`

- [ ] Add a compile check under Maven `release=8` for `paimon-native-io`.
- [ ] Add a review checklist banning `List.of`, `Map.of`, `Set.of`, `Path.of`, `Files.readString`, `InputStream.readAllBytes`, `Optional.stream`, and Java `var`.
- [ ] Ensure any test utilities also compile on JDK 8 or are guarded by profile assumptions.

### Gap 21: Rust Edition 2024 Export Syntax Needs Production Plan Coverage

**Why:** The spike uses `#[unsafe(no_mangle)]`, but the production Rust FFI plan only says “export symbols”. With edition 2024 and Rust 1.85, `no_mangle` must use the unsafe attribute form.

**Add to:** `2026-05-18-paimon-native-io-rust-ffi.md`

- [ ] Require all exported ABI functions to use the Rust 2024-compatible `#[unsafe(no_mangle)]` form.
- [ ] Add `cargo check -p paimon-io-c` as a separate task before Java JNR binding work starts.
- [ ] Add an `nm` symbol verification that runs after release and debug builds.

### Gap 22: `paimon-native-io` Module Lifecycle Is Ambiguous

**Why:** The plan both adds `paimon-native-io` to the root reactor and says default builds should not require Rust. It does not specify whether the module is always in the default reactor or only under `-Pnative-io`.

**Add to:** `2026-05-18-paimon-native-io-java-reader.md`

- [ ] Decide and record one module strategy: default reactor with Java-only compile, or profile-only module.
- [ ] If default reactor: add tests proving `mvn -DskipTests compile` succeeds without Rust and without native resources.
- [ ] If profile-only: add tests proving core SPI and ServiceLoader fallback compile without the module present.
- [ ] Update all `mvn -pl paimon-native-io` commands to match the chosen strategy.

### Gap 23: Native Module Dependency Scope Needs Enforcement

**Why:** The spec says native jar should include JNR/JFFI but not duplicate Paimon/Spark/Arrow main dependencies. The plan mentions this but does not test dependency scopes.

**Add to:** `2026-05-18-paimon-native-io-java-reader.md`

- [ ] Add `mvn dependency:tree -pl paimon-native-io` verification.
- [ ] Add checks that Spark dependencies are absent from `paimon-native-io`.
- [ ] Add checks that `paimon-core`, `paimon-format`, and `paimon-arrow` are not shaded into the with-dependencies jar.
- [ ] Add checks that `jnr-ffi`, `jffi`, and required runtime dependencies are present.

### Gap 24: Test Placement for Spark Integration Is Under-Specified

**Why:** The benchmark/correctness plan places large Spark tests under `paimon-spark-common`, but many SQL integration tests in this repo live in version-specific Spark modules or `paimon-spark-ut`. Putting all tests in common may fail due to Spark test harness availability.

**Add to:** `2026-05-18-paimon-native-io-correctness-benchmark.md`

- [ ] Identify existing Spark SQL test base classes before creating `NativeIOCorrectnessTestSuite`.
- [ ] Put pure Java helpers (`NativeIOMultisetHash`, report model, chart writer) in `paimon-spark-common`.
- [ ] Put Spark SQL end-to-end tests in the same module/test style used by existing Spark SQL tests, likely `paimon-spark-ut` or `paimon-spark-3.4`.
- [ ] Keep procedure unit tests near existing procedure tests under `paimon-spark-common`.

### Gap 25: Existing Build Outputs Pollute Grep/Validation Commands

**Why:** Some investigation commands matched `target/classes` and jars. Plans that ask agents to grep source should exclude `target/` or they may infer wrong files from generated output.

**Add to:** all plans as an execution note.

- [ ] Use `find ... -path '*/target/*' -prune -o ...` when locating source files.
- [ ] Use source-root-specific paths in grep commands.
- [ ] Do not use matches under `target/` as evidence for source ownership.

### Gap 26: Direct Local Paths in Plan Snippets May Need Portability Guard

**Why:** Plans include absolute local paths under `/Users/opay-20240095/...`. That is fine for this workspace, but subagents or CI jobs executing from a worktree may need `$PWD`-relative commands.

**Add to:** all plans.

- [ ] Add a preflight step that resolves `PAIMON_ROOT=$(pwd)` and verifies it contains `pom.xml`, `paimon-core`, and `paimon-spark`.
- [ ] Use `$PAIMON_ROOT` in commands after the initial workspace-specific references.
- [ ] Keep absolute links in documentation for Codex readability, but executable shell commands should prefer `$PAIMON_ROOT`.

### Gap 27: Benchmark Output Path Writes Need FileIO Abstraction

**Why:** The procedure writes JSON/CSV/PNG to `obs://...`; the plan does not specify whether it uses Paimon `FileIO`, Spark Hadoop FS, or OBS SDK. Using local Java file APIs would fail on OBS.

**Add to:** `2026-05-18-paimon-native-io-correctness-benchmark.md`

- [ ] Use Paimon `FileIO` resolved from `result_path` to write benchmark artifacts.
- [ ] Add tests using a local/mock `FileIO` for artifact writes.
- [ ] Add OBS write error classification and redaction for result artifact failures.
- [ ] Ensure chart generation writes to a temp local file only if it is then uploaded through `FileIO`, and temp files are cleaned up.
