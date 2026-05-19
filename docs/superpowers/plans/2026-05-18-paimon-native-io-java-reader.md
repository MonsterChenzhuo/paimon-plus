# Paimon Native IO Java Reader Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Implement the `paimon-native-io` Java module that loads the Rust library through JNR, reads Arrow RecordBatches, converts them to Paimon rows, and integrates native file readers into raw DV split reads.

**Architecture:** The module owns all heavy dependencies: JNR, Arrow Java, paimon-arrow, paimon-format, and the native resource. It provides a ServiceLoader factory back to core, a lightweight `NativeRawFileSplitReadProvider`, a `NativeRawFileSplitRead` that mirrors `RawFileSplitRead`, and file readers that preserve `FileRecordIterator.returnedPosition()` from the native row index vector.

**Tech Stack:** Java 8, Maven, JNR-FFI, Arrow Java C Data Interface, Paimon Arrow converters, Paimon format/parquet, ServiceLoader.

---

### File Structure

- Create: `paimon-native-io/pom.xml`
- Modify: `pom.xml` to add `paimon-native-io` module according to profile decisions.
- Create: `paimon-native-io/src/main/java/org/apache/paimon/nativeio/jnr/PaimonJnrLoader.java`
- Create: `paimon-native-io/src/main/java/org/apache/paimon/nativeio/jnr/LibPaimonNativeIO.java`
- Create: `paimon-native-io/src/main/java/org/apache/paimon/nativeio/PaimonNativeReaderBase.java`
- Create: `paimon-native-io/src/main/java/org/apache/paimon/nativeio/PaimonNativeReader.java`
- Create: `paimon-native-io/src/main/java/org/apache/paimon/nativeio/NativeFileRecordReader.java`
- Create: `paimon-native-io/src/main/java/org/apache/paimon/nativeio/NativeFileRecordIterator.java`
- Create: `paimon-native-io/src/main/java/org/apache/paimon/nativeio/NativeFormatReaderFactory.java`
- Create: `paimon-native-io/src/main/java/org/apache/paimon/nativeio/NativeParquetSchemaValidator.java`
- Create: `paimon-native-io/src/main/java/org/apache/paimon/nativeio/NativePreflightResult.java`
- Create: `paimon-native-io/src/main/java/org/apache/paimon/nativeio/NativeRawFileSplitRead.java`
- Create: `paimon-native-io/src/main/java/org/apache/paimon/nativeio/NativeRawFileSplitReadProvider.java`
- Create: `paimon-native-io/src/main/java/org/apache/paimon/nativeio/PaimonNativeSplitReadProviderFactory.java`
- Create: `paimon-native-io/src/main/resources/META-INF/services/org.apache.paimon.operation.nativeio.NativeSplitReadProviderFactory`
- Test: `paimon-native-io/src/test/java/org/apache/paimon/nativeio/*Test.java`

### Execution Notes

- Start commands with `PAIMON_ROOT=/Users/opay-20240095/IdeaProjects/nativeio/paimon`; use `$PAIMON_ROOT` in executable shell snippets.
- Decide one module lifecycle before coding: default reactor with Java-only compile, or profile-only module. All commands in this plan must match that decision.
- Default goal for this plan is default reactor with Java-only compile: `mvn -pl paimon-native-io -am -DskipTests compile` must work without Rust and without native resources.
- Java code must compile with release 8. Do not use `List.of`, `Map.of`, `Set.of`, `Path.of`, `Files.readString`, `InputStream.readAllBytes`, `Optional.stream`, Java `var`, or `module-info.java`.
- `PaimonJnrLoader` must not call `org.apache.paimon.utils.JNIUtils.load(...)`.
- Do not use files under `target/` as source evidence.

### Task 1: Add Maven Module and Service File

**Files:**
- Create: `paimon-native-io/pom.xml`
- Modify: `/Users/opay-20240095/IdeaProjects/nativeio/paimon/pom.xml`
- Create: `paimon-native-io/src/main/resources/META-INF/services/org.apache.paimon.operation.nativeio.NativeSplitReadProviderFactory`

- [ ] **Step 1: Write module POM**

Include dependencies on `paimon-core`, `paimon-format`, `paimon-arrow`, `jnr-ffi`, and test dependencies. Configure `-Pnative-io` to run cargo build and copy:

```text
target/native/linux/x86_64/libpaimon_native_io.so -> target/classes/linux/x86_64/libpaimon_native_io.so
target/native/darwin/aarch64/libpaimon_native_io.dylib -> target/classes/darwin/aarch64/libpaimon_native_io.dylib
```

Default Maven compile must not require Rust or native resources.

- [ ] **Step 2: Add service file content**

```text
org.apache.paimon.nativeio.PaimonNativeSplitReadProviderFactory
```

- [ ] **Step 3: Verify default compile**

Run: `cd $PAIMON_ROOT && mvn -pl paimon-native-io -am -DskipTests compile`

Expected: PASS without cargo build and without `.so/.dylib` resources.

- [ ] **Step 4: Commit**

```bash
git add pom.xml paimon-native-io/pom.xml paimon-native-io/src/main/resources/META-INF/services
git commit -m "feat(native-io): add java native io module"
```

### Task 2: Implement JNR Loader and ABI Interface

**Files:**
- Create: `PaimonJnrLoader.java`
- Create: `LibPaimonNativeIO.java`
- Test: `PaimonJnrLoaderTest.java`

- [ ] **Step 1: Write loader tests**

Tests must cover arch normalization: `amd64 -> x86_64`, `x86_64 -> x86_64`, `arm64 -> aarch64`, `aarch64 -> aarch64`; missing resource returns unavailable without throwing during ServiceLoader discovery; tmp extraction path includes resource hash.

Also cover:

```text
PaimonJnrLoader does not call JNIUtils
two classloaders can load the same resource without incompatible JNR binding types
prior unrelated JNIUtils/native load cannot mark paimon_native_io as loaded
noexec java.io.tmpdir can be bypassed by -Dpaimon.native-io.tmpdir or PAIMON_NATIVE_IO_TMPDIR
```

- [ ] **Step 2: Implement `LibPaimonNativeIO`**

Mirror the 17 Rust ABI functions. `CStatus` must follow LakeSoul shape:

```java
public static final class CStatus extends Struct {
    public final UTF8StringRef err = new UTF8StringRef();
    public final Signed32 status = new Signed32();
    public CStatus(Runtime runtime) { super(runtime); }
}
```

Use struct return for `start_reader`, `paimon_reader_get_schema`, and `next_record_batch_blocked`. Use `free_c_status(@Pinned @In @Transient CStatus status)`.

- [ ] **Step 3: Implement `PaimonJnrLoader`**

Use classloader-aware cache key `(classLoader, resourcePath, resourceHash)`. Extract to `-Dpaimon.native-io.tmpdir` or `PAIMON_NATIVE_IO_TMPDIR`, else `java.io.tmpdir`; write to unique temp file and atomic rename. Use `LibraryOption.LoadNow=true` and `IgnoreError=true`.

- [ ] **Step 4: Verify loader tests**

Run: `cd $PAIMON_ROOT && mvn -pl paimon-native-io -am -Dtest=PaimonJnrLoaderTest test`

Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add paimon-native-io/src/main/java/org/apache/paimon/nativeio/jnr paimon-native-io/src/test/java/org/apache/paimon/nativeio/PaimonJnrLoaderTest.java
git commit -m "feat(native-io): add jnr loader and abi binding"
```

### Task 3: Implement `PaimonNativeReader`

**Files:**
- Create: `PaimonNativeReaderBase.java`
- Create: `PaimonNativeReader.java`
- Test: `PaimonNativeReaderTest.java`

- [ ] **Step 1: Write lifecycle tests using mocked ABI**

Cover: config builder error frees config; reader creation consumes config; `status == 0` is EOF and does not import ArrowArray; `status < 0` with null error is ABI bug; close is idempotent after partial init; thread interrupted before `nextBatchBlocked`; interrupt detected after native returns; timeout/cancel errors are wrapped as non-corrupt IO errors.

- [ ] **Step 2: Implement base resource handling**

Manage Arrow allocator/dictionary provider lifecycle. Close every temporary `ArrowSchema` and `ArrowArray` in `finally`. Keep reader pointer and config pointer fields null after transfer/free.

- [ ] **Step 3: Implement reader builder calls**

`PaimonNativeReader` must expose:

```java
addFile(String file)
setTargetSchema(Schema schema)
setBatchSize(int batchSize)
setRowIndexColumn(String columnName)
setObjectStoreOption(String key, String value)
initializeReader()
getSchema()
nextBatchBlocked(long arrayAddr)
close()
```

All string inputs must be non-null and copied to native through builder calls. Errors must be `IOException` with redacted messages.

- [ ] **Step 4: Implement Arrow runtime compatibility checks**

Before native reader initialization, record Arrow Java version, allocator manager, `arrow.enable_unsafe_memory_access`, and `arrow.enable_null_check_for_get`. Reject incompatible allocator state rather than switching after Arrow classes are initialized. Do not introduce `arrow-memory-netty`; continue using Paimon `arrow-memory-unsafe`.

- [ ] **Step 5: Verify lifecycle tests**

Run: `cd $PAIMON_ROOT && mvn -pl paimon-native-io -am -Dtest=PaimonNativeReaderTest test`

Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add paimon-native-io/src/main/java/org/apache/paimon/nativeio/PaimonNativeReader*.java paimon-native-io/src/test/java/org/apache/paimon/nativeio/PaimonNativeReaderTest.java
git commit -m "feat(native-io): implement paimon native reader wrapper"
```

### Task 4: Implement Arrow Batch to FileRecordIterator

**Files:**
- Create: `NativeFileRecordReader.java`
- Create: `NativeFileRecordIterator.java`
- Test: `NativeFileRecordIteratorTest.java`

- [ ] **Step 1: Write iterator tests**

Construct a `VectorSchemaRoot` with two business vectors and `__paimon_native_row_index` as `BigIntVector`. Assert:

```java
iterator.next();
assertThat(iterator.returnedPosition()).isEqualTo(physicalRowId);
assertThat(iterator.filePath()).isEqualTo(filePath);
iterator.releaseBatch();
iterator.releaseBatch();
```

Also assert `returnedPosition()` before first row throws `IllegalStateException`.

Add rejection cases for `LargeUtf8`, `LargeBinary`, dictionary vector, `FixedSizeBinary`, decimal256, row index `UInt64`, row index `Int32`, nullable row index, and `TIMESTAMP_WITH_LOCAL_TIME_ZONE`.

- [ ] **Step 2: Implement schema validation**

On every imported batch, validate:

```text
root row count == CStatus.status
row_index vector exists
row_index is non-null signed 64-bit
business columns match read type order and Arrow type
all vectors have value count >= row count
row_index values are in [0, file.rowCount())
row_index is non-decreasing across batches for a file
```

- [ ] **Step 3: Implement conversion**

Use `paimon-arrow` converters for business columns. For zero business columns, create a `VectorizedColumnBatch` with `new ColumnVector[0]` and set `numRows` from row index vector value count.

- [ ] **Step 4: Enforce `native-io.max-batch-bytes`**

After `VectorSchemaRoot` import, estimate all vector buffer bytes. If the batch exceeds `NativeIOOptions.maxBatchBytes()`, release the root immediately and throw a non-corrupt `IOException`. This error must not be swallowed by `ignoreCorruptFiles`.

- [ ] **Step 5: Implement release and close behavior**

`NativeFileRecordIterator` owns `VectorSchemaRoot` until `releaseBatch()`. `NativeFileRecordReader.close()` releases outstanding batches, closes Arrow resources, then closes `PaimonNativeReader`. Close must be idempotent.

- [ ] **Step 6: Verify iterator tests**

Run: `cd $PAIMON_ROOT && mvn -pl paimon-native-io -am -Dtest=NativeFileRecordIteratorTest test`

Expected: PASS.

- [ ] **Step 7: Commit**

```bash
git add paimon-native-io/src/main/java/org/apache/paimon/nativeio/NativeFileRecord*.java paimon-native-io/src/test/java/org/apache/paimon/nativeio/NativeFileRecordIteratorTest.java
git commit -m "feat(native-io): convert native arrow batches to paimon rows"
```

### Task 5: Implement Native Parquet Preflight

**Files:**
- Create: `NativeParquetSchemaValidator.java`
- Create: `NativePreflightResult.java`
- Test: `NativeParquetSchemaValidatorTest.java`

- [ ] **Step 1: Write preflight tests**

Cover: timestamp INT96 returns `USE_JAVA/PARQUET_PHYSICAL_TYPE`; decimal physical mismatch returns `USE_JAVA/PARQUET_PHYSICAL_TYPE`; encrypted footer returns `USE_JAVA/PARQUET_UNSUPPORTED_FEATURE`; footer row count mismatch returns `USE_JAVA/PARQUET_METADATA_MISMATCH`; lost file with `ignoreLostFiles=true` returns `EMPTY`.

- [ ] **Step 2: Implement neutral paimon-format footer helper**

Add a small helper in `paimon-format` that returns Paimon-owned POJOs/enums only. Public/native APIs must not expose `ParquetMetadata`, `MessageType`, `ColumnDescriptor`, or any unshaded `org.apache.parquet.*` type. Add a classpath test proving runtime references use Paimon shaded Parquet packages.

- [ ] **Step 3: Implement result object**

```java
enum NativePreflightAction { USE_NATIVE, USE_JAVA, EMPTY, THROW }
```

Include reason, exception, validated file size, validated footer row count, and optional eTag/version fields.

- [ ] **Step 4: Implement validator**

Use Paimon `FileIO` to read Parquet footer through shaded Paimon format helpers. Do not expose unshaded `org.apache.parquet.*` classes in public return types. Validate type whitelist, timestamp precision <= 6, decimal physical encoding, codecs, encryption, field id/name/order, and row count.

- [ ] **Step 5: Verify preflight tests**

Run: `cd $PAIMON_ROOT && mvn -pl paimon-format,paimon-native-io -am -Dtest=NativeParquetSchemaValidatorTest test`

Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add paimon-native-io/src/main/java/org/apache/paimon/nativeio/NativeParquetSchemaValidator.java paimon-native-io/src/main/java/org/apache/paimon/nativeio/NativePreflightResult.java paimon-native-io/src/test/java/org/apache/paimon/nativeio/NativeParquetSchemaValidatorTest.java
git commit -m "feat(native-io): add parquet native preflight"
```

### Task 6: Implement Native Format Reader Factory

**Files:**
- Create: `NativeFormatReaderFactory.java`
- Test: `NativeFormatReaderFactoryTest.java`

- [ ] **Step 1: Write factory test**

Given `FormatReaderFactory.Context` for an OBS file and valid options, `createReader(context)` returns `NativeFileRecordReader`. Given OBS auth error from native initialization, it throws `NonCorruptFileReadException`.

- [ ] **Step 2: Implement factory**

Hold file meta, row count, actual physical read type, logical output type, native options, allocator, preflight metadata, and row index column. Implement only `createReader(Context)`. Let byte-range `createReader(Context, offset, length)` keep the default unsupported behavior.

- [ ] **Step 3: Verify factory test**

Run: `cd $PAIMON_ROOT && mvn -pl paimon-native-io -am -Dtest=NativeFormatReaderFactoryTest test`

Expected: PASS.

- [ ] **Step 4: Commit**

```bash
git add paimon-native-io/src/main/java/org/apache/paimon/nativeio/NativeFormatReaderFactory.java paimon-native-io/src/test/java/org/apache/paimon/nativeio/NativeFormatReaderFactoryTest.java
git commit -m "feat(native-io): add native format reader factory"
```

### Task 7: Implement Native Raw Split Read and Provider

**Files:**
- Create: `NativeRawFileSplitRead.java`
- Create: `NativeRawFileSplitReadProvider.java`
- Create: `PaimonNativeSplitReadProviderFactory.java`
- Test: `NativeRawFileSplitReadTest.java`

- [ ] **Step 1: Write split read tests**

Cover split-level native match, file-level native/Java mixed ordering, bitmap index selection fallback to Java, data filter/topN/limit rejection, schema cast/reorder rejection, and DV wrapping with `ApplyDeletionVectorReader`.

- [ ] **Step 2: Implement provider**

`NativeRawFileSplitReadProvider` implements `SplitReadProvider` directly and holds `LazyField<NativeRawFileSplitRead>`. It must apply `SplitReadConfig` the same way as raw provider, so `withFilter`, `withTopN`, `withLimit`, `forceKeepDelete`, and `applyReadType` are visible before lazy initialization.

- [ ] **Step 3: Implement raw split reader**

Copy the structure of `RawFileSplitRead`. Preserve:

```text
DeletionVector.Factory
FileIndexEvaluator.evaluate
EmptyFileRecordReader for skipped files
BitmapIndexResult Java fallback
DataFileRecordReader creation through FormatReaderFactory
ApplyBitmapIndexRecordReader for Java fallback
ApplyDeletionVectorReader wrapping
ConcatRecordReader preserving dataSplit.dataFiles() order
```

Clear or key the `formatReaderMappings` cache by read type/filter/topN/limit fingerprint so config changes cannot reuse stale mapping.

- [ ] **Step 4: Implement factory**

`PaimonNativeSplitReadProviderFactory` must be light: constructor and static initializers cannot load native libraries, bind JNR, create Arrow allocators, or touch Rust symbols.

- [ ] **Step 5: Verify split read tests**

Run: `cd $PAIMON_ROOT && mvn -pl paimon-native-io -am -Dtest=NativeRawFileSplitReadTest test`

Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add paimon-native-io/src/main/java/org/apache/paimon/nativeio/NativeRawFileSplitRead*.java paimon-native-io/src/main/java/org/apache/paimon/nativeio/PaimonNativeSplitReadProviderFactory.java paimon-native-io/src/test/java/org/apache/paimon/nativeio/NativeRawFileSplitReadTest.java
git commit -m "feat(native-io): add native raw split read provider"
```

### Task 8: Native Profile and Resource Packaging

**Files:**
- Modify: `paimon-native-io/pom.xml`

- [ ] **Step 1: Add cargo build profile**

`-Pnative-io` must run:

```text
cargo build --release -p paimon-io-c
copy target/release/libpaimon_native_io.so or .dylib to target/classes/<os>/<arch>/
```

Do not write cargo outputs into `src/main/resources`.

- [ ] **Step 2: Add with-dependencies packaging**

Produce a jar that includes JNR/JFFI runtime dependencies and native resources, but excludes Paimon/Spark/Arrow main dependencies. Preserve `META-INF/services/org.apache.paimon.operation.nativeio.NativeSplitReadProviderFactory` and `com/kenai/jffi/**`.

Use `ServicesResourceTransformer` or equivalent so service resources are merged. Add dependency-scope checks:

```bash
cd $PAIMON_ROOT
mvn -pl paimon-native-io -am dependency:tree
jar tf paimon-native-io/target/*with-dependencies*.jar | grep 'META-INF/services/org.apache.paimon.operation.nativeio.NativeSplitReadProviderFactory'
jar tf paimon-native-io/target/*with-dependencies*.jar | grep 'com/kenai/jffi/'
```

Expected: service file and `jffi` resources exist; Spark artifacts are absent; Paimon/Arrow main artifacts are not shaded into the with-dependencies jar.

- [ ] **Step 3: Verify native profile package**

Run: `cd $PAIMON_ROOT && mvn -pl paimon-native-io -am -Pnative-io -DskipTests package`

Expected: PASS and jar contains `linux/x86_64/libpaimon_native_io.so` on Linux or `darwin/aarch64/libpaimon_native_io.dylib` on Apple Silicon.

- [ ] **Step 4: Commit**

```bash
git add paimon-native-io/pom.xml
git commit -m "build(native-io): package native library resources"
```

### Task 9: Runtime Compatibility and Dependency Scope Smoke

**Files:**
- Test: `paimon-native-io/src/test/java/org/apache/paimon/nativeio/NativeRuntimeCompatibilityTest.java`
- Test: `paimon-native-io/src/test/java/org/apache/paimon/nativeio/NativeJarLayoutTest.java`

- [ ] **Step 1: Verify Arrow 15 primary runtime**

Run: `cd $PAIMON_ROOT && mvn -pl paimon-native-io -am -Dtest=NativeRuntimeCompatibilityTest test`

Expected: PASS and output records Arrow Java version, allocator manager, unsafe memory property, and null-check property.

- [ ] **Step 2: Verify Arrow 18 profile if Spark 3.5 profile is active**

Run the equivalent profile command used by this repo for Spark 3.5/Arrow 18.1.0, then run `NativeRuntimeCompatibilityTest`.

Expected: PASS or SKIPPED with a recorded reason when that profile is not enabled. This is compatibility smoke, not the Spark 3.4 acceptance baseline.

- [ ] **Step 3: Verify Java 8 API use**

Run:

```bash
cd $PAIMON_ROOT
grep -R -n "List.of\\|Map.of\\|Set.of\\|Path.of\\|Files.readString\\|InputStream.readAllBytes\\|Optional.stream\\|module-info" \
  paimon-native-io/src/main/java paimon-native-io/src/test/java || true
```

Expected: no matches introduced by native IO changes.

- [ ] **Step 4: Commit**

```bash
git add paimon-native-io/src/test/java/org/apache/paimon/nativeio/NativeRuntimeCompatibilityTest.java paimon-native-io/src/test/java/org/apache/paimon/nativeio/NativeJarLayoutTest.java
git commit -m "test(native-io): verify runtime and jar layout"
```

### Final Verification

- [ ] **Step 1: Run Java module tests without native profile**

Run: `cd $PAIMON_ROOT && mvn -pl paimon-native-io -am test`

Expected: PASS; tests that require Rust/native lib are skipped unless `-Pnative-io` is active.

- [ ] **Step 2: Run native smoke tests**

Run: `cd $PAIMON_ROOT && mvn -pl paimon-native-io -am -Pnative-io -Dtest='*Native*Smoke*Test' test`

Expected: PASS on a host with a built native library.

- [ ] **Step 3: Commit final fixes**

```bash
git add paimon-native-io
git commit -m "test(native-io): verify java native reader module"
```

### Self-Review Checklist

- [ ] ServiceLoader factory creation never loads `.so` or JNR.
- [ ] Native reader initialization failures free config/reader native handles.
- [ ] `returnedPosition()` reads from row index vector, not from a counter.
- [ ] `NativeRawFileSplitRead` preserves Java raw read behavior for DV, file index, ignoreLost, and ignoreCorrupt.
- [ ] All native/JNR/Arrow resources are closed idempotently.
- [ ] Arrow runtime compatibility, `JNIUtils` avoidance, shaded service/resource merge, and dependency scope checks are covered.
