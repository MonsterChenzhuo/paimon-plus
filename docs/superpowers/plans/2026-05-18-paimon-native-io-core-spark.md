# Paimon Native IO Core Spark Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add the optional Paimon core SPI, guards, options, and Spark dynamic option plumbing needed for Native IO without making `paimon-core` depend on Arrow, JNR, Spark, or native libraries.

**Architecture:** `paimon-core` owns only light abstractions: provider SPI, context, option parsing, applicability reasons, reporter, and split/file guard logic. `KeyValueTableRead` inserts a native provider before the existing raw provider when a provider is available and the split passes coarse checks. Spark injects a transient engine marker plus OBS dynamic options before reads; non-Spark engines remain rejected.

**Tech Stack:** Paimon core Java, Maven ServiceLoader, Spark connector option utilities, JUnit 5.

---

### File Structure

- Create: `paimon-core/src/main/java/org/apache/paimon/operation/nativeio/NativeSplitReadProviderFactory.java`
- Create: `paimon-core/src/main/java/org/apache/paimon/operation/nativeio/NativeSplitReadContext.java`
- Create: `paimon-core/src/main/java/org/apache/paimon/operation/nativeio/NativeSplitReadProviderLoader.java`
- Create: `paimon-core/src/main/java/org/apache/paimon/operation/nativeio/NativeApplicability.java`
- Create: `paimon-core/src/main/java/org/apache/paimon/operation/nativeio/NativeApplicabilityReporter.java`
- Create: `paimon-core/src/main/java/org/apache/paimon/operation/nativeio/NativeRejectReason.java`
- Create: `paimon-core/src/main/java/org/apache/paimon/operation/nativeio/NativeIOOptions.java`
- Create: `paimon-core/src/main/java/org/apache/paimon/operation/nativeio/SupportsNativeIO.java`
- Create: `paimon-core/src/main/java/org/apache/paimon/io/NonCorruptFileReadException.java`
- Modify: `paimon-api/src/main/java/org/apache/paimon/CoreOptions.java`
- Modify: `paimon-core/src/main/java/org/apache/paimon/KeyValueFileStore.java`
- Modify: `paimon-core/src/main/java/org/apache/paimon/table/PrimaryKeyFileStoreTable.java`
- Modify: `paimon-core/src/main/java/org/apache/paimon/table/source/KeyValueTableRead.java`
- Modify: `paimon-core/src/main/java/org/apache/paimon/io/DataFileRecordReader.java`
- Modify: `paimon-core/src/main/java/org/apache/paimon/schema/SchemaValidation.java`
- Modify: `paimon-core/src/main/java/org/apache/paimon/utils/FormatReaderMapping.java`
- Modify: `paimon-spark/paimon-spark-common/src/main/java/org/apache/paimon/spark/SparkCatalog.java`
- Modify: `paimon-spark/paimon-spark-common/src/main/scala/org/apache/paimon/spark/util/OptionUtils.scala`
- Modify: `paimon-spark/paimon-spark-common/src/main/scala/org/apache/paimon/spark/SparkSource.scala`
- Modify: `paimon-spark/paimon-spark-common/src/main/scala/org/apache/paimon/spark/PaimonSparkTableBase.scala`
- Modify: `paimon-spark/paimon-spark-common/src/main/scala/org/apache/paimon/spark/PaimonPartitionReaderFactory.scala`
- Test: `paimon-core/src/test/java/org/apache/paimon/operation/nativeio/NativeIOOptionsTest.java`
- Test: `paimon-core/src/test/java/org/apache/paimon/operation/nativeio/SupportsNativeIOTest.java`
- Test: `paimon-core/src/test/java/org/apache/paimon/operation/nativeio/NativeSplitReadProviderLoaderTest.java`
- Test: Spark connector tests for option propagation and read builder equality.

### Execution Notes

- Start commands with `PAIMON_ROOT=/Users/opay-20240095/IdeaProjects/nativeio/paimon`; use `$PAIMON_ROOT` in executable shell snippets.
- Add `-am` to Maven commands when running a module that depends on changed upstream modules.
- Spark read-path files in this repo are partly Scala; do not create Java replacements for `OptionUtils`, `SparkSource`, `PaimonSparkTableBase`, or `PaimonPartitionReaderFactory`.
- Exclude `target/` when discovering files: `find "$PAIMON_ROOT" -path '*/target/*' -prune -o -type f -print`.
- Java code must compile with release 8. Do not use `List.of`, `Map.of`, `Set.of`, `Path.of`, `Files.readString`, `InputStream.readAllBytes`, `Optional.stream`, Java `var`, or `module-info.java`.

### Task 1: Add Core Options

**Files:**
- Modify: `paimon-api/src/main/java/org/apache/paimon/CoreOptions.java`
- Test: `paimon-core/src/test/java/org/apache/paimon/operation/nativeio/NativeIOOptionsTest.java`

- [ ] **Step 1: Write failing option tests**

```java
package org.apache.paimon.operation.nativeio;

import org.apache.paimon.options.Options;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class NativeIOOptionsTest {
    @Test
    void defaultsToDisabled() {
        NativeIOOptions options = NativeIOOptions.from(new Options());
        assertThat(options.enabled()).isFalse();
        assertThat(options.batchSize()).isEqualTo(4096);
    }

    @Test
    void validatesBatchSize() {
        Options raw = new Options();
        raw.set("native-io.enabled", "true");
        raw.set("native-io.batch-size", "0");
        NativeIOOptions options = NativeIOOptions.from(raw);
        assertThat(options.valid().applicable()).isFalse();
        assertThat(options.valid().reason()).isEqualTo(NativeRejectReason.INVALID_CONFIG);
    }
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd /Users/opay-20240095/IdeaProjects/nativeio/paimon && mvn -pl paimon-core -Dtest=NativeIOOptionsTest test`

Expected: FAIL because `NativeIOOptions` and options do not exist.

- [ ] **Step 3: Add `CoreOptions` entries**

Add:

```java
public static final ConfigOption<Boolean> NATIVE_IO_ENABLED =
        key("native-io.enabled")
                .booleanType()
                .defaultValue(false)
                .withDescription("Whether to use native IO for supported raw-convertible splits.");

public static final ConfigOption<Integer> NATIVE_IO_BATCH_SIZE =
        key("native-io.batch-size")
                .intType()
                .defaultValue(4096)
                .withDescription("Target row count for native IO Arrow record batches.");

public static final ConfigOption<MemorySize> NATIVE_IO_MAX_BATCH_BYTES =
        key("native-io.max-batch-bytes")
                .memoryType()
                .defaultValue(MemorySize.ofMebiBytes(64))
                .withDescription("Internal limit for an imported native IO Arrow record batch.");

public static final ConfigOption<String> NATIVE_IO_INTERNAL_ENGINE =
        key("__paimon.internal.native-io.engine")
                .stringType()
                .noDefaultValue()
                .withDescription("Internal transient read option injected by Spark.");
```

- [ ] **Step 4: Implement `NativeIOOptions`**

```java
public final class NativeIOOptions {
    public static final String ENGINE_SPARK = "spark";
    private final Options options;

    private NativeIOOptions(Options options) {
        this.options = options;
    }

    public static NativeIOOptions from(Options options) {
        return new NativeIOOptions(options);
    }

    public boolean enabled() {
        return options.get(CoreOptions.NATIVE_IO_ENABLED);
    }

    public int batchSize() {
        return options.get(CoreOptions.NATIVE_IO_BATCH_SIZE);
    }

    public MemorySize maxBatchBytes() {
        return options.get(CoreOptions.NATIVE_IO_MAX_BATCH_BYTES);
    }

    public boolean engineSupportsNativeIO() {
        return ENGINE_SPARK.equals(options.getOptional(CoreOptions.NATIVE_IO_INTERNAL_ENGINE).orElse(null));
    }

    public NativeApplicability valid() {
        int batchSize = batchSize();
        if (batchSize < 1 || batchSize > 65536) {
            return NativeApplicability.rejected(NativeRejectReason.INVALID_CONFIG, "native-io.batch-size must be in [1, 65536]");
        }
        long maxBatchBytes = maxBatchBytes().getBytes();
        if (maxBatchBytes < MemorySize.ofMebiBytes(1).getBytes()
                || maxBatchBytes > MemorySize.ofMebiBytes(512).getBytes()) {
            return NativeApplicability.rejected(NativeRejectReason.INVALID_CONFIG, "native-io.max-batch-bytes must be in [1MB, 512MB]");
        }
        return NativeApplicability.applicable();
    }
}
```

- [ ] **Step 5: Verify tests pass**

Run: `cd $PAIMON_ROOT && mvn -pl paimon-core -am -Dtest=NativeIOOptionsTest test`

Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add paimon-api/src/main/java/org/apache/paimon/CoreOptions.java paimon-core/src/main/java/org/apache/paimon/operation/nativeio paimon-core/src/test/java/org/apache/paimon/operation/nativeio/NativeIOOptionsTest.java
git commit -m "feat(native-io): add core native io options"
```

### Task 2: Add SPI and ClassLoader-Safe Loader

**Files:**
- Create: `paimon-core/src/main/java/org/apache/paimon/operation/nativeio/NativeSplitReadProviderFactory.java`
- Create: `paimon-core/src/main/java/org/apache/paimon/operation/nativeio/NativeSplitReadContext.java`
- Create: `paimon-core/src/main/java/org/apache/paimon/operation/nativeio/NativeSplitReadProviderLoader.java`
- Test: `paimon-core/src/test/java/org/apache/paimon/operation/nativeio/NativeSplitReadProviderLoaderTest.java`

- [ ] **Step 1: Write loader tests**

```java
class NativeSplitReadProviderLoaderTest {
    @Test
    void missingProviderReturnsEmpty() {
        assertThat(NativeSplitReadProviderLoader.tryCreate(null, read -> {})).isEmpty();
    }
}
```

- [ ] **Step 2: Implement provider factory**

```java
public interface NativeSplitReadProviderFactory {
    SplitReadProvider create(NativeSplitReadContext context, Consumer<SplitRead<InternalRow>> config);
}
```

- [ ] **Step 3: Implement context**

`NativeSplitReadContext` must hold only serializable or reconstructable Paimon objects: `FileIO`, `SchemaManager`, `TableSchema`, value `RowType`, `FileFormatDiscover`, `FileStorePathFactory`, `CoreOptions`, `NativeIOOptions`, `engineName`, and `engineSupportsNativeIO`. It must not hold JNR pointers, Arrow allocators, native readers, or `VectorSchemaRoot`.

- [ ] **Step 4: Implement loader**

Use thread context classloader first. Cache by classloader using `WeakHashMap<ClassLoader, Optional<NativeSplitReadProviderFactory>>`. Catch `ServiceConfigurationError`, `LinkageError`, and `RuntimeException` from provider discovery and return empty. Provider construction must not load `.so`.

- [ ] **Step 5: Verify loader tests**

Run: `cd /Users/opay-20240095/IdeaProjects/nativeio/paimon && mvn -pl paimon-core -Dtest=NativeSplitReadProviderLoaderTest test`

Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add paimon-core/src/main/java/org/apache/paimon/operation/nativeio paimon-core/src/test/java/org/apache/paimon/operation/nativeio/NativeSplitReadProviderLoaderTest.java
git commit -m "feat(native-io): add optional split read provider spi"
```

### Task 3: Add Applicability and Guards

**Files:**
- Create: `NativeApplicability.java`, `NativeRejectReason.java`, `NativeApplicabilityReporter.java`, `SupportsNativeIO.java`
- Test: `SupportsNativeIOTest.java`

- [ ] **Step 1: Write guard tests**

```java
@Test
void rejectsNonSparkEngine() {
    NativeIOOptions options = NativeIOOptions.from(options("native-io.enabled", "true"));
    NativeApplicability result = SupportsNativeIO.checkEngine(options);
    assertThat(result.applicable()).isFalse();
    assertThat(result.reason()).isEqualTo(NativeRejectReason.UNSUPPORTED_ENGINE);
}

@Test
void rejectsUnsupportedTypeByDefault() {
    RowType rowType = RowType.of(new MapType(new VarCharType(), new IntType()));
    assertThat(SupportsNativeIO.hasUnsupportedTypes(rowType)).isTrue();
}
```

- [ ] **Step 2: Implement stable reject reasons**

Include all spec reasons: `DISABLED`, `UNSUPPORTED_ENGINE`, `NO_PROVIDER`, `NOT_DV_TABLE`, `STREAMING_SPLIT`, `FORCE_KEEP_DELETE`, `NOT_RAW_CONVERTIBLE`, `MISSING_DELETE_ROW_COUNT`, `NON_PARQUET_FILE`, `NON_OBS_PATH`, `MISSING_OBS_CONFIG`, `UNSUPPORTED_TYPE`, `PARQUET_PHYSICAL_TYPE`, `PARQUET_UNSUPPORTED_FEATURE`, `PARQUET_METADATA_MISMATCH`, `SCHEMA_CAST_OR_REORDER`, `READ_TYPE_MISMATCH`, `PARTITION_OR_SYSTEM_FIELDS`, `DATA_FILTER_TOPN_LIMIT`, `BITMAP_INDEX_SELECTION`, `INVALID_CONFIG`.

- [ ] **Step 3: Implement split guard**

`checkNativeSplit(DataSplit split, NativeIOOptions options, SplitReadProvider.Context context)` must reject disabled, non-Spark, no provider, non-DV table, force keep delete, streaming split, non-raw-convertible split, and missing delete row count. It must not inspect Arrow/JNR/native implementation classes.

- [ ] **Step 4: Implement file guard**

`checkNativeFile(...)` must reject non-Parquet, non-OBS path, missing OBS config, unsupported types, physical/logical mismatch, cast/reorder, partition/system fields, data filters/topN/limit, and bitmap index selection.

- [ ] **Step 5: Verify guard tests**

Run: `cd /Users/opay-20240095/IdeaProjects/nativeio/paimon && mvn -pl paimon-core -Dtest=SupportsNativeIOTest test`

Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add paimon-core/src/main/java/org/apache/paimon/operation/nativeio paimon-core/src/test/java/org/apache/paimon/operation/nativeio/SupportsNativeIOTest.java
git commit -m "feat(native-io): add native applicability guards"
```

### Task 4: Expose `FormatReaderMapping` Actual Read Type

**Files:**
- Modify: `paimon-core/src/main/java/org/apache/paimon/utils/FormatReaderMapping.java`
- Test: existing or new `FormatReaderMappingTest.java`

- [ ] **Step 1: Write accessor tests**

Create a mapping with identity projection and assert:

```java
assertThat(mapping.getActualReadRowType()).isEqualTo(expectedActualReadType);
assertThat(mapping.hasIdentityIndexMapping()).isTrue();
assertThat(mapping.hasNoCastMapping()).isTrue();
```

- [ ] **Step 2: Run test to verify failure**

Run: `cd /Users/opay-20240095/IdeaProjects/nativeio/paimon && mvn -pl paimon-core -Dtest=FormatReaderMappingTest test`

Expected: FAIL because accessors do not exist.

- [ ] **Step 3: Add final field and accessors**

Store the `actualReadRowType` already computed in `Builder.build(...)`. Do not recompute it from `readType`.

- [ ] **Step 4: Verify mapping tests**

Run: `cd /Users/opay-20240095/IdeaProjects/nativeio/paimon && mvn -pl paimon-core -Dtest=FormatReaderMappingTest test`

Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add paimon-core/src/main/java/org/apache/paimon/utils/FormatReaderMapping.java paimon-core/src/test/java
git commit -m "feat(native-io): expose actual format read type"
```

### Task 5: Wire Native Provider into KeyValue Read

**Files:**
- Modify: `paimon-core/src/main/java/org/apache/paimon/KeyValueFileStore.java`
- Modify: `paimon-core/src/main/java/org/apache/paimon/table/PrimaryKeyFileStoreTable.java`
- Modify: `paimon-core/src/main/java/org/apache/paimon/table/source/KeyValueTableRead.java`

- [ ] **Step 1: Add native context factory**

In `KeyValueFileStore`, add `newNativeSplitReadContext()` that packages existing raw read construction inputs into `NativeSplitReadContext`. It must not instantiate native implementation classes.

- [ ] **Step 2: Extend `KeyValueTableRead` constructor**

Add a nullable `NativeSplitReadContext`. Assemble providers with `ArrayList`:

```java
List<SplitReadProvider> providers = new ArrayList<>();
NativeSplitReadProviderLoader.tryCreate(nativeContext, this::config).ifPresent(providers::add);
providers.add(new PrimaryKeyTableRawFileSplitReadProvider(batchRawReadSupplier, this::config));
providers.add(new MergeFileSplitReadProvider(mergeReadSupplier, this::config));
providers.add(new IncrementalChangelogReadProvider(mergeReadSupplier, this::config));
providers.add(new IncrementalDiffReadProvider(mergeReadSupplier, this::config));
this.readProviders = Collections.unmodifiableList(providers);
```

- [ ] **Step 3: Update table read creation**

In `PrimaryKeyFileStoreTable.newRead()`, pass `store().newNativeSplitReadContext()` to `KeyValueTableRead` only for the primary-key read path.

- [ ] **Step 4: Verify Java fallback unchanged**

Run: `cd /Users/opay-20240095/IdeaProjects/nativeio/paimon && mvn -pl paimon-core -DskipITs -DskipE2E -Dtest=KeyValueTableReadTest test`

Expected: PASS. If no exact test exists, run `mvn -pl paimon-core -DskipITs -DskipE2E test` and expect PASS.

- [ ] **Step 5: Commit**

```bash
git add paimon-core/src/main/java/org/apache/paimon/KeyValueFileStore.java paimon-core/src/main/java/org/apache/paimon/table/PrimaryKeyFileStoreTable.java paimon-core/src/main/java/org/apache/paimon/table/source/KeyValueTableRead.java
git commit -m "feat(native-io): insert optional native split provider"
```

### Task 6: Preserve External IO Errors

**Files:**
- Create: `paimon-core/src/main/java/org/apache/paimon/io/NonCorruptFileReadException.java`
- Modify: `paimon-core/src/main/java/org/apache/paimon/io/DataFileRecordReader.java`

- [ ] **Step 1: Write a failing test**

Test `ignoreCorruptFiles=true` plus `NonCorruptFileReadException` from `FormatReaderFactory.createReader` still throws.

- [ ] **Step 2: Implement marker exception**

```java
public class NonCorruptFileReadException extends IOException {
    public NonCorruptFileReadException(String message, Throwable cause) {
        super(message, cause);
    }
    public NonCorruptFileReadException(String message) {
        super(message);
    }
}
```

- [ ] **Step 3: Adjust corrupt ignore logic**

In `DataFileRecordReader.ignoreCorruptException`, return false when the throwable or any cause is `NonCorruptFileReadException`.

- [ ] **Step 4: Verify test**

Run: `cd /Users/opay-20240095/IdeaProjects/nativeio/paimon && mvn -pl paimon-core -Dtest=DataFileRecordReaderTest test`

Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add paimon-core/src/main/java/org/apache/paimon/io/DataFileRecordReader.java paimon-core/src/main/java/org/apache/paimon/io/NonCorruptFileReadException.java paimon-core/src/test/java
git commit -m "fix(native-io): keep external io errors out of corrupt fallback"
```

### Task 7: Spark Dynamic Options and Engine Marker

**Files:**
- Modify: `paimon-spark/paimon-spark-common/src/main/scala/org/apache/paimon/spark/util/OptionUtils.scala`
- Modify: `paimon-spark/paimon-spark-common/src/main/scala/org/apache/paimon/spark/SparkSource.scala`
- Modify: `paimon-spark/paimon-spark-common/src/main/scala/org/apache/paimon/spark/PaimonSparkTableBase.scala`
- Modify: `paimon-spark/paimon-spark-common/src/main/scala/org/apache/paimon/spark/PaimonPartitionReaderFactory.scala`
- Modify: `SparkCatalog.java`
- Modify: Spark datasource files for V1 path and V1 catalog reads.
- Modify: KnownSplitsTable path if dynamic options are dropped.

- [ ] **Step 1: Write Spark option propagation tests**

Cover V2 catalog, V1 path, V1 catalog, and KnownSplitsTable. Assert final table options contain:

```text
native-io.enabled=true
__paimon.internal.native-io.engine=spark
fs.obs.endpoint
fs.obs.access.key
fs.obs.secret.key
```

Assert raw secret values are not printed by diagnostic `toString()`.

- [ ] **Step 2: Implement one helper**

Add a helper such as `OptionUtils.withSparkNativeReadOptions(table, sparkSession, extraOptions)` that:

```text
1. applies existing spark.paimon.* dynamic options
2. copies spark.hadoop.fs.obs.* fallback only for missing fs.obs.* keys
3. injects __paimon.internal.native-io.engine=spark
4. never writes internal options to catalog schema
```

- [ ] **Step 3: Route all Spark read entrances through the helper**

Update V2 catalog, V1 path, V1 catalog, and KnownSplitsTable creation/read paths so no Spark scan loses the engine marker or OBS fallback.

- [ ] **Step 4: Verify Spark tests and Scala compile**

Run:

```bash
cd $PAIMON_ROOT
mvn -pl paimon-spark/paimon-spark-common -am -Dtest='*Native*Option*Test,*SparkSource*Test' test
mvn -pl paimon-spark/paimon-spark-common -am -DskipTests compile
```

Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add paimon-spark/paimon-spark-common/src/main/java paimon-spark/paimon-spark-common/src/main/scala paimon-spark/paimon-spark-common/src/test
git commit -m "feat(native-io): propagate spark native read options"
```

### Task 8: Reject Persistent Internal Options

**Files:**
- Modify: `paimon-core/src/main/java/org/apache/paimon/schema/SchemaValidation.java`

- [ ] **Step 1: Write validation test**

Attempt to create or alter table schema with `__paimon.internal.native-io.engine=spark`; expect validation failure mentioning `__paimon.internal.*`.

- [ ] **Step 2: Implement validation**

In `validateTableSchema(TableSchema schema)`, reject any persisted option key with prefix `__paimon.internal.`.

- [ ] **Step 3: Verify validation**

Run: `cd /Users/opay-20240095/IdeaProjects/nativeio/paimon && mvn -pl paimon-core -Dtest=SchemaValidationTest test`

Expected: PASS.

- [ ] **Step 4: Commit**

```bash
git add paimon-core/src/main/java/org/apache/paimon/schema/SchemaValidation.java paimon-core/src/test/java
git commit -m "fix(native-io): reject persisted internal native options"
```

### Task 9: ReadBuilder Equality Fingerprint

**Files:**
- Modify: `paimon-core/src/main/java/org/apache/paimon/table/source/ReadBuilderImpl.java`
- Modify: Spark `PaimonPartitionReaderFactory` if native options participate there.

- [ ] **Step 1: Write equality tests**

Create two read builders for the same table and projection. One has `native-io.enabled=true`, one false. Assert `equals` is false and `hashCode` differs. Repeat for OBS endpoint and partition filter changes.

- [ ] **Step 2: Implement native option fingerprint**

Include native enabled, internal engine marker, batch size, OBS config key presence, and redacted endpoint in equality/hashCode. Do not include AK/SK/token values or secret hashes in `toString`, assert messages, or reports.

- [ ] **Step 3: Verify equality tests**

Run: `cd $PAIMON_ROOT && mvn -pl paimon-core,paimon-spark/paimon-spark-common -am -Dtest='*ReadBuilder*Test,*PartitionReaderFactory*Test' test`

Expected: PASS.

- [ ] **Step 4: Commit**

```bash
git add paimon-core/src/main/java/org/apache/paimon/table/source/ReadBuilderImpl.java paimon-spark/paimon-spark-common/src/main/java paimon-spark/paimon-spark-common/src/main/scala paimon-core/src/test/java paimon-spark/paimon-spark-common/src/test
git commit -m "fix(native-io): distinguish native read builder options"
```

### Final Verification

- [ ] **Step 1: Core compile without native module**

Run: `cd $PAIMON_ROOT && mvn -pl paimon-core -am -DskipITs -DskipE2E test`

Expected: PASS without requiring `paimon-native-io`, Arrow main dependency, JNR, or a native library.

- [ ] **Step 2: Spark common tests**

Run: `cd $PAIMON_ROOT && mvn -pl paimon-spark/paimon-spark-common -am -DskipITs -DskipE2E test`

Expected: PASS.

- [ ] **Step 3: Spark 3.4 target verification**

Run: `cd $PAIMON_ROOT && mvn -pl paimon-spark/paimon-spark-3.4 -am -DskipITs -DskipE2E test`

Expected: PASS. This is the PoC baseline Spark line; Spark 3.5/4.0 runs are compatibility smoke tests, not acceptance baseline.

- [ ] **Step 4: Java 8 API scan**

Run:

```bash
cd $PAIMON_ROOT
grep -R -n "List.of\\|Map.of\\|Set.of\\|Path.of\\|Files.readString\\|InputStream.readAllBytes\\|Optional.stream\\|module-info" \
  paimon-api/src/main/java paimon-core/src/main/java paimon-spark/paimon-spark-common/src/main || true
```

Expected: no matches introduced by native IO changes.

- [ ] **Step 5: Commit verification fixes**

```bash
git add paimon-api paimon-core paimon-spark
git commit -m "test(native-io): verify core spark native integration"
```

### Self-Review Checklist

- [ ] `paimon-core` main dependencies do not include Arrow, JNR, Spark, or native modules.
- [ ] Non-Spark reads are rejected by `UNSUPPORTED_ENGINE` even when table property enables native IO.
- [ ] Spark V2, V1 path, V1 catalog, and KnownSplitsTable all carry the transient engine marker.
- [ ] Persistent table options cannot contain `__paimon.internal.*`.
- [ ] Read equality/hashCode distinguishes native on/off without exposing secrets.
- [ ] Spark Scala source paths and Spark 3.4 baseline verification are reflected in the executed commands.
