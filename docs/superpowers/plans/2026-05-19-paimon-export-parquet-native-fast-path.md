# Paimon export_parquet Native Fast Path Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

> **Current checkout note:** core SPI, provider registration, export option validation, fallback controls, JNR FFI wrapper, JSON request/result models, Spark source-file planning, DV position extraction, predicate JSON conversion, Rust export execution, native writer target-size rolling, export read projection, OBS range read metrics, and temp-file-backed OBS multipart upload are present. The remaining production hardening items are direct streaming multipart writes, compressed-size-aware rolling, fuller schema evolution/type coverage, and real OBS end-to-end benchmark validation.

**Goal:** Build a native fast path for `CALL sys.export_parquet` so Spark tasks call Rust directly to read OBS Parquet, apply supported filters and DV, and write Parquet output without materializing JVM `InternalRow`.

**Architecture:** Keep Spark as the planner and task scheduler. Add a core ServiceLoader SPI so `paimon-spark-common` can discover native export support without a compile-time dependency on `paimon-native-io`; `paimon-native-io` implements the provider, performs driver-side preflight, and calls Rust JSON FFI inside executor tasks. The existing Java export path remains the fallback for unsupported tables, predicates, schemas, compression, and missing native configuration.

**Tech Stack:** Java 8, Spark 3.4, Paimon core/native-io modules, JNR-FFI, Rust, arrow-rs, parquet-rs, object_store-compatible OBS/S3 client, multipart upload.

---

## File Structure

Create:

- `/Users/opay-20240095/IdeaProjects/nativeio/paimon-plus/paimon-core/src/main/java/org/apache/paimon/operation/nativeio/export/NativeExportProviderFactory.java`
- `/Users/opay-20240095/IdeaProjects/nativeio/paimon-plus/paimon-core/src/main/java/org/apache/paimon/operation/nativeio/export/NativeExportProvider.java`
- `/Users/opay-20240095/IdeaProjects/nativeio/paimon-plus/paimon-core/src/main/java/org/apache/paimon/operation/nativeio/export/NativeExportContext.java`
- `/Users/opay-20240095/IdeaProjects/nativeio/paimon-plus/paimon-core/src/main/java/org/apache/paimon/operation/nativeio/export/NativeExportPreflightResult.java`
- `/Users/opay-20240095/IdeaProjects/nativeio/paimon-plus/paimon-core/src/main/java/org/apache/paimon/operation/nativeio/export/NativeExportPlanDescriptor.java`
- `/Users/opay-20240095/IdeaProjects/nativeio/paimon-plus/paimon-core/src/main/java/org/apache/paimon/operation/nativeio/export/NativeExportTaskResult.java`
- `/Users/opay-20240095/IdeaProjects/nativeio/paimon-plus/paimon-native-io/src/main/java/org/apache/paimon/nativeio/export/NativeExportOptions.java`
- `/Users/opay-20240095/IdeaProjects/nativeio/paimon-plus/paimon-native-io/src/main/java/org/apache/paimon/nativeio/export/NativeExportApplicability.java`
- `/Users/opay-20240095/IdeaProjects/nativeio/paimon-plus/paimon-native-io/src/main/java/org/apache/paimon/nativeio/export/NativeExportProviderFactoryImpl.java`
- `/Users/opay-20240095/IdeaProjects/nativeio/paimon-plus/paimon-native-io/src/main/java/org/apache/paimon/nativeio/export/NativeExportProviderImpl.java`
- `/Users/opay-20240095/IdeaProjects/nativeio/paimon-plus/paimon-native-io/src/main/resources/META-INF/services/org.apache.paimon.operation.nativeio.export.NativeExportProviderFactory`
- `/Users/opay-20240095/IdeaProjects/nativeio/paimon-plus/paimon-native-io/src/main/java/org/apache/paimon/nativeio/export/NativeExportMetrics.java`
- `/Users/opay-20240095/IdeaProjects/nativeio/paimon-plus/paimon-native-io/src/main/java/org/apache/paimon/nativeio/export/NativeExportFile.java`
- `/Users/opay-20240095/IdeaProjects/nativeio/paimon-plus/paimon-native-io/src/main/java/org/apache/paimon/nativeio/export/NativeExportTask.java`
- `/Users/opay-20240095/IdeaProjects/nativeio/paimon-plus/paimon-native-io/src/main/java/org/apache/paimon/nativeio/export/NativeExportResult.java`
- `/Users/opay-20240095/IdeaProjects/nativeio/paimon-plus/paimon-native-io/src/main/java/org/apache/paimon/nativeio/export/NativeExportJson.java`
- `/Users/opay-20240095/IdeaProjects/nativeio/paimon-plus/paimon-native-io/src/main/java/org/apache/paimon/nativeio/export/NativeExportPredicateJson.java`
- `/Users/opay-20240095/IdeaProjects/nativeio/paimon-plus/paimon-native-io/src/main/java/org/apache/paimon/nativeio/export/PaimonNativeExporter.java`
- `/Users/opay-20240095/IdeaProjects/nativeio/paimon-plus/paimon-native-io/src/main/java/org/apache/paimon/nativeio/export/NativeExportRunner.java`
- `/Users/opay-20240095/IdeaProjects/nativeio/paimon-plus/paimon-native-io/src/main/java/org/apache/paimon/nativeio/export/NativeExportPlanner.java`
- `/Users/opay-20240095/IdeaProjects/nativeio/paimon-plus/paimon-native-io/src/test/java/org/apache/paimon/nativeio/export/NativeExportOptionsTest.java`
- `/Users/opay-20240095/IdeaProjects/nativeio/paimon-plus/paimon-native-io/src/test/java/org/apache/paimon/nativeio/export/NativeExportJsonTest.java`
- `/Users/opay-20240095/IdeaProjects/nativeio/paimon-plus/paimon-native-io/src/test/java/org/apache/paimon/nativeio/export/NativeExportPredicateJsonTest.java`
- `/Users/opay-20240095/IdeaProjects/nativeio/paimon-plus/paimon-native-io/src/test/java/org/apache/paimon/nativeio/export/PaimonNativeExporterTest.java`
- `/Users/opay-20240095/IdeaProjects/nativeio/paimon-plus/paimon-native-io/rust/paimon-native-io-c/src/export.rs`
- `/Users/opay-20240095/IdeaProjects/nativeio/paimon-plus/paimon-native-io/rust/paimon-native-io-c/src/buffered_reader.rs`
- `/Users/opay-20240095/IdeaProjects/nativeio/paimon-plus/dev/native-io-export-benchmark/README.md`

Modify:

- `/Users/opay-20240095/IdeaProjects/nativeio/paimon-plus/paimon-api/src/main/java/org/apache/paimon/CoreOptions.java`
- `/Users/opay-20240095/IdeaProjects/nativeio/paimon-plus/paimon-core/src/main/java/org/apache/paimon/operation/nativeio/NativeRejectReason.java`
- `/Users/opay-20240095/IdeaProjects/nativeio/paimon-plus/paimon-core/src/main/java/org/apache/paimon/operation/nativeio/NativeIOOptions.java`
- `/Users/opay-20240095/IdeaProjects/nativeio/paimon-plus/paimon-native-io/src/main/java/org/apache/paimon/nativeio/jnr/LibPaimonNativeIO.java`
- `/Users/opay-20240095/IdeaProjects/nativeio/paimon-plus/paimon-native-io/rust/paimon-native-io-c/src/lib.rs`
- `/Users/opay-20240095/IdeaProjects/nativeio/paimon-plus/paimon-native-io/rust/paimon-native-io-c/Cargo.toml`
- `/Users/opay-20240095/IdeaProjects/nativeio/paimon-plus/paimon-spark/paimon-spark-common/src/main/java/org/apache/paimon/spark/procedure/ExportParquetProcedure.java`
- Existing native IO tests under `/Users/opay-20240095/IdeaProjects/nativeio/paimon-plus/paimon-core/src/test/java/org/apache/paimon/operation/nativeio/`
- Existing Rust tests in `/Users/opay-20240095/IdeaProjects/nativeio/paimon-plus/paimon-native-io/rust/paimon-native-io-c/src/lib.rs`

Responsibility boundaries:

- `paimon-api`: define user-visible config.
- `paimon-core`: define stable reject reasons and the native export ServiceLoader SPI only; it must not reference JNR, Spark classes, Arrow C Data, or `paimon-native-io` implementation classes.
- `paimon-native-io/src/main/java/org/apache/paimon/nativeio/export`: ServiceLoader provider implementation, Java request planning, JSON serialization, JNR wrapper, and task runner.
- `ExportParquetProcedure`: strategy selection, output directory lifecycle, fallback, and result aggregation through the core SPI; it must not import `org.apache.paimon.nativeio.export.*` implementation classes or know Rust request internals.
- Rust `export.rs`: parse versioned request, evaluate predicates/DV, write Parquet, return result JSON.
- Rust `object_store.rs`: OBS object_store registration and credential mapping.
- Rust `buffered_reader.rs`: object_store-backed buffered reader and IO metrics.
- Rust `multipart_writer.rs`: object_store multipart Parquet writer, target-size rolling, finish/abort.

LakeSoul reference files to consult during implementation:

- `LakeSoul/native-io/lakesoul-io-java/src/main/java/com/dmetasoul/lakesoul/lakesoul/io/NativeIOWriter.java` for opaque pointer lifecycle, blocked write status handling, flush result export, and abort-on-close.
- `LakeSoul/native-io/lakesoul-io-java/src/main/java/com/dmetasoul/lakesoul/lakesoul/local/LakeSoulLocalJavaWriter.java` for Hadoop/S3A option extraction into native object store options.
- `LakeSoul/rust/lakesoul-io/src/session.rs` for process-wide Tokio runtime, metadata cache, DataFusion runtime env, and object store registration.
- `LakeSoul/rust/lakesoul-io/src/writer/async_writer/multipart_writer.rs` for `ArrowWriter` over an in-memory buffer plus object_store multipart `finish()` / `abort()`.
- `LakeSoul/rust/lakesoul-io/src/writer/async_writer/partitioning_writer.rs` only for bounded async sink and flush/abort patterns; do not import LakeSoul PK/range partition semantics.

---

### Task 1: Native Export Options and Reject Reasons

**Files:**

- Modify: `/Users/opay-20240095/IdeaProjects/nativeio/paimon-plus/paimon-api/src/main/java/org/apache/paimon/CoreOptions.java`
- Modify: `/Users/opay-20240095/IdeaProjects/nativeio/paimon-plus/paimon-core/src/main/java/org/apache/paimon/operation/nativeio/NativeRejectReason.java`
- Modify: `/Users/opay-20240095/IdeaProjects/nativeio/paimon-plus/paimon-core/src/main/java/org/apache/paimon/operation/nativeio/NativeIOOptions.java`
- Create: `/Users/opay-20240095/IdeaProjects/nativeio/paimon-plus/paimon-native-io/src/main/java/org/apache/paimon/nativeio/export/NativeExportOptions.java`
- Test: `/Users/opay-20240095/IdeaProjects/nativeio/paimon-plus/paimon-native-io/src/test/java/org/apache/paimon/nativeio/export/NativeExportOptionsTest.java`

- [ ] **Step 1: Write failing tests for export option defaults and validation**

Add this test class:

```java
package org.apache.paimon.nativeio.export;

import org.apache.paimon.options.Options;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class NativeExportOptionsTest {

    @Test
    void defaultsKeepNativeExportDisabled() {
        NativeExportOptions options = NativeExportOptions.from(new Options());

        assertThat(options.enabled()).isFalse();
        assertThat(options.fallbackEnabled()).isTrue();
        assertThat(options.metricsEnabled()).isTrue();
        assertThat(options.readBufferSizeBytes()).isEqualTo(8L * 1024 * 1024);
        assertThat(options.readConcurrency()).isEqualTo(4);
        assertThat(options.writerBatchSize()).isEqualTo(8192);
        assertThat(options.writerRowGroupSize()).isEqualTo(250000);
        assertThat(options.multipartPartSizeBytes()).isEqualTo(64L * 1024 * 1024);
        assertThat(options.memoryLimitBytes()).isEqualTo(512L * 1024 * 1024);
        assertThat(options.runtimeThreads()).isEqualTo(4);
    }

    @Test
    void validatesExportTunables() {
        Options raw = new Options();
        raw.setString("native-io.export.enabled", "true");
        raw.setString("native-io.export.obs.read-buffer-size", "0 b");

        assertThat(NativeExportOptions.from(raw).valid().applicable()).isFalse();
        assertThat(NativeExportOptions.from(raw).valid().detail())
                .contains("native-io.export.obs.read-buffer-size");

        raw.setString("native-io.export.obs.read-buffer-size", "8 mb");
        raw.setString("native-io.export.obs.read-concurrency", "0");
        assertThat(NativeExportOptions.from(raw).valid().detail())
                .contains("native-io.export.obs.read-concurrency");

        raw.setString("native-io.export.obs.read-concurrency", "4");
        raw.setString("native-io.export.writer.batch-size", "0");
        assertThat(NativeExportOptions.from(raw).valid().detail())
                .contains("native-io.export.writer.batch-size");

        raw.setString("native-io.export.writer.batch-size", "8192");
        raw.setString("native-io.export.writer.row-group-size", "0");
        assertThat(NativeExportOptions.from(raw).valid().detail())
                .contains("native-io.export.writer.row-group-size");

        raw.setString("native-io.export.writer.row-group-size", "250000");
        raw.setString("native-io.export.writer.multipart-part-size", "1 mb");
        assertThat(NativeExportOptions.from(raw).valid().detail())
                .contains("native-io.export.writer.multipart-part-size");

        raw.setString("native-io.export.writer.multipart-part-size", "64 mb");
        raw.setString("native-io.export.memory-limit", "1 mb");
        assertThat(NativeExportOptions.from(raw).valid().detail())
                .contains("native-io.export.memory-limit");
    }
}
```

- [ ] **Step 2: Run test to verify it fails**

Run:

```bash
cd /Users/opay-20240095/IdeaProjects/nativeio/paimon-plus
./mvnw -pl paimon-native-io -DskipITs -Dcheckstyle.skip -Drat.skip -Dspotless.check.skip -Dtest=NativeExportOptionsTest test
```

Expected: compilation fails because `NativeExportOptions` and export config options do not exist.

- [ ] **Step 3: Add CoreOptions constants**

Insert these options near the existing `NATIVE_IO_*` options in `CoreOptions.java`:

```java
public static final ConfigOption<Boolean> NATIVE_IO_EXPORT_ENABLED =
        key("native-io.export.enabled")
                .booleanType()
                .defaultValue(false)
                .withDescription("Whether to enable native fast path for sys.export_parquet.");

public static final ConfigOption<Boolean> NATIVE_IO_EXPORT_FALLBACK_ENABLED =
        key("native-io.export.fallback.enabled")
                .booleanType()
                .defaultValue(true)
                .withDescription(
                        "Whether driver-side native export preflight may fall back to Java export.");

public static final ConfigOption<Boolean> NATIVE_IO_EXPORT_METRICS_ENABLED =
        key("native-io.export.metrics.enabled")
                .booleanType()
                .defaultValue(true)
                .withDescription("Whether native export should return and log task metrics.");

public static final ConfigOption<MemorySize> NATIVE_IO_EXPORT_OBS_READ_BUFFER_SIZE =
        key("native-io.export.obs.read-buffer-size")
                .memoryType()
                .defaultValue(MemorySize.ofMebiBytes(8))
                .withDescription("Sequential read buffer size used by native OBS export reader.");

public static final ConfigOption<Integer> NATIVE_IO_EXPORT_OBS_READ_CONCURRENCY =
        key("native-io.export.obs.read-concurrency")
                .intType()
                .defaultValue(4)
                .withDescription("Maximum per-task native OBS read concurrency for export.");

public static final ConfigOption<Integer> NATIVE_IO_EXPORT_WRITER_BATCH_SIZE =
        key("native-io.export.writer.batch-size")
                .intType()
                .defaultValue(8192)
                .withDescription("Arrow record batch size used by native export writer.");

public static final ConfigOption<Integer> NATIVE_IO_EXPORT_WRITER_ROW_GROUP_SIZE =
        key("native-io.export.writer.row-group-size")
                .intType()
                .defaultValue(250000)
                .withDescription("Maximum rows per row group used by native export writer.");

public static final ConfigOption<MemorySize> NATIVE_IO_EXPORT_WRITER_MULTIPART_PART_SIZE =
        key("native-io.export.writer.multipart-part-size")
                .memoryType()
                .defaultValue(MemorySize.ofMebiBytes(64))
                .withDescription("Multipart upload part size used by native export writer.");

public static final ConfigOption<MemorySize> NATIVE_IO_EXPORT_MEMORY_LIMIT =
        key("native-io.export.memory-limit")
                .memoryType()
                .defaultValue(MemorySize.ofMebiBytes(512))
                .withDescription("Soft memory limit per native export task.");

public static final ConfigOption<Integer> NATIVE_IO_EXPORT_RUNTIME_THREADS =
        key("native-io.export.runtime-threads")
                .intType()
                .defaultValue(4)
                .withDescription("Worker thread count for process-wide native export runtime.");

public static final ConfigOption<Boolean> NATIVE_IO_EXPORT_METADATA_CACHE_ENABLED =
        key("native-io.export.metadata-cache.enabled")
                .booleanType()
                .defaultValue(true)
                .withDescription("Whether native export may use process-wide parquet metadata cache.");
```

- [ ] **Step 4: Add export reject reasons**

Append these enum constants to `NativeRejectReason.java`:

```java
EXPORT_DISABLED,
EXPORT_INVALID_CONFIG,
EXPORT_UNSUPPORTED_COMPRESSION,
EXPORT_UNSUPPORTED_PREDICATE,
EXPORT_UNSUPPORTED_SCHEMA_EVOLUTION,
EXPORT_UNSUPPORTED_DV,
EXPORT_UNSUPPORTED_OUTPUT_TYPE,
EXPORT_NATIVE_LIBRARY_UNAVAILABLE
```

- [ ] **Step 5: Implement NativeExportOptions**

Create `NativeExportOptions.java`:

```java
package org.apache.paimon.nativeio.export;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.operation.nativeio.NativeApplicability;
import org.apache.paimon.operation.nativeio.NativeRejectReason;
import org.apache.paimon.options.Options;

/** Native export_parquet fast path options. */
public final class NativeExportOptions {

    private static final long MIN_READ_BUFFER_BYTES = 1024L * 1024;
    private static final long MAX_READ_BUFFER_BYTES = 128L * 1024 * 1024;
    private static final int MIN_READ_CONCURRENCY = 1;
    private static final int MAX_READ_CONCURRENCY = 64;
    private static final int MIN_WRITER_BATCH_SIZE = 1;
    private static final int MAX_WRITER_BATCH_SIZE = 65536;
    private static final int MIN_WRITER_ROW_GROUP_SIZE = 1024;
    private static final int MAX_WRITER_ROW_GROUP_SIZE = 10_000_000;
    private static final long MIN_MULTIPART_PART_SIZE_BYTES = 5L * 1024 * 1024;
    private static final long MAX_MULTIPART_PART_SIZE_BYTES = 512L * 1024 * 1024;
    private static final long MIN_MEMORY_LIMIT_BYTES = 64L * 1024 * 1024;
    private static final long MAX_MEMORY_LIMIT_BYTES = 16L * 1024 * 1024 * 1024;

    private final Options options;

    private NativeExportOptions(Options options) {
        this.options = options;
    }

    public static NativeExportOptions from(Options options) {
        return new NativeExportOptions(options);
    }

    public boolean enabled() {
        return options.get(CoreOptions.NATIVE_IO_EXPORT_ENABLED);
    }

    public boolean fallbackEnabled() {
        return options.get(CoreOptions.NATIVE_IO_EXPORT_FALLBACK_ENABLED);
    }

    public boolean metricsEnabled() {
        return options.get(CoreOptions.NATIVE_IO_EXPORT_METRICS_ENABLED);
    }

    public long readBufferSizeBytes() {
        return options.get(CoreOptions.NATIVE_IO_EXPORT_OBS_READ_BUFFER_SIZE).getBytes();
    }

    public int readConcurrency() {
        return options.get(CoreOptions.NATIVE_IO_EXPORT_OBS_READ_CONCURRENCY);
    }

    public int writerBatchSize() {
        return options.get(CoreOptions.NATIVE_IO_EXPORT_WRITER_BATCH_SIZE);
    }

    public int writerRowGroupSize() {
        return options.get(CoreOptions.NATIVE_IO_EXPORT_WRITER_ROW_GROUP_SIZE);
    }

    public long multipartPartSizeBytes() {
        return options.get(CoreOptions.NATIVE_IO_EXPORT_WRITER_MULTIPART_PART_SIZE).getBytes();
    }

    public long memoryLimitBytes() {
        return options.get(CoreOptions.NATIVE_IO_EXPORT_MEMORY_LIMIT).getBytes();
    }

    public int runtimeThreads() {
        return options.get(CoreOptions.NATIVE_IO_EXPORT_RUNTIME_THREADS);
    }

    public NativeApplicability valid() {
        if (!enabled()) {
            return NativeApplicability.rejected(
                    NativeRejectReason.EXPORT_DISABLED, "native export is disabled");
        }
        long readBufferSizeBytes = readBufferSizeBytes();
        if (readBufferSizeBytes < MIN_READ_BUFFER_BYTES
                || readBufferSizeBytes > MAX_READ_BUFFER_BYTES) {
            return NativeApplicability.rejected(
                    NativeRejectReason.EXPORT_INVALID_CONFIG,
                    "native-io.export.obs.read-buffer-size must be between 1 MB and 128 MB");
        }
        int readConcurrency = readConcurrency();
        if (readConcurrency < MIN_READ_CONCURRENCY || readConcurrency > MAX_READ_CONCURRENCY) {
            return NativeApplicability.rejected(
                    NativeRejectReason.EXPORT_INVALID_CONFIG,
                    "native-io.export.obs.read-concurrency must be between 1 and 64");
        }
        int writerBatchSize = writerBatchSize();
        if (writerBatchSize < MIN_WRITER_BATCH_SIZE
                || writerBatchSize > MAX_WRITER_BATCH_SIZE) {
            return NativeApplicability.rejected(
                    NativeRejectReason.EXPORT_INVALID_CONFIG,
                    "native-io.export.writer.batch-size must be between 1 and 65536");
        }
        int writerRowGroupSize = writerRowGroupSize();
        if (writerRowGroupSize < MIN_WRITER_ROW_GROUP_SIZE
                || writerRowGroupSize > MAX_WRITER_ROW_GROUP_SIZE) {
            return NativeApplicability.rejected(
                    NativeRejectReason.EXPORT_INVALID_CONFIG,
                    "native-io.export.writer.row-group-size must be between 1024 and 10000000");
        }
        long multipartPartSizeBytes = multipartPartSizeBytes();
        if (multipartPartSizeBytes < MIN_MULTIPART_PART_SIZE_BYTES
                || multipartPartSizeBytes > MAX_MULTIPART_PART_SIZE_BYTES) {
            return NativeApplicability.rejected(
                    NativeRejectReason.EXPORT_INVALID_CONFIG,
                    "native-io.export.writer.multipart-part-size must be between 5 MB and 512 MB");
        }
        long memoryLimitBytes = memoryLimitBytes();
        if (memoryLimitBytes < MIN_MEMORY_LIMIT_BYTES
                || memoryLimitBytes > MAX_MEMORY_LIMIT_BYTES) {
            return NativeApplicability.rejected(
                    NativeRejectReason.EXPORT_INVALID_CONFIG,
                    "native-io.export.memory-limit must be between 64 MB and 16 GB");
        }
        return NativeApplicability.yes();
    }
}
```

- [ ] **Step 6: Run tests**

Run:

```bash
cd /Users/opay-20240095/IdeaProjects/nativeio/paimon-plus
./mvnw -pl paimon-native-io -DskipITs -Dcheckstyle.skip -Drat.skip -Dspotless.check.skip -Dtest=NativeExportOptionsTest test
```

Expected: `NativeExportOptionsTest` passes.

- [ ] **Step 7: Commit**

```bash
git add paimon-api/src/main/java/org/apache/paimon/CoreOptions.java \
  paimon-core/src/main/java/org/apache/paimon/operation/nativeio/NativeRejectReason.java \
  paimon-native-io/src/main/java/org/apache/paimon/nativeio/export/NativeExportOptions.java \
  paimon-native-io/src/test/java/org/apache/paimon/nativeio/export/NativeExportOptionsTest.java
git commit -m "feat: add native export options"
```

---

### Task 1A: Core ServiceLoader SPI Boundary

**Files:**

- Create: `/Users/opay-20240095/IdeaProjects/nativeio/paimon-plus/paimon-core/src/main/java/org/apache/paimon/operation/nativeio/export/NativeExportProviderFactory.java`
- Create: `/Users/opay-20240095/IdeaProjects/nativeio/paimon-plus/paimon-core/src/main/java/org/apache/paimon/operation/nativeio/export/NativeExportProvider.java`
- Create: `/Users/opay-20240095/IdeaProjects/nativeio/paimon-plus/paimon-core/src/main/java/org/apache/paimon/operation/nativeio/export/NativeExportContext.java`
- Create: `/Users/opay-20240095/IdeaProjects/nativeio/paimon-plus/paimon-core/src/main/java/org/apache/paimon/operation/nativeio/export/NativeExportPreflightResult.java`
- Create: `/Users/opay-20240095/IdeaProjects/nativeio/paimon-plus/paimon-core/src/main/java/org/apache/paimon/operation/nativeio/export/NativeExportPlanDescriptor.java`
- Create: `/Users/opay-20240095/IdeaProjects/nativeio/paimon-plus/paimon-core/src/main/java/org/apache/paimon/operation/nativeio/export/NativeExportTaskResult.java`
- Test: `/Users/opay-20240095/IdeaProjects/nativeio/paimon-plus/paimon-core/src/test/java/org/apache/paimon/operation/nativeio/export/NativeExportProviderSpiTest.java`

- [ ] **Step 1: Write failing SPI contract tests**

Add a core test that asserts the SPI DTOs are serializable and do not depend on native implementation packages:

```java
package org.apache.paimon.operation.nativeio.export;

import org.junit.jupiter.api.Test;

import java.io.Serializable;

import static org.assertj.core.api.Assertions.assertThat;

class NativeExportProviderSpiTest {

    @Test
    void spiDtosAreSerializableAndCoreOnly() {
        assertThat(Serializable.class).isAssignableFrom(NativeExportContext.class);
        assertThat(Serializable.class).isAssignableFrom(NativeExportPreflightResult.class);
        assertThat(Serializable.class).isAssignableFrom(NativeExportPlanDescriptor.class);
        assertThat(Serializable.class).isAssignableFrom(NativeExportTaskResult.class);
        assertThat(NativeExportProvider.class.getName()).startsWith("org.apache.paimon.operation.nativeio.export");
        assertThat(NativeExportProviderFactory.class.getName()).startsWith("org.apache.paimon.operation.nativeio.export");
    }
}
```

- [ ] **Step 2: Run test to verify it fails**

Run:

```bash
cd /Users/opay-20240095/IdeaProjects/nativeio/paimon-plus
./mvnw -pl paimon-core -DskipITs -Dcheckstyle.skip -Drat.skip -Dspotless.check.skip -Dtest=NativeExportProviderSpiTest test
```

Expected: compilation fails because the SPI package does not exist.

- [ ] **Step 3: Implement core SPI interfaces**

Create `NativeExportProviderFactory`:

```java
package org.apache.paimon.operation.nativeio.export;

/** Factory discovered by ServiceLoader for optional native export support. */
public interface NativeExportProviderFactory {

    NativeExportProvider create();
}
```

Create `NativeExportProvider`:

```java
package org.apache.paimon.operation.nativeio.export;

import java.io.Serializable;
import java.util.List;

/** Optional native export provider used by Spark procedure code through core SPI only. */
public interface NativeExportProvider extends Serializable {

    NativeExportPreflightResult preflight(NativeExportContext context);

    NativeExportPlanDescriptor plan(NativeExportContext context);

    NativeExportTaskResult executeTask(byte[] taskPayload) throws Exception;
}
```

Create DTOs with final fields, constructor validation, and getters. Keep them limited to core-visible types: strings, primitive values, `Options`, serializable split descriptors already visible from core, opaque `byte[]` task payloads, and stable reject reason. Do not put Spark `JavaSparkContext`, JNR pointer, Arrow vector, or native request JSON into these DTOs.

- [ ] **Step 4: Add ServiceLoader discovery helper test**

Add a small static helper in `NativeExportProviderFactory` or a separate core utility that discovers factories with the current thread context classloader first, then falls back to the defining classloader. The test should use an empty classloader and assert empty discovery returns an empty list, not an exception.

- [ ] **Step 5: Run core SPI tests**

Run:

```bash
cd /Users/opay-20240095/IdeaProjects/nativeio/paimon-plus
./mvnw -pl paimon-core -DskipITs -Dcheckstyle.skip -Drat.skip -Dspotless.check.skip -Dtest=NativeExportProviderSpiTest test
```

Expected: SPI tests pass.

- [ ] **Step 6: Commit**

```bash
git add paimon-core/src/main/java/org/apache/paimon/operation/nativeio/export \
  paimon-core/src/test/java/org/apache/paimon/operation/nativeio/export/NativeExportProviderSpiTest.java
git commit -m "feat: add native export provider spi"
```

---

### Task 2: Java Request, Result, Metrics, and Redacted JSON

**Files:**

- Create: `/Users/opay-20240095/IdeaProjects/nativeio/paimon-plus/paimon-native-io/src/main/java/org/apache/paimon/nativeio/export/NativeExportMetrics.java`
- Create: `/Users/opay-20240095/IdeaProjects/nativeio/paimon-plus/paimon-native-io/src/main/java/org/apache/paimon/nativeio/export/NativeExportFile.java`
- Create: `/Users/opay-20240095/IdeaProjects/nativeio/paimon-plus/paimon-native-io/src/main/java/org/apache/paimon/nativeio/export/NativeExportTask.java`
- Create: `/Users/opay-20240095/IdeaProjects/nativeio/paimon-plus/paimon-native-io/src/main/java/org/apache/paimon/nativeio/export/NativeExportObjectStoreOptions.java`
- Create: `/Users/opay-20240095/IdeaProjects/nativeio/paimon-plus/paimon-native-io/src/main/java/org/apache/paimon/nativeio/export/NativeExportResult.java`
- Create: `/Users/opay-20240095/IdeaProjects/nativeio/paimon-plus/paimon-native-io/src/main/java/org/apache/paimon/nativeio/export/NativeExportJson.java`
- Test: `/Users/opay-20240095/IdeaProjects/nativeio/paimon-plus/paimon-native-io/src/test/java/org/apache/paimon/nativeio/export/NativeExportJsonTest.java`

- [ ] **Step 1: Write failing JSON round-trip and redaction tests**

Add `NativeExportJsonTest.java`:

```java
package org.apache.paimon.nativeio.export;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

class NativeExportJsonTest {

    @Test
    void serializesTaskAndRedactsSecrets() {
        Map<String, String> objectStore = new LinkedHashMap<>();
        objectStore.put("fs.obs.endpoint", "http://obs.example");
        objectStore.put("fs.obs.access.key", "ak-value");
        objectStore.put("fs.obs.secret.key", "sk-value");
        objectStore.put("fs.obs.session.token", "token-value");
        NativeExportFile file =
                new NativeExportFile(
                        "obs://bucket/table/file.parquet",
                        10L,
                        1000L,
                        2L,
                        Collections.singletonList(3L));
        NativeExportTask task =
                new NativeExportTask(
                        "obs://bucket/export",
                        "zstd",
                        536870912L,
                        8388608L,
                        4,
                        8192,
                        250000,
                        67108864L,
                        536870912L,
                        Arrays.asList("id", "score"),
                        "paimon-json-v1",
                        "{\"op\":\"true\"}",
                        objectStore,
                        Collections.singletonList(file));

        String json = NativeExportJson.toJson(task);
        assertThat(json).contains("\"output_path\":\"obs://bucket/export\"");
        assertThat(json).contains("\"projection\":[\"id\",\"score\"]");
        assertThat(json).contains("\"positions\":[3]");

        String redacted = NativeExportJson.toRedactedJson(task);
        assertThat(redacted).contains("\"fs.obs.access.key\":\"******\"");
        assertThat(redacted).contains("\"fs.obs.secret.key\":\"******\"");
        assertThat(redacted).contains("\"fs.obs.session.token\":\"******\"");
        assertThat(redacted).doesNotContain("ak-value");
        assertThat(redacted).doesNotContain("sk-value");
        assertThat(redacted).doesNotContain("token-value");
    }

    @Test
    void parsesResultAndAggregatesMetrics() {
        String resultJson =
                "{"
                        + "\"rows_output\":2,"
                        + "\"files_written\":[{\"path\":\"obs://bucket/out/part.parquet\",\"rows\":2,\"bytes\":128}],"
                        + "\"metrics\":{"
                        + "\"rows_read\":3,"
                        + "\"rows_output\":2,"
                        + "\"predicate_filtered_rows\":1,"
                        + "\"dv_filtered_rows\":0,"
                        + "\"parquet_row_groups_read\":2,"
                        + "\"parquet_row_groups_pruned\":1,"
                        + "\"peak_buffered_bytes\":1048576,"
                        + "\"writer_rolls\":1,"
                        + "\"obs_read_requests\":4,"
                        + "\"obs_read_retries\":0,"
                        + "\"obs_read_bytes\":4096,"
                        + "\"obs_write_requests\":1,"
                        + "\"obs_write_bytes\":128,"
                        + "\"read_ms\":10,"
                        + "\"decode_ms\":20,"
                        + "\"filter_ms\":5,"
                        + "\"encode_ms\":30,"
                        + "\"obs_write_ms\":40,"
                        + "\"multipart_finish_ms\":3"
                        + "}"
                        + "}";

        NativeExportResult result = NativeExportJson.resultFromJson(resultJson);

        assertThat(result.rowsOutput()).isEqualTo(2L);
        assertThat(result.filesWritten()).hasSize(1);
        assertThat(result.metrics().parquetRowGroupsPruned()).isEqualTo(1L);
        assertThat(result.metrics().peakBufferedBytes()).isEqualTo(1048576L);
        assertThat(result.metrics().writerRolls()).isEqualTo(1L);
        assertThat(result.metrics().obsReadRequests()).isEqualTo(4L);
        assertThat(result.metrics().obsWriteMs()).isEqualTo(40L);
        assertThat(result.metrics().multipartFinishMs()).isEqualTo(3L);
    }
}
```

- [ ] **Step 2: Run test to verify it fails**

Run:

```bash
cd /Users/opay-20240095/IdeaProjects/nativeio/paimon-plus
./mvnw -pl paimon-native-io -DskipITs -Dcheckstyle.skip -Drat.skip -Dspotless.check.skip -Dtest=NativeExportJsonTest test
```

Expected: compilation fails because model and JSON classes do not exist.

- [ ] **Step 3: Implement immutable serializable model classes**

Create each class with `final` fields, constructor validation, and getters named as used in the tests. Use `ArrayList` / `LinkedHashMap` defensive copies and expose unmodifiable views. `NativeExportFile` stores:

```java
private final String path;
private final long rowCount;
private final long fileSize;
private final long schemaId;
private final List<Long> deletedPositions;
```

`NativeExportTask` stores:

```java
private final String outputPath;
private final String compression;
private final long targetFileSizeBytes;
private final long readBufferSizeBytes;
private final int readConcurrency;
private final int writerBatchSize;
private final int writerRowGroupSize;
private final long multipartPartSizeBytes;
private final long memoryLimitBytes;
private final List<String> projection;
private final String predicateFormat;
private final String predicateJson;
private final Map<String, String> objectStoreOptions;
private final List<NativeExportFile> files;
```

`NativeExportMetrics` stores all metric fields from the spec and an `add` method:

```java
NativeExportMetrics add(NativeExportMetrics other) {
    return new NativeExportMetrics(
            rowsRead + other.rowsRead,
            rowsOutput + other.rowsOutput,
            predicateFilteredRows + other.predicateFilteredRows,
            dvFilteredRows + other.dvFilteredRows,
            parquetRowGroupsRead + other.parquetRowGroupsRead,
            parquetRowGroupsPruned + other.parquetRowGroupsPruned,
            Math.max(peakBufferedBytes, other.peakBufferedBytes),
            writerRolls + other.writerRolls,
            obsReadRequests + other.obsReadRequests,
            obsReadRetries + other.obsReadRetries,
            obsReadBytes + other.obsReadBytes,
            obsWriteRequests + other.obsWriteRequests,
            obsWriteBytes + other.obsWriteBytes,
            readMs + other.readMs,
            decodeMs + other.decodeMs,
            filterMs + other.filterMs,
            encodeMs + other.encodeMs,
            obsWriteMs + other.obsWriteMs,
            multipartFinishMs + other.multipartFinishMs);
}
```

- [ ] **Step 4: Implement NativeExportJson with deterministic JSON**

Use manual JSON construction and parsing with Jackson only if already available in `paimon-native-io` compile scope. If Jackson is not available, use the existing project JSON utility if present; otherwise add a small local writer/parser for this stable schema. The writer must preserve object-store key order and redact these keys in `toRedactedJson`:

```java
private static boolean isSecretKey(String key) {
    String normalized = key.toLowerCase(java.util.Locale.ROOT);
    return normalized.contains("access.key")
            || normalized.contains("secret.key")
            || normalized.contains("security.token")
            || normalized.contains("session.token")
            || normalized.endsWith(".ak")
            || normalized.endsWith(".sk");
}
```

Implement `NativeExportObjectStoreOptions` with tests for:

- `fs.obs.*` values overriding `fs.s3a.*` aliases.
- bucket parsed from `obs://bucket/path` and mismatch rejection.
- explicit rejection for unsupported credential provider-only configurations.
- redaction of access key, secret key, security token, and session token.

- [ ] **Step 5: Run tests**

Run:

```bash
cd /Users/opay-20240095/IdeaProjects/nativeio/paimon-plus
./mvnw -pl paimon-native-io -DskipITs -Dcheckstyle.skip -Drat.skip -Dspotless.check.skip -Dtest=NativeExportJsonTest test
```

Expected: `NativeExportJsonTest` passes.

- [ ] **Step 6: Commit**

```bash
git add paimon-native-io/src/main/java/org/apache/paimon/nativeio/export \
  paimon-native-io/src/test/java/org/apache/paimon/nativeio/export/NativeExportJsonTest.java
git commit -m "feat: add native export request models"
```

---

### Task 3: Predicate JSON Converter and Driver-Side Applicability

**Files:**

- Create: `/Users/opay-20240095/IdeaProjects/nativeio/paimon-plus/paimon-native-io/src/main/java/org/apache/paimon/nativeio/export/NativeExportPredicateJson.java`
- Create: `/Users/opay-20240095/IdeaProjects/nativeio/paimon-plus/paimon-native-io/src/main/java/org/apache/paimon/nativeio/export/NativeExportApplicability.java`
- Test: `/Users/opay-20240095/IdeaProjects/nativeio/paimon-plus/paimon-native-io/src/test/java/org/apache/paimon/nativeio/export/NativeExportPredicateJsonTest.java`

- [ ] **Step 1: Write failing predicate tests**

Add tests that use Paimon `PredicateBuilder`:

```java
package org.apache.paimon.nativeio.export;

import org.apache.paimon.operation.nativeio.NativeRejectReason;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.predicate.PredicateBuilder;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class NativeExportPredicateJsonTest {

    private static final RowType ROW_TYPE =
            RowType.of(
                    new org.apache.paimon.types.DataType[] {
                        DataTypes.BIGINT(), DataTypes.INT(), DataTypes.STRING(), DataTypes.DATE(), DataTypes.DECIMAL(20, 4)
                    },
                    new String[] {"id", "score", "name", "dt", "amount"});

    @Test
    void convertsSupportedAndPredicate() {
        PredicateBuilder builder = new PredicateBuilder(ROW_TYPE);
        Predicate predicate =
                PredicateBuilder.and(
                        builder.greaterOrEqual(1, 10), builder.equal(2, "alice"));

        NativeExportApplicability json = NativeExportPredicateJson.convert(ROW_TYPE, predicate);

        assertThat(json.applicable()).isTrue();
        assertThat(json.detail()).contains("\"op\":\"and\"");
        assertThat(json.detail()).contains("\"field\":\"score\"");
        assertThat(json.detail()).contains("\"field\":\"name\"");
        assertThat(json.detail()).contains("\"type\":\"INT\"");
    }

    @Test
    void encodesDateAndDecimalLiteralsWithStableTypes() {
        PredicateBuilder builder = new PredicateBuilder(ROW_TYPE);
        Predicate predicate =
                PredicateBuilder.and(
                        builder.equal(3, java.sql.Date.valueOf("2026-05-06")),
                        builder.greaterOrEqual(4, new java.math.BigDecimal("12.3456")));

        NativeExportApplicability json = NativeExportPredicateJson.convert(ROW_TYPE, predicate);

        assertThat(json.applicable()).isTrue();
        assertThat(json.detail()).contains("\"type\":\"DATE\"");
        assertThat(json.detail()).contains("\"epoch_day\"");
        assertThat(json.detail()).contains("\"type\":\"DECIMAL\"");
        assertThat(json.detail()).contains("\"precision\":20");
        assertThat(json.detail()).contains("\"scale\":4");
        assertThat(json.detail()).contains("\"unscaled\":\"123456\"");
    }

    @Test
    void rejectsUnsupportedPredicate() {
        PredicateBuilder builder = new PredicateBuilder(ROW_TYPE);
        Predicate predicate = builder.startsWith(2, "a");

        NativeExportApplicability json = NativeExportPredicateJson.convert(ROW_TYPE, predicate);

        assertThat(json.applicable()).isFalse();
        assertThat(json.reason()).isEqualTo(NativeRejectReason.EXPORT_UNSUPPORTED_PREDICATE);
    }

    @Test
    void nullPredicateBecomesTruePredicate() {
        NativeExportApplicability json = NativeExportPredicateJson.convert(ROW_TYPE, null);

        assertThat(json.applicable()).isTrue();
        assertThat(json.detail()).isEqualTo("{\"op\":\"true\"}");
    }
}
```

- [ ] **Step 2: Run test to verify it fails**

Run:

```bash
cd /Users/opay-20240095/IdeaProjects/nativeio/paimon-plus
./mvnw -pl paimon-native-io -DskipITs -Dcheckstyle.skip -Drat.skip -Dspotless.check.skip -Dtest=NativeExportPredicateJsonTest test
```

Expected: compilation fails because converter and applicability classes do not exist.

- [ ] **Step 3: Implement NativeExportApplicability**

Create a small value object matching `NativeApplicability` behavior but allowing successful detail payload:

```java
package org.apache.paimon.nativeio.export;

import org.apache.paimon.operation.nativeio.NativeRejectReason;

import javax.annotation.Nullable;

/** Applicability result for native export preflight and JSON conversion. */
public final class NativeExportApplicability {

    private final boolean applicable;
    @Nullable private final NativeRejectReason reason;
    private final String detail;

    private NativeExportApplicability(
            boolean applicable, @Nullable NativeRejectReason reason, String detail) {
        this.applicable = applicable;
        this.reason = reason;
        this.detail = detail;
    }

    public static NativeExportApplicability yes(String detail) {
        return new NativeExportApplicability(true, null, detail);
    }

    public static NativeExportApplicability rejected(NativeRejectReason reason, String detail) {
        return new NativeExportApplicability(false, reason, detail);
    }

    public boolean applicable() {
        return applicable;
    }

    @Nullable
    public NativeRejectReason reason() {
        return reason;
    }

    public String detail() {
        return detail;
    }
}
```

- [ ] **Step 4: Implement converter for first supported subset**

Implement `NativeExportPredicateJson.convert(RowType rowType, @Nullable Predicate predicate)`. Use the concrete predicate classes in the current Paimon predicate package. The output JSON shape must be:

```json
{"op":"and","children":[{"op":"gte","field":"score","literal":{"type":"INT","value":10}},{"op":"eq","field":"name","literal":{"type":"STRING","value":"alice"}}]}
```

Map operations:

- `Equal` -> `eq`
- `NotEqual` -> `neq`
- `GreaterThan` -> `gt`
- `GreaterOrEqual` -> `gte`
- `LessThan` -> `lt`
- `LessOrEqual` -> `lte`
- `IsNull` -> `is_null`
- `IsNotNull` -> `is_not_null`
- `In` -> `in`
- `And` -> `and`
- `Or` -> `or`
- `Not` -> `not`

Reject all other predicates with `EXPORT_UNSUPPORTED_PREDICATE`.

Literal encoding must be type-stable:

- DATE -> `{"type":"DATE","epoch_day":...}`
- TIMESTAMP -> `{"type":"TIMESTAMP","micros":...}`
- DECIMAL -> `{"type":"DECIMAL","precision":...,"scale":...,"unscaled":"..."}`
- STRING/BOOLEAN/integral/floating types -> `{"type":"...","value":...}`

- [ ] **Step 5: Run tests**

Run:

```bash
cd /Users/opay-20240095/IdeaProjects/nativeio/paimon-plus
./mvnw -pl paimon-native-io -DskipITs -Dcheckstyle.skip -Drat.skip -Dspotless.check.skip -Dtest=NativeExportPredicateJsonTest test
```

Expected: predicate converter tests pass.

- [ ] **Step 6: Commit**

```bash
git add paimon-native-io/src/main/java/org/apache/paimon/nativeio/export/NativeExportApplicability.java \
  paimon-native-io/src/main/java/org/apache/paimon/nativeio/export/NativeExportPredicateJson.java \
  paimon-native-io/src/test/java/org/apache/paimon/nativeio/export/NativeExportPredicateJsonTest.java
git commit -m "feat: convert export predicates for native path"
```

---

### Task 4: Rust Export Request, Result, and C ABI

**Files:**

- Create: `/Users/opay-20240095/IdeaProjects/nativeio/paimon-plus/paimon-native-io/rust/paimon-native-io-c/src/export.rs`
- Create: `/Users/opay-20240095/IdeaProjects/nativeio/paimon-plus/paimon-native-io/rust/paimon-native-io-c/src/export_runtime.rs`
- Modify: `/Users/opay-20240095/IdeaProjects/nativeio/paimon-plus/paimon-native-io/rust/paimon-native-io-c/src/lib.rs`
- Modify: `/Users/opay-20240095/IdeaProjects/nativeio/paimon-plus/paimon-native-io/rust/paimon-native-io-c/Cargo.toml`

- [ ] **Step 1: Write failing Rust tests for request/result parsing**

In `export.rs`, add tests with this shape:

```rust
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_export_request() {
        let json = r#"{
            "request_version":1,
            "output_path":"obs://bucket/out",
            "compression":"zstd",
            "target_file_size_bytes":536870912,
            "read_buffer_size_bytes":8388608,
            "read_concurrency":4,
            "writer_batch_size":8192,
            "writer_row_group_size":250000,
            "multipart_part_size_bytes":67108864,
            "memory_limit_bytes":536870912,
            "projection":["id"],
            "predicate_format":"paimon-json-v1",
            "predicate":{"op":"true"},
            "object_store":{"fs.obs.endpoint":"http://obs.example"},
            "files":[{"path":"file:/tmp/a.parquet","row_count":2,"file_size":10,"schema_id":1,"dv":{"format":"positions","positions":[1]}}]
        }"#;

        let request: ExportRequest = serde_json::from_str(json).unwrap();

        assert_eq!(request.output_path, "obs://bucket/out");
        assert_eq!(request.request_version, 1);
        assert_eq!(request.predicate_format, "paimon-json-v1");
        assert_eq!(request.writer_row_group_size, 250000);
        assert_eq!(request.multipart_part_size_bytes, 67108864);
        assert_eq!(request.memory_limit_bytes, 536870912);
        assert_eq!(request.files.len(), 1);
        assert_eq!(request.files[0].deleted_positions(), vec![1]);
    }

    #[test]
    fn rejects_unknown_predicate_format() {
        let json = r#"{
            "request_version":1,
            "output_path":"obs://bucket/out",
            "compression":"zstd",
            "target_file_size_bytes":536870912,
            "read_buffer_size_bytes":8388608,
            "read_concurrency":4,
            "writer_batch_size":8192,
            "writer_row_group_size":250000,
            "multipart_part_size_bytes":67108864,
            "memory_limit_bytes":536870912,
            "projection":["id"],
            "predicate_format":"unknown",
            "predicate":{"op":"true"},
            "object_store":{},
            "files":[]
        }"#;

        let request: ExportRequest = serde_json::from_str(json).unwrap();
        assert!(request.validate().unwrap_err().contains("UNKNOWN_PREDICATE_FORMAT"));
    }

    #[test]
    fn serializes_export_result() {
        let result = ExportResult {
            rows_output: 2,
            files_written: vec![ExportWrittenFile {
                path: "obs://bucket/out/part.parquet".to_string(),
                rows: 2,
                bytes: 128,
            }],
            metrics: ExportMetrics {
                rows_read: 3,
                rows_output: 2,
                predicate_filtered_rows: 1,
                dv_filtered_rows: 0,
                parquet_row_groups_read: 2,
                parquet_row_groups_pruned: 1,
                peak_buffered_bytes: 1048576,
                writer_rolls: 1,
                obs_read_requests: 4,
                obs_read_retries: 0,
                obs_read_bytes: 4096,
                obs_write_requests: 1,
                obs_write_bytes: 128,
                read_ms: 10,
                decode_ms: 20,
                filter_ms: 5,
                encode_ms: 30,
                obs_write_ms: 40,
                multipart_finish_ms: 3,
            },
        };

        let json = serde_json::to_string(&result).unwrap();
        assert!(json.contains("\"rows_output\":2"));
        assert!(json.contains("\"obs_read_requests\":4"));
    }
}
```

- [ ] **Step 2: Run Rust tests to verify they fail**

Run:

```bash
cd /Users/opay-20240095/IdeaProjects/nativeio/paimon-plus/paimon-native-io/rust
cargo test -p paimon-native-io-c export::tests::parses_export_request export::tests::serializes_export_result
```

Expected: tests fail because `export` module and structs do not exist.

- [ ] **Step 3: Add serde dependencies**

In `paimon-native-io-c/Cargo.toml`, add:

```toml
serde = { version = "1", features = ["derive"] }
serde_json = "1"
```

- [ ] **Step 4: Implement export request/result structs**

Create `export.rs` with `serde` structs matching the JSON in the spec:

```rust
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

#[derive(Debug, Deserialize)]
pub struct ExportRequest {
    pub request_version: u32,
    pub output_path: String,
    pub compression: String,
    pub target_file_size_bytes: u64,
    pub read_buffer_size_bytes: usize,
    pub read_concurrency: usize,
    pub writer_batch_size: usize,
    pub writer_row_group_size: usize,
    pub multipart_part_size_bytes: usize,
    pub memory_limit_bytes: usize,
    pub projection: Vec<String>,
    pub predicate_format: String,
    pub predicate: PredicateNode,
    pub object_store: HashMap<String, String>,
    pub files: Vec<ExportFile>,
}

#[derive(Debug, Deserialize)]
pub struct ExportFile {
    pub path: String,
    pub row_count: i64,
    pub file_size: u64,
    pub schema_id: i64,
    pub dv: Option<DeletionVectorSpec>,
}

#[derive(Debug, Deserialize)]
#[serde(tag = "format")]
pub enum DeletionVectorSpec {
    #[serde(rename = "positions")]
    Positions { positions: Vec<i64> },
}

impl ExportFile {
    pub fn deleted_positions(&self) -> Vec<i64> {
        match &self.dv {
            Some(DeletionVectorSpec::Positions { positions }) => positions.clone(),
            None => Vec::new(),
        }
    }
}

#[derive(Debug, Deserialize)]
pub struct PredicateNode {
    pub op: String,
    #[serde(default)]
    pub field: Option<String>,
    #[serde(default)]
    pub literal: Option<serde_json::Value>,
    #[serde(default)]
    pub children: Vec<PredicateNode>,
}

#[derive(Debug, Serialize)]
pub struct ExportResult {
    pub rows_output: u64,
    pub files_written: Vec<ExportWrittenFile>,
    pub metrics: ExportMetrics,
}

#[derive(Debug, Serialize)]
pub struct ExportWrittenFile {
    pub path: String,
    pub rows: u64,
    pub bytes: u64,
}

#[derive(Debug, Default, Serialize)]
pub struct ExportMetrics {
    pub rows_read: u64,
    pub rows_output: u64,
    pub predicate_filtered_rows: u64,
    pub dv_filtered_rows: u64,
    pub parquet_row_groups_read: u64,
    pub parquet_row_groups_pruned: u64,
    pub peak_buffered_bytes: u64,
    pub writer_rolls: u64,
    pub obs_read_requests: u64,
    pub obs_read_retries: u64,
    pub obs_read_bytes: u64,
    pub obs_write_requests: u64,
    pub obs_write_bytes: u64,
    pub read_ms: u64,
    pub decode_ms: u64,
    pub filter_ms: u64,
    pub encode_ms: u64,
    pub obs_write_ms: u64,
    pub multipart_finish_ms: u64,
}
```

Add `ExportRequest::validate()` and call it before any export execution. It must reject unknown `request_version` and unknown `predicate_format`; first phase supports only `request_version=1` and `predicate_format=paimon-json-v1`.

- [ ] **Step 5: Add process-wide runtime helper**

Create `export_runtime.rs` with a lazy process-wide Tokio runtime and optional parquet metadata cache placeholder. Add a unit test asserting repeated calls return the same runtime handle and do not create per-request worker pools. Runtime worker count is configured from request/options on first initialization; later mismatched values should log and keep the existing runtime.

- [ ] **Step 6: Add C ABI entry points**

In `lib.rs`, add:

```rust
mod export;

#[unsafe(no_mangle)]
pub extern "C" fn paimon_exporter_new() -> *mut export::Exporter {
    export::exporter_new()
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn paimon_exporter_free(exporter: *mut export::Exporter) {
    export::exporter_free(exporter)
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn paimon_exporter_export_parquet(
    exporter: *mut export::Exporter,
    request_json: *const std::os::raw::c_char,
    result_json: *mut *mut std::os::raw::c_char,
    error_message: *mut *mut std::os::raw::c_char,
) -> i32 {
    export::export_parquet_ffi(exporter, request_json, result_json, error_message)
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn paimon_string_free(value: *mut std::os::raw::c_char) {
    export::string_free(value)
}
```

Implement `Exporter`, `exporter_new`, `exporter_free`, `export_parquet_ffi`, and `string_free` in `export.rs`. At this task stage, `export_parquet_ffi` may return a valid empty `ExportResult` for a syntactically valid request; the full pipeline arrives in later tasks.

- [ ] **Step 7: Run Rust tests**

Run:

```bash
cd /Users/opay-20240095/IdeaProjects/nativeio/paimon-plus/paimon-native-io/rust
cargo test -p paimon-native-io-c export::tests
```

Expected: export parse/serialize/FFI unit tests pass.

- [ ] **Step 8: Commit**

```bash
git add paimon-native-io/rust/paimon-native-io-c/Cargo.toml \
  paimon-native-io/rust/paimon-native-io-c/src/lib.rs \
  paimon-native-io/rust/paimon-native-io-c/src/export.rs \
  paimon-native-io/rust/paimon-native-io-c/src/export_runtime.rs
git commit -m "feat: add native export ffi contract"
```

---

### Task 5: object_store-backed Buffered OBS and Local Parquet Reader

**Files:**

- Create: `/Users/opay-20240095/IdeaProjects/nativeio/paimon-plus/paimon-native-io/rust/paimon-native-io-c/src/buffered_reader.rs`
- Create: `/Users/opay-20240095/IdeaProjects/nativeio/paimon-plus/paimon-native-io/rust/paimon-native-io-c/src/export_object_store.rs`
- Modify: `/Users/opay-20240095/IdeaProjects/nativeio/paimon-plus/paimon-native-io/rust/paimon-native-io-c/src/lib.rs`
- Modify: `/Users/opay-20240095/IdeaProjects/nativeio/paimon-plus/paimon-native-io/rust/paimon-native-io-c/src/export.rs`

- [ ] **Step 1: Write failing buffered reader tests**

Add a fake range source test in `buffered_reader.rs`:

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::{Arc, Mutex};

    #[derive(Clone)]
    struct FakeRangeSource {
        data: Arc<Vec<u8>>,
        requests: Arc<Mutex<Vec<(u64, usize)>>>,
    }

    impl RangeSource for FakeRangeSource {
        fn len(&self) -> u64 {
            self.data.len() as u64
        }

        fn fetch_range(&self, start: u64, length: usize) -> parquet::errors::Result<bytes::Bytes> {
            self.requests.lock().unwrap().push((start, length));
            let start_usize = start as usize;
            let end = start_usize + length;
            Ok(bytes::Bytes::copy_from_slice(&self.data[start_usize..end]))
        }
    }

    #[test]
    fn merges_small_sequential_reads() {
        let source = FakeRangeSource {
            data: Arc::new((0..64u8).collect()),
            requests: Arc::new(Mutex::new(Vec::new())),
        };
        let mut read = BufferedRangeRead::new(source.clone(), 0, 16);
        let mut first = [0u8; 4];
        let mut second = [0u8; 4];

        std::io::Read::read_exact(&mut read, &mut first).unwrap();
        std::io::Read::read_exact(&mut read, &mut second).unwrap();

        assert_eq!(first, [0, 1, 2, 3]);
        assert_eq!(second, [4, 5, 6, 7]);
        assert_eq!(source.requests.lock().unwrap().as_slice(), &[(0, 16)]);
    }
}
```

- [ ] **Step 2: Run Rust test to verify it fails**

Run:

```bash
cd /Users/opay-20240095/IdeaProjects/nativeio/paimon-plus/paimon-native-io/rust
cargo test -p paimon-native-io-c buffered_reader::tests::merges_small_sequential_reads
```

Expected: test fails because `buffered_reader` does not exist.

- [ ] **Step 3: Implement RangeSource and buffered reader**

Create `buffered_reader.rs` with:

```rust
use bytes::Bytes;
use parquet::errors::{ParquetError, Result as ParquetResult};
use std::io::Read;

pub trait RangeSource: Clone + Send + Sync + 'static {
    fn len(&self) -> u64;
    fn fetch_range(&self, start: u64, length: usize) -> ParquetResult<Bytes>;
}

#[derive(Clone, Default, Debug)]
pub struct RangeMetrics {
    pub requests: u64,
    pub bytes: u64,
}

pub struct BufferedRangeRead<S: RangeSource> {
    source: S,
    position: u64,
    buffer_start: u64,
    buffer: Bytes,
    buffer_size: usize,
    metrics: RangeMetrics,
}

impl<S: RangeSource> BufferedRangeRead<S> {
    pub fn new(source: S, position: u64, buffer_size: usize) -> Self {
        Self {
            source,
            position,
            buffer_start: position,
            buffer: Bytes::new(),
            buffer_size: buffer_size.max(1),
            metrics: RangeMetrics::default(),
        }
    }

    pub fn metrics(&self) -> RangeMetrics {
        self.metrics.clone()
    }

    fn refill(&mut self) -> ParquetResult<()> {
        if self.position >= self.source.len() {
            self.buffer = Bytes::new();
            self.buffer_start = self.position;
            return Ok(());
        }
        let remaining = (self.source.len() - self.position) as usize;
        let length = self.buffer_size.min(remaining);
        let bytes = self.source.fetch_range(self.position, length)?;
        self.metrics.requests += 1;
        self.metrics.bytes += bytes.len() as u64;
        self.buffer_start = self.position;
        self.buffer = bytes;
        Ok(())
    }
}

impl<S: RangeSource> Read for BufferedRangeRead<S> {
    fn read(&mut self, out: &mut [u8]) -> std::io::Result<usize> {
        if out.is_empty() || self.position >= self.source.len() {
            return Ok(0);
        }
        let offset = self.position.saturating_sub(self.buffer_start) as usize;
        if offset >= self.buffer.len() {
            self.refill()
                .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e.to_string()))?;
        }
        let offset = self.position.saturating_sub(self.buffer_start) as usize;
        if offset >= self.buffer.len() {
            return Ok(0);
        }
        let available = &self.buffer[offset..];
        let length = available.len().min(out.len());
        out[..length].copy_from_slice(&available[..length]);
        self.position += length as u64;
        Ok(length)
    }
}
```

- [ ] **Step 4: Implement object_store OBS registration**

Create `export_object_store.rs` with a small adapter that maps request `object_store` options to an object_store-compatible S3/OBS client. Cover endpoint, bucket extraction from `obs://bucket/path`, path-style access, access key, secret key, optional security token, retry, and timeout.

Add tests using option maps only; credentials in assertion/debug output must be redacted.

- [ ] **Step 5: Refactor export reader to use buffered object_store reads**

Move reusable range source logic behind a type implementing `RangeSource`, backed by object_store for OBS and local file IO for tests. Keep `get_bytes(start, length)` direct for footer and random access, but route sequential `Read` through `BufferedRangeRead`.

- [ ] **Step 6: Run Rust tests**

Run:

```bash
cd /Users/opay-20240095/IdeaProjects/nativeio/paimon-plus/paimon-native-io/rust
cargo test -p paimon-native-io-c buffered_reader::tests
```

Expected: buffered reader tests pass.

- [ ] **Step 7: Commit**

```bash
git add paimon-native-io/rust/paimon-native-io-c/src/buffered_reader.rs \
  paimon-native-io/rust/paimon-native-io-c/src/export_object_store.rs \
  paimon-native-io/rust/paimon-native-io-c/src/lib.rs \
  paimon-native-io/rust/paimon-native-io-c/src/export.rs
git commit -m "feat: add object store buffered export reads"
```

---

### Task 6: Rust Native Export Pipeline

**Files:**

- Modify: `/Users/opay-20240095/IdeaProjects/nativeio/paimon-plus/paimon-native-io/rust/paimon-native-io-c/src/export.rs`
- Modify: `/Users/opay-20240095/IdeaProjects/nativeio/paimon-plus/paimon-native-io/rust/paimon-native-io-c/src/buffered_reader.rs`
- Create: `/Users/opay-20240095/IdeaProjects/nativeio/paimon-plus/paimon-native-io/rust/paimon-native-io-c/src/multipart_writer.rs`

- [ ] **Step 1: Write failing local export pipeline test**

Add a Rust test that writes a small local Parquet input, exports it, and reads output back:

```rust
#[test]
fn exports_local_parquet_with_projection_predicate_and_dv() {
    let input = write_test_parquet_with_id_score(vec![(1, 10), (2, 20), (3, 30)]);
    let output_dir = tempfile::tempdir().unwrap();
    let request = ExportRequest {
        request_version: 1,
        output_path: format!("file:{}", output_dir.path().display()),
        compression: "zstd".to_string(),
        target_file_size_bytes: 1024 * 1024,
        read_buffer_size_bytes: 1024 * 1024,
        read_concurrency: 1,
        writer_batch_size: 1024,
        writer_row_group_size: 1024,
        multipart_part_size_bytes: 8 * 1024 * 1024,
        memory_limit_bytes: 64 * 1024 * 1024,
        projection: vec!["id".to_string(), "score".to_string()],
        predicate_format: "paimon-json-v1".to_string(),
        predicate: serde_json::from_str(r#"{"op":"gt","field":"score","literal":{"type":"INT","value":10}}"#).unwrap(),
        object_store: std::collections::HashMap::new(),
        files: vec![ExportFile {
            path: input,
            row_count: 3,
            file_size: 0,
            schema_id: 1,
            dv: Some(DeletionVectorSpec::Positions { positions: vec![2] }),
        }],
    };

    let result = export_parquet(request).unwrap();

    assert_eq!(result.rows_output, 1);
    assert_eq!(result.metrics.rows_read, 3);
    assert_eq!(result.metrics.predicate_filtered_rows, 1);
    assert_eq!(result.metrics.dv_filtered_rows, 1);
    assert_eq!(read_test_output_rows(&result.files_written[0].path), vec![(2, 20)]);
}
```

Implement `write_test_parquet_with_id_score` and `read_test_output_rows` in the test module using arrow-rs `ArrowWriter` and `ParquetRecordBatchReaderBuilder`.

- [ ] **Step 2: Run Rust test to verify it fails**

Run:

```bash
cd /Users/opay-20240095/IdeaProjects/nativeio/paimon-plus/paimon-native-io/rust
cargo test -p paimon-native-io-c export::tests::exports_local_parquet_with_projection_predicate_and_dv
```

Expected: test fails because `export_parquet` pipeline is not implemented.

- [ ] **Step 3: Implement predicate evaluator**

Implement evaluator over Arrow arrays for the supported first-stage operations:

- `true`
- `and`
- `or`
- `eq`
- `neq`
- `gt`
- `gte`
- `lt`
- `lte`
- `is_null`
- `is_not_null`

For this task, support `Int64`, `Int32`, and `Utf8` first. Reject unsupported arrays by returning an error string that includes `unsupported predicate type`.

- [ ] **Step 4: Implement DV filtering**

Convert deleted positions to a sorted `Vec<i64>` and filter each batch by file-local row offset. Count filtered rows in `metrics.dv_filtered_rows`.

- [ ] **Step 5: Add row group pruning hook and metrics**

Add `parquet_row_groups_read` and `parquet_row_groups_pruned` metrics to the pipeline. If first implementation uses arrow-rs without row group selector, set `parquet_row_groups_pruned=0` and count row groups read from parquet metadata. Keep a small interface around predicate-to-row-group-statistics evaluation so the next iteration can swap in DataFusion `ParquetSource` / `FileScanConfig` pruning.

- [ ] **Step 6: Add memory-limit accounting and writer rolling metrics**

Track estimated in-flight bytes for read buffer, current Arrow batch, writer buffer, and multipart part buffer. Update `peak_buffered_bytes` on every batch. If estimated bytes exceed `memory_limit_bytes`, flush/roll writer before reading more input. Increment `writer_rolls` whenever target file size or memory limit closes the current writer and opens a new one.

- [ ] **Step 7: Implement multipart Parquet writer and target-size rolling**

Use arrow-rs `ArrowWriter` with zstd properties. For OBS output, write through object_store multipart upload following the LakeSoul `MultiPartAsyncWriter` pattern: `ArrowWriter` writes into an in-memory buffer, flushed row groups become multipart parts, close calls multipart `finish()`, and failure calls multipart `abort()`. For local tests, provide a local writer implementation behind the same trait.

Start a new file when the current writer estimated bytes exceed `target_file_size_bytes`. Use deterministic test file naming for local tests and UUID naming in non-test execution:

```text
part-native-${ordinal}-${uuid}.parquet
```

- [ ] **Step 8: Run Rust pipeline tests**

Run:

```bash
cd /Users/opay-20240095/IdeaProjects/nativeio/paimon-plus/paimon-native-io/rust
cargo test -p paimon-native-io-c export::tests
```

Expected: export request, result, and local pipeline tests pass.

- [ ] **Step 9: Commit**

```bash
git add paimon-native-io/rust/paimon-native-io-c/src/export.rs \
  paimon-native-io/rust/paimon-native-io-c/src/buffered_reader.rs \
  paimon-native-io/rust/paimon-native-io-c/src/multipart_writer.rs
git commit -m "feat: implement native parquet export pipeline"
```

---

### Task 7: Java JNR Export Wrapper and Runner

**Files:**

- Modify: `/Users/opay-20240095/IdeaProjects/nativeio/paimon-plus/paimon-native-io/src/main/java/org/apache/paimon/nativeio/jnr/LibPaimonNativeIO.java`
- Create: `/Users/opay-20240095/IdeaProjects/nativeio/paimon-plus/paimon-native-io/src/main/java/org/apache/paimon/nativeio/export/PaimonNativeExporter.java`
- Create: `/Users/opay-20240095/IdeaProjects/nativeio/paimon-plus/paimon-native-io/src/main/java/org/apache/paimon/nativeio/export/NativeExportRunner.java`
- Test: `/Users/opay-20240095/IdeaProjects/nativeio/paimon-plus/paimon-native-io/src/test/java/org/apache/paimon/nativeio/export/PaimonNativeExporterTest.java`

- [ ] **Step 1: Write failing wrapper tests using a fake library**

Add a package-private constructor to `PaimonNativeExporter` accepting `LibPaimonNativeIO` for tests. Write a fake library that returns a result JSON and assert the Java wrapper parses it.

```java
@Test
void exporterParsesNativeResult() throws Exception {
    FakeLibPaimonNativeIO lib = new FakeLibPaimonNativeIO();
    PaimonNativeExporter exporter = new PaimonNativeExporter(lib);

    NativeExportResult result = exporter.exportParquet(sampleTask());

    assertThat(result.rowsOutput()).isEqualTo(2L);
    assertThat(result.metrics().obsReadRequests()).isEqualTo(4L);
    assertThat(lib.lastRequest()).contains("\"output_path\"");
}
```

- [ ] **Step 2: Run test to verify it fails**

Run:

```bash
cd /Users/opay-20240095/IdeaProjects/nativeio/paimon-plus
./mvnw -pl paimon-native-io -DskipITs -Dcheckstyle.skip -Drat.skip -Dspotless.check.skip -Dtest=PaimonNativeExporterTest test
```

Expected: compilation fails because export JNR methods and wrapper do not exist.

- [ ] **Step 3: Extend LibPaimonNativeIO**

Add methods:

```java
Pointer paimon_exporter_new();

void paimon_exporter_free(Pointer exporter);

int paimon_exporter_export_parquet(
        Pointer exporter, String requestJson, PointerByReference resultJson, PointerByReference errorMessage);

void paimon_string_free(Pointer value);
```

Use `jnr.ffi.byref.PointerByReference`.

Do not add a new native library loader. `PaimonNativeExporter` must obtain `LibPaimonNativeIO` from existing `PaimonJnrLoader.current()`, and tests must cover unavailable loader behavior by injecting a fake library or fake loader result.

- [ ] **Step 4: Implement PaimonNativeExporter**

`PaimonNativeExporter.exportParquet(NativeExportTask task)` must:

1. Create exporter pointer.
2. Serialize task with `NativeExportJson.toJson`.
3. Call native FFI.
4. On status `0`, parse result JSON and free result string.
5. On non-zero status, read error message, free error string, and throw `IOException`.
6. Always free exporter pointer.

- [ ] **Step 5: Implement NativeExportRunner**

Create a `Serializable` runner with:

```java
public NativeExportResult run(NativeExportTask task) throws IOException {
    return new PaimonNativeExporter().exportParquet(task);
}
```

Keep runner free of Spark classes so it is easy to test.

- [ ] **Step 6: Run wrapper tests**

Run:

```bash
cd /Users/opay-20240095/IdeaProjects/nativeio/paimon-plus
./mvnw -pl paimon-native-io -DskipITs -Dcheckstyle.skip -Drat.skip -Dspotless.check.skip -Dtest=PaimonNativeExporterTest test
```

Expected: wrapper tests pass without loading the real native library.

- [ ] **Step 7: Commit**

```bash
git add paimon-native-io/src/main/java/org/apache/paimon/nativeio/jnr/LibPaimonNativeIO.java \
  paimon-native-io/src/main/java/org/apache/paimon/nativeio/export/PaimonNativeExporter.java \
  paimon-native-io/src/main/java/org/apache/paimon/nativeio/export/NativeExportRunner.java \
  paimon-native-io/src/test/java/org/apache/paimon/nativeio/export/PaimonNativeExporterTest.java
git commit -m "feat: wrap native parquet export ffi"
```

---

### Task 8: Planner and ExportParquetProcedure Integration

**Files:**

- Create: `/Users/opay-20240095/IdeaProjects/nativeio/paimon-plus/paimon-native-io/src/main/java/org/apache/paimon/nativeio/export/NativeExportPlanner.java`
- Create: `/Users/opay-20240095/IdeaProjects/nativeio/paimon-plus/paimon-native-io/src/main/java/org/apache/paimon/nativeio/export/NativeExportProviderFactoryImpl.java`
- Create: `/Users/opay-20240095/IdeaProjects/nativeio/paimon-plus/paimon-native-io/src/main/java/org/apache/paimon/nativeio/export/NativeExportProviderImpl.java`
- Create: `/Users/opay-20240095/IdeaProjects/nativeio/paimon-plus/paimon-native-io/src/main/resources/META-INF/services/org.apache.paimon.operation.nativeio.export.NativeExportProviderFactory`
- Modify: `/Users/opay-20240095/IdeaProjects/nativeio/paimon-plus/paimon-spark/paimon-spark-common/src/main/java/org/apache/paimon/spark/procedure/ExportParquetProcedure.java`
- Test: existing Spark procedure tests or new tests under `/Users/opay-20240095/IdeaProjects/nativeio/paimon-plus/paimon-spark/paimon-spark-common/src/test/scala/`

- [ ] **Step 1: Write failing task-count unit test**

Add a test around `ExportParquetProcedure` or extract task count logic into a package-private method. The test must assert:

```java
assertThat(nativeTaskCount(32, 11)).isEqualTo(11);
assertThat(nativeTaskCount(32, 100)).isEqualTo(32);
assertThat(nativeTaskCount(1, 100)).isEqualTo(1);
```

Expected: current Java export `numPartitions` still considers `target_file_size`; native count logic does not exist.

- [ ] **Step 2: Implement native provider registration**

Create `NativeExportProviderFactoryImpl`:

```java
package org.apache.paimon.nativeio.export;

import org.apache.paimon.operation.nativeio.export.NativeExportProvider;
import org.apache.paimon.operation.nativeio.export.NativeExportProviderFactory;

/** ServiceLoader entry point for paimon-native-io export support. */
public final class NativeExportProviderFactoryImpl implements NativeExportProviderFactory {

    @Override
    public NativeExportProvider create() {
        return new NativeExportProviderImpl();
    }
}
```

Register the implementation in `META-INF/services/org.apache.paimon.operation.nativeio.export.NativeExportProviderFactory`:

```text
org.apache.paimon.nativeio.export.NativeExportProviderFactoryImpl
```

The provider factory constructor must not load the native library. It may allocate Java-only helper objects only.

- [ ] **Step 3: Implement NativeExportPlanner preflight**

`NativeExportPlanner` responsibilities:

1. Read `NativeIOOptions` and `NativeExportOptions`.
2. Build `NativeExportObjectStoreOptions` from Spark Hadoop conf, catalog options, and table filesystem options.
3. Validate engine support and OBS config.
4. Validate compression is `zstd`.
5. Validate table is DV PK table using `CoreOptions.deletionVectorsEnabled()`.
6. Validate files are Parquet and `obs://`.
7. Convert output projection into field names.
8. Convert predicate using `NativeExportPredicateJson`.
9. Convert splits into `NativeExportTask`.
10. Return `NativeExportApplicability.rejected(reason, detail)` with stable reason on any failure.

Preflight must reject credential-provider-only configurations that cannot be serialized into the native request, bucket mismatch between `obs://bucket/path` and explicit bucket option, and unsupported native library availability from `PaimonJnrLoader.current().loadFailure()`.

- [ ] **Step 4: Implement conservative DV conversion**

For the first implementation, support only deleted-position lists that can be extracted safely from current `DeletionVector` APIs. If the current API does not expose iteration, add a small package-private helper next to existing DV code and test it. If large bitmap serialization is not available yet, reject with `EXPORT_UNSUPPORTED_DV` instead of expanding unbounded data.

- [ ] **Step 5: Implement NativeExportProviderImpl**

`NativeExportProviderImpl` bridges the core SPI to native implementation classes:

```java
public final class NativeExportProviderImpl implements NativeExportProvider {

    @Override
    public NativeExportPreflightResult preflight(NativeExportContext context) {
        return new NativeExportPlanner().preflight(context);
    }

    @Override
    public NativeExportPlanDescriptor plan(NativeExportContext context) {
        NativeExportPlan plan = new NativeExportPlanner().plan(context);
        return plan.toDescriptor();
    }

    @Override
    public NativeExportTaskResult executeTask(byte[] taskPayload) throws Exception {
        NativeExportTask task = NativeExportJson.taskFromPayload(taskPayload);
        NativeExportResult result = new NativeExportRunner().run(task);
        return NativeExportTaskResult.fromNativeResult(result);
    }
}
```

`NativeExportPlan.toDescriptor()` serializes each `NativeExportTask` into an opaque `byte[]` payload using the same versioned JSON contract. Do not make `paimon-spark-common` import `org.apache.paimon.nativeio.export.NativeExportRunner`; Spark procedure only sees `NativeExportProvider`, `NativeExportPlanDescriptor`, and `NativeExportTaskResult`.

- [ ] **Step 6: Integrate strategy in ExportParquetProcedure**

Refactor the existing `export` method:

```text
plan splits once
prepare output dir once
if native planner says applicable:
    run native export
else if fallback enabled:
    run existing Java export
else:
    throw exception with reject reason and detail
```

Rename existing body helpers:

- existing map/mapPartitions path -> `javaExport`
- native path -> `nativeExport`

Native path must discover providers through the core SPI:

```java
List<NativeExportProviderFactory> factories =
        NativeExportProviderFactory.discover(Thread.currentThread().getContextClassLoader());
```

No provider means `NO_PROVIDER` and Java fallback unless `native-io.export.fail-on-fallback=true`.

If the SPI execution contract carries task payloads back to Spark procedure, the native path uses only core SPI DTOs:

```java
List<NativeExportTaskResult> results =
        jsc.parallelize(plan.taskPayloads(), nativeTaskCount(parallelism, plan.taskPayloads().size()))
                .map(payload -> provider.executeTask(payload))
                .collect();
```

Then sum `rowsOutput`, log aggregated metrics when enabled, write `_SUCCESS`, and return row count.

- [ ] **Step 7: Run Java tests**

Run:

```bash
cd /Users/opay-20240095/IdeaProjects/nativeio/paimon-plus
./mvnw -pl paimon-spark/paimon-spark-3.4 -am -DskipITs -Dcheckstyle.skip -Drat.skip -Dspotless.check.skip test
```

Expected: Spark module tests pass. If the module path differs in this checkout, run:

```bash
./mvnw -pl :paimon-spark-3.4_2.12 -am -DskipITs -Dcheckstyle.skip -Drat.skip -Dspotless.check.skip test
```

- [ ] **Step 8: Commit**

```bash
git add paimon-native-io/src/main/java/org/apache/paimon/nativeio/export/NativeExportPlanner.java \
  paimon-native-io/src/main/java/org/apache/paimon/nativeio/export/NativeExportProviderFactoryImpl.java \
  paimon-native-io/src/main/java/org/apache/paimon/nativeio/export/NativeExportProviderImpl.java \
  paimon-native-io/src/main/resources/META-INF/services/org.apache.paimon.operation.nativeio.export.NativeExportProviderFactory \
  paimon-spark/paimon-spark-common/src/main/java/org/apache/paimon/spark/procedure/ExportParquetProcedure.java \
  paimon-spark/paimon-spark-common/src/test
git commit -m "feat: route export_parquet to native fast path"
```

---

### Task 9: End-to-End Validation and Benchmark Harness

**Files:**

- Create: `/Users/opay-20240095/IdeaProjects/nativeio/paimon-plus/dev/native-io-export-benchmark/README.md`
- Modify or create E2E tests under `/Users/opay-20240095/IdeaProjects/nativeio/paimon-plus/paimon-e2e-tests/`

- [ ] **Step 1: Add benchmark runbook**

Create `README.md` with these three commands adapted to the local cluster paths:

```bash
# Java reader + Java export
/data/soft/spark-3.4.4/bin/spark-sql \
  --master yarn \
  --queue root.risk \
  --conf spark.paimon.native-io.enabled=false \
  --conf spark.sql.catalog.paimon=org.apache.paimon.spark.SparkCatalog \
  --conf spark.sql.catalog.paimon.warehouse=obs://opay-datalake/user/xiangyi.zhu/paimon_test \
  --conf spark.sql.extensions=org.apache.paimon.spark.extensions.PaimonSparkSessionExtensions \
  --jars /data/paimonnative/paimon-spark-3.4_2.12-1.4-SNAPSHOT.jar

# Native reader + Java export
/data/soft/spark-3.4.4/bin/spark-sql \
  --master yarn \
  --queue root.risk \
  --conf spark.paimon.native-io.enabled=true \
  --conf spark.paimon.native-io.export.enabled=false \
  --conf spark.sql.catalog.paimon=org.apache.paimon.spark.SparkCatalog \
  --conf spark.sql.catalog.paimon.warehouse=obs://opay-datalake/user/xiangyi.zhu/paimon_test \
  --conf spark.sql.extensions=org.apache.paimon.spark.extensions.PaimonSparkSessionExtensions \
  --jars /data/paimonnative/paimon-native-io-1.4-SNAPSHOT.jar,/data/paimonnative/paimon-spark-3.4_2.12-1.4-SNAPSHOT.jar

# Native export fast path
/data/soft/spark-3.4.4/bin/spark-sql \
  --master yarn \
  --queue root.risk \
  --conf spark.paimon.native-io.enabled=true \
  --conf spark.paimon.native-io.export.enabled=true \
  --conf spark.sql.catalog.paimon=org.apache.paimon.spark.SparkCatalog \
  --conf spark.sql.catalog.paimon.warehouse=obs://opay-datalake/user/xiangyi.zhu/paimon_test \
  --conf spark.sql.extensions=org.apache.paimon.spark.extensions.PaimonSparkSessionExtensions \
  --jars /data/paimonnative/paimon-native-io-1.4-SNAPSHOT.jar,/data/paimonnative/paimon-spark-3.4_2.12-1.4-SNAPSHOT.jar
```

The runbook must include the common SQL body and require distinct output paths for each mode.

- [ ] **Step 2: Add correctness checklist to benchmark README**

Document these checks:

```bash
spark-cli app-summary <appId> --format json
spark-cli slow-stages <appId> --top 5 --format json
```

For each output path, compare:

- returned row count
- output file count
- total output bytes
- Parquet schema
- sampled checksum over deterministic columns

- [ ] **Step 3: Run full unit suite for touched modules**

Run:

```bash
cd /Users/opay-20240095/IdeaProjects/nativeio/paimon-plus
./mvnw -pl paimon-native-io,paimon-core,paimon-api -am -DskipITs -Dcheckstyle.skip -Drat.skip -Dspotless.check.skip test
cd /Users/opay-20240095/IdeaProjects/nativeio/paimon-plus/paimon-native-io/rust
cargo test -p paimon-native-io-c
```

Expected: Java and Rust unit suites pass.

- [ ] **Step 4: Run OBS E2E benchmark**

Run the three benchmark modes from the README against the same table snapshot and predicate. Record the three Spark application ids and native export summary metrics.

Expected:

- row counts match across all modes
- schema matches across all modes
- native export fast path is at least 2x faster than native reader + Java export for the target wide table, or metrics identify the dominant remaining bottleneck

- [ ] **Step 5: Commit**

```bash
git add dev/native-io-export-benchmark/README.md paimon-e2e-tests
git commit -m "test: document native export benchmark"
```

---

## Self-Review Checklist

- Spec coverage:
  - Core ServiceLoader SPI boundary: Task 1A, Task 8.
  - Options and applicability: Task 1, Task 3, Task 8.
  - Java request/result and redaction: Task 2, Task 7.
  - Rust FFI: Task 4.
  - Buffered OBS read: Task 5.
  - Native read/filter/DV/write pipeline: Task 6.
  - `ExportParquetProcedure` strategy and `target_file_size` concurrency: Task 8.
  - Metrics and benchmark: Task 2, Task 6, Task 9.
  - LakeSoul native IO reference points: File Structure reference list, Task 4 runtime, Task 5 object_store reader, Task 6 multipart writer.
- Placeholder scan:
  - The plan intentionally rejects unsupported behavior instead of leaving open-ended implementation gaps.
  - Each task has a failing test, implementation target, verification command, and commit command.
- Type consistency:
  - Java model fields match spec JSON keys.
  - Rust `ExportRequest` / `ExportResult` match Java `NativeExportTask` / `NativeExportResult`.
  - JNR method names match C ABI names.

---

## Execution Options

1. Subagent-Driven: dispatch a fresh worker per task, review after each task, best for this multi-module Java/Rust change.
2. Inline Execution: execute tasks in this session using checkpoints after each task.
