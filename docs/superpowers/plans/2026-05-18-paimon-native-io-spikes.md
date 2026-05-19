# Paimon Native IO Spike Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Prove the high-risk integration points for the Paimon Native IO PoC before production code lands.

**Architecture:** Build small, disposable spikes under `paimon-native-io/rust/` and focused JVM tests under `paimon-native-io/src/test/` to validate row index semantics, OBS object store reads, DataFusion 51 APIs, JNR struct mapping, and core SPI dependency boundaries. Each spike produces a concrete pass/fail artifact and a short markdown result note that later plans can rely on.

**Tech Stack:** Maven, Rust 2024, DataFusion 51, arrow-rs 57, Arrow Java 15, JNR-FFI, JDK 8, Paimon core test harness.

---

### File Structure

- Create: `paimon-native-io/rust/Cargo.toml` for a minimal Rust workspace used by spikes.
- Create: `paimon-native-io/rust/rust-toolchain.toml` to pin the Rust toolchain.
- Create: `paimon-native-io/rust/spikes/row_index_check/Cargo.toml` and `src/main.rs` to verify DataFusion/arrow-rs row index behavior.
- Create: `paimon-native-io/rust/spikes/obs_object_store_check/Cargo.toml` and `src/main.rs` to verify `obs://` range reads through an `ObjectStore` adapter.
- Create: `paimon-native-io/rust/spikes/jnr_status_check/Cargo.toml` and `src/lib.rs` to export a minimal `CStatus` cdylib.
- Create: `paimon-native-io/src/test/java/org/apache/paimon/nativeio/spike/JnrStatusMappingTest.java` to prove the JNR mapping on JDK 8.
- Create: `paimon-native-io/src/test/java/org/apache/paimon/nativeio/spike/ServiceLoaderBoundaryTest.java` to prove `paimon-core` only sees the SPI and no Arrow/JNR classes are needed to keep Java fallback available.
- Create: `docs/superpowers/spike-results/2026-05-18-native-io-spikes.md` to record exact outcomes and decisions.
- Read-only references: `LakeSoul/rust/lakesoul-io/src/session.rs`, `LakeSoul/rust/lakesoul-io/src/reader.rs`, `LakeSoul/rust/lakesoul-io-c/src/lib.rs`, `LakeSoul/native-io/lakesoul-io-java/src/main/java/com/dmetasoul/lakesoul/lakesoul/io/jnr/LibLakeSoulIO.java`, `obs-rust-sdk/src/client.rs`.

### Task 1: Scaffold Spike Workspace

**Files:**
- Create: `paimon-native-io/rust/Cargo.toml`
- Create: `paimon-native-io/rust/rust-toolchain.toml`

- [ ] **Step 1: Write the Rust workspace manifest**

```toml
[workspace]
resolver = "2"
members = [
  "spikes/row_index_check",
  "spikes/obs_object_store_check",
  "spikes/jnr_status_check"
]

[workspace.dependencies]
anyhow = "1"
arrow = "57"
arrow-array = "57"
arrow-schema = "57"
datafusion = "51"
datafusion-common = "51"
datafusion-datasource = "51"
datafusion-datasource-parquet = "51"
datafusion-execution = "51"
datafusion-physical-plan = "51"
datafusion-session = "51"
futures = "0.3"
object_store = { version = "0.13", default-features = false }
parquet = { version = "57", features = ["async", "zstd", "lz4", "snap"] }
tokio = { version = "1", features = ["rt-multi-thread", "macros"] }
url = "2"
```

- [ ] **Step 2: Pin Rust**

```toml
[toolchain]
channel = "1.85.0"
components = ["rustfmt", "clippy"]
```

- [ ] **Step 3: Verify the empty workspace**

Run: `cd /Users/opay-20240095/IdeaProjects/nativeio/paimon/paimon-native-io/rust && cargo metadata --format-version 1 >/tmp/paimon-native-io-cargo-metadata.json`

Expected: command exits `0` and `/tmp/paimon-native-io-cargo-metadata.json` contains all three spike package names after later tasks add them.

- [ ] **Step 4: Commit**

```bash
git add paimon-native-io/rust/Cargo.toml paimon-native-io/rust/rust-toolchain.toml
git commit -m "chore(native-io): scaffold spike rust workspace"
```

### Task 2: Row Index Semantics Spike

**Files:**
- Create: `paimon-native-io/rust/spikes/row_index_check/Cargo.toml`
- Create: `paimon-native-io/rust/spikes/row_index_check/src/main.rs`

- [ ] **Step 1: Write the spike package manifest**

```toml
[package]
name = "row_index_check"
version = "0.1.0"
edition = "2024"

[dependencies]
anyhow = { workspace = true }
arrow-array = { workspace = true }
arrow-schema = { workspace = true }
datafusion = { workspace = true }
datafusion-datasource = { workspace = true }
datafusion-datasource-parquet = { workspace = true }
parquet = { workspace = true }
tokio = { workspace = true }
```

- [ ] **Step 2: Write the row index probe**

```rust
use anyhow::{bail, Result};

#[tokio::main]
async fn main() -> Result<()> {
    let parquet_path = std::env::args()
        .nth(1)
        .ok_or_else(|| anyhow::anyhow!("usage: row_index_check <parquet-file>"))?;
    println!("ROW_INDEX_SPIKE_INPUT={parquet_path}");
    println!("CHECKS=compile_datafusion_51,row_index_api,row_index_absolute_position");
    bail!("wire this spike to DataFusion 51 FileScanConfigBuilder and fail until _row_index values are printed");
}
```

- [ ] **Step 3: Run the spike and capture expected initial failure**

Run: `cd /Users/opay-20240095/IdeaProjects/nativeio/paimon/paimon-native-io/rust && cargo run -p row_index_check -- /tmp/native-io-row-index.parquet`

Expected: FAIL with `wire this spike to DataFusion 51 FileScanConfigBuilder`; this confirms the harness is wired before real API exploration starts.

- [ ] **Step 4: Replace the intentional failure with the real DataFusion scan**

Use `FileScanConfigBuilder` and the current DataFusion 51 `ParquetSource`/`ParquetFormat` API to scan a generated multi-row-group Parquet file with row index enabled. Print:

```text
ROW_INDEX_COLUMN_PRESENT=true
ROW_INDEX_SCOPE=absolute
FIRST_VALUES=0,1,2
AFTER_ROW_GROUP_BOUNDARY=...
```

If the API does not expose a row index column, print:

```text
ROW_INDEX_COLUMN_PRESENT=false
WRAPPER_REQUIRED=true
```

- [ ] **Step 5: Verify the final row index decision**

Run: `cd /Users/opay-20240095/IdeaProjects/nativeio/paimon/paimon-native-io/rust && cargo run -p row_index_check -- /tmp/native-io-row-index.parquet | tee /tmp/row-index-spike.out`

Expected: output contains either `ROW_INDEX_SCOPE=absolute` or `WRAPPER_REQUIRED=true`. Any other result fails the spike.

- [ ] **Step 6: Commit**

```bash
git add paimon-native-io/rust/spikes/row_index_check
git commit -m "test(native-io): spike arrow row index semantics"
```

### Task 3: OBS ObjectStore Spike

**Files:**
- Create: `paimon-native-io/rust/spikes/obs_object_store_check/Cargo.toml`
- Create: `paimon-native-io/rust/spikes/obs_object_store_check/src/main.rs`

- [ ] **Step 1: Write the OBS spike manifest**

```toml
[package]
name = "obs_object_store_check"
version = "0.1.0"
edition = "2024"

[dependencies]
anyhow = { workspace = true }
bytes = "1"
object_store = { workspace = true }
tokio = { workspace = true }
url = { workspace = true }
```

- [ ] **Step 2: Write the OBS config validation harness**

```rust
use anyhow::{bail, ensure, Result};
use url::Url;

fn main() -> Result<()> {
    let url = std::env::args().nth(1).ok_or_else(|| anyhow::anyhow!("usage: obs_object_store_check obs://bucket/key"))?;
    let parsed = Url::parse(&url)?;
    ensure!(parsed.scheme() == "obs", "native IO only supports obs:// paths");
    ensure!(parsed.host_str().is_some(), "obs bucket is required");
    ensure!(std::env::var("FS_OBS_ENDPOINT").is_ok(), "FS_OBS_ENDPOINT is required");
    ensure!(std::env::var("FS_OBS_ACCESS_KEY").is_ok(), "FS_OBS_ACCESS_KEY is required");
    ensure!(std::env::var("FS_OBS_SECRET_KEY").is_ok(), "FS_OBS_SECRET_KEY is required");
    bail!("wire this spike to the migrated obs-rust-sdk client and object_store::ObjectStore");
}
```

- [ ] **Step 3: Verify guard behavior before network code**

Run: `cd /Users/opay-20240095/IdeaProjects/nativeio/paimon/paimon-native-io/rust && cargo run -p obs_object_store_check -- file:///tmp/a.parquet`

Expected: FAIL with `native IO only supports obs:// paths`.

- [ ] **Step 4: Migrate only the read-path OBS client code needed for the spike**

Copy and adapt the relevant pieces from:

```text
/Users/opay-20240095/IdeaProjects/nativeio/obs-rust-sdk/src/config.rs
/Users/opay-20240095/IdeaProjects/nativeio/obs-rust-sdk/src/auth.rs
/Users/opay-20240095/IdeaProjects/nativeio/obs-rust-sdk/src/client.rs
/Users/opay-20240095/IdeaProjects/nativeio/obs-rust-sdk/src/operations/object/head_object.rs
/Users/opay-20240095/IdeaProjects/nativeio/obs-rust-sdk/src/operations/object/get_object.rs
/Users/opay-20240095/IdeaProjects/nativeio/obs-rust-sdk/src/error.rs
```

The spike must perform `head` and one `range` read. It must reject userinfo in the endpoint and redact AK/SK/token in all errors.

- [ ] **Step 5: Verify OBS read**

Run:

```bash
cd /Users/opay-20240095/IdeaProjects/nativeio/paimon/paimon-native-io/rust
FS_OBS_ENDPOINT=https://obs.example.internal \
FS_OBS_ACCESS_KEY=example-ak \
FS_OBS_SECRET_KEY=example-sk \
cargo run -p obs_object_store_check -- obs://bucket/path/to/file.parquet | tee /tmp/obs-spike.out
```

Expected: output contains `HEAD_OK=true`, `RANGE_OK=true`, and does not contain `example-ak` or `example-sk`.

- [ ] **Step 6: Commit**

```bash
git add paimon-native-io/rust/spikes/obs_object_store_check
git commit -m "test(native-io): spike obs object store reads"
```

### Task 4: JNR CStatus Mapping Spike

**Files:**
- Create: `paimon-native-io/rust/spikes/jnr_status_check/Cargo.toml`
- Create: `paimon-native-io/rust/spikes/jnr_status_check/src/lib.rs`
- Create: `paimon-native-io/src/test/java/org/apache/paimon/nativeio/spike/JnrStatusMappingTest.java`

- [ ] **Step 1: Write the cdylib manifest**

```toml
[package]
name = "jnr_status_check"
version = "0.1.0"
edition = "2024"

[lib]
name = "paimon_jnr_status_check"
crate-type = ["cdylib"]
```

- [ ] **Step 2: Write the Rust CStatus exports**

```rust
use std::ffi::CString;
use std::os::raw::{c_char, c_int};
use std::ptr::NonNull;

#[repr(C)]
pub struct CStatus {
    pub err: *const c_char,
    pub status: c_int,
}

#[unsafe(no_mangle)]
pub extern "C" fn status_ok() -> NonNull<CStatus> {
    NonNull::new(Box::into_raw(Box::new(CStatus { err: std::ptr::null(), status: 7 }))).unwrap()
}

#[unsafe(no_mangle)]
pub extern "C" fn status_err() -> NonNull<CStatus> {
    let err = CString::new("synthetic error").unwrap().into_raw();
    NonNull::new(Box::into_raw(Box::new(CStatus { err, status: -1 }))).unwrap()
}

#[unsafe(no_mangle)]
pub extern "C" fn free_c_status(status: *mut CStatus) {
    if status.is_null() {
        return;
    }
    unsafe {
        let boxed = Box::from_raw(status);
        if !boxed.err.is_null() {
            let _ = CString::from_raw(boxed.err as *mut c_char);
        }
    }
}
```

- [ ] **Step 3: Write the Java JNR smoke test**

```java
package org.apache.paimon.nativeio.spike;

import jnr.ffi.LibraryLoader;
import jnr.ffi.Struct;
import jnr.ffi.annotations.In;
import jnr.ffi.annotations.Pinned;
import jnr.ffi.annotations.Transient;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class JnrStatusMappingTest {
    public static final class CStatus extends Struct {
        public final UTF8StringRef err = new UTF8StringRef();
        public final Signed32 status = new Signed32();
        public CStatus(jnr.ffi.Runtime runtime) { super(runtime); }
    }

    public interface Lib {
        CStatus status_ok();
        CStatus status_err();
        void free_c_status(@Pinned @In @Transient CStatus status);
    }

    @Test
    void mapsStructReturnAndFreesStatus() {
        String libPath = System.getProperty("paimon.jnr.status.lib");
        Lib lib = LibraryLoader.create(Lib.class).load(libPath);
        CStatus ok = lib.status_ok();
        assertThat(ok.status.intValue()).isEqualTo(7);
        assertThat(ok.err.get()).isNull();
        lib.free_c_status(ok);
        CStatus err = lib.status_err();
        assertThat(err.status.intValue()).isEqualTo(-1);
        assertThat(err.err.get()).isEqualTo("synthetic error");
        lib.free_c_status(err);
    }
}
```

- [ ] **Step 4: Verify JNR mapping**

Run:

```bash
cd /Users/opay-20240095/IdeaProjects/nativeio/paimon
(cd paimon-native-io/rust && cargo build -p jnr_status_check)
mvn -pl paimon-native-io -Dpaimon.jnr.status.lib=$PWD/paimon-native-io/rust/target/debug/libpaimon_jnr_status_check.dylib -Dtest=JnrStatusMappingTest test
```

Expected on macOS: PASS. On Linux replace `.dylib` with `.so`; expected PASS on JDK 8.

- [ ] **Step 5: Commit**

```bash
git add paimon-native-io/rust/spikes/jnr_status_check paimon-native-io/src/test/java/org/apache/paimon/nativeio/spike/JnrStatusMappingTest.java
git commit -m "test(native-io): spike jnr cstatus mapping"
```

### Task 5: Core SPI Boundary Spike

**Files:**
- Create: `paimon-native-io/src/test/java/org/apache/paimon/nativeio/spike/ServiceLoaderBoundaryTest.java`
- Record: `docs/superpowers/spike-results/2026-05-18-native-io-spikes.md`

- [ ] **Step 1: Write a boundary test that fails until SPI exists**

```java
package org.apache.paimon.nativeio.spike;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class ServiceLoaderBoundaryTest {
    @Test
    void coreFallbackDoesNotRequireNativeImplementationClasses() {
        assertThat(classExists("org.apache.paimon.operation.nativeio.NativeSplitReadProviderFactory"))
                .isTrue();
        assertThat(classExists("org.apache.arrow.vector.VectorSchemaRoot"))
                .as("paimon-core main path must not need Arrow classes")
                .isFalse();
    }

    private static boolean classExists(String className) {
        try {
            Class.forName(className, false, ServiceLoaderBoundaryTest.class.getClassLoader());
            return true;
        } catch (ClassNotFoundException e) {
            return false;
        }
    }
}
```

- [ ] **Step 2: Run it and confirm the current failure**

Run: `cd /Users/opay-20240095/IdeaProjects/nativeio/paimon && mvn -pl paimon-native-io -Dtest=ServiceLoaderBoundaryTest test`

Expected: FAIL because `NativeSplitReadProviderFactory` does not exist yet.

- [ ] **Step 3: Record spike outcomes**

Create the result note with this exact structure:

```markdown
# Paimon Native IO Spike Results

Date: 2026-05-18

## Row Index

- Result: pass or wrapper-required
- Evidence: `/tmp/row-index-spike.out`
- Decision: use DataFusion row index directly or implement wrapper

## OBS ObjectStore

- Result: pass or blocked
- Evidence: `/tmp/obs-spike.out`
- Decision: migrate read-path OBS client into `paimon-io/src/obs`

## JNR CStatus

- Result: pass or pointer-return-required
- Evidence: Maven/JDK output
- Decision: mirror LakeSoul struct return or switch to pointer mapping

## Core SPI Boundary

- Result: pass after SPI task or blocked
- Evidence: `ServiceLoaderBoundaryTest`
- Decision: keep Arrow/JNR out of `paimon-core` main dependencies
```

- [ ] **Step 4: Commit**

```bash
git add paimon-native-io/src/test/java/org/apache/paimon/nativeio/spike/ServiceLoaderBoundaryTest.java docs/superpowers/spike-results/2026-05-18-native-io-spikes.md
git commit -m "docs(native-io): record spike decisions"
```

### Task 6: Close Spike Red-Phase Loops

**Files:**
- Modify: `docs/superpowers/spike-results/2026-05-18-native-io-spikes.md`
- Verify: `paimon-native-io/rust/spikes/**`

- [ ] **Step 1: Confirm no intentional spike failure remains**

Run:

```bash
PAIMON_ROOT=/Users/opay-20240095/IdeaProjects/nativeio/paimon
grep -R -n "wire this spike\|intentional failure" \
  "$PAIMON_ROOT/paimon-native-io/rust/spikes" \
  "$PAIMON_ROOT/docs/superpowers/spike-results/2026-05-18-native-io-spikes.md" || true
```

Expected: no `wire this spike` lines remain.

- [ ] **Step 2: Verify spike result note is decisive**

`docs/superpowers/spike-results/2026-05-18-native-io-spikes.md` must contain one explicit decision for each area:

```text
Row Index: direct-datafusion or wrapper-required
OBS ObjectStore: migrated-client-ok or blocked
JNR CStatus: struct-return-ok or pointer-return-required
Core SPI Boundary: core-is-lightweight or redesign-required
```

- [ ] **Step 3: Re-run source discovery without target pollution**

Run:

```bash
PAIMON_ROOT=/Users/opay-20240095/IdeaProjects/nativeio/paimon
find "$PAIMON_ROOT/paimon-native-io/rust/spikes" -path '*/target/*' -prune -o -type f -print
```

Expected: only source files are used as evidence; no `target/` files are considered source ownership.

- [ ] **Step 4: Commit**

```bash
git add paimon-native-io/rust/spikes docs/superpowers/spike-results/2026-05-18-native-io-spikes.md
git commit -m "test(native-io): close spike decisions"
```

### Self-Review Checklist

- [ ] Every spike has a single pass/fail command.
- [ ] Row index result explicitly says absolute, relative-plus-offset wrapper, or unsupported.
- [ ] OBS output is scanned for raw AK/SK/token strings.
- [ ] JNR spike runs on Linux x86_64 JDK 8 before production FFI code is written.
- [ ] Core SPI spike proves Java fallback does not require Arrow/JNR/native classes.
- [ ] No red-phase placeholder failure remains after Task 6.
