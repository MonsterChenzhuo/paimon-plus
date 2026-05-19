# Paimon Native IO Rust FFI Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build the Rust `paimon-io` and `paimon-io-c` crates that read OBS Parquet files through DataFusion and expose a 17-function C ABI to Java.

**Architecture:** Cherry-pick the LakeSoul IO shape into a trimmed Paimon Rust workspace: `paimon-io` owns config/session/reader/helpers/OBS object store, while `paimon-io-c` owns the stable C ABI and panic/error conversion. Java passes files, Arrow target schema, row index column, batch size, and `fs.obs.*` options through builder calls; Rust returns Arrow C Data record batches containing business columns plus the row index column.

**Tech Stack:** Rust 2024, DataFusion 51 split crates, arrow-rs 57, parquet 57, object_store 0.13, Tokio global runtime, `anyhow`, C Data Interface.

---

### File Structure

- Create: `paimon-native-io/rust/paimon-io/Cargo.toml`
- Create: `paimon-native-io/rust/paimon-io/src/lib.rs`
- Create: `paimon-native-io/rust/paimon-io/src/config.rs`
- Create: `paimon-native-io/rust/paimon-io/src/session.rs`
- Create: `paimon-native-io/rust/paimon-io/src/reader.rs`
- Create: `paimon-native-io/rust/paimon-io/src/helpers.rs`
- Create: `paimon-native-io/rust/paimon-io/src/object_store.rs`
- Create: `paimon-native-io/rust/paimon-io/src/obs/{mod.rs,client.rs,config.rs,signer.rs,error.rs,xml.rs}`
- Create: `paimon-native-io/rust/paimon-io-c/Cargo.toml`
- Create: `paimon-native-io/rust/paimon-io-c/src/lib.rs`
- Modify: `paimon-native-io/rust/Cargo.toml` to add `paimon-io` and `paimon-io-c` workspace members.
- Test: `paimon-native-io/rust/paimon-io/tests/config_test.rs`
- Test: `paimon-native-io/rust/paimon-io-c/tests/ffi_status_test.rs`

### Execution Notes

- Start commands with `PAIMON_ROOT=/Users/opay-20240095/IdeaProjects/nativeio/paimon`; use `$PAIMON_ROOT` in executable shell snippets.
- Exclude build outputs during source discovery: `find "$PAIMON_ROOT" -path '*/target/*' -prune -o -type f -print`.
- All Rust 2024 exported ABI functions must use `#[unsafe(no_mangle)]`.
- Run `cargo check -p paimon-io-c` before Java/JNR binding work starts.

### Task 1: Create `paimon-io` Crate

**Files:**
- Modify: `paimon-native-io/rust/Cargo.toml`
- Create: `paimon-native-io/rust/paimon-io/Cargo.toml`
- Create: `paimon-native-io/rust/paimon-io/src/lib.rs`

- [ ] **Step 1: Add workspace member**

```toml
members = [
  "paimon-io",
  "paimon-io-c",
  "spikes/row_index_check",
  "spikes/obs_object_store_check",
  "spikes/jnr_status_check"
]
```

- [ ] **Step 2: Write `paimon-io/Cargo.toml`**

```toml
[package]
name = "paimon-io"
version = "0.1.0"
edition = "2024"

[dependencies]
anyhow = { workspace = true }
arrow-array = { workspace = true }
arrow-schema = { workspace = true }
async-trait = { workspace = true }
bytes = { workspace = true }
datafusion = { workspace = true }
datafusion-common = { workspace = true }
datafusion-datasource = { workspace = true }
datafusion-datasource-parquet = { workspace = true }
datafusion-execution = { workspace = true }
datafusion-physical-plan = { workspace = true }
datafusion-session = { workspace = true }
futures = { workspace = true }
object_store = { workspace = true }
parquet = { workspace = true }
tokio = { workspace = true }
tokio-stream = { workspace = true }
url = { workspace = true }
```

- [ ] **Step 3: Write crate entrypoint**

```rust
pub mod config;
pub mod helpers;
pub mod object_store;
pub mod obs;
pub mod reader;
pub mod session;

pub type Result<T> = anyhow::Result<T>;
```

- [ ] **Step 4: Verify compile failure is only missing modules**

Run: `cd /Users/opay-20240095/IdeaProjects/nativeio/paimon/paimon-native-io/rust && cargo check -p paimon-io`

Expected: FAIL only for missing module files until later tasks add them.

- [ ] **Step 5: Commit**

```bash
git add paimon-native-io/rust/Cargo.toml paimon-native-io/rust/paimon-io
git commit -m "feat(native-io): add paimon rust io crate"
```

### Task 2: Implement Config Builder

**Files:**
- Create: `paimon-native-io/rust/paimon-io/src/config.rs`
- Create: `paimon-native-io/rust/paimon-io/tests/config_test.rs`

- [ ] **Step 1: Write failing config tests**

```rust
use paimon_io::config::PaimonIOConfigBuilder;

#[test]
fn rejects_empty_files() {
    let err = PaimonIOConfigBuilder::new().build().unwrap_err().to_string();
    assert!(err.contains("at least one file"));
}

#[test]
fn rejects_missing_obs_config() {
    let err = PaimonIOConfigBuilder::new()
        .add_file("obs://bucket/a.parquet")
        .row_index_column("__paimon_native_row_index")
        .build()
        .unwrap_err()
        .to_string();
    assert!(err.contains("fs.obs.endpoint"));
}
```

- [ ] **Step 2: Run tests to verify failure**

Run: `cd /Users/opay-20240095/IdeaProjects/nativeio/paimon/paimon-native-io/rust && cargo test -p paimon-io --test config_test`

Expected: FAIL because `PaimonIOConfigBuilder` is undefined.

- [ ] **Step 3: Implement config and builder**

```rust
use anyhow::{ensure, Result};
use arrow_schema::SchemaRef;
use std::collections::HashMap;

#[derive(Clone)]
pub struct PaimonIOConfig {
    pub files: Vec<String>,
    pub target_schema: Option<SchemaRef>,
    pub batch_size: usize,
    pub row_index_column: String,
    pub object_store_options: HashMap<String, String>,
}

pub struct PaimonIOConfigBuilder {
    files: Vec<String>,
    target_schema: Option<SchemaRef>,
    batch_size: usize,
    row_index_column: Option<String>,
    object_store_options: HashMap<String, String>,
}

impl PaimonIOConfigBuilder {
    pub fn new() -> Self {
        Self {
            files: Vec::new(),
            target_schema: None,
            batch_size: 4096,
            row_index_column: None,
            object_store_options: HashMap::new(),
        }
    }

    pub fn add_file(mut self, file: impl Into<String>) -> Self {
        self.files.push(file.into());
        self
    }

    pub fn target_schema(mut self, schema: SchemaRef) -> Self {
        self.target_schema = Some(schema);
        self
    }

    pub fn batch_size(mut self, batch_size: usize) -> Self {
        self.batch_size = batch_size;
        self
    }

    pub fn row_index_column(mut self, name: impl Into<String>) -> Self {
        self.row_index_column = Some(name.into());
        self
    }

    pub fn object_store_option(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.object_store_options.insert(key.into().to_ascii_lowercase(), value.into());
        self
    }

    pub fn build(self) -> Result<PaimonIOConfig> {
        ensure!(!self.files.is_empty(), "at least one file is required");
        ensure!((1..=65536).contains(&self.batch_size), "batch size must be in [1, 65536]");
        let row_index_column = self.row_index_column
            .filter(|s| !s.is_empty())
            .ok_or_else(|| anyhow::anyhow!("row index column is required"))?;
        for key in ["fs.obs.endpoint", "fs.obs.access.key", "fs.obs.secret.key"] {
            ensure!(self.object_store_options.contains_key(key), "{key} is required");
        }
        Ok(PaimonIOConfig {
            files: self.files,
            target_schema: self.target_schema,
            batch_size: self.batch_size,
            row_index_column,
            object_store_options: self.object_store_options,
        })
    }
}

impl Default for PaimonIOConfigBuilder {
    fn default() -> Self { Self::new() }
}
```

- [ ] **Step 4: Verify tests pass**

Run: `cd /Users/opay-20240095/IdeaProjects/nativeio/paimon/paimon-native-io/rust && cargo test -p paimon-io --test config_test`

Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add paimon-native-io/rust/paimon-io/src/config.rs paimon-native-io/rust/paimon-io/tests/config_test.rs
git commit -m "feat(native-io): add rust io config builder"
```

### Task 3: Implement OBS Read-Only ObjectStore

**Files:**
- Create: `paimon-native-io/rust/paimon-io/src/obs/mod.rs`
- Create: `paimon-native-io/rust/paimon-io/src/obs/config.rs`
- Create: `paimon-native-io/rust/paimon-io/src/obs/client.rs`
- Create: `paimon-native-io/rust/paimon-io/src/obs/signer.rs`
- Create: `paimon-native-io/rust/paimon-io/src/obs/error.rs`
- Create: `paimon-native-io/rust/paimon-io/src/obs/xml.rs`
- Create: `paimon-native-io/rust/paimon-io/src/object_store.rs`

- [ ] **Step 1: Migrate read-path OBS modules**

Copy and adapt only read-path code from `/Users/opay-20240095/IdeaProjects/nativeio/obs-rust-sdk/src`. Keep config, signing, `head_object`, range `get_object`, XML parsing, error classification, timeout, and redaction helpers. Do not migrate multipart or write operations.

- [ ] **Step 2: Implement endpoint validation**

```rust
pub fn normalize_endpoint(endpoint: &str) -> anyhow::Result<String> {
    if endpoint.contains('@') {
        anyhow::bail!("fs.obs.endpoint must not contain userinfo");
    }
    if endpoint.starts_with("http://") || endpoint.starts_with("https://") {
        Ok(endpoint.trim_end_matches('/').to_string())
    } else {
        Ok(format!("https://{}", endpoint.trim_end_matches('/')))
    }
}
```

- [ ] **Step 3: Implement `ObsObjectStore` read methods**

`ObjectStore::head` must map OBS content length and last modified into `ObjectMeta`. `ObjectStore::get_opts` must honor `GetOptions.range`; unsupported conditional options must return an explicit unsupported error, not an unconditional read.

- [ ] **Step 4: Verify non-OBS schemes fail**

Run: `cd /Users/opay-20240095/IdeaProjects/nativeio/paimon/paimon-native-io/rust && cargo test -p paimon-io obs`

Expected: PASS for unit tests covering endpoint normalization, secret redaction, non-OBS rejection, unsupported conditional read options, and range length validation.

- [ ] **Step 5: Commit**

```bash
git add paimon-native-io/rust/paimon-io/src/obs paimon-native-io/rust/paimon-io/src/object_store.rs
git commit -m "feat(native-io): add read-only obs object store"
```

### Task 4: Implement Session and Reader

**Files:**
- Create: `paimon-native-io/rust/paimon-io/src/helpers.rs`
- Create: `paimon-native-io/rust/paimon-io/src/session.rs`
- Create: `paimon-native-io/rust/paimon-io/src/reader.rs`

- [ ] **Step 1: Implement helper contracts**

```rust
pub fn redacted_value(key: &str, value: &str) -> String {
    let lowered = key.to_ascii_lowercase();
    if lowered.contains("secret") || lowered.contains("access.key") || lowered.contains("token") || lowered.contains("password") {
        format!("<redacted:{}>", value.len())
    } else {
        value.to_string()
    }
}
```

Also implement projection index computation against the Parquet physical schema. Row index is virtual and must not be included in projection indices.

- [ ] **Step 2: Implement global runtime without aborting JVM**

Use `std::sync::OnceLock<Result<Arc<Runtime>, String>>` or equivalent so runtime creation failure becomes an error returned through FFI. Parse `PAIMON_NATIVE_IO_WORKER_THREADS`; invalid, zero, or too-large values fall back to `4` with a redacted warning.

- [ ] **Step 3: Implement physical plan construction**

Mirror the direct LakeSoul path:

```text
register obs://bucket ObjectStore
head files
infer/validate file schema
build FileScanConfigBuilder
append row_index to output schema through native DataFusion support or the row-index wrapper selected by spike
create physical plan
execute stream
```

Set DataFusion options: `target_partitions=1`, round-robin repartition off, parquet pruning on, dictionary off, `schema_force_view_types=false` when available, and batch size from config. Also preserve LakeSoul stability details:

```text
RuntimeEnv metadata cache configured by PAIMON_NATIVE_IO_FILE_META_CACHE_LIMIT
metadata fetch concurrency from DataFusion execution options
filter invalid Parquet files with size < 8 before metadata read
clear field metadata before schema merge
timestamp NTZ session timezone fixed to UTC or explicit empty value
```

- [ ] **Step 4: Implement blocking reader wrapper**

`PaimonReader::start()` builds and executes the physical plan. `SyncSendableMutablePaimonReader::next_rb_blocked()` blocks on the global runtime and skips zero-row batches until it returns a positive-row batch, EOF, or error.

- [ ] **Step 5: Add object consistency and short-read tests**

Tests must cover:

```text
preflight size/eTag/version differs from native head -> external consistency error
OBS range returns 206 with shorter body than requested -> error
OBS full get returns 200 with shorter body than content-length -> error
short read is not classified as corrupt and is not silently retried to success
```

- [ ] **Step 6: Verify Rust tests**

Run: `cd $PAIMON_ROOT/paimon-native-io/rust && cargo test -p paimon-io`

Expected: PASS, including config, helper, OBS, and reader tests using local/mocked object store fixtures.

- [ ] **Step 7: Commit**

```bash
git add paimon-native-io/rust/paimon-io/src/helpers.rs paimon-native-io/rust/paimon-io/src/session.rs paimon-native-io/rust/paimon-io/src/reader.rs
git commit -m "feat(native-io): implement rust datafusion reader"
```

### Task 5: Implement `paimon-io-c` 17-Function ABI

**Files:**
- Create: `paimon-native-io/rust/paimon-io-c/Cargo.toml`
- Create: `paimon-native-io/rust/paimon-io-c/src/lib.rs`
- Create: `paimon-native-io/rust/paimon-io-c/tests/ffi_status_test.rs`

- [ ] **Step 1: Write the cdylib manifest**

```toml
[package]
name = "paimon-io-c"
version = "0.1.0"
edition = "2024"

[lib]
name = "paimon_native_io"
crate-type = ["cdylib", "rlib"]

[dependencies]
anyhow = { workspace = true }
arrow-array = { workspace = true }
arrow-schema = { workspace = true }
paimon-io = { path = "../paimon-io" }
```

- [ ] **Step 2: Implement ABI structs and null-safe frees**

```rust
#[repr(C)]
pub struct CStatus {
    pub err: *const std::os::raw::c_char,
    pub status: std::os::raw::c_int,
}
```

All exported functions must wrap their body in panic handling and return `CStatus(-1, redacted_error)` or `CResult.err`; no panic crosses FFI. In Rust 2024, every exported function must use `#[unsafe(no_mangle)]`, for example:

```rust
#[unsafe(no_mangle)]
pub extern "C" fn start_reader(...) -> NonNull<CStatus> {
    ...
}
```

- [ ] **Step 3: Implement exactly these exported symbols**

```text
new_paimon_io_config_builder
paimon_config_builder_add_file
paimon_config_builder_with_target_schema
paimon_config_builder_with_batch_size
paimon_config_builder_with_row_index_column
paimon_config_builder_set_object_store_option
create_paimon_io_config_from_builder
check_io_config_created
create_paimon_reader_with_global_runtime
check_reader_created
start_reader
paimon_reader_get_schema
next_record_batch_blocked
free_paimon_reader
free_c_status
free_paimon_io_config_builder
free_paimon_io_config
```

- [ ] **Step 4: Add ABI tests**

Tests must cover null-safe free, `CStatus` invariant violations, invalid UTF-8 input, zero schema/array addresses, config consumed by reader creation, and secret redaction.

- [ ] **Step 5: Run FFI compile check before Java binding**

Run: `cd $PAIMON_ROOT/paimon-native-io/rust && cargo check -p paimon-io-c`

Expected: PASS.

- [ ] **Step 6: Verify exported symbol count**

Run:

```bash
cd $PAIMON_ROOT/paimon-native-io/rust
cargo build -p paimon-io-c
nm -g target/debug/libpaimon_native_io.* | grep ' T ' | grep -E 'paimon|reader|config|status' | sort
cargo build --release -p paimon-io-c
nm -g target/release/libpaimon_native_io.* | grep ' T ' | grep -E 'paimon|reader|config|status' | sort
```

Expected: output contains the 17 symbols above and no writer, merge, Substrait, runtime builder, or callback symbols.

- [ ] **Step 7: Commit**

```bash
git add paimon-native-io/rust/paimon-io-c
git commit -m "feat(native-io): expose paimon native io c abi"
```

### Task 6: Final Rust Verification

**Files:**
- No new files; verify crate set.

- [ ] **Step 1: Run formatting**

Run: `cd /Users/opay-20240095/IdeaProjects/nativeio/paimon/paimon-native-io/rust && cargo fmt --all -- --check`

Expected: PASS.

- [ ] **Step 2: Run clippy**

Run: `cd /Users/opay-20240095/IdeaProjects/nativeio/paimon/paimon-native-io/rust && cargo clippy --workspace --all-targets -- -D warnings`

Expected: PASS.

- [ ] **Step 3: Run all Rust tests**

Run: `cd /Users/opay-20240095/IdeaProjects/nativeio/paimon/paimon-native-io/rust && cargo test --workspace`

Expected: PASS.

- [ ] **Step 4: Commit**

```bash
git add paimon-native-io/rust
git commit -m "test(native-io): verify rust native io crates"
```

### Self-Review Checklist

- [ ] No HDFS/S3/OSS/file support exists in native object store registration.
- [ ] `paimon-io-c` exports exactly the agreed 17 ABI functions.
- [ ] All FFI string inputs are copied into Rust-owned strings immediately.
- [ ] `next_record_batch_blocked` never returns `status == 0` for a zero-row batch.
- [ ] All errors from OBS/config/panic paths redact AK/SK/token.
- [ ] DataFusion metadata cache, size `< 8` filtering, field metadata cleanup, and short-read checks are covered by tests.
