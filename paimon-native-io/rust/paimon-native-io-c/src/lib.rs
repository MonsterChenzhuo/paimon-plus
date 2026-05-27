use std::collections::{HashMap, HashSet, VecDeque};
use std::ffi::{c_char, CStr, CString};
use std::fs::File;
use std::future::Future;
use std::io::{Read, Write};
use std::ops::Range;
use std::panic::{catch_unwind, AssertUnwindSafe};
use std::ptr;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, OnceLock};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use arrow_array::ffi::{FFI_ArrowArray, FFI_ArrowSchema};
use arrow_array::{
    Array, ArrayRef, BooleanArray, Float32Array, Float64Array, Int16Array, Int32Array, Int64Array,
    Int8Array, RecordBatch, RecordBatchReader, StringArray, StructArray,
};
use arrow_schema::{ArrowError, DataType, Field, Schema, SchemaRef};
use arrow_select::filter::filter_record_batch;
use bytes::Bytes;
use futures::future::{BoxFuture, FutureExt};
use futures::StreamExt;
use huaweicloud_sdk_rust_obs::{Client, CompletedPart, Config, Credentials};
use parquet::arrow::arrow_reader::{
    ArrowPredicateFn, ArrowReaderOptions, ParquetRecordBatchReader,
    ParquetRecordBatchReaderBuilder, RowFilter,
};
use parquet::arrow::async_reader::{AsyncFileReader, ParquetRecordBatchStream};
use parquet::arrow::ArrowWriter;
use parquet::arrow::{ParquetRecordBatchStreamBuilder, ProjectionMask};
use parquet::basic::{Compression, ZstdLevel};
use parquet::errors::{ParquetError, Result as ParquetResult};
use parquet::file::metadata::{PageIndexPolicy, ParquetMetaData, ParquetMetaDataReader};
use parquet::file::properties::WriterProperties;
use parquet::file::reader::{ChunkReader, Length};
use parquet::schema::types::SchemaDescriptor;
use serde_json::Value;
use url::Url;

const DEFAULT_ROW_INDEX_COLUMN: &str = "__paimon_native_row_index";
const MIN_MULTIPART_PART_SIZE_BYTES: u64 = 5 * 1024 * 1024;
const OBS_ACCESS_KEY_OPTIONS: &[&str] = &["fs.obs.access.key", "fs.obs.accessKey", "fs.obs.ak"];
const OBS_ACCESS_KEY_ENV: &[&str] = &[
    "OBS_ACCESS_KEY_ID",
    "OBS_ACCESS_KEY",
    "HUAWEICLOUD_OBS_ACCESS_KEY_ID",
    "AWS_ACCESS_KEY_ID",
];
const OBS_SECRET_KEY_OPTIONS: &[&str] = &["fs.obs.secret.key", "fs.obs.secretKey", "fs.obs.sk"];
const OBS_SECRET_KEY_ENV: &[&str] = &[
    "OBS_SECRET_ACCESS_KEY",
    "OBS_SECRET_KEY",
    "HUAWEICLOUD_OBS_SECRET_ACCESS_KEY",
    "AWS_SECRET_ACCESS_KEY",
];
const OBS_ENDPOINT_OPTIONS: &[&str] = &["fs.obs.endpoint", "fs.obs.endpoint.region"];
const OBS_ENDPOINT_ENV: &[&str] = &["OBS_ENDPOINT", "HUAWEICLOUD_OBS_ENDPOINT"];
const OBS_SECURITY_TOKEN_OPTIONS: &[&str] = &[
    "fs.obs.security.token",
    "fs.obs.session.token",
    "fs.obs.token",
];
const OBS_SECURITY_TOKEN_ENV: &[&str] = &[
    "OBS_SECURITY_TOKEN",
    "OBS_SESSION_TOKEN",
    "HUAWEICLOUD_OBS_SECURITY_TOKEN",
    "AWS_SESSION_TOKEN",
];
const RUNTIME_THREAD_ENV: &[&str] = &[
    "PAIMON_NATIVE_IO_WORKER_THREADS",
    "PAIMON_NATIVE_IO_RUNTIME_THREADS",
];
const DEFAULT_RUNTIME_THREADS: usize = 4;
const DEFAULT_OBS_REQUEST_TIMEOUT_MS: u64 = 30_000;
const DEFAULT_OBS_CONNECT_TIMEOUT_MS: u64 = 10_000;
const READER_BATCH_CELL_BUDGET: usize = 256 * 1024;
const MIN_READER_BATCH_ROWS: usize = 16;
const RANGE_COALESCE_MAX_GAP_BYTES: u64 = 64 * 1024;

static GLOBAL_RUNTIME: OnceLock<Result<tokio::runtime::Runtime, String>> = OnceLock::new();

type NativeDiagnosticsCallback = Option<unsafe extern "C" fn(event_json: *const c_char)>;

struct NativeExportDiagnostics {
    operation_id: String,
    callback: NativeDiagnosticsCallback,
    output_path: Option<String>,
    runtime_threads: Option<usize>,
    sequence: u64,
}

#[derive(Default)]
struct NativeDiagnosticEvent<'a> {
    event_type: &'a str,
    phase: Option<&'a str>,
    file_path: Option<&'a str>,
    object_operation: Option<&'a str>,
    duration_ms: Option<u64>,
    rows: Option<u64>,
    bytes: Option<u64>,
    queue_depth: Option<usize>,
    native_memory_bytes: Option<u64>,
    peak_buffered_bytes: Option<u64>,
    metrics_json: Option<String>,
}

impl NativeExportDiagnostics {
    fn disabled() -> Self {
        Self {
            operation_id: "native-export-parquet-disabled".to_string(),
            callback: None,
            output_path: None,
            runtime_threads: None,
            sequence: 0,
        }
    }

    fn new(operation_id: String, callback: NativeDiagnosticsCallback) -> Self {
        Self {
            operation_id,
            callback,
            output_path: None,
            runtime_threads: None,
            sequence: 0,
        }
    }

    fn set_task_context(&mut self, task: &ExportTask) {
        self.output_path = Some(task.output_path.clone());
        self.runtime_threads = Some(task.runtime_threads);
    }

    fn emit(&mut self, event: NativeDiagnosticEvent<'_>) {
        let Some(callback) = self.callback else {
            return;
        };
        self.sequence = self.sequence.saturating_add(1);
        let event_id = format!("{}-native-{}", self.operation_id, self.sequence);
        let object_request_id = event
            .object_operation
            .map(|operation| format!("{}-{}-{}", self.operation_id, operation, self.sequence));
        let payload = serde_json::json!({
            "version": 1,
            "event_id": event_id,
            "event_time": current_time_millis(),
            "event_type": event.event_type,
            "operation_id": self.operation_id,
            "operation_name": "native-export-parquet",
            "phase": event.phase,
            "file_path": event.file_path,
            "output_path": self.output_path,
            "object_request_id": object_request_id,
            "object_operation": event.object_operation,
            "duration_ms": event.duration_ms,
            "rows": event.rows,
            "bytes": event.bytes,
            "queue_depth": event.queue_depth,
            "runtime_threads": self.runtime_threads,
            "native_memory_bytes": event.native_memory_bytes,
            "peak_buffered_bytes": event.peak_buffered_bytes,
            "metrics_json": event.metrics_json,
        });
        if let Ok(json) = CString::new(payload.to_string()) {
            unsafe {
                callback(json.as_ptr());
            }
        }
    }
}

#[repr(C)]
pub struct ReaderConfig {
    files: Vec<String>,
    batch_size: usize,
    row_index_column: String,
    target_columns: Option<Vec<String>>,
    object_store_options: HashMap<String, String>,
    deleted_positions: HashMap<String, HashSet<i64>>,
    last_error: Option<CString>,
}

#[repr(C)]
pub struct Reader {
    schema: SchemaRef,
    file_readers: Vec<FileBatchReader>,
    current_file: usize,
    row_index_column: String,
    last_error: Option<CString>,
}

struct FileBatchReader {
    reader: ParquetRecordBatchReader,
    row_offset: i64,
    deleted_positions: HashSet<i64>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct ObsObjectLocation {
    bucket: String,
    key: String,
}

#[derive(Clone)]
struct ObsObjectChunkReader {
    inner: Arc<ObsObjectInner>,
}

#[derive(Default)]
struct ObsReadMetrics {
    requests: AtomicU64,
    retries: AtomicU64,
    bytes: AtomicU64,
}

struct ObsObjectInner {
    client: Client,
    bucket: String,
    key: String,
    len: u64,
    runtime_threads: usize,
    read_buffer_size_bytes: usize,
    read_concurrency: usize,
    range_cache: Mutex<Option<ObsRangeCache>>,
    metrics: Option<Arc<ObsReadMetrics>>,
}

#[derive(Clone, Debug)]
struct ObsRangeCache {
    start: u64,
    bytes: Bytes,
}

struct ObsObjectRead {
    source: ObsObjectChunkReader,
    position: u64,
}

struct ExportReader {
    reader: AsyncFileBatchReader,
    obs_metrics: Option<Arc<ObsReadMetrics>>,
    late_materialization: bool,
}

struct AsyncFileBatchReader {
    stream: ParquetRecordBatchStream<Box<dyn AsyncFileReader>>,
    row_offset: i64,
}

struct LateMaterializationFilter {
    predicate: Value,
    partition: HashMap<String, String>,
    predicate_columns: Vec<String>,
}

#[derive(Debug)]
struct CoalescedRangeSlice {
    original_index: usize,
    offset: usize,
    length: usize,
}

#[derive(Debug)]
struct CoalescedRange {
    range: Range<u64>,
    slices: Vec<CoalescedRangeSlice>,
}

impl ReaderConfig {
    fn new() -> Self {
        Self {
            files: Vec::new(),
            batch_size: 4096,
            row_index_column: DEFAULT_ROW_INDEX_COLUMN.to_string(),
            target_columns: None,
            object_store_options: HashMap::new(),
            deleted_positions: HashMap::new(),
            last_error: None,
        }
    }

    fn set_error(&mut self, error: impl ToString) -> i32 {
        self.last_error = cstring(error.to_string());
        -1
    }
}

impl Reader {
    fn set_error(&mut self, error: impl ToString) -> i32 {
        self.last_error = cstring(error.to_string());
        -1
    }

    fn next_batch(&mut self) -> Result<Option<RecordBatch>, String> {
        loop {
            if self.current_file >= self.file_readers.len() {
                return Ok(None);
            }

            let file_reader = &mut self.file_readers[self.current_file];
            let batch = match file_reader.reader.next() {
                Some(Ok(batch)) => batch,
                Some(Err(e)) => return Err(e.to_string()),
                None => {
                    self.current_file += 1;
                    continue;
                }
            };

            let input_rows = batch.num_rows();
            let batch = append_row_index(batch, &self.row_index_column, file_reader.row_offset)?;
            file_reader.row_offset += input_rows as i64;
            let batch = filter_deleted_positions(
                batch,
                &self.row_index_column,
                &file_reader.deleted_positions,
            )?;
            return Ok(Some(batch));
        }
    }
}

fn error_reader(error: impl ToString) -> Reader {
    Reader {
        schema: Arc::new(Schema::empty()),
        file_readers: Vec::new(),
        current_file: 0,
        row_index_column: DEFAULT_ROW_INDEX_COLUMN.to_string(),
        last_error: cstring(error.to_string()),
    }
}

fn cstring(value: String) -> Option<CString> {
    Some(
        CString::new(value.replace('\0', "\\0"))
            .unwrap_or_else(|_| CString::new("native io error").unwrap()),
    )
}

fn panic_message(payload: Box<dyn std::any::Any + Send>) -> String {
    if let Some(message) = payload.downcast_ref::<&str>() {
        (*message).to_string()
    } else if let Some(message) = payload.downcast_ref::<String>() {
        message.clone()
    } else {
        "unknown native panic".to_string()
    }
}

unsafe fn catch_config_i32<F>(config: *mut ReaderConfig, action: F) -> i32
where
    F: FnOnce(&mut ReaderConfig) -> i32,
{
    if config.is_null() {
        return -1;
    }
    match catch_unwind(AssertUnwindSafe(|| action(&mut *config))) {
        Ok(status) => status,
        Err(payload) => (*config).set_error(format!("native panic: {}", panic_message(payload))),
    }
}

unsafe fn catch_reader_i32<F>(reader: *mut Reader, action: F) -> i32
where
    F: FnOnce(&mut Reader) -> i32,
{
    if reader.is_null() {
        return -1;
    }
    match catch_unwind(AssertUnwindSafe(|| action(&mut *reader))) {
        Ok(status) => status,
        Err(payload) => (*reader).set_error(format!("native panic: {}", panic_message(payload))),
    }
}

unsafe fn cstr_to_string(value: *const c_char) -> Result<String, String> {
    if value.is_null() {
        return Err("null string pointer".to_string());
    }
    CStr::from_ptr(value)
        .to_str()
        .map(|s| s.to_string())
        .map_err(|e| e.to_string())
}

unsafe fn cstr_to_non_empty_string(
    value: *const c_char,
    field_name: &str,
) -> Result<String, String> {
    let value = cstr_to_string(value)?;
    if value.trim().is_empty() {
        Err(format!("{} must be non-empty", field_name))
    } else {
        Ok(value)
    }
}

impl ObsObjectLocation {
    fn parse(path: &str) -> Result<Self, String> {
        let url = Url::parse(path).map_err(|e| e.to_string())?;
        if url.scheme() != "obs" {
            return Err(format!(
                "unsupported path scheme for OBS reader: {}",
                url.scheme()
            ));
        }
        if url.query().is_some() {
            return Err("OBS path must not contain query component".to_string());
        }
        if url.fragment().is_some() {
            return Err("OBS path must not contain fragment component".to_string());
        }
        let bucket = url
            .host_str()
            .ok_or_else(|| "OBS path missing bucket".to_string())?;
        let key = url.path().trim_start_matches('/');
        if key.is_empty() {
            return Err("OBS path missing object key".to_string());
        }
        Ok(Self {
            bucket: bucket.to_string(),
            key: key.to_string(),
        })
    }
}

fn range_header(start: u64, length: usize) -> String {
    format!("bytes={}-{}", start, start + length as u64 - 1)
}

fn option<'a>(options: &'a HashMap<String, String>, keys: &[&str]) -> Option<&'a String> {
    keys.iter()
        .find_map(|key| options.get(*key).filter(|value| is_non_blank(value)))
}

fn is_non_blank(value: &str) -> bool {
    !value.trim().is_empty()
}

fn option_or_env_with<F>(
    options: &HashMap<String, String>,
    keys: &[&str],
    env_keys: &[&str],
    env_lookup: F,
) -> Option<String>
where
    F: Fn(&str) -> Option<String>,
{
    option(options, keys).cloned().or_else(|| {
        env_keys
            .iter()
            .find_map(|key| env_lookup(key).filter(|value| is_non_blank(value)))
    })
}

fn option_or_env(
    options: &HashMap<String, String>,
    keys: &[&str],
    env_keys: &[&str],
) -> Option<String> {
    option_or_env_with(options, keys, env_keys, |key| std::env::var(key).ok())
}

fn runtime_worker_threads_with<F>(env_lookup: F) -> usize
where
    F: Fn(&str) -> Option<String>,
{
    RUNTIME_THREAD_ENV
        .iter()
        .find_map(|key| env_lookup(key))
        .and_then(|value| value.parse::<usize>().ok())
        .filter(|threads| *threads > 0)
        .unwrap_or(DEFAULT_RUNTIME_THREADS)
}

fn runtime_worker_threads() -> usize {
    runtime_worker_threads_with(|key| std::env::var(key).ok())
}

fn global_runtime_with_threads(
    worker_threads: usize,
) -> Result<&'static tokio::runtime::Runtime, String> {
    let worker_threads = worker_threads.max(1);
    match GLOBAL_RUNTIME.get_or_init(|| {
        tokio::runtime::Builder::new_multi_thread()
            .worker_threads(worker_threads)
            .enable_all()
            .build()
            .map_err(|e| e.to_string())
    }) {
        Ok(runtime) => Ok(runtime),
        Err(error) => Err(error.clone()),
    }
}

#[cfg(test)]
fn build_obs_client(options: &HashMap<String, String>) -> Result<Client, String> {
    build_obs_client_with_timeouts(
        options,
        DEFAULT_OBS_REQUEST_TIMEOUT_MS,
        DEFAULT_OBS_CONNECT_TIMEOUT_MS,
    )
}

fn build_obs_client_with_timeouts(
    options: &HashMap<String, String>,
    request_timeout_ms: u64,
    connect_timeout_ms: u64,
) -> Result<Client, String> {
    let access_key = option_or_env(options, OBS_ACCESS_KEY_OPTIONS, OBS_ACCESS_KEY_ENV)
        .ok_or_else(|| "missing fs.obs.access.key or OBS_ACCESS_KEY_ID".to_string())?;
    let secret_key = option_or_env(options, OBS_SECRET_KEY_OPTIONS, OBS_SECRET_KEY_ENV)
        .ok_or_else(|| "missing fs.obs.secret.key or OBS_SECRET_ACCESS_KEY".to_string())?;
    let endpoint = option_or_env(options, OBS_ENDPOINT_OPTIONS, OBS_ENDPOINT_ENV)
        .ok_or_else(|| "missing fs.obs.endpoint or OBS_ENDPOINT".to_string())?;
    let security_token = option_or_env(options, OBS_SECURITY_TOKEN_OPTIONS, OBS_SECURITY_TOKEN_ENV);

    let credentials = match security_token {
        Some(token) => Credentials::new_with_token(access_key, secret_key, token),
        None => Credentials::new(access_key, secret_key),
    };

    let mut builder = Config::builder()
        .credentials(credentials)
        .endpoint(endpoint.clone())
        .timeout(Duration::from_millis(request_timeout_ms.max(1)))
        .connect_timeout(Duration::from_millis(connect_timeout_ms.max(1)));

    if endpoint.starts_with("http://") {
        builder = builder.secure(false);
    }

    let config = builder.build().map_err(|e| e.to_string())?;
    Client::from_config(config).map_err(|e| e.to_string())
}

impl ObsObjectChunkReader {
    fn new(path: &str, options: &HashMap<String, String>) -> Result<Self, String> {
        Self::new_with_runtime_metrics(
            path,
            options,
            None,
            runtime_worker_threads(),
            8 * 1024 * 1024,
            4,
            DEFAULT_OBS_REQUEST_TIMEOUT_MS,
            DEFAULT_OBS_CONNECT_TIMEOUT_MS,
        )
    }

    fn new_with_runtime_metrics(
        path: &str,
        options: &HashMap<String, String>,
        metrics: Option<Arc<ObsReadMetrics>>,
        runtime_threads: usize,
        read_buffer_size_bytes: usize,
        read_concurrency: usize,
        request_timeout_ms: u64,
        connect_timeout_ms: u64,
    ) -> Result<Self, String> {
        let location = ObsObjectLocation::parse(path)?;
        let client =
            build_obs_client_with_timeouts(options, request_timeout_ms, connect_timeout_ms)?;
        let runtime_threads = runtime_threads.max(1);
        let len = global_runtime_with_threads(runtime_threads)?
            .block_on(async {
                client
                    .head_object()
                    .bucket(&location.bucket)
                    .key(&location.key)
                    .send()
                    .await
            })
            .map_err(|e| {
                format!(
                    "OBS head_object failed for obs://{}/{}: {}",
                    location.bucket, location.key, e
                )
            })?
            .content_length()
            .ok_or_else(|| "OBS head_object response missing Content-Length".to_string())?;

        Ok(Self {
            inner: Arc::new(ObsObjectInner {
                client,
                bucket: location.bucket,
                key: location.key,
                len,
                runtime_threads,
                read_buffer_size_bytes,
                read_concurrency: read_concurrency.max(1),
                range_cache: Mutex::new(None),
                metrics,
            }),
        })
    }

    fn fetch_range(&self, start: u64, length: usize) -> ParquetResult<Bytes> {
        if length == 0 {
            return Ok(Bytes::new());
        }
        let end = start
            .checked_add(length as u64)
            .ok_or_else(|| ParquetError::General("OBS range overflow".to_string()))?;
        if start > self.inner.len || end > self.inner.len {
            return Err(ParquetError::EOF(format!(
                "Expected to read {} bytes at offset {}, while object has length {}",
                length, start, self.inner.len
            )));
        }

        let mut cache =
            self.inner.range_cache.lock().map_err(|e| {
                ParquetError::General(format!("OBS range cache lock poisoned: {}", e))
            })?;
        read_buffered_range(
            &mut cache,
            self.inner.len,
            self.inner.read_buffer_size_bytes,
            start,
            length,
            |fetch_start, fetch_length| self.fetch_range_uncached(fetch_start, fetch_length),
        )
    }

    fn fetch_range_uncached(&self, start: u64, length: usize) -> ParquetResult<Bytes> {
        let range = range_header(start, length);
        let range_for_error = range.clone();
        if let Some(metrics) = &self.inner.metrics {
            metrics.requests.fetch_add(1, Ordering::Relaxed);
        }
        let bytes = global_runtime_with_threads(self.inner.runtime_threads)
            .map_err(ParquetError::General)?
            .block_on(async {
                self.inner
                    .client
                    .get_object()
                    .bucket(&self.inner.bucket)
                    .key(&self.inner.key)
                    .range(range)
                    .send()
                    .await
            })
            .map_err(|e| {
                ParquetError::General(format!(
                    "OBS get_object failed for obs://{}/{} range {}: {}",
                    self.inner.bucket, self.inner.key, range_for_error, e
                ))
            })?
            .into_body();
        if bytes.len() != length {
            return Err(ParquetError::EOF(format!(
                "Expected to read {} bytes at offset {}, OBS returned {}",
                length,
                start,
                bytes.len()
            )));
        }
        if let Some(metrics) = &self.inner.metrics {
            metrics
                .bytes
                .fetch_add(bytes.len() as u64, Ordering::Relaxed);
        }
        Ok(bytes)
    }

    async fn fetch_range_async(&self, start: u64, length: usize) -> ParquetResult<Bytes> {
        if length == 0 {
            return Ok(Bytes::new());
        }
        let end = start
            .checked_add(length as u64)
            .ok_or_else(|| ParquetError::General("OBS range overflow".to_string()))?;
        if start > self.inner.len || end > self.inner.len {
            return Err(ParquetError::EOF(format!(
                "Expected to read {} bytes at offset {}, while object has length {}",
                length, start, self.inner.len
            )));
        }

        let fetch_length = {
            let cache = self.inner.range_cache.lock().map_err(|e| {
                ParquetError::General(format!("OBS range cache lock poisoned: {}", e))
            })?;
            if let Some(cache) = cache.as_ref() {
                let cache_end = cache.start + cache.bytes.len() as u64;
                if start >= cache.start && end <= cache_end {
                    let offset = (start - cache.start) as usize;
                    return Ok(cache.bytes.slice(offset..offset + length));
                }
            }
            if length >= self.inner.read_buffer_size_bytes {
                length
            } else {
                let remaining = self.inner.len.saturating_sub(start);
                let remaining = usize::try_from(remaining).unwrap_or(usize::MAX);
                self.inner.read_buffer_size_bytes.max(length).min(remaining)
            }
        };

        let bytes = self.fetch_range_uncached_async(start, fetch_length).await?;
        if bytes.len() < length {
            return Err(ParquetError::EOF(format!(
                "Expected buffered read to return at least {} bytes at offset {}, got {}",
                length,
                start,
                bytes.len()
            )));
        }
        if length < self.inner.read_buffer_size_bytes {
            let mut cache = self.inner.range_cache.lock().map_err(|e| {
                ParquetError::General(format!("OBS range cache lock poisoned: {}", e))
            })?;
            *cache = Some(ObsRangeCache {
                start,
                bytes: bytes.clone(),
            });
        }
        Ok(bytes.slice(0..length))
    }

    async fn fetch_range_exact_async(&self, range: Range<u64>) -> ParquetResult<Bytes> {
        let length = range_length(&range)?;
        if range.start > self.inner.len || range.end > self.inner.len {
            return Err(ParquetError::EOF(format!(
                "Expected to read range {}..{}, while object has length {}",
                range.start, range.end, self.inner.len
            )));
        }
        self.fetch_range_uncached_async(range.start, length).await
    }

    async fn fetch_range_uncached_async(&self, start: u64, length: usize) -> ParquetResult<Bytes> {
        if length == 0 {
            return Ok(Bytes::new());
        }
        let range = range_header(start, length);
        let range_for_error = range.clone();
        if let Some(metrics) = &self.inner.metrics {
            metrics.requests.fetch_add(1, Ordering::Relaxed);
        }
        let bytes = self
            .inner
            .client
            .get_object()
            .bucket(&self.inner.bucket)
            .key(&self.inner.key)
            .range(range)
            .send()
            .await
            .map_err(|e| {
                ParquetError::General(format!(
                    "OBS get_object failed for obs://{}/{} range {}: {}",
                    self.inner.bucket, self.inner.key, range_for_error, e
                ))
            })?
            .into_body();
        if bytes.len() != length {
            return Err(ParquetError::EOF(format!(
                "Expected to read {} bytes at offset {}, OBS returned {}",
                length,
                start,
                bytes.len()
            )));
        }
        if let Some(metrics) = &self.inner.metrics {
            metrics
                .bytes
                .fetch_add(bytes.len() as u64, Ordering::Relaxed);
        }
        Ok(bytes)
    }
}

fn range_length(range: &Range<u64>) -> ParquetResult<usize> {
    if range.end < range.start {
        return Err(ParquetError::General(format!(
            "invalid byte range {}..{}",
            range.start, range.end
        )));
    }
    usize::try_from(range.end - range.start)
        .map_err(|_| ParquetError::General("byte range length exceeds usize".to_string()))
}

async fn fetch_byte_ranges_concurrently<F, Fut>(
    ranges: Vec<Range<u64>>,
    concurrency: usize,
    fetch: F,
) -> ParquetResult<Vec<Bytes>>
where
    F: FnMut(Range<u64>) -> Fut,
    Fut: Future<Output = ParquetResult<Bytes>>,
{
    let results = futures::stream::iter(ranges)
        .map(fetch)
        .buffered(concurrency.max(1))
        .collect::<Vec<_>>()
        .await;
    results.into_iter().collect()
}

fn coalesce_byte_ranges(
    ranges: Vec<Range<u64>>,
    max_gap_bytes: u64,
    max_merged_range_bytes: usize,
) -> ParquetResult<Vec<CoalescedRange>> {
    let max_merged_range_bytes = u64::try_from(max_merged_range_bytes.max(1))
        .map_err(|_| ParquetError::General("max merged range exceeds u64".to_string()))?;
    let mut indexed_ranges = ranges.into_iter().enumerate().collect::<Vec<_>>();
    indexed_ranges.sort_by_key(|(_, range)| (range.start, range.end));

    let mut coalesced = Vec::<CoalescedRange>::new();
    for (original_index, range) in indexed_ranges {
        let length = range_length(&range)?;
        if let Some(current) = coalesced.last_mut() {
            let current_end = current.range.end;
            let merged_end = current_end.max(range.end);
            let merged_len = merged_end.saturating_sub(current.range.start);
            let gap = range.start.saturating_sub(current_end);
            let contained = range.end <= current_end;
            if contained || (gap <= max_gap_bytes && merged_len <= max_merged_range_bytes) {
                let offset = usize::try_from(range.start.saturating_sub(current.range.start))
                    .map_err(|_| {
                        ParquetError::General("coalesced range offset exceeds usize".to_string())
                    })?;
                current.range.end = merged_end;
                current.slices.push(CoalescedRangeSlice {
                    original_index,
                    offset,
                    length,
                });
                continue;
            }
        }

        coalesced.push(CoalescedRange {
            range: range.clone(),
            slices: vec![CoalescedRangeSlice {
                original_index,
                offset: 0,
                length,
            }],
        });
    }
    Ok(coalesced)
}

async fn fetch_byte_ranges_coalesced<F, Fut>(
    ranges: Vec<Range<u64>>,
    concurrency: usize,
    max_gap_bytes: u64,
    max_merged_range_bytes: usize,
    fetch: F,
) -> ParquetResult<Vec<Bytes>>
where
    F: FnMut(Range<u64>) -> Fut,
    Fut: Future<Output = ParquetResult<Bytes>>,
{
    let original_len = ranges.len();
    let coalesced = coalesce_byte_ranges(ranges, max_gap_bytes, max_merged_range_bytes)?;
    let merged_ranges = coalesced
        .iter()
        .map(|coalesced| coalesced.range.clone())
        .collect::<Vec<_>>();
    let merged_bytes = fetch_byte_ranges_concurrently(merged_ranges, concurrency, fetch).await?;
    let mut results = vec![None; original_len];
    for (coalesced, bytes) in coalesced.into_iter().zip(merged_bytes.into_iter()) {
        for slice in coalesced.slices {
            let end = slice.offset.checked_add(slice.length).ok_or_else(|| {
                ParquetError::General("coalesced range slice overflow".to_string())
            })?;
            if end > bytes.len() {
                return Err(ParquetError::EOF(format!(
                    "Expected coalesced range {}..{} to contain slice {}..{}, got {} bytes",
                    coalesced.range.start,
                    coalesced.range.end,
                    slice.offset,
                    end,
                    bytes.len()
                )));
            }
            results[slice.original_index] = Some(bytes.slice(slice.offset..end));
        }
    }
    results
        .into_iter()
        .map(|bytes| {
            bytes.ok_or_else(|| ParquetError::General("missing coalesced range slice".to_string()))
        })
        .collect()
}

impl AsyncFileReader for ObsObjectChunkReader {
    fn get_bytes(&mut self, range: Range<u64>) -> BoxFuture<'_, ParquetResult<Bytes>> {
        async move {
            let length = range_length(&range)?;
            self.fetch_range_async(range.start, length).await
        }
        .boxed()
    }

    fn get_byte_ranges(
        &mut self,
        ranges: Vec<Range<u64>>,
    ) -> BoxFuture<'_, ParquetResult<Vec<Bytes>>> {
        let reader = self.clone();
        let concurrency = self.inner.read_concurrency;
        let max_merged_range_bytes = self.inner.read_buffer_size_bytes;
        async move {
            fetch_byte_ranges_coalesced(
                ranges,
                concurrency,
                RANGE_COALESCE_MAX_GAP_BYTES,
                max_merged_range_bytes,
                move |range| {
                    let reader = reader.clone();
                    async move { reader.fetch_range_exact_async(range).await }
                },
            )
            .await
        }
        .boxed()
    }

    fn get_metadata<'a>(
        &'a mut self,
        options: Option<&'a ArrowReaderOptions>,
    ) -> BoxFuture<'a, ParquetResult<Arc<ParquetMetaData>>> {
        async move {
            let file_size = self.inner.len;
            let metadata_opts = options.map(|value| value.metadata_options().clone());
            let page_index = options.map(|value| value.page_index()).unwrap_or(false);
            let metadata_reader = ParquetMetaDataReader::new()
                .with_page_index_policy(PageIndexPolicy::from(page_index))
                .with_metadata_options(metadata_opts);
            let metadata = metadata_reader
                .load_and_finish(&mut *self, file_size)
                .await?;
            Ok(Arc::new(metadata))
        }
        .boxed()
    }
}

fn read_buffered_range<F>(
    cache: &mut Option<ObsRangeCache>,
    object_len: u64,
    read_buffer_size_bytes: usize,
    start: u64,
    length: usize,
    mut fetch: F,
) -> ParquetResult<Bytes>
where
    F: FnMut(u64, usize) -> ParquetResult<Bytes>,
{
    if length == 0 {
        return Ok(Bytes::new());
    }
    if let Some(cache) = cache.as_ref() {
        let cache_end = cache.start + cache.bytes.len() as u64;
        let request_end = start
            .checked_add(length as u64)
            .ok_or_else(|| ParquetError::General("OBS range overflow".to_string()))?;
        if start >= cache.start && request_end <= cache_end {
            let offset = (start - cache.start) as usize;
            return Ok(cache.bytes.slice(offset..offset + length));
        }
    }

    let fetch_length = if length >= read_buffer_size_bytes {
        length
    } else {
        let remaining = object_len.saturating_sub(start);
        let remaining = usize::try_from(remaining).unwrap_or(usize::MAX);
        read_buffer_size_bytes.max(length).min(remaining)
    };
    let bytes = fetch(start, fetch_length)?;
    if bytes.len() < length {
        return Err(ParquetError::EOF(format!(
            "Expected buffered read to return at least {} bytes at offset {}, got {}",
            length,
            start,
            bytes.len()
        )));
    }
    if length < read_buffer_size_bytes {
        *cache = Some(ObsRangeCache {
            start,
            bytes: bytes.clone(),
        });
    }
    Ok(bytes.slice(0..length))
}

impl Length for ObsObjectChunkReader {
    fn len(&self) -> u64 {
        self.inner.len
    }
}

impl ChunkReader for ObsObjectChunkReader {
    type T = ObsObjectRead;

    fn get_read(&self, start: u64) -> ParquetResult<Self::T> {
        if start > self.inner.len {
            return Err(ParquetError::EOF(format!(
                "Expected to read at offset {}, while object has length {}",
                start, self.inner.len
            )));
        }
        Ok(ObsObjectRead {
            source: self.clone(),
            position: start,
        })
    }

    fn get_bytes(&self, start: u64, length: usize) -> ParquetResult<Bytes> {
        self.fetch_range(start, length)
    }
}

impl Read for ObsObjectRead {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        if buf.is_empty() || self.position >= self.source.inner.len {
            return Ok(0);
        }
        let remaining = (self.source.inner.len - self.position) as usize;
        let length = buf.len().min(remaining);
        let bytes = self
            .source
            .fetch_range(self.position, length)
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e.to_string()))?;
        buf[..bytes.len()].copy_from_slice(&bytes);
        self.position += bytes.len() as u64;
        Ok(bytes.len())
    }
}

fn open_local(path: &str) -> Result<File, String> {
    let local_path = path
        .strip_prefix("file://")
        .or_else(|| path.strip_prefix("file:"))
        .unwrap_or(path);
    File::open(local_path).map_err(|e| e.to_string())
}

async fn open_local_async(path: &str) -> Result<tokio::fs::File, String> {
    let local_path = path
        .strip_prefix("file://")
        .or_else(|| path.strip_prefix("file:"))
        .unwrap_or(path);
    tokio::fs::File::open(local_path)
        .await
        .map_err(|e| e.to_string())
}

fn append_row_index(
    batch: RecordBatch,
    row_index_column: &str,
    first_row: i64,
) -> Result<RecordBatch, String> {
    let num_rows = batch.num_rows();
    let mut fields = batch.schema().fields().to_vec();
    let mut columns = batch.columns().to_vec();
    let row_index: ArrayRef = Arc::new(Int64Array::from_iter_values(
        (0..num_rows).map(|i| first_row + i as i64),
    ));
    fields.push(Arc::new(Field::new(
        row_index_column,
        DataType::Int64,
        false,
    )));
    columns.push(row_index);
    let schema = Arc::new(Schema::new(fields));
    RecordBatch::try_new(schema, columns).map_err(|e| e.to_string())
}

fn schema_with_row_index(schema: SchemaRef, row_index_column: &str) -> SchemaRef {
    let mut fields = schema.fields().to_vec();
    fields.push(Arc::new(Field::new(
        row_index_column,
        DataType::Int64,
        false,
    )));
    Arc::new(Schema::new(fields))
}

fn adaptive_reader_batch_size(
    configured_batch_size: usize,
    projected_column_count: usize,
) -> usize {
    let configured_batch_size = configured_batch_size.max(1);
    let projected_column_count = projected_column_count.max(1);
    let budget_rows =
        (READER_BATCH_CELL_BUDGET / projected_column_count).max(MIN_READER_BATCH_ROWS);
    configured_batch_size.min(budget_rows)
}

fn build_file_batch_reader<R: ChunkReader + 'static>(
    reader: R,
    batch_size: usize,
    row_index_column: &str,
    target_columns: Option<&[String]>,
) -> Result<(SchemaRef, FileBatchReader), String> {
    let mut builder =
        ParquetRecordBatchReaderBuilder::try_new(reader).map_err(|e| e.to_string())?;
    let mut projected_column_count = builder.parquet_schema().columns().len();
    if let Some(target_columns) = target_columns {
        let parquet_schema = builder.parquet_schema();
        let available_columns: HashSet<String> = parquet_schema
            .columns()
            .iter()
            .map(|column| column.path().string())
            .collect();
        let selected_columns: Vec<&str> = target_columns
            .iter()
            .map(String::as_str)
            .filter(|column| available_columns.contains(*column))
            .collect();
        projected_column_count = selected_columns.len();
        let mask = ProjectionMask::columns(parquet_schema, selected_columns);
        builder = builder.with_projection(mask);
    }
    let batch_size = adaptive_reader_batch_size(batch_size, projected_column_count);
    let reader = builder
        .with_batch_size(batch_size)
        .build()
        .map_err(|e| e.to_string())?;
    let schema = schema_with_row_index(reader.schema(), row_index_column);
    Ok((
        schema,
        FileBatchReader {
            reader,
            row_offset: 0,
            deleted_positions: HashSet::new(),
        },
    ))
}

async fn build_async_file_batch_reader(
    reader: Box<dyn AsyncFileReader>,
    batch_size: usize,
    row_index_column: &str,
    output_columns: &[String],
    fallback_columns: &[String],
    late_filter: Option<LateMaterializationFilter>,
) -> Result<(SchemaRef, AsyncFileBatchReader, bool), String> {
    let mut builder = ParquetRecordBatchStreamBuilder::new(reader)
        .await
        .map_err(|e| e.to_string())?;
    let row_filter = late_filter
        .map(|filter| build_export_row_filter(builder.parquet_schema(), filter))
        .transpose()?
        .flatten();
    let late_materialization = row_filter.is_some();
    if let Some(row_filter) = row_filter {
        builder = builder.with_row_filter(row_filter);
    }
    let target_columns = if late_materialization {
        output_columns
    } else {
        fallback_columns
    };
    let mut projected_column_count = builder.parquet_schema().columns().len();
    if !target_columns.is_empty() {
        let parquet_schema = builder.parquet_schema();
        let available_columns: HashSet<String> = parquet_schema
            .columns()
            .iter()
            .map(|column| column.path().string())
            .collect();
        let selected_columns: Vec<&str> = target_columns
            .iter()
            .map(String::as_str)
            .filter(|column| available_columns.contains(*column))
            .collect();
        projected_column_count = selected_columns.len();
        let mask = ProjectionMask::columns(parquet_schema, selected_columns);
        builder = builder.with_projection(mask);
    }
    let batch_size = adaptive_reader_batch_size(batch_size, projected_column_count);
    let stream = builder
        .with_batch_size(batch_size)
        .build()
        .map_err(|e| e.to_string())?;
    let schema = schema_with_row_index(stream.schema().clone(), row_index_column);
    Ok((
        schema,
        AsyncFileBatchReader {
            stream,
            row_offset: 0,
        },
        late_materialization,
    ))
}

fn build_export_row_filter(
    parquet_schema: &SchemaDescriptor,
    filter: LateMaterializationFilter,
) -> Result<Option<RowFilter>, String> {
    let available_columns: HashSet<String> = parquet_schema
        .columns()
        .iter()
        .map(|column| column.path().string())
        .collect();
    let selected_columns: Vec<&str> = filter
        .predicate_columns
        .iter()
        .map(String::as_str)
        .filter(|column| available_columns.contains(*column))
        .collect();
    if selected_columns.is_empty() {
        return Ok(None);
    }

    let predicate = filter.predicate;
    let partition = filter.partition;
    let mask = ProjectionMask::columns(parquet_schema, selected_columns);
    let arrow_predicate = ArrowPredicateFn::new(mask, move |batch: RecordBatch| {
        let mut values = Vec::with_capacity(batch.num_rows());
        for row in 0..batch.num_rows() {
            values.push(
                eval_predicate(&predicate, &batch, &partition, row).map_err(|error| {
                    ArrowError::ComputeError(format!("native export predicate failed: {}", error))
                })?,
            );
        }
        Ok(BooleanArray::from(values))
    });
    Ok(Some(RowFilter::new(vec![Box::new(arrow_predicate)])))
}

fn build_file_readers(config: &ReaderConfig) -> Result<(SchemaRef, Vec<FileBatchReader>), String> {
    if config.files.is_empty() {
        return Err("native reader requires at least one file".to_string());
    }

    let mut schema: Option<SchemaRef> = None;
    let mut file_readers = Vec::new();
    for file in &config.files {
        let (file_schema, mut file_reader) = if file.starts_with("obs://") {
            build_file_batch_reader(
                ObsObjectChunkReader::new(file, &config.object_store_options)?,
                config.batch_size,
                &config.row_index_column,
                config.target_columns.as_deref(),
            )?
        } else {
            build_file_batch_reader(
                open_local(file)?,
                config.batch_size,
                &config.row_index_column,
                config.target_columns.as_deref(),
            )?
        };
        file_reader.deleted_positions = config
            .deleted_positions
            .get(file)
            .cloned()
            .unwrap_or_default();
        if let Some(schema) = &schema {
            if schema.as_ref() != file_schema.as_ref() {
                return Err(format!("native reader file schema mismatch for {}", file));
            }
        } else {
            schema = Some(file_schema);
        }
        file_readers.push(file_reader);
    }

    Ok((
        schema.ok_or_else(|| "schema must exist when files are not empty".to_string())?,
        file_readers,
    ))
}

#[unsafe(no_mangle)]
pub extern "C" fn paimon_reader_config_new() -> *mut ReaderConfig {
    match catch_unwind(|| Box::into_raw(Box::new(ReaderConfig::new()))) {
        Ok(config) => config,
        Err(_) => ptr::null_mut(),
    }
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn paimon_reader_config_free(config: *mut ReaderConfig) {
    let _ = catch_unwind(AssertUnwindSafe(|| {
        if !config.is_null() {
            drop(Box::from_raw(config));
        }
    }));
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn paimon_reader_config_add_file(
    config: *mut ReaderConfig,
    file: *const c_char,
) -> i32 {
    catch_config_i32(config, |config| {
        match cstr_to_non_empty_string(file, "file path") {
            Ok(file) => {
                config.files.push(file);
                0
            }
            Err(e) => config.set_error(e),
        }
    })
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn paimon_reader_config_set_target_schema(
    config: *mut ReaderConfig,
    schema_json: *const c_char,
) -> i32 {
    catch_config_i32(config, |config| {
        match cstr_to_string(schema_json)
            .and_then(|json| serde_json::from_str::<Vec<String>>(&json).map_err(|e| e.to_string()))
        {
            Ok(columns) => {
                config.target_columns = Some(columns);
                0
            }
            Err(e) => config.set_error(e),
        }
    })
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn paimon_reader_config_set_batch_size(
    config: *mut ReaderConfig,
    batch_size: i32,
) -> i32 {
    catch_config_i32(config, |config| {
        if batch_size <= 0 {
            return config.set_error("batch size must be positive");
        }
        config.batch_size = batch_size as usize;
        0
    })
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn paimon_reader_config_set_row_index_column(
    config: *mut ReaderConfig,
    column_name: *const c_char,
) -> i32 {
    catch_config_i32(config, |config| {
        match cstr_to_non_empty_string(column_name, "row index column") {
            Ok(name) => {
                config.row_index_column = name;
                0
            }
            Err(e) => config.set_error(e),
        }
    })
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn paimon_reader_config_set_object_store_option(
    config: *mut ReaderConfig,
    key: *const c_char,
    value: *const c_char,
) -> i32 {
    catch_config_i32(config, |config| {
        match (
            cstr_to_non_empty_string(key, "object store option key"),
            cstr_to_non_empty_string(value, "object store option value"),
        ) {
            (Ok(key), Ok(value)) => {
                config.object_store_options.insert(key, value);
                0
            }
            (Err(e), _) | (_, Err(e)) => config.set_error(e),
        }
    })
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn paimon_reader_config_add_deleted_position(
    config: *mut ReaderConfig,
    file: *const c_char,
    position: i64,
) -> i32 {
    catch_config_i32(config, |config| {
        if position < 0 {
            return config.set_error("deleted position must be non-negative");
        }
        match cstr_to_non_empty_string(file, "file path") {
            Ok(file) => {
                config
                    .deleted_positions
                    .entry(file)
                    .or_default()
                    .insert(position);
                0
            }
            Err(e) => config.set_error(e),
        }
    })
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn paimon_reader_config_add_deleted_positions(
    config: *mut ReaderConfig,
    file: *const c_char,
    positions: *const i64,
    position_count: i32,
) -> i32 {
    catch_config_i32(config, |config| {
        if position_count < 0 {
            return config.set_error("deleted position count must be non-negative");
        }
        if position_count > 0 && positions.is_null() {
            return config.set_error("deleted positions pointer must not be null");
        }
        let positions: &[i64] = if position_count == 0 {
            &[]
        } else {
            std::slice::from_raw_parts(positions, position_count as usize)
        };
        if positions.iter().any(|position| *position < 0) {
            return config.set_error("deleted position must be non-negative");
        }
        match cstr_to_non_empty_string(file, "file path") {
            Ok(file) => {
                config
                    .deleted_positions
                    .entry(file)
                    .or_default()
                    .extend(positions.iter().copied());
                0
            }
            Err(e) => config.set_error(e),
        }
    })
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn paimon_reader_config_last_error(
    config: *mut ReaderConfig,
) -> *const c_char {
    match catch_unwind(AssertUnwindSafe(|| {
        if config.is_null() {
            return ptr::null();
        }
        (*config)
            .last_error
            .as_ref()
            .map(|e| e.as_ptr())
            .unwrap_or(ptr::null())
    })) {
        Ok(error) => error,
        Err(_) => ptr::null(),
    }
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn paimon_reader_new(config: *mut ReaderConfig) -> *mut Reader {
    if config.is_null() {
        return ptr::null_mut();
    }
    match catch_unwind(AssertUnwindSafe(|| match build_file_readers(&*config) {
        Ok((schema, file_readers)) => Reader {
            schema,
            file_readers,
            current_file: 0,
            row_index_column: (*config).row_index_column.clone(),
            last_error: None,
        },
        Err(e) => error_reader(e),
    })) {
        Ok(reader) => Box::into_raw(Box::new(reader)),
        Err(payload) => Box::into_raw(Box::new(error_reader(format!(
            "native panic: {}",
            panic_message(payload)
        )))),
    }
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn paimon_reader_free(reader: *mut Reader) {
    let _ = catch_unwind(AssertUnwindSafe(|| {
        if !reader.is_null() {
            drop(Box::from_raw(reader));
        }
    }));
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn paimon_reader_next_record_batch_blocked(
    reader: *mut Reader,
    arrow_array_address: isize,
) -> i32 {
    catch_reader_i32(reader, |reader| {
        if arrow_array_address == 0 {
            return reader.set_error("null ArrowArray address");
        }

        loop {
            let batch = match reader.next_batch() {
                Ok(Some(batch)) => batch,
                Ok(None) => return 0,
                Err(e) => return reader.set_error(e),
            };
            let rows = batch.num_rows();
            if rows == 0 {
                continue;
            }

            let struct_array: Arc<StructArray> = Arc::new(batch.into());
            let ffi_array = FFI_ArrowArray::new(&struct_array.to_data());
            (&ffi_array as *const FFI_ArrowArray)
                .copy_to(arrow_array_address as *mut FFI_ArrowArray, 1);
            std::mem::forget(ffi_array);
            return rows as i32;
        }
    })
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn paimon_reader_get_schema(
    reader: *mut Reader,
    arrow_schema_address: isize,
) -> i32 {
    catch_reader_i32(reader, |reader| {
        if arrow_schema_address == 0 {
            return reader.set_error("null ArrowSchema address");
        }

        match FFI_ArrowSchema::try_from(reader.schema.as_ref()) {
            Ok(schema) => {
                ptr::write_unaligned(arrow_schema_address as *mut FFI_ArrowSchema, schema);
                0
            }
            Err(e) => reader.set_error(e),
        }
    })
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn paimon_reader_last_error(reader: *mut Reader) -> *const c_char {
    match catch_unwind(AssertUnwindSafe(|| {
        if reader.is_null() {
            return ptr::null();
        }
        (*reader)
            .last_error
            .as_ref()
            .map(|e| e.as_ptr())
            .unwrap_or(ptr::null())
    })) {
        Ok(error) => error,
        Err(_) => ptr::null(),
    }
}

pub struct Exporter;

#[derive(Clone, Debug)]
struct ExportFile {
    path: String,
    partition: HashMap<String, String>,
    deleted_positions: HashSet<u64>,
}

#[derive(Clone, Debug)]
struct ExportTask {
    output_path: String,
    compression: String,
    target_file_size_bytes: u64,
    read_buffer_size_bytes: u64,
    read_concurrency: usize,
    obs_request_timeout_ms: u64,
    obs_connect_timeout_ms: u64,
    writer_batch_size: usize,
    writer_row_group_size: usize,
    multipart_part_size_bytes: u64,
    memory_limit_bytes: u64,
    runtime_threads: usize,
    metadata_cache_enabled: bool,
    projection: Vec<String>,
    predicate_format: String,
    predicate: Value,
    object_store_options: HashMap<String, String>,
    files: Vec<ExportFile>,
}

#[derive(Clone, Debug)]
enum ScalarValue {
    Null,
    Bool(bool),
    I64(i64),
    F64(f64),
    Utf8(String),
}

fn parse_obs_prefix(path: &str) -> Result<(String, String), String> {
    let url = Url::parse(path).map_err(|e| e.to_string())?;
    if url.scheme() != "obs" {
        return Err(format!(
            "unsupported path scheme for OBS prefix: {}",
            url.scheme()
        ));
    }
    if url.query().is_some() {
        return Err("OBS prefix must not contain query parameters".to_string());
    }
    if url.fragment().is_some() {
        return Err("OBS prefix must not contain fragment".to_string());
    }
    let bucket = url
        .host_str()
        .ok_or_else(|| "OBS path missing bucket".to_string())?;
    Ok((
        bucket.to_string(),
        url.path().trim_start_matches('/').to_string(),
    ))
}

fn parse_export_task(request_json: &str) -> Result<ExportTask, String> {
    let root: Value = serde_json::from_str(request_json).map_err(|e| e.to_string())?;
    let version = root
        .get("request_version")
        .and_then(Value::as_i64)
        .ok_or_else(|| "MISSING_REQUEST_VERSION".to_string())?;
    if version != 1 {
        return Err(format!("UNSUPPORTED_REQUEST_VERSION: {}", version));
    }
    let predicate_format = string_field(&root, "predicate_format")?;
    if predicate_format != "paimon-json-v1" {
        return Err(format!(
            "UNSUPPORTED_PREDICATE_FORMAT: {}",
            predicate_format
        ));
    }
    let files = root
        .get("files")
        .and_then(Value::as_array)
        .ok_or_else(|| "MISSING_FILES".to_string())?
        .iter()
        .map(parse_export_file)
        .collect::<Result<Vec<_>, _>>()?;
    Ok(ExportTask {
        output_path: string_field(&root, "output_path")?,
        compression: string_field(&root, "compression")?,
        target_file_size_bytes: root
            .get("target_file_size_bytes")
            .and_then(Value::as_u64)
            .unwrap_or(u64::MAX),
        read_buffer_size_bytes: root
            .get("read_buffer_size_bytes")
            .and_then(Value::as_u64)
            .unwrap_or(8 * 1024 * 1024),
        read_concurrency: root
            .get("read_concurrency")
            .and_then(Value::as_u64)
            .unwrap_or(4) as usize,
        obs_request_timeout_ms: root
            .get("obs_request_timeout_ms")
            .and_then(Value::as_u64)
            .unwrap_or(DEFAULT_OBS_REQUEST_TIMEOUT_MS),
        obs_connect_timeout_ms: root
            .get("obs_connect_timeout_ms")
            .and_then(Value::as_u64)
            .unwrap_or(DEFAULT_OBS_CONNECT_TIMEOUT_MS),
        writer_batch_size: root
            .get("writer_batch_size")
            .and_then(Value::as_u64)
            .unwrap_or(8192) as usize,
        writer_row_group_size: root
            .get("writer_row_group_size")
            .and_then(Value::as_u64)
            .unwrap_or(250000) as usize,
        multipart_part_size_bytes: root
            .get("multipart_part_size_bytes")
            .and_then(Value::as_u64)
            .unwrap_or(64 * 1024 * 1024),
        memory_limit_bytes: root
            .get("memory_limit_bytes")
            .and_then(Value::as_u64)
            .unwrap_or(512 * 1024 * 1024),
        runtime_threads: root
            .get("runtime_threads")
            .and_then(Value::as_u64)
            .unwrap_or(DEFAULT_RUNTIME_THREADS as u64) as usize,
        metadata_cache_enabled: root
            .get("metadata_cache_enabled")
            .and_then(Value::as_bool)
            .unwrap_or(true),
        projection: root
            .get("projection")
            .and_then(Value::as_array)
            .ok_or_else(|| "MISSING_PROJECTION".to_string())?
            .iter()
            .map(|v| {
                v.as_str()
                    .map(|s| s.to_string())
                    .ok_or_else(|| "projection contains non-string value".to_string())
            })
            .collect::<Result<Vec<_>, _>>()?,
        predicate_format,
        predicate: root
            .get("predicate_json")
            .and_then(Value::as_str)
            .map(|s| serde_json::from_str(s).map_err(|e| e.to_string()))
            .transpose()?
            .unwrap_or_else(|| serde_json::json!({"op":"true"})),
        object_store_options: root
            .get("object_store")
            .and_then(Value::as_object)
            .map(|map| {
                map.iter()
                    .filter_map(|(key, value)| value.as_str().map(|v| (key.clone(), v.to_string())))
                    .collect()
            })
            .unwrap_or_default(),
        files,
    })
}

#[derive(Debug)]
struct ActiveExportWriter {
    output_path: String,
    local_output: Option<String>,
    multipart_part_size_bytes: u64,
    runtime_threads: usize,
    obs_request_timeout_ms: u64,
    obs_connect_timeout_ms: u64,
    cleanup: TempFileCleanup,
    writer: ExportWriter,
    rows: u64,
    buffered_bytes: u64,
    reported_obs_write_ms: u64,
}

#[derive(Debug)]
enum ExportWriter {
    Local(ArrowWriter<File>),
    Obs(ArrowWriter<ObsMultipartWriter>),
}

#[derive(Debug)]
struct TempFileCleanup {
    path: String,
    enabled: bool,
}

impl Drop for TempFileCleanup {
    fn drop(&mut self) {
        if self.enabled {
            let _ = std::fs::remove_file(&self.path);
        }
    }
}

#[derive(Default)]
struct UploadStats {
    requests: u64,
    bytes: u64,
    write_ms: u64,
    multipart_finish_ms: u64,
}

struct FinishedExportOutput {
    bytes: u64,
    upload_stats: Option<UploadStats>,
}

#[derive(Debug)]
struct MultipartWriteBuffer {
    part_size: usize,
    buffer: Vec<u8>,
}

impl MultipartWriteBuffer {
    fn new(part_size: usize) -> Result<Self, String> {
        if part_size == 0 {
            return Err("multipart part size must be positive".to_string());
        }
        Ok(Self {
            part_size,
            buffer: Vec::with_capacity(part_size.min(8 * 1024 * 1024)),
        })
    }

    #[cfg(test)]
    fn write_bytes<F>(&mut self, bytes: &[u8], mut emit_part: F) -> Result<(), String>
    where
        F: FnMut(Vec<u8>) -> Result<(), String>,
    {
        let mut offset = 0usize;
        while offset < bytes.len() {
            if !self.buffer.is_empty() {
                let take = self
                    .remaining_capacity()
                    .min(bytes.len().saturating_sub(offset));
                self.extend_from_slice(&bytes[offset..offset + take]);
                offset += take;
                if self.is_full() {
                    emit_part(self.take_remaining())?;
                }
                continue;
            }

            let remaining = bytes.len() - offset;
            if remaining >= self.part_size {
                let end = offset + self.part_size;
                emit_part(bytes[offset..end].to_vec())?;
                offset = end;
            } else {
                self.extend_from_slice(&bytes[offset..]);
                break;
            }
        }
        Ok(())
    }

    #[cfg(test)]
    fn finish<F>(&mut self, mut emit_part: F) -> Result<(), String>
    where
        F: FnMut(Vec<u8>) -> Result<(), String>,
    {
        if !self.buffer.is_empty() {
            emit_part(std::mem::take(&mut self.buffer))?;
        }
        Ok(())
    }

    fn take_remaining(&mut self) -> Vec<u8> {
        std::mem::take(&mut self.buffer)
    }

    fn part_size(&self) -> usize {
        self.part_size
    }

    fn buffered_len(&self) -> usize {
        self.buffer.len()
    }

    fn remaining_capacity(&self) -> usize {
        self.part_size.saturating_sub(self.buffer.len())
    }

    fn is_full(&self) -> bool {
        self.buffer.len() >= self.part_size
    }

    fn extend_from_slice(&mut self, bytes: &[u8]) {
        self.buffer.extend_from_slice(bytes);
    }
}

struct ObsMultipartWriter {
    output_path: String,
    bucket: String,
    key: String,
    client: Client,
    runtime_threads: usize,
    max_inflight_parts: usize,
    buffer: MultipartWriteBuffer,
    upload_id: Option<String>,
    completed_parts: Vec<(i32, CompletedPart)>,
    inflight_parts: VecDeque<PendingPartUpload>,
    inflight_bytes: u64,
    next_part_number: i32,
    stats: UploadStats,
    completed: bool,
}

struct PendingPartUpload {
    part_number: i32,
    bytes: u64,
    handle: tokio::task::JoinHandle<Result<(CompletedPart, u64), String>>,
}

impl std::fmt::Debug for ObsMultipartWriter {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ObsMultipartWriter")
            .field("output_path", &self.output_path)
            .field("buffered_bytes", &self.buffer.buffered_len())
            .field("upload_id", &self.upload_id.as_ref().map(|_| "<set>"))
            .field("completed_parts", &self.completed_parts.len())
            .field("inflight_parts", &self.inflight_parts.len())
            .field("inflight_bytes", &self.inflight_bytes)
            .field("max_inflight_parts", &self.max_inflight_parts)
            .field("next_part_number", &self.next_part_number)
            .field("completed", &self.completed)
            .finish()
    }
}

impl ObsMultipartWriter {
    fn new(
        output_path: &str,
        options: &HashMap<String, String>,
        multipart_part_size_bytes: u64,
        memory_limit_bytes: u64,
        runtime_threads: usize,
        request_timeout_ms: u64,
        connect_timeout_ms: u64,
    ) -> Result<Self, String> {
        let part_size = usize::try_from(multipart_part_size_bytes)
            .map_err(|_| "multipart part size exceeds usize".to_string())?;
        let (bucket, key) = parse_obs_prefix(output_path)?;
        let runtime_threads = runtime_threads.max(1);
        let max_inflight_parts = multipart_max_inflight_parts(
            runtime_threads,
            memory_limit_bytes,
            multipart_part_size_bytes,
        );
        export_log(format!(
            "multipart_writer_config output_path={} part_size={} max_inflight_parts={} memory_limit_bytes={}",
            output_path, multipart_part_size_bytes, max_inflight_parts, memory_limit_bytes
        ));
        Ok(Self {
            output_path: output_path.to_string(),
            bucket,
            key,
            client: build_obs_client_with_timeouts(
                options,
                request_timeout_ms,
                connect_timeout_ms,
            )?,
            runtime_threads,
            max_inflight_parts,
            buffer: MultipartWriteBuffer::new(part_size)?,
            upload_id: None,
            completed_parts: Vec::new(),
            inflight_parts: VecDeque::new(),
            inflight_bytes: 0,
            next_part_number: 1,
            stats: UploadStats::default(),
            completed: false,
        })
    }

    fn write_inner(&mut self, bytes: &[u8]) -> Result<(), String> {
        let mut offset = 0usize;
        while offset < bytes.len() {
            if self.buffer.buffered_len() > 0 {
                let take = self
                    .buffer
                    .remaining_capacity()
                    .min(bytes.len().saturating_sub(offset));
                self.buffer.extend_from_slice(&bytes[offset..offset + take]);
                offset += take;
                if self.buffer.is_full() {
                    let part = self.buffer.take_remaining();
                    self.submit_part(part)?;
                }
                continue;
            }

            let remaining = bytes.len() - offset;
            if remaining >= self.buffer.part_size() {
                let end = offset + self.buffer.part_size();
                self.submit_part(bytes[offset..end].to_vec())?;
                offset = end;
            } else {
                self.buffer.extend_from_slice(&bytes[offset..]);
                break;
            }
        }
        Ok(())
    }

    fn complete(mut self) -> Result<UploadStats, String> {
        if self.upload_id.is_none() {
            let body = self.buffer.take_remaining();
            self.put_object(body)?;
            self.completed = true;
            return Ok(std::mem::take(&mut self.stats));
        }

        let final_part = self.buffer.take_remaining();
        self.submit_part(final_part)?;
        self.drain_part_uploads()?;

        let upload_id = self
            .upload_id
            .clone()
            .ok_or_else(|| "OBS multipart upload missing upload id".to_string())?;
        self.completed_parts
            .sort_by_key(|(part_number, _)| *part_number);
        let completed_parts = std::mem::take(&mut self.completed_parts)
            .into_iter()
            .map(|(_, part)| part)
            .collect();
        let finish_start = std::time::Instant::now();
        let complete_result = global_runtime_with_threads(self.runtime_threads)?.block_on(async {
            self.client
                .complete_multipart_upload()
                .bucket(&self.bucket)
                .key(&self.key)
                .upload_id(&upload_id)
                .parts(completed_parts)
                .send()
                .await
        });
        let finish_ms = elapsed_ms(finish_start);
        self.stats.write_ms += finish_ms;
        self.stats.multipart_finish_ms += finish_ms;
        complete_result.map_err(|e| {
            format!(
                "OBS complete_multipart_upload failed for {}: {}",
                self.output_path, e
            )
        })?;
        self.stats.requests += 1;
        self.completed = true;
        export_log(format!(
            "multipart_complete_done output_path={} requests={} bytes={} elapsed_ms={}",
            self.output_path, self.stats.requests, self.stats.bytes, finish_ms
        ));
        Ok(std::mem::take(&mut self.stats))
    }

    fn put_object(&mut self, body: Vec<u8>) -> Result<(), String> {
        let bytes = body.len() as u64;
        let start = std::time::Instant::now();
        export_log(format!(
            "put_object_start output_path={} bytes={}",
            self.output_path, bytes
        ));
        global_runtime_with_threads(self.runtime_threads)?
            .block_on(async {
                self.client
                    .put_object()
                    .bucket(&self.bucket)
                    .key(&self.key)
                    .body(body)
                    .content_type("application/octet-stream")
                    .send()
                    .await
            })
            .map_err(|e| format!("OBS put_object failed for {}: {}", self.output_path, e))?;
        let elapsed = elapsed_ms(start);
        self.stats.requests += 1;
        self.stats.bytes += bytes;
        self.stats.write_ms += elapsed;
        export_log(format!(
            "put_object_done output_path={} bytes={} elapsed_ms={}",
            self.output_path, bytes, elapsed
        ));
        Ok(())
    }

    fn ensure_multipart_upload(&mut self) -> Result<(), String> {
        if self.upload_id.is_some() {
            return Ok(());
        }
        let start = std::time::Instant::now();
        export_log(format!(
            "multipart_initiate_start output_path={}",
            self.output_path
        ));
        let initiate = global_runtime_with_threads(self.runtime_threads)?
            .block_on(async {
                self.client
                    .initiate_multipart_upload()
                    .bucket(&self.bucket)
                    .key(&self.key)
                    .content_type("application/octet-stream")
                    .send()
                    .await
            })
            .map_err(|e| {
                format!(
                    "OBS initiate_multipart_upload failed for {}: {}",
                    self.output_path, e
                )
            })?;
        let elapsed = elapsed_ms(start);
        self.stats.requests += 1;
        self.stats.write_ms += elapsed;
        self.upload_id = Some(initiate.upload_id().to_string());
        export_log(format!(
            "multipart_initiate_done output_path={} elapsed_ms={}",
            self.output_path, elapsed
        ));
        Ok(())
    }

    fn submit_part(&mut self, body: Vec<u8>) -> Result<(), String> {
        if body.is_empty() {
            return Ok(());
        }
        while self.inflight_parts.len() >= self.max_inflight_parts() {
            self.wait_for_next_part()?;
        }
        if self.next_part_number > 10000 {
            return Err(format!(
                "OBS multipart upload would exceed 10000 parts for {}",
                self.output_path
            ));
        }
        self.ensure_multipart_upload()?;
        let upload_id = self
            .upload_id
            .clone()
            .ok_or_else(|| "OBS multipart upload missing upload id".to_string())?;
        let part_number = self.next_part_number;
        let bytes = body.len() as u64;
        export_log(format!(
            "multipart_part_start output_path={} part={} bytes={}",
            self.output_path, part_number, bytes
        ));
        let client = self.client.clone();
        let bucket = self.bucket.clone();
        let key = self.key.clone();
        let output_path = self.output_path.clone();
        let handle = global_runtime_with_threads(self.runtime_threads)?.spawn(async move {
            let start = std::time::Instant::now();
            let part = client
                .upload_part()
                .bucket(&bucket)
                .key(&key)
                .upload_id(&upload_id)
                .part_number(part_number)
                .body(body)
                .send()
                .await
                .map_err(|e| {
                    format!(
                        "OBS upload_part failed for {} part {}: {}",
                        output_path, part_number, e
                    )
                })?;
            Ok((
                CompletedPart::new(part_number, part.etag()),
                elapsed_ms(start),
            ))
        });
        self.inflight_parts.push_back(PendingPartUpload {
            part_number,
            bytes,
            handle,
        });
        self.inflight_bytes = self.inflight_bytes.saturating_add(bytes);
        self.next_part_number += 1;
        Ok(())
    }

    fn max_inflight_parts(&self) -> usize {
        self.max_inflight_parts
    }

    fn wait_for_next_part(&mut self) -> Result<(), String> {
        let Some(pending) = self.inflight_parts.pop_front() else {
            return Ok(());
        };
        let wait_start = std::time::Instant::now();
        let (part, upload_elapsed_ms) = global_runtime_with_threads(self.runtime_threads)?
            .block_on(pending.handle)
            .map_err(|e| {
                format!(
                    "OBS upload_part task failed for {} part {}: {}",
                    self.output_path, pending.part_number, e
                )
            })??;
        self.stats.write_ms += elapsed_ms(wait_start);
        self.stats.requests += 1;
        self.stats.bytes += pending.bytes;
        self.inflight_bytes = self.inflight_bytes.saturating_sub(pending.bytes);
        self.completed_parts.push((pending.part_number, part));
        export_log(format!(
            "multipart_part_done output_path={} part={} bytes={} upload_elapsed_ms={}",
            self.output_path, pending.part_number, pending.bytes, upload_elapsed_ms
        ));
        Ok(())
    }

    fn drain_part_uploads(&mut self) -> Result<(), String> {
        while !self.inflight_parts.is_empty() {
            self.wait_for_next_part()?;
        }
        Ok(())
    }

    fn buffered_len(&self) -> usize {
        self.buffer
            .buffered_len()
            .saturating_add(usize::try_from(self.inflight_bytes).unwrap_or(usize::MAX))
    }

    fn write_ms(&self) -> u64 {
        self.stats.write_ms
    }

    fn inflight_part_count(&self) -> usize {
        self.inflight_parts.len()
    }
}

fn multipart_max_inflight_parts(
    runtime_threads: usize,
    memory_limit_bytes: u64,
    multipart_part_size_bytes: u64,
) -> usize {
    let runtime_threads = runtime_threads.max(1);
    let memory_part_cap_u64 = memory_limit_bytes
        .checked_div(multipart_part_size_bytes.max(1))
        .unwrap_or(0)
        .max(1);
    let memory_part_cap = usize::try_from(memory_part_cap_u64).unwrap_or(usize::MAX);
    runtime_threads.min(memory_part_cap).max(1)
}

impl Write for ObsMultipartWriter {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.write_inner(buf).map_err(std::io::Error::other)?;
        Ok(buf.len())
    }

    fn write_all(&mut self, buf: &[u8]) -> std::io::Result<()> {
        self.write_inner(buf).map_err(std::io::Error::other)
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

impl Drop for ObsMultipartWriter {
    fn drop(&mut self) {
        if self.completed {
            return;
        }
        let Some(upload_id) = self.upload_id.take() else {
            return;
        };
        export_log(format!(
            "multipart_abort_start output_path={}",
            self.output_path
        ));
        for pending in self.inflight_parts.drain(..) {
            pending.handle.abort();
        }
        self.inflight_bytes = 0;
        if let Ok(runtime) = global_runtime_with_threads(self.runtime_threads) {
            let _ = runtime.block_on(async {
                self.client
                    .abort_multipart_upload()
                    .bucket(&self.bucket)
                    .key(&self.key)
                    .upload_id(&upload_id)
                    .send()
                    .await
            });
        }
    }
}

impl ExportWriter {
    fn write(&mut self, batch: &RecordBatch) -> Result<(), String> {
        match self {
            ExportWriter::Local(writer) => writer.write(batch),
            ExportWriter::Obs(writer) => writer.write(batch),
        }
        .map_err(|e| e.to_string())
    }

    fn flush(&mut self) -> Result<(), String> {
        match self {
            ExportWriter::Local(writer) => writer.flush(),
            ExportWriter::Obs(writer) => writer.flush(),
        }
        .map_err(|e| e.to_string())
    }

    fn memory_size(&self) -> usize {
        match self {
            ExportWriter::Local(writer) => writer.memory_size(),
            ExportWriter::Obs(writer) => writer.memory_size(),
        }
    }

    fn in_progress_size(&self) -> usize {
        match self {
            ExportWriter::Local(writer) => writer.in_progress_size(),
            ExportWriter::Obs(writer) => writer.in_progress_size(),
        }
    }

    fn in_progress_rows(&self) -> usize {
        match self {
            ExportWriter::Local(writer) => writer.in_progress_rows(),
            ExportWriter::Obs(writer) => writer.in_progress_rows(),
        }
    }

    fn bytes_written(&self) -> usize {
        match self {
            ExportWriter::Local(writer) => writer.bytes_written(),
            ExportWriter::Obs(writer) => writer.bytes_written(),
        }
    }

    fn multipart_buffered_bytes(&self) -> usize {
        match self {
            ExportWriter::Local(_) => 0,
            ExportWriter::Obs(writer) => writer.inner().buffered_len(),
        }
    }

    fn multipart_inflight_parts(&self) -> usize {
        match self {
            ExportWriter::Local(_) => 0,
            ExportWriter::Obs(writer) => writer.inner().inflight_part_count(),
        }
    }

    fn obs_write_ms(&self) -> u64 {
        match self {
            ExportWriter::Local(_) => 0,
            ExportWriter::Obs(writer) => writer.inner().write_ms(),
        }
    }

    fn finish(self, local_output: Option<&str>) -> Result<FinishedExportOutput, String> {
        match self {
            ExportWriter::Local(writer) => {
                let _file = writer.into_inner().map_err(|e| e.to_string())?;
                let local_output =
                    local_output.ok_or_else(|| "local export writer missing path".to_string())?;
                let bytes = std::fs::metadata(local_output)
                    .map_err(|e| e.to_string())?
                    .len();
                Ok(FinishedExportOutput {
                    bytes,
                    upload_stats: None,
                })
            }
            ExportWriter::Obs(writer) => {
                let obs_writer = writer.into_inner().map_err(|e| e.to_string())?;
                let stats = obs_writer.complete()?;
                let bytes = stats.bytes;
                Ok(FinishedExportOutput {
                    bytes,
                    upload_stats: Some(stats),
                })
            }
        }
    }
}

#[cfg(test)]
#[derive(Debug, Clone, Eq, PartialEq)]
struct MultipartPartRange {
    part_number: i32,
    offset: u64,
    length: usize,
}

fn parse_export_file(value: &Value) -> Result<ExportFile, String> {
    let partition = value
        .get("partition")
        .and_then(Value::as_object)
        .map(|map| {
            map.iter()
                .filter_map(|(key, value)| value.as_str().map(|v| (key.clone(), v.to_string())))
                .collect()
        })
        .unwrap_or_default();
    let deleted_positions = value
        .get("positions")
        .and_then(Value::as_array)
        .map(|positions| {
            positions
                .iter()
                .filter_map(Value::as_u64)
                .collect::<HashSet<u64>>()
        })
        .unwrap_or_default();
    Ok(ExportFile {
        path: string_field(value, "path")?,
        partition,
        deleted_positions,
    })
}

fn string_field(value: &Value, name: &str) -> Result<String, String> {
    value
        .get(name)
        .and_then(Value::as_str)
        .map(|s| s.to_string())
        .ok_or_else(|| format!("missing string field: {}", name))
}

fn export_parquet(request_json: &str) -> Result<String, String> {
    let mut diagnostics = NativeExportDiagnostics::disabled();
    export_parquet_with_diagnostics(request_json, &mut diagnostics)
}

fn export_parquet_with_diagnostics(
    request_json: &str,
    diagnostics: &mut NativeExportDiagnostics,
) -> Result<String, String> {
    let task = parse_export_task(request_json)?;
    diagnostics.set_task_context(&task);
    diagnostics.emit(NativeDiagnosticEvent {
        event_type: "REQUEST_PARSED",
        phase: Some("PLAN"),
        metrics_json: Some(format!(
            "{{\"files\":{},\"projection\":{}}}",
            task.files.len(),
            task.projection.len()
        )),
        ..Default::default()
    });
    validate_export_task_options(&task)?;
    if !task.compression.eq_ignore_ascii_case("zstd") {
        return Err(format!("UNSUPPORTED_COMPRESSION: {}", task.compression));
    }
    if task.predicate_format != "paimon-json-v1" {
        return Err(format!(
            "UNSUPPORTED_PREDICATE_FORMAT: {}",
            task.predicate_format
        ));
    }
    let mut rows_read = 0u64;
    let mut rows_output = 0u64;
    let mut predicate_filtered_rows = 0u64;
    let mut dv_filtered_rows = 0u64;
    let mut parquet_row_groups_read = 0u64;
    let mut peak_buffered_bytes = 0u64;
    let mut writer_rolls = 0u64;
    let mut obs_read_requests = 0u64;
    let mut obs_read_retries = 0u64;
    let mut obs_read_bytes = 0u64;
    let mut obs_write_requests = 0u64;
    let mut obs_write_bytes = 0u64;
    let mut decode_ms = 0u64;
    let mut filter_ms = 0u64;
    let mut encode_ms = 0u64;
    let mut obs_write_ms = 0u64;
    let mut multipart_finish_ms = 0u64;
    let mut files_written = Vec::new();
    let mut next_output_ordinal = 0usize;

    export_log(format!(
        "start output_path={} files={} projection={} request_timeout_ms={} connect_timeout_ms={} runtime_threads={}",
        task.output_path,
        task.files.len(),
        task.projection.len(),
        task.obs_request_timeout_ms,
        task.obs_connect_timeout_ms,
        task.runtime_threads
    ));

    let runtime = global_runtime_with_threads(task.runtime_threads)?;
    for (file_ordinal, file) in task.files.iter().enumerate() {
        export_log(format!(
            "file_start ordinal={} path={}",
            file_ordinal, file.path
        ));
        diagnostics.emit(NativeDiagnosticEvent {
            event_type: "FILE_START",
            phase: Some("OPEN_READER"),
            file_path: Some(&file.path),
            ..Default::default()
        });
        let reader_start = std::time::Instant::now();
        diagnostics.emit(NativeDiagnosticEvent {
            event_type: "PHASE_START",
            phase: Some("OPEN_READER"),
            file_path: Some(&file.path),
            object_operation: Some("open_reader"),
            ..Default::default()
        });
        let (schema, mut export_reader) = runtime.block_on(build_export_reader(
            file,
            &task,
            task.writer_batch_size,
            &task.object_store_options,
        ))?;
        let reader_elapsed_ms = elapsed_ms(reader_start);
        diagnostics.emit(NativeDiagnosticEvent {
            event_type: "READER_READY",
            phase: Some("READ"),
            file_path: Some(&file.path),
            object_operation: Some("open_reader"),
            duration_ms: Some(reader_elapsed_ms),
            metrics_json: Some(format!(
                "{{\"late_materialization\":{}}}",
                export_reader.late_materialization
            )),
            ..Default::default()
        });
        export_log(format!(
            "reader_ready ordinal={} elapsed_ms={}",
            file_ordinal, reader_elapsed_ms
        ));
        let _ = schema;
        let reader = &mut export_reader.reader;
        let mut file_row_groups = 0u64;
        let mut file_batches = 0u64;
        let mut file_rows_read = 0u64;
        let mut file_rows_output = 0u64;
        let mut writer: Option<ActiveExportWriter> = None;

        loop {
            let row_group_index = file_row_groups;
            let row_group_start = std::time::Instant::now();
            diagnostics.emit(NativeDiagnosticEvent {
                event_type: "PHASE_START",
                phase: Some("READ"),
                file_path: Some(&file.path),
                object_operation: Some("read_next_row_group"),
                metrics_json: Some(format!("{{\"row_group_index\":{}}}", row_group_index)),
                ..Default::default()
            });
            let mut row_group_reader = match runtime.block_on(reader.stream.next_row_group()) {
                Ok(Some(row_group_reader)) => row_group_reader,
                Ok(None) => break,
                Err(error) => return Err(error.to_string()),
            };
            let row_group_ready_elapsed_ms = elapsed_ms(row_group_start);
            decode_ms += row_group_ready_elapsed_ms;
            parquet_row_groups_read += 1;
            file_row_groups += 1;
            diagnostics.emit(NativeDiagnosticEvent {
                event_type: "PHASE_PROGRESS",
                phase: Some("READ"),
                file_path: Some(&file.path),
                object_operation: Some("read_row_group"),
                duration_ms: Some(row_group_ready_elapsed_ms),
                metrics_json: Some(format!("{{\"row_group_index\":{}}}", row_group_index)),
                ..Default::default()
            });

            let mut row_group_batches = 0u64;
            let mut row_group_rows_read = 0u64;
            let mut row_group_rows_output = 0u64;
            loop {
                let row_group_batch_index = row_group_batches;
                let file_batch_index = file_batches;
                let decode_start = std::time::Instant::now();
                diagnostics.emit(NativeDiagnosticEvent {
                    event_type: "PHASE_START",
                    phase: Some("READ"),
                    file_path: Some(&file.path),
                    object_operation: Some("read_next_batch"),
                    metrics_json: Some(format!(
                        "{{\"row_group_index\":{},\"row_group_batch_index\":{},\"batch_index\":{}}}",
                        row_group_index, row_group_batch_index, file_batch_index
                    )),
                    ..Default::default()
                });
                let batch = match row_group_reader.next() {
                    Some(batch) => batch.map_err(|e| e.to_string())?,
                    None => break,
                };
                let read_elapsed_ms = elapsed_ms(decode_start);
                decode_ms += read_elapsed_ms;
                file_batches += 1;
                row_group_batches += 1;
                let batch = append_row_index(batch, DEFAULT_ROW_INDEX_COLUMN, reader.row_offset)?;
                reader.row_offset += batch.num_rows() as i64;
                let input_rows = batch.num_rows();
                rows_read += input_rows as u64;
                file_rows_read += input_rows as u64;
                row_group_rows_read += input_rows as u64;
                diagnostics.emit(NativeDiagnosticEvent {
                    event_type: "PHASE_END",
                    phase: Some("READ"),
                    file_path: Some(&file.path),
                    object_operation: Some("read_next_batch"),
                    duration_ms: Some(read_elapsed_ms),
                    rows: Some(input_rows as u64),
                    metrics_json: Some(format!(
                        "{{\"row_group_index\":{},\"row_group_batch_index\":{},\"batch_index\":{}}}",
                        row_group_index, row_group_batch_index, file_batch_index
                    )),
                    ..Default::default()
                });
                let filter_start = std::time::Instant::now();
                diagnostics.emit(NativeDiagnosticEvent {
                    event_type: "PHASE_START",
                    phase: Some("FILTER"),
                    file_path: Some(&file.path),
                    rows: Some(input_rows as u64),
                    ..Default::default()
                });
                let (batch, filtered_by_dv) = if export_reader.late_materialization {
                    (batch, 0)
                } else {
                    apply_export_filters(batch, file, &task.predicate)?
                };
                let filter_elapsed_ms = elapsed_ms(filter_start);
                filter_ms += filter_elapsed_ms;
                dv_filtered_rows += filtered_by_dv as u64;
                predicate_filtered_rows +=
                    input_rows.saturating_sub(filtered_by_dv + batch.num_rows()) as u64;
                diagnostics.emit(NativeDiagnosticEvent {
                    event_type: "PHASE_END",
                    phase: Some("FILTER"),
                    file_path: Some(&file.path),
                    duration_ms: Some(filter_elapsed_ms),
                    rows: Some(batch.num_rows() as u64),
                    metrics_json: Some(format!(
                        "{{\"dv_filtered_rows\":{},\"predicate_filtered_rows\":{}}}",
                        filtered_by_dv,
                        input_rows.saturating_sub(filtered_by_dv + batch.num_rows())
                    )),
                    ..Default::default()
                });
                if batch.num_rows() == 0 {
                    continue;
                }
                let projected = project_export_batch(batch, &task.projection, &file.partition)?;
                rows_output += projected.num_rows() as u64;
                file_rows_output += projected.num_rows() as u64;
                row_group_rows_output += projected.num_rows() as u64;
                let write_start = std::time::Instant::now();
                diagnostics.emit(NativeDiagnosticEvent {
                    event_type: "PHASE_START",
                    phase: Some("WRITE"),
                    file_path: Some(&file.path),
                    rows: Some(projected.num_rows() as u64),
                    ..Default::default()
                });
                write_projected_export_batch(
                    projected,
                    &task,
                    &mut writer,
                    &mut next_output_ordinal,
                    &mut files_written,
                    &mut writer_rolls,
                    &mut peak_buffered_bytes,
                    &mut obs_write_requests,
                    &mut obs_write_bytes,
                    &mut encode_ms,
                    &mut obs_write_ms,
                    &mut multipart_finish_ms,
                )?;
                diagnostics.emit(NativeDiagnosticEvent {
                    event_type: "PHASE_END",
                    phase: Some("WRITE"),
                    file_path: Some(&file.path),
                    duration_ms: Some(elapsed_ms(write_start)),
                    rows: Some(file_rows_output),
                    queue_depth: writer
                        .as_ref()
                        .map(|value| value.writer.multipart_inflight_parts()),
                    peak_buffered_bytes: Some(peak_buffered_bytes),
                    ..Default::default()
                });
            }

            diagnostics.emit(NativeDiagnosticEvent {
                event_type: "PHASE_END",
                phase: Some("READ"),
                file_path: Some(&file.path),
                object_operation: Some("read_row_group"),
                duration_ms: Some(elapsed_ms(row_group_start)),
                rows: Some(row_group_rows_read),
                metrics_json: Some(format!(
                    "{{\"row_group_index\":{},\"batches\":{},\"rows_output\":{}}}",
                    row_group_index, row_group_batches, row_group_rows_output
                )),
                ..Default::default()
            });
            diagnostics.emit(NativeDiagnosticEvent {
                event_type: "PHASE_PROGRESS",
                phase: Some("READ"),
                file_path: Some(&file.path),
                object_operation: Some("read_next_batch"),
                rows: Some(file_rows_read),
                metrics_json: Some(format!(
                    "{{\"row_groups\":{},\"batches\":{},\"rows_output\":{}}}",
                    file_row_groups, file_batches, file_rows_output
                )),
                ..Default::default()
            });
            if file_row_groups == 1 || file_row_groups % 100 == 0 {
                export_log(format!(
                    "file_progress ordinal={} row_groups={} batches={} rows_read={} rows_output={}",
                    file_ordinal, file_row_groups, file_batches, file_rows_read, file_rows_output
                ));
            }
        }

        if let Some(writer) = writer.take() {
            let finish_start = std::time::Instant::now();
            diagnostics.emit(NativeDiagnosticEvent {
                event_type: "PHASE_START",
                phase: Some("MULTIPART_FINISH"),
                file_path: Some(&file.path),
                queue_depth: Some(writer.writer.multipart_inflight_parts()),
                ..Default::default()
            });
            finish_export_writer(
                writer,
                &mut files_written,
                &mut obs_write_requests,
                &mut obs_write_bytes,
                &mut encode_ms,
                &mut obs_write_ms,
                &mut multipart_finish_ms,
            )?;
            diagnostics.emit(NativeDiagnosticEvent {
                event_type: "PHASE_END",
                phase: Some("MULTIPART_FINISH"),
                file_path: Some(&file.path),
                duration_ms: Some(elapsed_ms(finish_start)),
                rows: Some(file_rows_output),
                peak_buffered_bytes: Some(peak_buffered_bytes),
                ..Default::default()
            });
        }
        if let Some(metrics) = export_reader.obs_metrics {
            obs_read_requests += metrics.requests.load(Ordering::Relaxed);
            obs_read_retries += metrics.retries.load(Ordering::Relaxed);
            obs_read_bytes += metrics.bytes.load(Ordering::Relaxed);
        }
        export_log(format!(
            "file_done ordinal={} row_groups={} batches={} rows_read={} rows_output={}",
            file_ordinal, file_row_groups, file_batches, file_rows_read, file_rows_output
        ));
        diagnostics.emit(NativeDiagnosticEvent {
            event_type: "FILE_END",
            phase: Some("CLOSE"),
            file_path: Some(&file.path),
            rows: Some(file_rows_output),
            bytes: Some(obs_read_bytes.saturating_add(obs_write_bytes)),
            peak_buffered_bytes: Some(peak_buffered_bytes),
            metrics_json: Some(format!(
                "{{\"row_groups\":{},\"batches\":{},\"rows_read\":{},\"rows_output\":{},\"obs_read_requests\":{},\"obs_write_requests\":{}}}",
                file_row_groups,
                file_batches,
                file_rows_read,
                file_rows_output,
                obs_read_requests,
                obs_write_requests
            )),
            ..Default::default()
        });
    }

    export_log(format!(
        "done output_path={} rows_read={} rows_output={} files_written={}",
        task.output_path,
        rows_read,
        rows_output,
        files_written.len()
    ));

    let result_json = serde_json::json!({
        "rows_output": rows_output,
        "files_written": files_written,
        "metrics": {
            "rows_read": rows_read,
            "rows_output": rows_output,
            "predicate_filtered_rows": predicate_filtered_rows,
            "dv_filtered_rows": dv_filtered_rows,
            "parquet_row_groups_read": parquet_row_groups_read,
            "parquet_row_groups_pruned": 0,
            "peak_buffered_bytes": peak_buffered_bytes,
            "writer_rolls": writer_rolls,
            "obs_read_requests": obs_read_requests,
            "obs_read_retries": obs_read_retries,
            "obs_read_bytes": obs_read_bytes,
            "obs_write_requests": obs_write_requests,
            "obs_write_bytes": obs_write_bytes,
            "read_ms": 0,
            "decode_ms": decode_ms,
            "filter_ms": filter_ms,
            "encode_ms": encode_ms,
            "obs_write_ms": obs_write_ms,
            "multipart_finish_ms": multipart_finish_ms
        }
    })
    .to_string();
    diagnostics.emit(NativeDiagnosticEvent {
        event_type: "RUNTIME_SNAPSHOT",
        phase: Some("CLOSE"),
        rows: Some(rows_output),
        bytes: Some(obs_read_bytes.saturating_add(obs_write_bytes)),
        peak_buffered_bytes: Some(peak_buffered_bytes),
        metrics_json: Some(result_json.clone()),
        ..Default::default()
    });
    Ok(result_json)
}

fn validate_export_task_options(task: &ExportTask) -> Result<(), String> {
    if task.read_buffer_size_bytes == 0 {
        return Err("INVALID_EXPORT_CONFIG: read_buffer_size_bytes must be positive".to_string());
    }
    if task.read_concurrency == 0 {
        return Err("INVALID_EXPORT_CONFIG: read_concurrency must be positive".to_string());
    }
    if task.obs_request_timeout_ms == 0 {
        return Err("INVALID_EXPORT_CONFIG: obs_request_timeout_ms must be positive".to_string());
    }
    if task.obs_connect_timeout_ms == 0 {
        return Err("INVALID_EXPORT_CONFIG: obs_connect_timeout_ms must be positive".to_string());
    }
    if task.writer_batch_size == 0 {
        return Err("INVALID_EXPORT_CONFIG: writer_batch_size must be positive".to_string());
    }
    if task.writer_row_group_size == 0 {
        return Err("INVALID_EXPORT_CONFIG: writer_row_group_size must be positive".to_string());
    }
    if task.multipart_part_size_bytes == 0 {
        return Err(
            "INVALID_EXPORT_CONFIG: multipart_part_size_bytes must be positive".to_string(),
        );
    }
    if task.multipart_part_size_bytes < MIN_MULTIPART_PART_SIZE_BYTES {
        return Err(format!(
            "INVALID_EXPORT_CONFIG: multipart_part_size_bytes must be at least {} bytes",
            MIN_MULTIPART_PART_SIZE_BYTES
        ));
    }
    if task.memory_limit_bytes == 0 {
        return Err("INVALID_EXPORT_CONFIG: memory_limit_bytes must be positive".to_string());
    }
    if task.memory_limit_bytes < task.multipart_part_size_bytes {
        return Err(
            "INVALID_EXPORT_CONFIG: memory_limit_bytes must be greater than or equal to multipart_part_size_bytes"
                .to_string(),
        );
    }
    if task.output_path.to_lowercase().starts_with("obs://") {
        parse_obs_prefix(&task.output_path)
            .map_err(|e| format!("INVALID_EXPORT_CONFIG: invalid output_path: {}", e))?;
    }
    if task.runtime_threads == 0 {
        return Err("INVALID_EXPORT_CONFIG: runtime_threads must be positive".to_string());
    }
    let _metadata_cache_enabled = task.metadata_cache_enabled;
    Ok(())
}

fn write_projected_export_batch(
    mut batch: RecordBatch,
    task: &ExportTask,
    writer: &mut Option<ActiveExportWriter>,
    next_output_ordinal: &mut usize,
    files_written: &mut Vec<Value>,
    writer_rolls: &mut u64,
    peak_buffered_bytes: &mut u64,
    obs_write_requests: &mut u64,
    obs_write_bytes: &mut u64,
    encode_ms: &mut u64,
    obs_write_ms: &mut u64,
    multipart_finish_ms: &mut u64,
) -> Result<(), String> {
    while batch.num_rows() > 0 {
        if writer.is_none() {
            *writer = Some(create_export_writer(
                task,
                batch.schema(),
                *next_output_ordinal,
            )?);
            *next_output_ordinal += 1;
        }

        let roll_limit = export_roll_limit(task);
        if should_roll_export_writer(writer.as_ref().unwrap(), roll_limit) {
            let finished = writer.take().unwrap();
            finish_export_writer(
                finished,
                files_written,
                obs_write_requests,
                obs_write_bytes,
                encode_ms,
                obs_write_ms,
                multipart_finish_ms,
            )?;
            *writer_rolls += 1;
            continue;
        }

        let to_write = next_export_write_rows(writer.as_ref().unwrap(), task, batch.num_rows());
        let chunk = batch.slice(0, to_write);
        write_export_chunk(
            writer.as_mut().unwrap(),
            &chunk,
            task.memory_limit_bytes,
            peak_buffered_bytes,
            encode_ms,
            obs_write_ms,
        )?;
        if should_roll_export_writer(writer.as_ref().unwrap(), roll_limit) {
            let finished = writer.take().unwrap();
            finish_export_writer(
                finished,
                files_written,
                obs_write_requests,
                obs_write_bytes,
                encode_ms,
                obs_write_ms,
                multipart_finish_ms,
            )?;
            *writer_rolls += 1;
        }
        if to_write == batch.num_rows() {
            break;
        }
        batch = batch.slice(to_write, batch.num_rows() - to_write);
    }
    Ok(())
}

fn create_export_writer(
    task: &ExportTask,
    schema: SchemaRef,
    ordinal: usize,
) -> Result<ActiveExportWriter, String> {
    let output_path = output_part_path(&task.output_path, ordinal)?;
    let writer_properties = Some(
        WriterProperties::builder()
            .set_max_row_group_size(task.writer_row_group_size.max(1))
            .set_write_batch_size(task.writer_batch_size.max(1))
            .set_compression(Compression::ZSTD(ZstdLevel::default()))
            .build(),
    );
    let (local_output, cleanup, writer) = if output_path.starts_with("obs://") {
        export_log(format!(
            "writer_create output_path={} mode=obs-streaming-multipart part_size={}",
            output_path, task.multipart_part_size_bytes
        ));
        let obs_writer = ObsMultipartWriter::new(
            &output_path,
            &task.object_store_options,
            task.multipart_part_size_bytes,
            task.memory_limit_bytes,
            task.runtime_threads,
            task.obs_request_timeout_ms,
            task.obs_connect_timeout_ms,
        )?;
        let writer = ArrowWriter::try_new(obs_writer, schema, writer_properties)
            .map_err(|e| e.to_string())?;
        (
            None,
            TempFileCleanup {
                path: String::new(),
                enabled: false,
            },
            ExportWriter::Obs(writer),
        )
    } else {
        let local_output = local_output_path(&output_path)?;
        export_log(format!(
            "writer_create output_path={} local_output={}",
            output_path, local_output
        ));
        if let Some(parent) = std::path::Path::new(&local_output).parent() {
            std::fs::create_dir_all(parent).map_err(|e| e.to_string())?;
        }
        let writer_file = File::create(&local_output).map_err(|e| e.to_string())?;
        let writer = ArrowWriter::try_new(writer_file, schema, writer_properties)
            .map_err(|e| e.to_string())?;
        (
            Some(local_output),
            TempFileCleanup {
                path: String::new(),
                enabled: false,
            },
            ExportWriter::Local(writer),
        )
    };
    Ok(ActiveExportWriter {
        output_path,
        local_output,
        cleanup,
        multipart_part_size_bytes: task.multipart_part_size_bytes,
        runtime_threads: task.runtime_threads,
        obs_request_timeout_ms: task.obs_request_timeout_ms,
        obs_connect_timeout_ms: task.obs_connect_timeout_ms,
        writer,
        rows: 0,
        buffered_bytes: 0,
        reported_obs_write_ms: 0,
    })
}

fn write_export_chunk(
    writer: &mut ActiveExportWriter,
    batch: &RecordBatch,
    memory_limit_bytes: u64,
    peak_buffered_bytes: &mut u64,
    encode_ms: &mut u64,
    obs_write_ms: &mut u64,
) -> Result<(), String> {
    let encode_start = std::time::Instant::now();
    writer.writer.write(batch)?;
    if writer.writer.memory_size() as u64 >= memory_limit_bytes {
        writer.writer.flush()?;
    }
    let elapsed = elapsed_ms(encode_start);
    let obs_delta = writer
        .writer
        .obs_write_ms()
        .saturating_sub(writer.reported_obs_write_ms);
    writer.reported_obs_write_ms += obs_delta;
    *obs_write_ms += obs_delta;
    *encode_ms += elapsed.saturating_sub(obs_delta);
    writer.rows += batch.num_rows() as u64;
    writer.buffered_bytes = writer_buffered_bytes(writer);
    *peak_buffered_bytes = (*peak_buffered_bytes)
        .max(writer.buffered_bytes)
        .max(record_batch_memory_size(batch))
        .max(writer.writer.memory_size() as u64)
        .max(writer.writer.in_progress_size() as u64)
        .max(writer.writer.multipart_buffered_bytes() as u64);
    Ok(())
}

fn finish_export_writer(
    writer: ActiveExportWriter,
    files_written: &mut Vec<Value>,
    obs_write_requests: &mut u64,
    obs_write_bytes: &mut u64,
    encode_ms: &mut u64,
    obs_write_ms: &mut u64,
    multipart_finish_ms: &mut u64,
) -> Result<(), String> {
    let ActiveExportWriter {
        output_path,
        local_output,
        multipart_part_size_bytes: _multipart_part_size_bytes,
        runtime_threads: _runtime_threads,
        obs_request_timeout_ms: _obs_request_timeout_ms,
        obs_connect_timeout_ms: _obs_connect_timeout_ms,
        cleanup: _cleanup,
        writer,
        rows,
        reported_obs_write_ms,
        ..
    } = writer;
    export_log(format!(
        "writer_finish_start output_path={} rows={}",
        output_path, rows
    ));
    let encode_start = std::time::Instant::now();
    let finished = writer.finish(local_output.as_deref())?;
    let elapsed = elapsed_ms(encode_start);
    if let Some(stats) = finished.upload_stats {
        let obs_delta = stats.write_ms.saturating_sub(reported_obs_write_ms);
        *obs_write_ms += obs_delta;
        *obs_write_requests += stats.requests;
        *obs_write_bytes += stats.bytes;
        *multipart_finish_ms += stats.multipart_finish_ms;
        *encode_ms += elapsed.saturating_sub(obs_delta);
        export_log(format!(
            "upload_done output_path={} requests={} bytes={} elapsed_ms={}",
            output_path, stats.requests, stats.bytes, stats.write_ms
        ));
    } else {
        *encode_ms += elapsed;
    }
    files_written.push(serde_json::json!({
        "path": output_path,
        "rows": rows,
        "bytes": finished.bytes
    }));
    Ok(())
}

fn export_roll_limit(task: &ExportTask) -> u64 {
    task.target_file_size_bytes
}

fn next_export_write_rows(
    writer: &ActiveExportWriter,
    task: &ExportTask,
    remaining_rows: usize,
) -> usize {
    let row_group_size = task.writer_row_group_size.max(1);
    let in_progress_rows = writer.writer.in_progress_rows();
    let rows_until_row_group_boundary = row_group_size.saturating_sub(in_progress_rows).max(1);
    remaining_rows.min(rows_until_row_group_boundary)
}

fn should_roll_export_writer(writer: &ActiveExportWriter, roll_limit: u64) -> bool {
    roll_limit != u64::MAX && writer.rows > 0 && writer_estimated_output_bytes(writer) >= roll_limit
}

fn writer_estimated_output_bytes(writer: &ActiveExportWriter) -> u64 {
    writer.writer.bytes_written() as u64 + writer.writer.in_progress_size() as u64
}

fn writer_buffered_bytes(writer: &ActiveExportWriter) -> u64 {
    (writer.writer.memory_size() as u64)
        .max(writer.writer.in_progress_size() as u64)
        .max(writer.writer.multipart_buffered_bytes() as u64)
}

fn record_batch_memory_size(batch: &RecordBatch) -> u64 {
    batch.get_array_memory_size() as u64
}

fn elapsed_ms(start: std::time::Instant) -> u64 {
    start.elapsed().as_millis() as u64
}

fn current_time_millis() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

fn export_log(message: impl AsRef<str>) {
    eprintln!("[paimon-native-export] {}", message.as_ref());
}

async fn build_export_reader(
    file: &ExportFile,
    task: &ExportTask,
    batch_size: usize,
    options: &HashMap<String, String>,
) -> Result<(SchemaRef, ExportReader), String> {
    let read_columns = export_read_columns(task);
    let late_filter = export_late_materialization_filter(file, task);
    if file.path.starts_with("obs://") {
        let metrics = Arc::new(ObsReadMetrics::default());
        let (schema, reader, late_materialization) = build_async_file_batch_reader(
            Box::new(ObsObjectChunkReader::new_with_runtime_metrics(
                &file.path,
                options,
                Some(metrics.clone()),
                task.runtime_threads,
                task.read_buffer_size_bytes as usize,
                task.read_concurrency,
                task.obs_request_timeout_ms,
                task.obs_connect_timeout_ms,
            )?),
            batch_size,
            DEFAULT_ROW_INDEX_COLUMN,
            &task.projection,
            &read_columns,
            late_filter,
        )
        .await?;
        Ok((
            schema,
            ExportReader {
                reader,
                obs_metrics: Some(metrics),
                late_materialization,
            },
        ))
    } else {
        let (schema, reader, late_materialization) = build_async_file_batch_reader(
            Box::new(open_local_async(&file.path).await?),
            batch_size,
            DEFAULT_ROW_INDEX_COLUMN,
            &task.projection,
            &read_columns,
            late_filter,
        )
        .await?;
        Ok((
            schema,
            ExportReader {
                reader,
                obs_metrics: None,
                late_materialization,
            },
        ))
    }
}

fn export_late_materialization_filter(
    file: &ExportFile,
    task: &ExportTask,
) -> Option<LateMaterializationFilter> {
    if !file.deleted_positions.is_empty() {
        return None;
    }
    if task
        .projection
        .iter()
        .any(|column| column == DEFAULT_ROW_INDEX_COLUMN)
    {
        return None;
    }
    let mut predicate_columns = Vec::new();
    collect_predicate_fields(&task.predicate, &mut predicate_columns);
    if predicate_columns.is_empty() {
        return None;
    }
    if predicate_columns
        .iter()
        .any(|column| column == DEFAULT_ROW_INDEX_COLUMN)
    {
        return None;
    }
    Some(LateMaterializationFilter {
        predicate: task.predicate.clone(),
        partition: file.partition.clone(),
        predicate_columns,
    })
}

fn export_read_columns(task: &ExportTask) -> Vec<String> {
    let mut columns = Vec::new();
    for column in &task.projection {
        push_unique_column(&mut columns, column);
    }
    collect_predicate_fields(&task.predicate, &mut columns);
    columns
}

fn collect_predicate_fields(predicate: &Value, columns: &mut Vec<String>) {
    if let Some(field) = predicate.get("field").and_then(Value::as_str) {
        push_unique_column(columns, field);
    }
    if let Some(children) = predicate.get("children").and_then(Value::as_array) {
        for child in children {
            collect_predicate_fields(child, columns);
        }
    }
}

fn push_unique_column(columns: &mut Vec<String>, column: &str) {
    if !columns.iter().any(|existing| existing == column) {
        columns.push(column.to_string());
    }
}

fn filter_deleted_positions(
    batch: RecordBatch,
    row_index_column: &str,
    deleted_positions: &HashSet<i64>,
) -> Result<RecordBatch, String> {
    if deleted_positions.is_empty() {
        return Ok(batch);
    }
    let row_indexes = batch
        .column_by_name(row_index_column)
        .ok_or_else(|| format!("native row index column {} is missing", row_index_column))?
        .as_any()
        .downcast_ref::<Int64Array>()
        .ok_or_else(|| format!("native row index column {} is not int64", row_index_column))?;
    let mut values = Vec::with_capacity(batch.num_rows());
    for row in 0..batch.num_rows() {
        values.push(!deleted_positions.contains(&row_indexes.value(row)));
    }
    let mask = BooleanArray::from(values);
    filter_record_batch(&batch, &mask).map_err(|e| e.to_string())
}

fn apply_export_filters(
    batch: RecordBatch,
    file: &ExportFile,
    predicate: &Value,
) -> Result<(RecordBatch, usize), String> {
    let mut values = Vec::with_capacity(batch.num_rows());
    let mut dv_filtered = 0usize;
    for row in 0..batch.num_rows() {
        let row_index = scalar_at(&batch, DEFAULT_ROW_INDEX_COLUMN, row)
            .and_then(|value| match value {
                ScalarValue::I64(v) => Some(v as u64),
                _ => None,
            })
            .unwrap_or(row as u64);
        let keep_dv = !file.deleted_positions.contains(&row_index);
        if !keep_dv {
            dv_filtered += 1;
        }
        values.push(keep_dv && eval_predicate(predicate, &batch, &file.partition, row)?);
    }
    let mask = BooleanArray::from(values);
    filter_record_batch(&batch, &mask)
        .map(|filtered| (filtered, dv_filtered))
        .map_err(|e| e.to_string())
}

fn project_export_batch(
    batch: RecordBatch,
    projection: &[String],
    partition: &HashMap<String, String>,
) -> Result<RecordBatch, String> {
    let mut fields = Vec::new();
    let mut columns = Vec::new();
    for name in projection {
        if let Ok(index) = batch.schema().index_of(name) {
            fields.push(batch.schema().field(index).clone());
            columns.push(batch.column(index).clone());
        } else if let Some(value) = partition.get(name) {
            fields.push(Field::new(name, DataType::Utf8, true));
            columns.push(Arc::new(StringArray::from_iter_values(
                std::iter::repeat(value.as_str()).take(batch.num_rows()),
            )) as ArrayRef);
        } else {
            return Err(format!(
                "PARQUET_SCHEMA_MISMATCH: missing projected field {}",
                name
            ));
        }
    }
    RecordBatch::try_new(Arc::new(Schema::new(fields)), columns).map_err(|e| e.to_string())
}

fn eval_predicate(
    predicate: &Value,
    batch: &RecordBatch,
    partition: &HashMap<String, String>,
    row: usize,
) -> Result<bool, String> {
    let op = predicate
        .get("op")
        .and_then(Value::as_str)
        .unwrap_or("true");
    match op {
        "true" => Ok(true),
        "and" => {
            for child in predicate
                .get("children")
                .and_then(Value::as_array)
                .unwrap_or(&Vec::new())
            {
                if !eval_predicate(child, batch, partition, row)? {
                    return Ok(false);
                }
            }
            Ok(true)
        }
        "or" => {
            for child in predicate
                .get("children")
                .and_then(Value::as_array)
                .unwrap_or(&Vec::new())
            {
                if eval_predicate(child, batch, partition, row)? {
                    return Ok(true);
                }
            }
            Ok(false)
        }
        "is_null" | "is_not_null" => {
            let field = string_field(predicate, "field")?;
            let is_null = matches!(
                field_value(batch, partition, &field, row),
                ScalarValue::Null
            );
            Ok((op == "is_null" && is_null) || (op == "is_not_null" && !is_null))
        }
        "in" => {
            let field = string_field(predicate, "field")?;
            let value = field_value(batch, partition, &field, row);
            for literal in predicate
                .get("literals")
                .and_then(Value::as_array)
                .unwrap_or(&Vec::new())
            {
                if compare_scalar(&value, &literal_value(literal)?)
                    == Some(std::cmp::Ordering::Equal)
                {
                    return Ok(true);
                }
            }
            Ok(false)
        }
        "eq" | "ne" | "lt" | "le" | "gt" | "ge" => {
            let field = string_field(predicate, "field")?;
            let value = field_value(batch, partition, &field, row);
            let literal = literal_value(
                predicate
                    .get("literal")
                    .ok_or_else(|| "predicate missing literal".to_string())?,
            )?;
            let ordering = compare_scalar(&value, &literal);
            Ok(match op {
                "eq" => ordering == Some(std::cmp::Ordering::Equal),
                "ne" => ordering != Some(std::cmp::Ordering::Equal),
                "lt" => ordering == Some(std::cmp::Ordering::Less),
                "le" => matches!(
                    ordering,
                    Some(std::cmp::Ordering::Less) | Some(std::cmp::Ordering::Equal)
                ),
                "gt" => ordering == Some(std::cmp::Ordering::Greater),
                "ge" => matches!(
                    ordering,
                    Some(std::cmp::Ordering::Greater) | Some(std::cmp::Ordering::Equal)
                ),
                _ => false,
            })
        }
        other => Err(format!("UNSUPPORTED_PREDICATE: {}", other)),
    }
}

fn field_value(
    batch: &RecordBatch,
    partition: &HashMap<String, String>,
    field: &str,
    row: usize,
) -> ScalarValue {
    if let Some(value) = scalar_at(batch, field, row) {
        value
    } else {
        partition
            .get(field)
            .map(|value| ScalarValue::Utf8(value.clone()))
            .unwrap_or(ScalarValue::Null)
    }
}

fn scalar_at(batch: &RecordBatch, field: &str, row: usize) -> Option<ScalarValue> {
    let index = batch.schema().index_of(field).ok()?;
    let column = batch.column(index);
    if column.is_null(row) {
        return Some(ScalarValue::Null);
    }
    if let Some(array) = column.as_any().downcast_ref::<BooleanArray>() {
        return Some(ScalarValue::Bool(array.value(row)));
    }
    if let Some(array) = column.as_any().downcast_ref::<Int8Array>() {
        return Some(ScalarValue::I64(array.value(row) as i64));
    }
    if let Some(array) = column.as_any().downcast_ref::<Int16Array>() {
        return Some(ScalarValue::I64(array.value(row) as i64));
    }
    if let Some(array) = column.as_any().downcast_ref::<Int32Array>() {
        return Some(ScalarValue::I64(array.value(row) as i64));
    }
    if let Some(array) = column.as_any().downcast_ref::<Int64Array>() {
        return Some(ScalarValue::I64(array.value(row)));
    }
    if let Some(array) = column.as_any().downcast_ref::<Float32Array>() {
        return Some(ScalarValue::F64(array.value(row) as f64));
    }
    if let Some(array) = column.as_any().downcast_ref::<Float64Array>() {
        return Some(ScalarValue::F64(array.value(row)));
    }
    if let Some(array) = column.as_any().downcast_ref::<StringArray>() {
        return Some(ScalarValue::Utf8(array.value(row).to_string()));
    }
    None
}

fn literal_value(value: &Value) -> Result<ScalarValue, String> {
    let value = value
        .get("value")
        .ok_or_else(|| "literal missing value".to_string())?;
    if value.is_null() {
        return Ok(ScalarValue::Null);
    }
    if let Some(v) = value.as_bool() {
        return Ok(ScalarValue::Bool(v));
    }
    if let Some(v) = value.as_i64() {
        return Ok(ScalarValue::I64(v));
    }
    if let Some(v) = value.as_f64() {
        return Ok(ScalarValue::F64(v));
    }
    if let Some(v) = value.as_str() {
        return Ok(ScalarValue::Utf8(v.to_string()));
    }
    Err("unsupported literal value".to_string())
}

fn compare_scalar(left: &ScalarValue, right: &ScalarValue) -> Option<std::cmp::Ordering> {
    match (left, right) {
        (ScalarValue::Null, _) | (_, ScalarValue::Null) => None,
        (ScalarValue::Bool(a), ScalarValue::Bool(b)) => Some(a.cmp(b)),
        (ScalarValue::I64(a), ScalarValue::I64(b)) => Some(a.cmp(b)),
        (ScalarValue::I64(a), ScalarValue::F64(b)) => (*a as f64).partial_cmp(b),
        (ScalarValue::F64(a), ScalarValue::I64(b)) => a.partial_cmp(&(*b as f64)),
        (ScalarValue::F64(a), ScalarValue::F64(b)) => a.partial_cmp(b),
        (ScalarValue::Utf8(a), ScalarValue::Utf8(b)) => Some(a.cmp(b)),
        (ScalarValue::Utf8(a), ScalarValue::I64(b)) => a.parse::<i64>().ok().map(|v| v.cmp(b)),
        (ScalarValue::I64(a), ScalarValue::Utf8(b)) => b.parse::<i64>().ok().map(|v| a.cmp(&v)),
        _ => None,
    }
}

fn output_part_path(output_path: &str, ordinal: usize) -> Result<String, String> {
    let file_name = format!(
        "part-native-{}-{}-{}.parquet",
        std::process::id(),
        ordinal,
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_err(|e| e.to_string())?
            .as_nanos()
    );
    Ok(format!(
        "{}/{}",
        output_path.trim_end_matches('/'),
        file_name
    ))
}

fn local_output_path(path: &str) -> Result<String, String> {
    if path.starts_with("obs://") {
        let mut local = std::env::temp_dir();
        local.push(path.rsplit('/').next().unwrap_or("part-native.parquet"));
        return Ok(local.to_string_lossy().to_string());
    }
    Ok(path
        .strip_prefix("file://")
        .or_else(|| path.strip_prefix("file:"))
        .unwrap_or(path)
        .to_string())
}

#[cfg(test)]
fn multipart_part_ranges(
    file_size: u64,
    multipart_part_size_bytes: u64,
) -> Result<Vec<MultipartPartRange>, String> {
    if multipart_part_size_bytes == 0 {
        return Err("multipart part size must be positive".to_string());
    }
    let mut ranges = Vec::new();
    let mut offset = 0u64;
    let mut part_number = 1i32;
    while offset < file_size {
        if part_number > 10000 {
            return Err("multipart upload would exceed 10000 parts".to_string());
        }
        let remaining = file_size - offset;
        let length = remaining.min(multipart_part_size_bytes);
        ranges.push(MultipartPartRange {
            part_number,
            offset,
            length: usize::try_from(length)
                .map_err(|_| "multipart part size exceeds usize".to_string())?,
        });
        offset += length;
        part_number += 1;
    }
    Ok(ranges)
}

#[unsafe(no_mangle)]
pub extern "C" fn paimon_exporter_new() -> *mut Exporter {
    match catch_unwind(|| Box::into_raw(Box::new(Exporter))) {
        Ok(exporter) => exporter,
        Err(_) => ptr::null_mut(),
    }
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn paimon_exporter_free(exporter: *mut Exporter) {
    let _ = catch_unwind(AssertUnwindSafe(|| {
        if !exporter.is_null() {
            drop(Box::from_raw(exporter));
        }
    }));
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn paimon_exporter_export_parquet(
    exporter: *mut Exporter,
    request_json: *const c_char,
    result_json: *mut *mut c_char,
    error_message: *mut *mut c_char,
) -> i32 {
    paimon_exporter_export_parquet_inner(
        exporter,
        ptr::null(),
        request_json,
        None,
        result_json,
        error_message,
    )
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn paimon_exporter_export_parquet_with_diagnostics(
    exporter: *mut Exporter,
    operation_id: *const c_char,
    request_json: *const c_char,
    diagnostics_callback: NativeDiagnosticsCallback,
    result_json: *mut *mut c_char,
    error_message: *mut *mut c_char,
) -> i32 {
    paimon_exporter_export_parquet_inner(
        exporter,
        operation_id,
        request_json,
        diagnostics_callback,
        result_json,
        error_message,
    )
}

unsafe fn paimon_exporter_export_parquet_inner(
    exporter: *mut Exporter,
    operation_id: *const c_char,
    request_json: *const c_char,
    diagnostics_callback: NativeDiagnosticsCallback,
    result_json: *mut *mut c_char,
    error_message: *mut *mut c_char,
) -> i32 {
    if exporter.is_null() || result_json.is_null() || error_message.is_null() {
        return -1;
    }
    match catch_unwind(AssertUnwindSafe(|| {
        let operation_id = if operation_id.is_null() {
            "native-export-parquet-unknown".to_string()
        } else {
            cstr_to_string(operation_id)?
        };
        let mut diagnostics = NativeExportDiagnostics::new(operation_id, diagnostics_callback);
        diagnostics.emit(NativeDiagnosticEvent {
            event_type: "FFI_ENTERED",
            phase: Some("PARSE_REQUEST"),
            ..Default::default()
        });
        cstr_to_string(request_json)
            .and_then(|request| export_parquet_with_diagnostics(&request, &mut diagnostics))
    })) {
        Ok(Ok(result)) => {
            *result_json = CString::new(result).unwrap().into_raw();
            0
        }
        Ok(Err(error)) => {
            *error_message = CString::new(error.replace('\0', "\\0")).unwrap().into_raw();
            -1
        }
        Err(payload) => {
            *error_message = CString::new(format!("native panic: {}", panic_message(payload)))
                .unwrap()
                .into_raw();
            -1
        }
    }
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn paimon_string_free(value: *mut c_char) {
    let _ = catch_unwind(AssertUnwindSafe(|| {
        if !value.is_null() {
            drop(CString::from_raw(value));
        }
    }));
}

#[cfg(test)]
mod tests {
    use super::*;
    use parquet::arrow::ArrowWriter;
    use parquet::file::properties::WriterProperties;
    use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};

    static TEMP_FILE_SEQUENCE: AtomicU64 = AtomicU64::new(0);
    static CAPTURED_DIAGNOSTIC_EVENTS: OnceLock<Mutex<Vec<Value>>> = OnceLock::new();

    fn temp_parquet_path(name: &str) -> String {
        let mut path = std::env::temp_dir();
        path.push(format!(
            "paimon-native-io-{}-{}-{}-{}.parquet",
            name,
            std::process::id(),
            TEMP_FILE_SEQUENCE.fetch_add(1, Ordering::Relaxed),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        path.to_string_lossy().to_string()
    }

    fn write_i64_parquet(values: Vec<i64>) -> String {
        write_i64_parquet_columns(vec![("id", values)])
    }

    fn write_i64_parquet_columns(columns: Vec<(&str, Vec<i64>)>) -> String {
        let path = temp_parquet_path("i64");
        let schema = Arc::new(Schema::new(
            columns
                .iter()
                .map(|(name, _)| Field::new(*name, DataType::Int64, false))
                .collect::<Vec<_>>(),
        ));
        let arrays = columns
            .into_iter()
            .map(|(_, values)| Arc::new(Int64Array::from_iter_values(values)) as ArrayRef)
            .collect();
        let batch = RecordBatch::try_new(schema.clone(), arrays).unwrap();
        let file = File::create(&path).unwrap();
        let mut writer = ArrowWriter::try_new(file, schema, None).unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();
        path
    }

    fn write_i64_parquet_with_row_group_size(values: Vec<i64>, row_group_size: usize) -> String {
        let path = temp_parquet_path("row-groups");
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
        let properties = WriterProperties::builder()
            .set_max_row_group_size(row_group_size)
            .build();
        let file = File::create(&path).unwrap();
        let mut writer = ArrowWriter::try_new(file, schema.clone(), Some(properties)).unwrap();
        let mut offset = 0;
        while offset < values.len() {
            let end = (offset + row_group_size).min(values.len());
            let batch = RecordBatch::try_new(
                schema.clone(),
                vec![Arc::new(Int64Array::from_iter_values(
                    values[offset..end].iter().copied(),
                )) as ArrayRef],
            )
            .unwrap();
            writer.write(&batch).unwrap();
            writer.flush().unwrap();
            offset = end;
        }
        writer.close().unwrap();
        path
    }

    fn write_string_parquet_column(name: &str, values: Vec<String>) -> String {
        let path = temp_parquet_path("string");
        let schema = Arc::new(Schema::new(vec![Field::new(name, DataType::Utf8, false)]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(StringArray::from(values)) as ArrayRef],
        )
        .unwrap();
        let file = File::create(&path).unwrap();
        let mut writer = ArrowWriter::try_new(file, schema, None).unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();
        path
    }

    unsafe extern "C" fn capture_diagnostic_event(event_json: *const c_char) {
        if event_json.is_null() {
            return;
        }
        let event = unsafe { CStr::from_ptr(event_json) }
            .to_str()
            .unwrap()
            .to_string();
        let event: Value = serde_json::from_str(&event).unwrap();
        CAPTURED_DIAGNOSTIC_EVENTS
            .get_or_init(|| Mutex::new(Vec::new()))
            .lock()
            .unwrap()
            .push(event);
    }

    fn captured_diagnostic_events() -> Vec<Value> {
        CAPTURED_DIAGNOSTIC_EVENTS
            .get_or_init(|| Mutex::new(Vec::new()))
            .lock()
            .unwrap()
            .clone()
    }

    #[test]
    fn concurrent_range_fetch_respects_limit_and_preserves_order() {
        let runtime = global_runtime_with_threads(4).unwrap();
        let active = Arc::new(AtomicUsize::new(0));
        let max_active = Arc::new(AtomicUsize::new(0));
        let ranges = (0..8).map(|value| value..value + 1).collect::<Vec<_>>();

        let results = runtime
            .block_on(fetch_byte_ranges_concurrently(ranges, 3, {
                let active = Arc::clone(&active);
                let max_active = Arc::clone(&max_active);
                move |range| {
                    let active = Arc::clone(&active);
                    let max_active = Arc::clone(&max_active);
                    async move {
                        let current = active.fetch_add(1, Ordering::SeqCst) + 1;
                        max_active.fetch_max(current, Ordering::SeqCst);
                        tokio::time::sleep(Duration::from_millis(20)).await;
                        active.fetch_sub(1, Ordering::SeqCst);
                        Ok(Bytes::from(vec![range.start as u8]))
                    }
                }
            }))
            .unwrap();

        assert_eq!(max_active.load(Ordering::SeqCst), 3);
        assert_eq!(
            results.iter().map(|bytes| bytes[0]).collect::<Vec<_>>(),
            vec![0, 1, 2, 3, 4, 5, 6, 7]
        );
    }

    #[test]
    fn coalesced_range_fetch_merges_ranges_and_preserves_order() {
        let runtime = global_runtime_with_threads(4).unwrap();
        let source = Bytes::from((0u8..64u8).collect::<Vec<_>>());
        let fetched_ranges = Arc::new(Mutex::new(Vec::new()));
        let active = Arc::new(AtomicUsize::new(0));
        let max_active = Arc::new(AtomicUsize::new(0));
        let ranges = vec![10..12, 0..4, 4..8, 30..32, 8..10, 33..35];

        let results = runtime
            .block_on(fetch_byte_ranges_coalesced(ranges, 2, 1, 16, {
                let source = source.clone();
                let fetched_ranges = Arc::clone(&fetched_ranges);
                let active = Arc::clone(&active);
                let max_active = Arc::clone(&max_active);
                move |range| {
                    let source = source.clone();
                    let fetched_ranges = Arc::clone(&fetched_ranges);
                    let active = Arc::clone(&active);
                    let max_active = Arc::clone(&max_active);
                    async move {
                        let current = active.fetch_add(1, Ordering::SeqCst) + 1;
                        max_active.fetch_max(current, Ordering::SeqCst);
                        tokio::time::sleep(Duration::from_millis(20)).await;
                        active.fetch_sub(1, Ordering::SeqCst);
                        fetched_ranges.lock().unwrap().push(range.clone());
                        Ok(source.slice(range.start as usize..range.end as usize))
                    }
                }
            }))
            .unwrap();

        assert_eq!(
            results
                .iter()
                .map(|bytes| bytes.iter().copied().collect::<Vec<_>>())
                .collect::<Vec<_>>(),
            vec![
                vec![10, 11],
                vec![0, 1, 2, 3],
                vec![4, 5, 6, 7],
                vec![30, 31],
                vec![8, 9],
                vec![33, 34]
            ]
        );
        let mut fetched_ranges = fetched_ranges.lock().unwrap().clone();
        fetched_ranges.sort_by_key(|range| (range.start, range.end));
        assert_eq!(fetched_ranges, vec![0..12, 30..35]);
        assert_eq!(max_active.load(Ordering::SeqCst), 2);
    }

    #[test]
    fn file_batch_reader_reduces_batch_rows_for_wide_projection() {
        let row_count = 1024;
        let column_count = 512;
        let columns = (0..column_count)
            .map(|column| {
                (
                    format!("c{}", column),
                    (0..row_count).map(|row| row as i64).collect::<Vec<_>>(),
                )
            })
            .collect::<Vec<_>>();
        let parquet_columns = columns
            .iter()
            .map(|(name, values)| (name.as_str(), values.clone()))
            .collect::<Vec<_>>();
        let input = write_i64_parquet_columns(parquet_columns);

        let (_, mut file_reader) =
            build_file_batch_reader(open_local(&input).unwrap(), 1024, "__row_index", None)
                .unwrap();
        let batch = file_reader.reader.next().unwrap().unwrap();
        assert_eq!(batch.num_rows(), 512);

        let _ = std::fs::remove_file(input);
    }

    #[test]
    fn native_export_writes_local_parquet_with_predicate_and_partition() {
        let input = write_i64_parquet_columns(vec![("id", vec![1, 2, 3])]);
        let mut output_dir = std::env::temp_dir();
        output_dir.push(format!(
            "paimon-native-export-{}-{}",
            std::process::id(),
            TEMP_FILE_SEQUENCE.fetch_add(1, Ordering::Relaxed)
        ));
        let request = serde_json::json!({
            "request_version": 1,
            "output_path": output_dir.to_string_lossy(),
            "compression": "zstd",
            "writer_batch_size": 2,
            "projection": ["id", "dt"],
            "predicate_format": "paimon-json-v1",
            "predicate_json": serde_json::json!({
                "op": "and",
                "children": [
                    {"op": "ge", "field": "id", "literal": {"type": "BIGINT", "value": 2}},
                    {"op": "eq", "field": "dt", "literal": {"type": "VARCHAR", "value": "2026-05-06"}}
                ]
            }).to_string(),
            "object_store": {},
            "files": [{
                "path": input,
                "row_count": 3,
                "file_size": 0,
                "schema_id": 0,
                "partition": {"dt": "2026-05-06"},
                "positions": [2]
            }]
        });

        let result = export_parquet(&request.to_string()).unwrap();
        let result: Value = serde_json::from_str(&result).unwrap();
        assert_eq!(result["rows_output"].as_u64(), Some(1));
        let output_file = result["files_written"][0]["path"].as_str().unwrap();

        let (schema, mut file_reader) =
            build_file_batch_reader(open_local(output_file).unwrap(), 8, "__row_index", None)
                .unwrap();
        assert!(schema.field_with_name("dt").is_ok());
        let batch = file_reader.reader.next().unwrap().unwrap();
        let id = batch
            .column(batch.schema().index_of("id").unwrap())
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        let dt = batch
            .column(batch.schema().index_of("dt").unwrap())
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(id.value(0), 2);
        assert_eq!(dt.value(0), "2026-05-06");

        let _ = std::fs::remove_file(input);
        let _ = std::fs::remove_dir_all(output_dir);
    }

    #[test]
    fn native_export_reports_row_group_and_batch_read_progress() {
        let input = write_i64_parquet_with_row_group_size(vec![1, 2, 3, 4], 2);
        let mut output_dir = std::env::temp_dir();
        output_dir.push(format!(
            "paimon-native-export-diagnostics-{}-{}",
            std::process::id(),
            TEMP_FILE_SEQUENCE.fetch_add(1, Ordering::Relaxed)
        ));
        let request = serde_json::json!({
            "request_version": 1,
            "output_path": output_dir.to_string_lossy(),
            "compression": "zstd",
            "writer_batch_size": 1,
            "projection": ["id"],
            "predicate_format": "paimon-json-v1",
            "predicate_json": serde_json::json!({"op":"true"}).to_string(),
            "object_store": {},
            "files": [{
                "path": input,
                "row_count": 4,
                "file_size": 0,
                "schema_id": 0,
                "partition": {},
                "positions": []
            }]
        });
        let mut diagnostics = NativeExportDiagnostics::new(
            "test-export-progress".to_string(),
            Some(capture_diagnostic_event),
        );

        let result: Value = serde_json::from_str(
            &export_parquet_with_diagnostics(&request.to_string(), &mut diagnostics).unwrap(),
        )
        .unwrap();
        assert_eq!(result["rows_output"].as_u64(), Some(4));

        let events = captured_diagnostic_events();
        let row_group_progress = events
            .iter()
            .filter(|event| {
                event["operation_id"] == "test-export-progress"
                    && event["event_type"] == "PHASE_PROGRESS"
                    && event["phase"] == "READ"
                    && event["object_operation"] == "read_row_group"
            })
            .count();
        let row_group_end = events
            .iter()
            .filter(|event| {
                event["operation_id"] == "test-export-progress"
                    && event["event_type"] == "PHASE_END"
                    && event["phase"] == "READ"
                    && event["object_operation"] == "read_row_group"
            })
            .count();
        let batch_end = events
            .iter()
            .filter(|event| {
                event["operation_id"] == "test-export-progress"
                    && event["event_type"] == "PHASE_END"
                    && event["phase"] == "READ"
                    && event["object_operation"] == "read_next_batch"
            })
            .count();

        assert_eq!(row_group_progress, 2);
        assert_eq!(row_group_end, 2);
        assert_eq!(batch_end, 4);

        let _ = std::fs::remove_file(input);
        let _ = std::fs::remove_dir_all(output_dir);
    }

    #[test]
    fn native_export_uses_late_materialization_for_predicate_only_column() {
        let input = write_i64_parquet_columns(vec![
            ("id", vec![1, 2, 3, 4]),
            ("score", vec![10, 20, 30, 40]),
            ("payload", vec![100, 200, 300, 400]),
        ]);
        let mut output_dir = std::env::temp_dir();
        output_dir.push(format!(
            "paimon-native-export-late-materialization-{}-{}",
            std::process::id(),
            TEMP_FILE_SEQUENCE.fetch_add(1, Ordering::Relaxed)
        ));
        let request = serde_json::json!({
            "request_version": 1,
            "output_path": output_dir.to_string_lossy(),
            "compression": "zstd",
            "writer_batch_size": 2,
            "projection": ["id", "payload"],
            "predicate_format": "paimon-json-v1",
            "predicate_json": serde_json::json!({
                "op": "ge",
                "field": "score",
                "literal": {"type": "BIGINT", "value": 30}
            }).to_string(),
            "object_store": {},
            "files": [{
                "path": input,
                "row_count": 4,
                "file_size": 0,
                "schema_id": 0,
                "partition": {},
                "positions": []
            }]
        });
        let mut diagnostics = NativeExportDiagnostics::new(
            "test-late-materialization".to_string(),
            Some(capture_diagnostic_event),
        );

        let result: Value = serde_json::from_str(
            &export_parquet_with_diagnostics(&request.to_string(), &mut diagnostics).unwrap(),
        )
        .unwrap();
        assert_eq!(result["rows_output"].as_u64(), Some(2));

        let events = captured_diagnostic_events();
        assert!(events.iter().any(|event| {
            event["operation_id"] == "test-late-materialization"
                && event["event_type"] == "READER_READY"
                && event["metrics_json"]
                    .as_str()
                    .and_then(|json| serde_json::from_str::<Value>(json).ok())
                    .and_then(|metrics| metrics["late_materialization"].as_bool())
                    == Some(true)
        }));

        let output_file = result["files_written"][0]["path"].as_str().unwrap();
        let (schema, mut file_reader) =
            build_file_batch_reader(open_local(output_file).unwrap(), 8, "__row_index", None)
                .unwrap();
        assert!(schema.field_with_name("score").is_err());
        let batch = file_reader.reader.next().unwrap().unwrap();
        let id = batch
            .column(batch.schema().index_of("id").unwrap())
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        let payload = batch
            .column(batch.schema().index_of("payload").unwrap())
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(id.values(), &[3, 4]);
        assert_eq!(payload.values(), &[300, 400]);

        let _ = std::fs::remove_file(input);
        let _ = std::fs::remove_dir_all(output_dir);
    }

    #[test]
    fn native_export_disables_late_materialization_when_deletion_vector_exists() {
        let input = write_i64_parquet_columns(vec![
            ("id", vec![1, 2, 3, 4]),
            ("score", vec![10, 20, 30, 40]),
        ]);
        let mut output_dir = std::env::temp_dir();
        output_dir.push(format!(
            "paimon-native-export-late-materialization-dv-{}-{}",
            std::process::id(),
            TEMP_FILE_SEQUENCE.fetch_add(1, Ordering::Relaxed)
        ));
        let request = serde_json::json!({
            "request_version": 1,
            "output_path": output_dir.to_string_lossy(),
            "compression": "zstd",
            "writer_batch_size": 2,
            "projection": ["id"],
            "predicate_format": "paimon-json-v1",
            "predicate_json": serde_json::json!({
                "op": "ge",
                "field": "score",
                "literal": {"type": "BIGINT", "value": 20}
            }).to_string(),
            "object_store": {},
            "files": [{
                "path": input,
                "row_count": 4,
                "file_size": 0,
                "schema_id": 0,
                "partition": {},
                "positions": [2]
            }]
        });
        let mut diagnostics = NativeExportDiagnostics::new(
            "test-late-materialization-dv".to_string(),
            Some(capture_diagnostic_event),
        );

        let result: Value = serde_json::from_str(
            &export_parquet_with_diagnostics(&request.to_string(), &mut diagnostics).unwrap(),
        )
        .unwrap();
        assert_eq!(result["rows_output"].as_u64(), Some(2));

        let events = captured_diagnostic_events();
        assert!(events.iter().any(|event| {
            event["operation_id"] == "test-late-materialization-dv"
                && event["event_type"] == "READER_READY"
                && event["metrics_json"]
                    .as_str()
                    .and_then(|json| serde_json::from_str::<Value>(json).ok())
                    .and_then(|metrics| metrics["late_materialization"].as_bool())
                    == Some(false)
        }));

        let output_file = result["files_written"][0]["path"].as_str().unwrap();
        let (_, mut file_reader) =
            build_file_batch_reader(open_local(output_file).unwrap(), 8, "__row_index", None)
                .unwrap();
        let batch = file_reader.reader.next().unwrap().unwrap();
        let id = batch
            .column(batch.schema().index_of("id").unwrap())
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(id.values(), &[2, 4]);

        let _ = std::fs::remove_file(input);
        let _ = std::fs::remove_dir_all(output_dir);
    }

    #[test]
    fn native_export_disables_late_materialization_for_row_index_predicate() {
        let input = write_i64_parquet_columns(vec![("id", vec![1, 2, 3, 4])]);
        let mut output_dir = std::env::temp_dir();
        output_dir.push(format!(
            "paimon-native-export-late-materialization-row-index-{}-{}",
            std::process::id(),
            TEMP_FILE_SEQUENCE.fetch_add(1, Ordering::Relaxed)
        ));
        let request = serde_json::json!({
            "request_version": 1,
            "output_path": output_dir.to_string_lossy(),
            "compression": "zstd",
            "writer_batch_size": 2,
            "projection": ["id"],
            "predicate_format": "paimon-json-v1",
            "predicate_json": serde_json::json!({
                "op": "ge",
                "field": DEFAULT_ROW_INDEX_COLUMN,
                "literal": {"type": "BIGINT", "value": 2}
            }).to_string(),
            "object_store": {},
            "files": [{
                "path": input,
                "row_count": 4,
                "file_size": 0,
                "schema_id": 0,
                "partition": {},
                "positions": []
            }]
        });
        let mut diagnostics = NativeExportDiagnostics::new(
            "test-late-materialization-row-index".to_string(),
            Some(capture_diagnostic_event),
        );

        let result: Value = serde_json::from_str(
            &export_parquet_with_diagnostics(&request.to_string(), &mut diagnostics).unwrap(),
        )
        .unwrap();
        assert_eq!(result["rows_output"].as_u64(), Some(2));

        let events = captured_diagnostic_events();
        assert!(events.iter().any(|event| {
            event["operation_id"] == "test-late-materialization-row-index"
                && event["event_type"] == "READER_READY"
                && event["metrics_json"]
                    .as_str()
                    .and_then(|json| serde_json::from_str::<Value>(json).ok())
                    .and_then(|metrics| metrics["late_materialization"].as_bool())
                    == Some(false)
        }));

        let output_file = result["files_written"][0]["path"].as_str().unwrap();
        let (_, mut file_reader) =
            build_file_batch_reader(open_local(output_file).unwrap(), 8, "__row_index", None)
                .unwrap();
        let batch = file_reader.reader.next().unwrap().unwrap();
        let id = batch
            .column(batch.schema().index_of("id").unwrap())
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(id.values(), &[3, 4]);

        let _ = std::fs::remove_file(input);
        let _ = std::fs::remove_dir_all(output_dir);
    }

    #[test]
    fn native_export_local_output_does_not_report_obs_write_metrics() {
        let input = write_i64_parquet_columns(vec![("id", vec![1, 2, 3])]);
        let mut output_dir = std::env::temp_dir();
        output_dir.push(format!(
            "paimon-native-export-local-metrics-{}-{}",
            std::process::id(),
            TEMP_FILE_SEQUENCE.fetch_add(1, Ordering::Relaxed)
        ));
        let request = serde_json::json!({
            "request_version": 1,
            "output_path": output_dir.to_string_lossy(),
            "compression": "zstd",
            "writer_batch_size": 3,
            "projection": ["id"],
            "predicate_format": "paimon-json-v1",
            "predicate_json": serde_json::json!({"op":"true"}).to_string(),
            "object_store": {},
            "files": [{
                "path": input,
                "row_count": 3,
                "file_size": 0,
                "schema_id": 0,
                "partition": {},
                "positions": []
            }]
        });

        let result: Value =
            serde_json::from_str(&export_parquet(&request.to_string()).unwrap()).unwrap();
        assert_eq!(result["rows_output"].as_u64(), Some(3));
        assert_eq!(result["metrics"]["obs_write_requests"].as_u64(), Some(0));
        assert_eq!(result["metrics"]["obs_write_bytes"].as_u64(), Some(0));

        let _ = std::fs::remove_file(input);
        let _ = std::fs::remove_dir_all(output_dir);
    }

    #[test]
    fn multipart_part_ranges_cover_file_in_order() {
        assert_eq!(
            multipart_part_ranges(10, 4).unwrap(),
            vec![
                MultipartPartRange {
                    part_number: 1,
                    offset: 0,
                    length: 4
                },
                MultipartPartRange {
                    part_number: 2,
                    offset: 4,
                    length: 4
                },
                MultipartPartRange {
                    part_number: 3,
                    offset: 8,
                    length: 2
                }
            ]
        );
        assert!(multipart_part_ranges(0, 4).unwrap().is_empty());
        assert!(multipart_part_ranges(10, 0).is_err());
    }

    #[test]
    fn parse_export_task_preserves_native_tuning_options() {
        let request = serde_json::json!({
            "request_version": 1,
            "output_path": "obs://bucket/out",
            "compression": "zstd",
            "target_file_size_bytes": 536870912u64,
            "read_buffer_size_bytes": 16 * 1024 * 1024u64,
            "read_concurrency": 8,
            "obs_request_timeout_ms": 45000u64,
            "obs_connect_timeout_ms": 12000u64,
            "writer_batch_size": 4096,
            "writer_row_group_size": 131072,
            "multipart_part_size_bytes": 32 * 1024 * 1024u64,
            "memory_limit_bytes": 256 * 1024 * 1024u64,
            "runtime_threads": 6,
            "metadata_cache_enabled": false,
            "projection": ["id"],
            "predicate_format": "paimon-json-v1",
            "predicate_json": serde_json::json!({"op":"true"}).to_string(),
            "object_store": {},
            "files": []
        });

        let task = parse_export_task(&request.to_string()).unwrap();
        assert_eq!(task.read_buffer_size_bytes, 16 * 1024 * 1024);
        assert_eq!(task.read_concurrency, 8);
        assert_eq!(task.obs_request_timeout_ms, 45000);
        assert_eq!(task.obs_connect_timeout_ms, 12000);
        assert_eq!(task.writer_batch_size, 4096);
        assert_eq!(task.writer_row_group_size, 131072);
        assert_eq!(task.multipart_part_size_bytes, 32 * 1024 * 1024);
        assert_eq!(task.memory_limit_bytes, 256 * 1024 * 1024);
        assert_eq!(task.runtime_threads, 6);
        assert!(!task.metadata_cache_enabled);
    }

    #[test]
    fn native_export_rejects_memory_limit_smaller_than_multipart_part_size() {
        let request = serde_json::json!({
            "request_version": 1,
            "output_path": "obs://bucket/out",
            "compression": "zstd",
            "multipart_part_size_bytes": 128 * 1024 * 1024u64,
            "memory_limit_bytes": 64 * 1024 * 1024u64,
            "projection": ["id"],
            "predicate_format": "paimon-json-v1",
            "predicate_json": serde_json::json!({"op":"true"}).to_string(),
            "object_store": {},
            "files": []
        });

        let error = export_parquet(&request.to_string()).unwrap_err();
        assert!(error.contains("memory_limit_bytes"));
        assert!(error.contains("multipart_part_size_bytes"));
    }

    #[test]
    fn native_export_rejects_obs_output_path_with_query_or_fragment() {
        let base_request = serde_json::json!({
            "request_version": 1,
            "output_path": "obs://bucket/out",
            "compression": "zstd",
            "projection": ["id"],
            "predicate_format": "paimon-json-v1",
            "predicate_json": serde_json::json!({"op":"true"}).to_string(),
            "object_store": {},
            "files": []
        });

        let mut query_request = base_request.clone();
        query_request["output_path"] = serde_json::json!("obs://bucket/out?version=1");
        let query_error = export_parquet(&query_request.to_string()).unwrap_err();
        assert!(query_error.contains("query"));

        let mut fragment_request = base_request;
        fragment_request["output_path"] = serde_json::json!("obs://bucket/out#fragment");
        let fragment_error = export_parquet(&fragment_request.to_string()).unwrap_err();
        assert!(fragment_error.contains("fragment"));
    }

    #[test]
    fn multipart_inflight_part_cap_respects_runtime_threads_and_memory_limit() {
        assert_eq!(
            multipart_max_inflight_parts(8, 128 * 1024 * 1024, 64 * 1024 * 1024),
            2
        );
        assert_eq!(
            multipart_max_inflight_parts(2, 512 * 1024 * 1024, 64 * 1024 * 1024),
            2
        );
        assert_eq!(
            multipart_max_inflight_parts(8, 32 * 1024 * 1024, 64 * 1024 * 1024),
            1
        );
        assert_eq!(
            multipart_max_inflight_parts(0, 128 * 1024 * 1024, 64 * 1024 * 1024),
            1
        );
    }

    #[test]
    fn buffered_range_reads_reuse_cached_obs_ranges() {
        let data = Bytes::from((0..200u8).collect::<Vec<_>>());
        let mut cache = None;
        let mut fetches = Vec::new();

        let first =
            read_buffered_range(&mut cache, data.len() as u64, 32, 4, 8, |start, length| {
                fetches.push((start, length));
                Ok(data.slice(start as usize..start as usize + length))
            })
            .unwrap();
        assert_eq!(&first[..], &data[4..12]);
        assert_eq!(fetches, vec![(4, 32)]);

        let second =
            read_buffered_range(&mut cache, data.len() as u64, 32, 12, 8, |start, length| {
                fetches.push((start, length));
                Ok(data.slice(start as usize..start as usize + length))
            })
            .unwrap();
        assert_eq!(&second[..], &data[12..20]);
        assert_eq!(fetches, vec![(4, 32)]);

        let third =
            read_buffered_range(&mut cache, data.len() as u64, 32, 40, 4, |start, length| {
                fetches.push((start, length));
                Ok(data.slice(start as usize..start as usize + length))
            })
            .unwrap();
        assert_eq!(&third[..], &data[40..44]);
        assert_eq!(fetches, vec![(4, 32), (40, 32)]);
    }

    #[test]
    fn streaming_multipart_buffer_emits_full_parts_before_finish() {
        let mut buffer = MultipartWriteBuffer::new(5).unwrap();
        let mut parts = Vec::new();

        buffer
            .write_bytes(b"abc", |part| {
                parts.push(part);
                Ok(())
            })
            .unwrap();
        assert!(parts.is_empty());
        assert_eq!(buffer.buffered_len(), 3);

        buffer
            .write_bytes(b"defghijk", |part| {
                parts.push(part);
                Ok(())
            })
            .unwrap();
        assert_eq!(parts, vec![b"abcde".to_vec(), b"fghij".to_vec()]);
        assert_eq!(buffer.buffered_len(), 1);

        buffer
            .finish(|part| {
                parts.push(part);
                Ok(())
            })
            .unwrap();
        assert_eq!(
            parts,
            vec![b"abcde".to_vec(), b"fghij".to_vec(), b"k".to_vec()]
        );
    }

    #[test]
    fn native_export_rejects_invalid_multipart_part_size() {
        let request = serde_json::json!({
            "request_version": 1,
            "output_path": "obs://bucket/out",
            "compression": "zstd",
            "multipart_part_size_bytes": 1,
            "projection": ["id"],
            "predicate_format": "paimon-json-v1",
            "predicate_json": serde_json::json!({"op":"true"}).to_string(),
            "object_store": {},
            "files": []
        });

        let error = export_parquet(&request.to_string()).unwrap_err();
        assert!(error.contains("multipart_part_size_bytes must be at least"));
    }

    #[test]
    fn native_export_rolls_output_files_by_target_size() {
        let input = write_i64_parquet_columns(vec![("id", (0..20).collect())]);
        let mut output_dir = std::env::temp_dir();
        output_dir.push(format!(
            "paimon-native-export-roll-{}-{}",
            std::process::id(),
            TEMP_FILE_SEQUENCE.fetch_add(1, Ordering::Relaxed)
        ));
        let request = serde_json::json!({
            "request_version": 1,
            "output_path": output_dir.to_string_lossy(),
            "compression": "zstd",
            "target_file_size_bytes": 64,
            "writer_batch_size": 20,
            "writer_row_group_size": 4,
            "memory_limit_bytes": 64 * 1024 * 1024u64,
            "projection": ["id"],
            "predicate_format": "paimon-json-v1",
            "predicate_json": serde_json::json!({"op":"true"}).to_string(),
            "object_store": {},
            "files": [{
                "path": input,
                "row_count": 20,
                "file_size": 0,
                "schema_id": 0,
                "partition": {},
                "positions": []
            }]
        });

        let result = export_parquet(&request.to_string()).unwrap();
        let result: Value = serde_json::from_str(&result).unwrap();
        assert_eq!(result["rows_output"].as_u64(), Some(20));
        assert!(
            result["files_written"].as_array().unwrap().len() > 1,
            "expected target size rolling to create multiple files: {}",
            result
        );
        assert!(result["metrics"]["writer_rolls"].as_u64().unwrap() > 0);
        assert!(result["metrics"]["peak_buffered_bytes"].as_u64().unwrap() > 0);

        let _ = std::fs::remove_file(input);
        let _ = std::fs::remove_dir_all(output_dir);
    }

    #[test]
    fn native_export_rolling_uses_encoded_parquet_size_not_arrow_memory() {
        let input = write_string_parquet_column(
            "payload",
            (0..2000)
                .map(|_| "compressible-payload-value".repeat(8))
                .collect(),
        );
        let mut output_dir = std::env::temp_dir();
        output_dir.push(format!(
            "paimon-native-export-encoded-roll-{}-{}",
            std::process::id(),
            TEMP_FILE_SEQUENCE.fetch_add(1, Ordering::Relaxed)
        ));
        let request = serde_json::json!({
            "request_version": 1,
            "output_path": output_dir.to_string_lossy(),
            "compression": "zstd",
            "target_file_size_bytes": 8192,
            "writer_batch_size": 2000,
            "writer_row_group_size": 2000,
            "memory_limit_bytes": 64 * 1024 * 1024u64,
            "projection": ["payload"],
            "predicate_format": "paimon-json-v1",
            "predicate_json": serde_json::json!({"op":"true"}).to_string(),
            "object_store": {},
            "files": [{
                "path": input,
                "row_count": 2000,
                "file_size": 0,
                "schema_id": 0,
                "partition": {},
                "positions": []
            }]
        });

        let result = export_parquet(&request.to_string()).unwrap();
        let result: Value = serde_json::from_str(&result).unwrap();
        assert_eq!(result["rows_output"].as_u64(), Some(2000));
        let files = result["files_written"].as_array().unwrap();
        assert!(
            files.len() <= 3,
            "expected compressed-size rolling to avoid many tiny files: {}",
            result
        );

        let _ = std::fs::remove_file(input);
        let _ = std::fs::remove_dir_all(output_dir);
    }

    #[test]
    fn native_export_read_columns_include_predicate_fields() {
        let task = ExportTask {
            output_path: "/tmp/out".to_string(),
            compression: "zstd".to_string(),
            target_file_size_bytes: u64::MAX,
            read_buffer_size_bytes: 8 * 1024 * 1024,
            read_concurrency: 4,
            obs_request_timeout_ms: DEFAULT_OBS_REQUEST_TIMEOUT_MS,
            obs_connect_timeout_ms: DEFAULT_OBS_CONNECT_TIMEOUT_MS,
            writer_batch_size: 1024,
            writer_row_group_size: 1024,
            multipart_part_size_bytes: 64 * 1024 * 1024,
            memory_limit_bytes: 1024 * 1024,
            runtime_threads: DEFAULT_RUNTIME_THREADS,
            metadata_cache_enabled: true,
            projection: vec!["id".to_string(), "dt".to_string()],
            predicate_format: "paimon-json-v1".to_string(),
            predicate: serde_json::json!({
                "op": "and",
                "children": [
                    {"op": "gt", "field": "score", "literal": {"type": "BIGINT", "value": 10}},
                    {"op": "eq", "field": "dt", "literal": {"type": "VARCHAR", "value": "2026-05-06"}}
                ]
            }),
            object_store_options: HashMap::new(),
            files: Vec::new(),
        };

        assert_eq!(
            export_read_columns(&task),
            vec!["id".to_string(), "dt".to_string(), "score".to_string()]
        );
    }

    #[test]
    fn native_export_writer_carries_configured_runtime_threads() {
        let mut output_dir = std::env::temp_dir();
        output_dir.push(format!(
            "paimon-native-export-runtime-{}-{}",
            std::process::id(),
            TEMP_FILE_SEQUENCE.fetch_add(1, Ordering::Relaxed)
        ));
        let task = ExportTask {
            output_path: output_dir.to_string_lossy().to_string(),
            compression: "zstd".to_string(),
            target_file_size_bytes: u64::MAX,
            read_buffer_size_bytes: 8 * 1024 * 1024,
            read_concurrency: 4,
            obs_request_timeout_ms: 45_000,
            obs_connect_timeout_ms: 12_000,
            writer_batch_size: 1024,
            writer_row_group_size: 1024,
            multipart_part_size_bytes: 64 * 1024 * 1024,
            memory_limit_bytes: 1024 * 1024,
            runtime_threads: 7,
            metadata_cache_enabled: true,
            projection: vec!["id".to_string()],
            predicate_format: "paimon-json-v1".to_string(),
            predicate: serde_json::json!({"op":"true"}),
            object_store_options: HashMap::new(),
            files: Vec::new(),
        };
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));

        let writer = create_export_writer(&task, schema, 0).unwrap();

        assert_eq!(writer.runtime_threads, 7);
        assert_eq!(writer.obs_request_timeout_ms, 45_000);
        assert_eq!(writer.obs_connect_timeout_ms, 12_000);
        if let Some(local_output) = writer.local_output {
            let _ = std::fs::remove_file(local_output);
        }
        let _ = std::fs::remove_dir_all(output_dir);
    }

    #[test]
    fn native_export_can_filter_on_non_output_column_after_projection_pushdown() {
        let input =
            write_i64_parquet_columns(vec![("id", vec![1, 2, 3]), ("score", vec![5, 20, 7])]);
        let mut output_dir = std::env::temp_dir();
        output_dir.push(format!(
            "paimon-native-export-filter-only-{}-{}",
            std::process::id(),
            TEMP_FILE_SEQUENCE.fetch_add(1, Ordering::Relaxed)
        ));
        let request = serde_json::json!({
            "request_version": 1,
            "output_path": output_dir.to_string_lossy(),
            "compression": "zstd",
            "writer_batch_size": 4,
            "projection": ["id"],
            "predicate_format": "paimon-json-v1",
            "predicate_json": serde_json::json!({
                "op": "gt",
                "field": "score",
                "literal": {"type": "BIGINT", "value": 10}
            }).to_string(),
            "object_store": {},
            "files": [{
                "path": input,
                "row_count": 3,
                "file_size": 0,
                "schema_id": 0,
                "partition": {},
                "positions": []
            }]
        });

        let result: Value =
            serde_json::from_str(&export_parquet(&request.to_string()).unwrap()).unwrap();
        assert_eq!(result["rows_output"].as_u64(), Some(1));
        let output_file = result["files_written"][0]["path"].as_str().unwrap();
        let (schema, mut file_reader) =
            build_file_batch_reader(open_local(output_file).unwrap(), 8, "__row_index", None)
                .unwrap();
        assert!(schema.field_with_name("score").is_err());
        let batch = file_reader.reader.next().unwrap().unwrap();
        let id = batch
            .column(batch.schema().index_of("id").unwrap())
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(id.value(0), 2);

        let _ = std::fs::remove_file(input);
        let _ = std::fs::remove_dir_all(output_dir);
    }

    #[test]
    fn creates_inclusive_http_range_header() {
        assert_eq!(range_header(0, 1), "bytes=0-0");
        assert_eq!(range_header(7, 3), "bytes=7-9");
    }

    #[test]
    fn parses_obs_location() {
        let location = ObsObjectLocation::parse("obs://bucket-a/path/to/file.parquet").unwrap();
        assert_eq!(location.bucket, "bucket-a");
        assert_eq!(location.key, "path/to/file.parquet");
    }

    #[test]
    fn parses_obs_prefix_with_empty_key() {
        let (bucket, prefix) = parse_obs_prefix("obs://bucket-a/").unwrap();
        assert_eq!(bucket, "bucket-a");
        assert_eq!(prefix, "");
    }

    #[test]
    fn rejects_obs_prefix_with_query_or_fragment() {
        let query_error = super::parse_obs_prefix("obs://bucket-a/export?version=1").unwrap_err();
        assert!(query_error.contains("query"));

        let fragment_error = super::parse_obs_prefix("obs://bucket-a/export#fragment").unwrap_err();
        assert!(fragment_error.contains("fragment"));
    }

    #[test]
    fn obs_option_prefers_explicit_value_over_environment() {
        let mut options = HashMap::new();
        options.insert(
            "fs.obs.endpoint".to_string(),
            "configured-endpoint".to_string(),
        );

        let endpoint = option_or_env_with(&options, OBS_ENDPOINT_OPTIONS, OBS_ENDPOINT_ENV, |_| {
            Some("env-endpoint".to_string())
        })
        .unwrap();

        assert_eq!(endpoint, "configured-endpoint");
    }

    #[test]
    fn obs_option_uses_environment_fallback() {
        let options = HashMap::new();

        let access_key = option_or_env_with(
            &options,
            OBS_ACCESS_KEY_OPTIONS,
            OBS_ACCESS_KEY_ENV,
            |key| {
                if key == "OBS_ACCESS_KEY_ID" {
                    Some("env-ak".to_string())
                } else {
                    None
                }
            },
        )
        .unwrap();

        assert_eq!(access_key, "env-ak");
    }

    #[test]
    fn obs_option_ignores_empty_config_and_environment_values() {
        let mut options = HashMap::new();
        options.insert("fs.obs.security.token".to_string(), String::new());

        let security_token = option_or_env_with(
            &options,
            OBS_SECURITY_TOKEN_OPTIONS,
            OBS_SECURITY_TOKEN_ENV,
            |_| Some(String::new()),
        );

        assert!(security_token.is_none());
    }

    #[test]
    fn obs_option_ignores_blank_config_and_environment_values() {
        let mut options = HashMap::new();
        options.insert("fs.obs.endpoint".to_string(), "   ".to_string());

        let endpoint =
            option_or_env_with(&options, OBS_ENDPOINT_OPTIONS, OBS_ENDPOINT_ENV, |key| {
                if key == "OBS_ENDPOINT" {
                    Some("obs-endpoint".to_string())
                } else {
                    None
                }
            });

        assert_eq!(endpoint.as_deref(), Some("obs-endpoint"));

        let secret_key = option_or_env_with(
            &HashMap::new(),
            OBS_SECRET_KEY_OPTIONS,
            OBS_SECRET_KEY_ENV,
            |_| Some(" \t ".to_string()),
        );

        assert!(secret_key.is_none());
    }

    #[test]
    fn runtime_worker_threads_uses_default_for_missing_or_invalid_value() {
        assert_eq!(
            runtime_worker_threads_with(|_| None),
            DEFAULT_RUNTIME_THREADS
        );
        assert_eq!(
            runtime_worker_threads_with(|_| Some("0".to_string())),
            DEFAULT_RUNTIME_THREADS
        );
        assert_eq!(
            runtime_worker_threads_with(|_| Some("not-a-number".to_string())),
            DEFAULT_RUNTIME_THREADS
        );
    }

    #[test]
    fn runtime_worker_threads_uses_positive_configured_value() {
        assert_eq!(
            runtime_worker_threads_with(|key| {
                if key == "PAIMON_NATIVE_IO_WORKER_THREADS" {
                    Some("8".to_string())
                } else {
                    None
                }
            }),
            8
        );
        assert_eq!(
            runtime_worker_threads_with(|key| {
                if key == "PAIMON_NATIVE_IO_RUNTIME_THREADS" {
                    Some("6".to_string())
                } else {
                    None
                }
            }),
            6
        );
        assert_eq!(
            runtime_worker_threads_with(|key| {
                if key == "PAIMON_NATIVE_IO_WORKER_THREADS" {
                    Some("8".to_string())
                } else if key == "PAIMON_NATIVE_IO_RUNTIME_THREADS" {
                    Some("6".to_string())
                } else {
                    None
                }
            }),
            8
        );
    }

    fn parse_obs_prefix(path: &str) -> Result<(String, String), String> {
        let url = Url::parse(path).map_err(|e| e.to_string())?;
        if url.scheme() != "obs" {
            return Err(format!(
                "unsupported path scheme for OBS prefix: {}",
                url.scheme()
            ));
        }
        let bucket = url
            .host_str()
            .ok_or_else(|| "OBS path missing bucket".to_string())?;
        Ok((
            bucket.to_string(),
            url.path().trim_start_matches('/').to_string(),
        ))
    }

    fn first_parquet_under_configured_obs_path(path: &str) -> Result<String, String> {
        if path.ends_with(".parquet") {
            return Ok(path.to_string());
        }

        let (bucket, key) = parse_obs_prefix(path)?;
        let prefix = if key.is_empty() || key.ends_with('/') {
            key
        } else {
            format!("{}/", key)
        };
        let client = build_obs_client(&HashMap::new())?;
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .enable_all()
            .build()
            .map_err(|e| e.to_string())?;
        let max_scan_pages = std::env::var("PAIMON_NATIVE_IO_OBS_SCAN_PAGES")
            .ok()
            .and_then(|value| value.parse::<usize>().ok())
            .unwrap_or(20);

        let (object_key, scanned_objects) = runtime.block_on(async {
            let mut continuation_token = None;
            let mut scanned_objects = 0;
            for _ in 0..max_scan_pages {
                let mut request = client
                    .list_objects_v2()
                    .bucket(&bucket)
                    .prefix(&prefix)
                    .max_keys(1000);
                if let Some(token) = continuation_token.as_deref() {
                    request = request.continuation_token(token);
                }

                let result = request.send().await.map_err(|e| e.to_string())?;
                scanned_objects += result.contents().len();
                if let Some(object) = result
                    .contents()
                    .iter()
                    .find(|object| object.key().ends_with(".parquet") && object.size() > 0)
                {
                    return Ok::<_, String>((Some(object.key().to_string()), scanned_objects));
                }

                if !result.is_truncated() {
                    break;
                }
                continuation_token = result.next_continuation_token().map(str::to_string);
                if continuation_token.is_none() {
                    break;
                }
            }
            Ok((None, scanned_objects))
        })?;
        let object_key = object_key.ok_or_else(|| {
            format!(
                "no parquet object found under OBS path {} after scanning {} objects in up to {} pages",
                path, scanned_objects, max_scan_pages
            )
        })?;

        Ok(format!("obs://{}/{}", bucket, object_key))
    }

    #[test]
    #[ignore = "requires OBS credentials and PAIMON_NATIVE_IO_OBS_PATH or default test prefix"]
    fn reads_first_parquet_from_configured_obs_prefix() {
        let path = std::env::var("PAIMON_NATIVE_IO_OBS_PATH")
            .unwrap_or_else(|_| "obs://bigdata-datalens/warehouse".to_string());
        let file = first_parquet_under_configured_obs_path(&path).unwrap();

        let mut config = ReaderConfig::new();
        config.files.push(file);
        config.batch_size = 16;

        let (schema, file_readers) = build_file_readers(&config).unwrap();
        let mut reader = Reader {
            schema,
            file_readers,
            current_file: 0,
            row_index_column: config.row_index_column,
            last_error: None,
        };

        let batch = reader.next_batch().unwrap().unwrap();
        assert!(batch.num_rows() > 0);
    }

    #[test]
    #[ignore = "requires OBS credentials with write access and PAIMON_NATIVE_IO_OBS_WRITE_TEST_URI"]
    fn uploads_and_reads_parquet_from_obs() {
        let test_prefix = std::env::var("PAIMON_NATIVE_IO_OBS_WRITE_TEST_URI")
            .unwrap_or_else(|_| "obs://bigdata-datalens/paimon-native-io-test/".to_string());
        let (bucket, prefix) = parse_obs_prefix(&test_prefix).unwrap();
        let key = format!(
            "{}native-read-{}-{}.parquet",
            if prefix.is_empty() || prefix.ends_with('/') {
                prefix
            } else {
                format!("{}/", prefix)
            },
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        );
        let obs_path = format!("obs://{}/{}", bucket, key);

        let local_path = write_i64_parquet(vec![11, 22, 33]);
        let body = std::fs::read(&local_path).unwrap();
        std::fs::remove_file(&local_path).unwrap();

        let client = build_obs_client(&HashMap::new()).unwrap();
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .enable_all()
            .build()
            .unwrap();

        runtime
            .block_on(async {
                client
                    .put_object()
                    .bucket(&bucket)
                    .key(&key)
                    .body(body)
                    .content_type("application/octet-stream")
                    .send()
                    .await
                    .map_err(|e| e.to_string())
            })
            .unwrap();

        let read_result = (|| {
            let mut config = ReaderConfig::new();
            config.files.push(obs_path);
            config.batch_size = 2;

            let (schema, file_readers) = build_file_readers(&config)?;
            let mut reader = Reader {
                schema,
                file_readers,
                current_file: 0,
                row_index_column: config.row_index_column,
                last_error: None,
            };
            let batch = reader
                .next_batch()?
                .ok_or_else(|| "empty OBS parquet".to_string())?;
            Ok::<_, String>(batch.num_rows())
        })();

        let delete_result = runtime.block_on(async {
            client
                .delete_object()
                .bucket(&bucket)
                .key(&key)
                .send()
                .await
                .map_err(|e| e.to_string())
        });
        delete_result.unwrap();

        assert_eq!(read_result.unwrap(), 2);
    }

    #[test]
    fn adds_row_index_to_arrow_schema() {
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
        let indexed = schema_with_row_index(schema, "__row_index");
        assert_eq!(indexed.fields().len(), 2);
        assert_eq!(indexed.field(1).name(), "__row_index");
        assert_eq!(indexed.field(1).data_type(), &DataType::Int64);
        assert!(!indexed.field(1).is_nullable());
    }

    #[test]
    fn rejects_obs_paths_with_query_or_fragment() {
        let query_error =
            ObsObjectLocation::parse("obs://bucket/warehouse/table/data.parquet?version=1")
                .unwrap_err();
        assert!(query_error.contains("query"));

        let fragment_error =
            ObsObjectLocation::parse("obs://bucket/warehouse/table/data.parquet#fragment")
                .unwrap_err();
        assert!(fragment_error.contains("fragment"));
    }

    #[test]
    fn reads_local_parquet_batches_with_monotonic_row_index() {
        let path = write_i64_parquet(vec![10, 20, 30]);
        let mut config = ReaderConfig::new();
        config.files.push(path.clone());
        config.batch_size = 2;
        config.row_index_column = "__row_index".to_string();

        let (schema, file_readers) = build_file_readers(&config).unwrap();
        assert_eq!(schema.field(0).name(), "id");
        assert_eq!(schema.field(1).name(), "__row_index");

        let mut reader = Reader {
            schema,
            file_readers,
            current_file: 0,
            row_index_column: config.row_index_column,
            last_error: None,
        };

        let first = reader.next_batch().unwrap().unwrap();
        assert_eq!(first.num_rows(), 2);
        let first_index = first
            .column(1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(first_index.value(0), 0);
        assert_eq!(first_index.value(1), 1);

        let second = reader.next_batch().unwrap().unwrap();
        assert_eq!(second.num_rows(), 1);
        let second_index = second
            .column(1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(second_index.value(0), 2);
        assert!(reader.next_batch().unwrap().is_none());

        std::fs::remove_file(path).unwrap();
    }

    #[test]
    fn filters_reader_batches_with_deleted_positions() {
        let path = write_i64_parquet(vec![10, 20, 30, 40]);
        let mut config = ReaderConfig::new();
        config.files.push(path.clone());
        config.batch_size = 4;
        config.row_index_column = "__row_index".to_string();
        config
            .deleted_positions
            .entry(path.clone())
            .or_default()
            .extend([1_i64, 3_i64]);

        let (schema, file_readers) = build_file_readers(&config).unwrap();
        let mut reader = Reader {
            schema,
            file_readers,
            current_file: 0,
            row_index_column: config.row_index_column,
            last_error: None,
        };

        let batch = reader.next_batch().unwrap().unwrap();
        assert_eq!(batch.num_rows(), 2);
        let values = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        let indexes = batch
            .column(1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(values.value(0), 10);
        assert_eq!(indexes.value(0), 0);
        assert_eq!(values.value(1), 30);
        assert_eq!(indexes.value(1), 2);
        assert!(reader.next_batch().unwrap().is_none());

        std::fs::remove_file(path).unwrap();
    }

    #[test]
    fn projects_local_parquet_columns() {
        let path = write_i64_parquet_columns(vec![("id", vec![10, 20]), ("score", vec![100, 200])]);
        let mut config = ReaderConfig::new();
        config.files.push(path.clone());
        config.batch_size = 2;
        config.row_index_column = "__row_index".to_string();
        config.target_columns = Some(vec!["score".to_string()]);

        let (schema, file_readers) = build_file_readers(&config).unwrap();
        assert_eq!(schema.fields().len(), 2);
        assert_eq!(schema.field(0).name(), "score");
        assert_eq!(schema.field(1).name(), "__row_index");

        let mut reader = Reader {
            schema,
            file_readers,
            current_file: 0,
            row_index_column: config.row_index_column,
            last_error: None,
        };
        let batch = reader.next_batch().unwrap().unwrap();
        assert_eq!(batch.schema().field(0).name(), "score");
        assert_eq!(
            batch
                .column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .value(0),
            100
        );

        std::fs::remove_file(path).unwrap();
    }

    #[test]
    fn supports_explicit_empty_projection_with_row_index() {
        let path = write_i64_parquet(vec![10, 20]);
        let mut config = ReaderConfig::new();
        config.files.push(path.clone());
        config.batch_size = 2;
        config.row_index_column = "__row_index".to_string();
        config.target_columns = Some(Vec::new());

        let (schema, file_readers) = build_file_readers(&config).unwrap();
        assert_eq!(schema.fields().len(), 1);
        assert_eq!(schema.field(0).name(), "__row_index");

        let mut reader = Reader {
            schema,
            file_readers,
            current_file: 0,
            row_index_column: config.row_index_column,
            last_error: None,
        };
        let batch = reader.next_batch().unwrap().unwrap();
        assert_eq!(batch.num_columns(), 1);
        assert_eq!(batch.schema().field(0).name(), "__row_index");
        assert_eq!(
            batch
                .column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .value(1),
            1
        );

        std::fs::remove_file(path).unwrap();
    }

    #[test]
    fn rejects_multiple_files_with_different_schema() {
        let first = write_i64_parquet_columns(vec![("id", vec![10])]);
        let second = write_i64_parquet_columns(vec![("id", vec![10]), ("score", vec![100])]);
        let mut config = ReaderConfig::new();
        config.files.push(first.clone());
        config.files.push(second.clone());
        config.batch_size = 2;

        let error = match build_file_readers(&config) {
            Ok(_) => panic!("expected schema mismatch"),
            Err(error) => error,
        };
        assert!(error.contains("file schema mismatch"));

        std::fs::remove_file(first).unwrap();
        std::fs::remove_file(second).unwrap();
    }

    #[test]
    fn ffi_rejects_null_arrow_output_addresses() {
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
        let reader = Box::into_raw(Box::new(Reader {
            schema,
            file_readers: Vec::new(),
            current_file: 0,
            row_index_column: "__row_index".to_string(),
            last_error: None,
        }));

        unsafe {
            assert_eq!(paimon_reader_get_schema(reader, 0), -1);
            let error = CStr::from_ptr(paimon_reader_last_error(reader))
                .to_str()
                .unwrap();
            assert!(error.contains("null ArrowSchema address"));

            assert_eq!(paimon_reader_next_record_batch_blocked(reader, 0), -1);
            let error = CStr::from_ptr(paimon_reader_last_error(reader))
                .to_str()
                .unwrap();
            assert!(error.contains("null ArrowArray address"));

            paimon_reader_free(reader);
        }
    }

    #[test]
    fn ffi_exposes_config_error_detail() {
        unsafe {
            let config = paimon_reader_config_new();
            assert!(!config.is_null());
            assert_eq!(paimon_reader_config_set_batch_size(config, 0), -1);
            let error = CStr::from_ptr(paimon_reader_config_last_error(config))
                .to_str()
                .unwrap();
            assert!(error.contains("batch size must be positive"));
            paimon_reader_config_free(config);
        }
    }

    #[test]
    fn ffi_accepts_deleted_position_batches() {
        unsafe {
            let config = paimon_reader_config_new();
            assert!(!config.is_null());
            let file = CString::new("file:/tmp/data.parquet").unwrap();
            let positions = [1_i64, 3_i64, 5_i64];

            assert_eq!(
                paimon_reader_config_add_deleted_positions(
                    config,
                    file.as_ptr(),
                    positions.as_ptr(),
                    positions.len() as i32,
                ),
                0
            );
            let deleted_positions = (*config)
                .deleted_positions
                .get("file:/tmp/data.parquet")
                .unwrap();
            assert_eq!(deleted_positions.len(), 3);
            assert!(deleted_positions.contains(&1));
            assert!(deleted_positions.contains(&3));
            assert!(deleted_positions.contains(&5));

            let negative = [-1_i64];
            assert_eq!(
                paimon_reader_config_add_deleted_positions(
                    config,
                    file.as_ptr(),
                    negative.as_ptr(),
                    negative.len() as i32,
                ),
                -1
            );
            let error = CStr::from_ptr(paimon_reader_config_last_error(config))
                .to_str()
                .unwrap();
            assert!(error.contains("deleted position must be non-negative"));

            assert_eq!(
                paimon_reader_config_add_deleted_positions(config, file.as_ptr(), ptr::null(), 1),
                -1
            );
            let error = CStr::from_ptr(paimon_reader_config_last_error(config))
                .to_str()
                .unwrap();
            assert!(error.contains("deleted positions pointer must not be null"));

            paimon_reader_config_free(config);
        }
    }

    #[test]
    fn ffi_rejects_empty_required_config_strings() {
        unsafe {
            let empty = CString::new("").unwrap();
            let value = CString::new("value").unwrap();
            let config = paimon_reader_config_new();
            assert!(!config.is_null());

            assert_eq!(paimon_reader_config_add_file(config, empty.as_ptr()), -1);
            let error = CStr::from_ptr(paimon_reader_config_last_error(config))
                .to_str()
                .unwrap();
            assert!(error.contains("file path must be non-empty"));

            assert_eq!(
                paimon_reader_config_set_row_index_column(config, empty.as_ptr()),
                -1
            );
            let error = CStr::from_ptr(paimon_reader_config_last_error(config))
                .to_str()
                .unwrap();
            assert!(error.contains("row index column must be non-empty"));

            assert_eq!(
                paimon_reader_config_set_object_store_option(
                    config,
                    empty.as_ptr(),
                    value.as_ptr()
                ),
                -1
            );
            let error = CStr::from_ptr(paimon_reader_config_last_error(config))
                .to_str()
                .unwrap();
            assert!(error.contains("object store option key must be non-empty"));

            assert_eq!(
                paimon_reader_config_set_object_store_option(
                    config,
                    value.as_ptr(),
                    empty.as_ptr()
                ),
                -1
            );
            let error = CStr::from_ptr(paimon_reader_config_last_error(config))
                .to_str()
                .unwrap();
            assert!(error.contains("object store option value must be non-empty"));

            paimon_reader_config_free(config);
        }
    }
}
