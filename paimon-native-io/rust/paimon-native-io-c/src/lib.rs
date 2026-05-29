use std::collections::{HashMap, HashSet};
use std::ffi::{c_char, CStr, CString};
use std::fs::File;
use std::future::Future;
use std::io::Read;
use std::ops::Range;
use std::panic::{catch_unwind, AssertUnwindSafe};
use std::ptr;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, OnceLock};
use std::time::Duration;

use arrow_array::ffi::{FFI_ArrowArray, FFI_ArrowSchema};
use arrow_array::{
    Array, ArrayRef, BooleanArray, Int64Array, RecordBatch, RecordBatchReader, StructArray,
};
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use arrow_select::filter::filter_record_batch;
use bytes::Bytes;
use futures::future::{BoxFuture, FutureExt};
use futures::StreamExt;
use huaweicloud_sdk_rust_obs::{Client, Config, Credentials};
use parquet::arrow::arrow_reader::{
    ArrowReaderOptions, ParquetRecordBatchReader, ParquetRecordBatchReaderBuilder,
};
use parquet::arrow::async_reader::AsyncFileReader;
use parquet::arrow::ProjectionMask;
use parquet::errors::{ParquetError, Result as ParquetResult};
use parquet::file::metadata::{PageIndexPolicy, ParquetMetaData, ParquetMetaDataReader};
use parquet::file::reader::{ChunkReader, Length};
use url::Url;

const DEFAULT_ROW_INDEX_COLUMN: &str = "__paimon_native_row_index";
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
    use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};

    static TEMP_FILE_SEQUENCE: AtomicU64 = AtomicU64::new(0);

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
