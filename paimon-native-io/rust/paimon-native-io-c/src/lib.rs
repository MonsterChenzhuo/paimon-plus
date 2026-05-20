use std::collections::{HashMap, HashSet};
use std::ffi::{c_char, CStr, CString};
use std::fs::File;
use std::io::Read;
use std::panic::{catch_unwind, AssertUnwindSafe};
use std::ptr;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, OnceLock};

use arrow_array::ffi::{FFI_ArrowArray, FFI_ArrowSchema};
use arrow_array::{
    Array, ArrayRef, BooleanArray, Float32Array, Float64Array, Int16Array, Int32Array, Int64Array,
    Int8Array, RecordBatch, RecordBatchReader, StringArray, StructArray,
};
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use arrow_select::filter::filter_record_batch;
use bytes::Bytes;
use huaweicloud_sdk_rust_obs::{Client, Config, Credentials};
use parquet::arrow::arrow_reader::{ParquetRecordBatchReader, ParquetRecordBatchReaderBuilder};
use parquet::arrow::ArrowWriter;
use parquet::arrow::ProjectionMask;
use parquet::basic::{Compression, ZstdLevel};
use parquet::errors::{ParquetError, Result as ParquetResult};
use parquet::file::properties::WriterProperties;
use parquet::file::reader::{ChunkReader, Length};
use serde_json::Value;
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

static GLOBAL_RUNTIME: OnceLock<Result<tokio::runtime::Runtime, String>> = OnceLock::new();

#[repr(C)]
pub struct ReaderConfig {
    files: Vec<String>,
    batch_size: usize,
    row_index_column: String,
    target_columns: Option<Vec<String>>,
    object_store_options: HashMap<String, String>,
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
    metrics: Option<Arc<ObsReadMetrics>>,
}

struct ObsObjectRead {
    source: ObsObjectChunkReader,
    position: u64,
}

struct ExportReader {
    reader: FileBatchReader,
    obs_metrics: Option<Arc<ObsReadMetrics>>,
}

impl ReaderConfig {
    fn new() -> Self {
        Self {
            files: Vec::new(),
            batch_size: 4096,
            row_index_column: DEFAULT_ROW_INDEX_COLUMN.to_string(),
            target_columns: None,
            object_store_options: HashMap::new(),
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

            let batch = append_row_index(batch, &self.row_index_column, file_reader.row_offset)?;
            file_reader.row_offset += batch.num_rows() as i64;
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

fn global_runtime() -> Result<&'static tokio::runtime::Runtime, String> {
    match GLOBAL_RUNTIME.get_or_init(|| {
        tokio::runtime::Builder::new_multi_thread()
            .worker_threads(runtime_worker_threads())
            .enable_all()
            .build()
            .map_err(|e| e.to_string())
    }) {
        Ok(runtime) => Ok(runtime),
        Err(error) => Err(error.clone()),
    }
}

fn build_obs_client(options: &HashMap<String, String>) -> Result<Client, String> {
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
        .endpoint(endpoint.clone());

    if endpoint.starts_with("http://") {
        builder = builder.secure(false);
    }

    let config = builder.build().map_err(|e| e.to_string())?;
    Client::from_config(config).map_err(|e| e.to_string())
}

impl ObsObjectChunkReader {
    fn new(path: &str, options: &HashMap<String, String>) -> Result<Self, String> {
        Self::new_with_metrics(path, options, None)
    }

    fn new_with_metrics(
        path: &str,
        options: &HashMap<String, String>,
        metrics: Option<Arc<ObsReadMetrics>>,
    ) -> Result<Self, String> {
        let location = ObsObjectLocation::parse(path)?;
        let client = build_obs_client(options)?;
        let len = global_runtime()?
            .block_on(async {
                client
                    .head_object()
                    .bucket(&location.bucket)
                    .key(&location.key)
                    .send()
                    .await
            })
            .map_err(|e| e.to_string())?
            .content_length()
            .ok_or_else(|| "OBS head_object response missing Content-Length".to_string())?;

        Ok(Self {
            inner: Arc::new(ObsObjectInner {
                client,
                bucket: location.bucket,
                key: location.key,
                len,
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

        let range = range_header(start, length);
        if let Some(metrics) = &self.inner.metrics {
            metrics.requests.fetch_add(1, Ordering::Relaxed);
        }
        let bytes = global_runtime()
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
            .map_err(|e| ParquetError::General(e.to_string()))?
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

fn build_file_batch_reader<R: ChunkReader + 'static>(
    reader: R,
    batch_size: usize,
    row_index_column: &str,
    target_columns: Option<&[String]>,
) -> Result<(SchemaRef, FileBatchReader), String> {
    let mut builder =
        ParquetRecordBatchReaderBuilder::try_new(reader).map_err(|e| e.to_string())?;
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
        let mask = ProjectionMask::columns(parquet_schema, selected_columns);
        builder = builder.with_projection(mask);
    }
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
        let (file_schema, file_reader) = if file.starts_with("obs://") {
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
    writer_batch_size: usize,
    writer_row_group_size: usize,
    memory_limit_bytes: u64,
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
        writer_batch_size: root
            .get("writer_batch_size")
            .and_then(Value::as_u64)
            .unwrap_or(8192) as usize,
        writer_row_group_size: root
            .get("writer_row_group_size")
            .and_then(Value::as_u64)
            .unwrap_or(250000) as usize,
        memory_limit_bytes: root
            .get("memory_limit_bytes")
            .and_then(Value::as_u64)
            .unwrap_or(512 * 1024 * 1024),
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
    local_output: String,
    writer: ArrowWriter<File>,
    rows: u64,
    buffered_bytes: u64,
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
    let task = parse_export_task(request_json)?;
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
    let mut decode_ms = 0u64;
    let mut filter_ms = 0u64;
    let mut encode_ms = 0u64;
    let mut obs_write_ms = 0u64;
    let mut files_written = Vec::new();
    let mut next_output_ordinal = 0usize;

    for (file_ordinal, file) in task.files.iter().enumerate() {
        let (schema, mut export_reader) = build_export_reader(
            file,
            &task,
            task.writer_batch_size,
            &task.object_store_options,
        )?;
        let _ = schema;
        let reader = &mut export_reader.reader;
        let _ = file_ordinal;
        let mut writer: Option<ActiveExportWriter> = None;

        loop {
            let decode_start = std::time::Instant::now();
            let batch = match reader.reader.next() {
                Some(batch) => batch.map_err(|e| e.to_string())?,
                None => break,
            };
            decode_ms += elapsed_ms(decode_start);
            parquet_row_groups_read += 1;
            let batch = append_row_index(batch, DEFAULT_ROW_INDEX_COLUMN, reader.row_offset)?;
            reader.row_offset += batch.num_rows() as i64;
            let input_rows = batch.num_rows();
            rows_read += input_rows as u64;
            let filter_start = std::time::Instant::now();
            let (batch, filtered_by_dv) = apply_export_filters(batch, file, &task.predicate)?;
            filter_ms += elapsed_ms(filter_start);
            dv_filtered_rows += filtered_by_dv as u64;
            predicate_filtered_rows +=
                input_rows.saturating_sub(filtered_by_dv + batch.num_rows()) as u64;
            if batch.num_rows() == 0 {
                continue;
            }
            let projected = project_export_batch(batch, &task.projection, &file.partition)?;
            rows_output += projected.num_rows() as u64;
            write_projected_export_batch(
                projected,
                &task,
                &mut writer,
                &mut next_output_ordinal,
                &mut files_written,
                &mut writer_rolls,
                &mut peak_buffered_bytes,
                &mut encode_ms,
                &mut obs_write_ms,
            )?;
        }

        if let Some(writer) = writer.take() {
            finish_export_writer(
                writer,
                &mut files_written,
                &task.object_store_options,
                &mut encode_ms,
                &mut obs_write_ms,
            )?;
        }
        if let Some(metrics) = export_reader.obs_metrics {
            obs_read_requests += metrics.requests.load(Ordering::Relaxed);
            obs_read_retries += metrics.retries.load(Ordering::Relaxed);
            obs_read_bytes += metrics.bytes.load(Ordering::Relaxed);
        }
    }

    Ok(serde_json::json!({
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
            "obs_write_requests": files_written.len(),
            "obs_write_bytes": files_written.iter().map(|f| f.get("bytes").and_then(Value::as_u64).unwrap_or(0)).sum::<u64>(),
            "read_ms": 0,
            "decode_ms": decode_ms,
            "filter_ms": filter_ms,
            "encode_ms": encode_ms,
            "obs_write_ms": obs_write_ms,
            "multipart_finish_ms": 0
        }
    }).to_string())
}

fn write_projected_export_batch(
    mut batch: RecordBatch,
    task: &ExportTask,
    writer: &mut Option<ActiveExportWriter>,
    next_output_ordinal: &mut usize,
    files_written: &mut Vec<Value>,
    writer_rolls: &mut u64,
    peak_buffered_bytes: &mut u64,
    encode_ms: &mut u64,
    obs_write_ms: &mut u64,
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
        let buffered = writer.as_ref().unwrap().buffered_bytes;
        let batch_bytes = record_batch_memory_size(&batch);
        if roll_limit != u64::MAX && buffered > 0 && buffered + batch_bytes > roll_limit {
            let finished = writer.take().unwrap();
            finish_export_writer(
                finished,
                files_written,
                &task.object_store_options,
                encode_ms,
                obs_write_ms,
            )?;
            *writer_rolls += 1;
            continue;
        }

        if roll_limit != u64::MAX && buffered + batch_bytes > roll_limit && batch.num_rows() > 1 {
            let remaining = roll_limit.saturating_sub(buffered).max(1);
            let rows = batch.num_rows() as u64;
            let to_write = ((rows * remaining) / batch_bytes.max(1)).max(1);
            if to_write < rows {
                let to_write = to_write as usize;
                let head = batch.slice(0, to_write);
                let tail = batch.slice(to_write, batch.num_rows() - to_write);
                write_export_chunk(
                    writer.as_mut().unwrap(),
                    &head,
                    peak_buffered_bytes,
                    encode_ms,
                )?;
                batch = tail;
                if writer.as_ref().unwrap().buffered_bytes >= roll_limit {
                    let finished = writer.take().unwrap();
                    finish_export_writer(
                        finished,
                        files_written,
                        &task.object_store_options,
                        encode_ms,
                        obs_write_ms,
                    )?;
                    *writer_rolls += 1;
                }
                continue;
            }
        }

        write_export_chunk(
            writer.as_mut().unwrap(),
            &batch,
            peak_buffered_bytes,
            encode_ms,
        )?;
        break;
    }
    Ok(())
}

fn create_export_writer(
    task: &ExportTask,
    schema: SchemaRef,
    ordinal: usize,
) -> Result<ActiveExportWriter, String> {
    let output_path = output_part_path(&task.output_path, ordinal)?;
    let local_output = local_output_path(&output_path)?;
    if let Some(parent) = std::path::Path::new(&local_output).parent() {
        std::fs::create_dir_all(parent).map_err(|e| e.to_string())?;
    }
    let writer_file = File::create(&local_output).map_err(|e| e.to_string())?;
    let writer = ArrowWriter::try_new(
        writer_file,
        schema,
        Some(
            WriterProperties::builder()
                .set_max_row_group_size(task.writer_row_group_size.max(1))
                .set_write_batch_size(task.writer_batch_size.max(1))
                .set_compression(Compression::ZSTD(ZstdLevel::default()))
                .build(),
        ),
    )
    .map_err(|e| e.to_string())?;
    Ok(ActiveExportWriter {
        output_path,
        local_output,
        writer,
        rows: 0,
        buffered_bytes: 0,
    })
}

fn write_export_chunk(
    writer: &mut ActiveExportWriter,
    batch: &RecordBatch,
    peak_buffered_bytes: &mut u64,
    encode_ms: &mut u64,
) -> Result<(), String> {
    let encode_start = std::time::Instant::now();
    writer.writer.write(batch).map_err(|e| e.to_string())?;
    *encode_ms += elapsed_ms(encode_start);
    writer.rows += batch.num_rows() as u64;
    writer.buffered_bytes += record_batch_memory_size(batch);
    *peak_buffered_bytes = (*peak_buffered_bytes)
        .max(writer.buffered_bytes)
        .max(writer.writer.memory_size() as u64)
        .max(writer.writer.in_progress_size() as u64);
    Ok(())
}

fn finish_export_writer(
    writer: ActiveExportWriter,
    files_written: &mut Vec<Value>,
    object_store_options: &HashMap<String, String>,
    encode_ms: &mut u64,
    obs_write_ms: &mut u64,
) -> Result<(), String> {
    let ActiveExportWriter {
        output_path,
        local_output,
        writer,
        rows,
        ..
    } = writer;
    let encode_start = std::time::Instant::now();
    writer.close().map_err(|e| e.to_string())?;
    *encode_ms += elapsed_ms(encode_start);
    let bytes = std::fs::metadata(&local_output)
        .map_err(|e| e.to_string())?
        .len();
    if output_path.starts_with("obs://") {
        let obs_start = std::time::Instant::now();
        upload_local_file_to_obs(&local_output, &output_path, object_store_options)?;
        *obs_write_ms += elapsed_ms(obs_start);
        let _ = std::fs::remove_file(&local_output);
    }
    files_written.push(serde_json::json!({
        "path": output_path,
        "rows": rows,
        "bytes": bytes
    }));
    Ok(())
}

fn export_roll_limit(task: &ExportTask) -> u64 {
    task.target_file_size_bytes
        .min(task.memory_limit_bytes.max(1))
}

fn record_batch_memory_size(batch: &RecordBatch) -> u64 {
    batch.get_array_memory_size() as u64
}

fn elapsed_ms(start: std::time::Instant) -> u64 {
    start.elapsed().as_millis() as u64
}

fn build_export_reader(
    file: &ExportFile,
    task: &ExportTask,
    batch_size: usize,
    options: &HashMap<String, String>,
) -> Result<(SchemaRef, ExportReader), String> {
    let read_columns = export_read_columns(task);
    if file.path.starts_with("obs://") {
        let metrics = Arc::new(ObsReadMetrics::default());
        let (schema, reader) = build_file_batch_reader(
            ObsObjectChunkReader::new_with_metrics(&file.path, options, Some(metrics.clone()))?,
            batch_size,
            DEFAULT_ROW_INDEX_COLUMN,
            Some(&read_columns),
        )?;
        Ok((
            schema,
            ExportReader {
                reader,
                obs_metrics: Some(metrics),
            },
        ))
    } else {
        let (schema, reader) = build_file_batch_reader(
            open_local(&file.path)?,
            batch_size,
            DEFAULT_ROW_INDEX_COLUMN,
            Some(&read_columns),
        )?;
        Ok((
            schema,
            ExportReader {
                reader,
                obs_metrics: None,
            },
        ))
    }
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

fn upload_local_file_to_obs(
    local_path: &str,
    output_path: &str,
    options: &HashMap<String, String>,
) -> Result<(), String> {
    let (bucket, key) = parse_obs_prefix(output_path)?;
    let body = std::fs::read(local_path).map_err(|e| e.to_string())?;
    let client = build_obs_client(options)?;
    global_runtime()?.block_on(async {
        client
            .put_object()
            .bucket(&bucket)
            .key(&key)
            .body(body)
            .content_type("application/octet-stream")
            .send()
            .await
            .map_err(|e| e.to_string())
            .map(|_| ())
    })
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
    if exporter.is_null() || result_json.is_null() || error_message.is_null() {
        return -1;
    }
    match catch_unwind(AssertUnwindSafe(|| {
        cstr_to_string(request_json).and_then(|r| export_parquet(&r))
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
    use std::sync::atomic::{AtomicU64, Ordering};

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
            "memory_limit_bytes": 1024,
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
    fn native_export_read_columns_include_predicate_fields() {
        let task = ExportTask {
            output_path: "/tmp/out".to_string(),
            compression: "zstd".to_string(),
            target_file_size_bytes: u64::MAX,
            writer_batch_size: 1024,
            writer_row_group_size: 1024,
            memory_limit_bytes: 1024 * 1024,
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
