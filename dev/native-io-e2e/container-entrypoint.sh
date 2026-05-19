#!/usr/bin/env bash
set -euo pipefail

PAIMON_HOME="${PAIMON_HOME:-/work/nativeio/paimon}"
NATIVEIO_HOME="${NATIVEIO_HOME:-/work/nativeio}"
WAREHOUSE="${PAIMON_E2E_WAREHOUSE:-obs://bigdata-datalens/warehouse}"
ROW_COUNT="${PAIMON_E2E_ROW_COUNT:-10000}"
DATABASE="${PAIMON_E2E_DATABASE:-nativeio_e2e}"
TABLE="${PAIMON_E2E_TABLE:-dv_pk_$(date +%Y%m%d_%H%M%S)}"
THREADS="${PAIMON_E2E_BUILD_THREADS:-1C}"

require_env() {
  local name="$1"
  if [ -z "${!name:-}" ]; then
    echo "Missing required environment variable: ${name}" >&2
    exit 2
  fi
}

require_file() {
  local path="$1"
  if [ ! -e "${path}" ]; then
    echo "Missing required path: ${path}" >&2
    exit 2
  fi
}

require_env OBS_ACCESS_KEY_ID
require_env OBS_SECRET_ACCESS_KEY
require_env OBS_ENDPOINT
require_file "${PAIMON_HOME}/pom.xml"
require_file "${NATIVEIO_HOME}/obs-rust-sdk/Cargo.toml"

cd "${PAIMON_HOME}"

echo "== Environment =="
java -version
mvn -version
rustc --version
cargo --version
spark-submit --version
echo "warehouse=${WAREHOUSE}"
echo "rows=${ROW_COUNT}"
echo "table=paimon.${DATABASE}.${TABLE}"

echo "== Build Paimon Spark 3.5, OBS plugin, and native IO =="
mvn \
  -T "${THREADS}" \
  -pl :paimon-spark-3.5_2.12,:paimon-obs,:paimon-native-io \
  -am \
  -Pfast-build,spark3,native-io \
  -DskipTests \
  -Dscala.binary.version=2.12 \
  -Dspark.version=3.5.8 \
  -Dpaimon-spark-common.spark.version=3.5.8 \
  package

SPARK_JAR="${PAIMON_HOME}/paimon-spark/paimon-spark-3.5/target/paimon-spark-3.5_2.12-1.4-SNAPSHOT.jar"
OBS_JAR="${PAIMON_HOME}/paimon-filesystems/paimon-obs/target/paimon-obs-1.4-SNAPSHOT.jar"
NATIVE_JAR="${PAIMON_HOME}/paimon-native-io/target/paimon-native-io-1.4-SNAPSHOT.jar"
for jar in "${SPARK_JAR}" "${OBS_JAR}" "${NATIVE_JAR}"; do
  require_file "${jar}"
done

echo "== Verify Linux native library is packaged =="
jar tf "${NATIVE_JAR}" | grep -E 'paimon-native-io/linux/(aarch64|x86_64)/libpaimon_native_io\.so$'

TEMP_DIR="$(mktemp -d /tmp/paimon-nativeio-e2e.XXXXXX)"
trap 'rm -rf "${TEMP_DIR}"' EXIT

SQL_FILE="${TEMP_DIR}/e2e.sql"
cat > "${SQL_FILE}" <<SQL
CREATE DATABASE IF NOT EXISTS paimon.${DATABASE};
DROP TABLE IF EXISTS paimon.${DATABASE}.${TABLE};

CREATE TABLE paimon.${DATABASE}.${TABLE} (
  pt INT,
  pk INT,
  v BIGINT,
  payload STRING
) USING paimon
PARTITIONED BY (pt)
TBLPROPERTIES (
  'bucket' = '4',
  'file.format' = 'parquet',
  'deletion-vectors.enabled' = 'true',
  'primary-key' = 'pt,pk'
);

INSERT INTO paimon.${DATABASE}.${TABLE}
SELECT
  1,
  CAST(id AS INT),
  CAST(id * 10 AS BIGINT),
  CONCAT('seed-', CAST(id AS STRING))
FROM range(${ROW_COUNT});

DELETE FROM paimon.${DATABASE}.${TABLE}
WHERE pt = 1 AND pk < 100;

UPDATE paimon.${DATABASE}.${TABLE}
SET v = v + 1, payload = CONCAT(payload, '-u')
WHERE pt = 1 AND pk >= 100 AND pk < 200;

INSERT INTO paimon.${DATABASE}.${TABLE}
SELECT
  1,
  CAST(id + ${ROW_COUNT} AS INT),
  CAST(id * 10 AS BIGINT),
  CONCAT('new-', CAST(id AS STRING))
FROM range(100);

SELECT CONCAT(
  'PAIMON_NATIVEIO_E2E_RESULT ',
  'count=', CAST(COUNT(*) AS STRING),
  ',sum=', CAST(SUM(v) AS STRING),
  ',min_pk=', CAST(MIN(pk) AS STRING),
  ',max_pk=', CAST(MAX(pk) AS STRING),
  ',updated=', CAST(SUM(CASE WHEN payload LIKE '%-u' THEN 1 ELSE 0 END) AS STRING)
) FROM paimon.${DATABASE}.${TABLE};

DROP TABLE paimon.${DATABASE}.${TABLE};
SQL

SPARK_CONF=(
  --master local[2]
  --driver-memory "${PAIMON_E2E_DRIVER_MEMORY:-2g}"
  --jars "${SPARK_JAR},${OBS_JAR},${NATIVE_JAR}"
  --conf spark.sql.extensions=org.apache.paimon.spark.extensions.PaimonSparkSessionExtensions
  --conf spark.sql.catalog.paimon=org.apache.paimon.spark.SparkCatalog
  --conf spark.sql.catalog.paimon.warehouse="${WAREHOUSE}"
  --conf spark.sql.catalog.paimon.fs.obs.access.key="${OBS_ACCESS_KEY_ID}"
  --conf spark.sql.catalog.paimon.fs.obs.secret.key="${OBS_SECRET_ACCESS_KEY}"
  --conf spark.sql.catalog.paimon.fs.obs.endpoint="${OBS_ENDPOINT}"
  --conf spark.hadoop.fs.obs.access.key="${OBS_ACCESS_KEY_ID}"
  --conf spark.hadoop.fs.obs.secret.key="${OBS_SECRET_ACCESS_KEY}"
  --conf spark.hadoop.fs.obs.endpoint="${OBS_ENDPOINT}"
  --conf spark.driver.userClassPathFirst=true
  --conf spark.executor.userClassPathFirst=true
  --conf spark.paimon.native-io.enabled=true
  --conf spark.paimon.native-io.batch-size=1024
)

if [ -n "${OBS_SECURITY_TOKEN:-}" ]; then
  SPARK_CONF+=(
    --conf spark.sql.catalog.paimon.fs.obs.session.token="${OBS_SECURITY_TOKEN}"
    --conf spark.hadoop.fs.obs.session.token="${OBS_SECURITY_TOKEN}"
  )
fi

echo "== Run Spark SQL E2E =="
SPARK_OUTPUT="${TEMP_DIR}/spark-sql.out"
set +e
"${SPARK_HOME}/bin/spark-sql" "${SPARK_CONF[@]}" -f "${SQL_FILE}" 2>&1 | tee "${SPARK_OUTPUT}"
STATUS=${PIPESTATUS[0]}
set -e
OUTPUT="$(cat "${SPARK_OUTPUT}")"
if [ "${STATUS}" -ne 0 ]; then
  echo "spark-sql failed with status ${STATUS}" >&2
  exit "${STATUS}"
fi

RESULT_LINE="$(printf '%s\n' "${OUTPUT}" | grep 'PAIMON_NATIVEIO_E2E_RESULT' | tail -1 | tr -d '\r')"
EXPECTED_SUM=$((10 * ROW_COUNT * (ROW_COUNT - 1) / 2 + 100))
EXPECTED="PAIMON_NATIVEIO_E2E_RESULT count=${ROW_COUNT},sum=${EXPECTED_SUM},min_pk=100,max_pk=$((ROW_COUNT + 99)),updated=100"

if [[ "${RESULT_LINE}" != *"${EXPECTED}"* ]]; then
  echo "Unexpected E2E result." >&2
  echo "expected: ${EXPECTED}" >&2
  echo "actual:   ${RESULT_LINE}" >&2
  exit 1
fi

echo "PAIMON_NATIVEIO_E2E_OK ${EXPECTED}"
