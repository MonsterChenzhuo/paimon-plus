#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PAIMON_HOME="$(cd "${SCRIPT_DIR}/../.." && pwd)"
NATIVEIO_HOME="$(cd "${PAIMON_HOME}/.." && pwd)"

IMAGE="${PAIMON_NATIVEIO_E2E_IMAGE:-paimon-nativeio-spark358-java8:local}"
SPARK_RUNTIME_IMAGE="${PAIMON_E2E_SPARK_RUNTIME_IMAGE:-apache/spark:3.5.8-scala2.12-java11-python3-ubuntu}"
WAREHOUSE="${PAIMON_E2E_WAREHOUSE:-obs://bigdata-datalens/warehouse}"
ROW_COUNT="${PAIMON_E2E_ROW_COUNT:-10000}"
MAVEN_TGZ="${SCRIPT_DIR}/apache-maven-3.9.9-bin.tar.gz"

require_env() {
  local name="$1"
  if [ -z "${!name:-}" ]; then
    echo "Missing required environment variable: ${name}" >&2
    exit 2
  fi
}

require_env OBS_ACCESS_KEY_ID
require_env OBS_SECRET_ACCESS_KEY
require_env OBS_ENDPOINT

if [ ! -f "${MAVEN_TGZ}" ]; then
  curl -fL --retry 3 --connect-timeout 20 \
    https://archive.apache.org/dist/maven/maven-3/3.9.9/binaries/apache-maven-3.9.9-bin.tar.gz \
    -o "${MAVEN_TGZ}"
fi

docker build \
  --build-arg SPARK_RUNTIME_IMAGE="${SPARK_RUNTIME_IMAGE}" \
  -t "${IMAGE}" \
  -f "${SCRIPT_DIR}/Dockerfile" \
  "${SCRIPT_DIR}"

docker run --rm \
  -e OBS_ACCESS_KEY_ID \
  -e OBS_SECRET_ACCESS_KEY \
  -e OBS_ENDPOINT \
  -e OBS_SECURITY_TOKEN="${OBS_SECURITY_TOKEN:-}" \
  -e PAIMON_E2E_WAREHOUSE="${WAREHOUSE}" \
  -e PAIMON_E2E_ROW_COUNT="${ROW_COUNT}" \
  -e PAIMON_E2E_DATABASE="${PAIMON_E2E_DATABASE:-nativeio_e2e}" \
  -e PAIMON_E2E_TABLE="${PAIMON_E2E_TABLE:-}" \
  -e PAIMON_E2E_BUILD_THREADS="${PAIMON_E2E_BUILD_THREADS:-1C}" \
  -e PAIMON_E2E_DRIVER_MEMORY="${PAIMON_E2E_DRIVER_MEMORY:-2g}" \
  -v "${NATIVEIO_HOME}:/work/nativeio" \
  -v "${HOME}/.m2:/root/.m2" \
  -w /work/nativeio/paimon \
  "${IMAGE}"
