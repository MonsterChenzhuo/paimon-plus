#!/usr/bin/env bash
################################################################################
#  Licensed to the Apache Software Foundation (ASF) under one
#  or more contributor license agreements.  See the NOTICE file
#  distributed with this work for additional information
#  regarding copyright ownership.  The ASF licenses this file
#  to you under the Apache License, Version 2.0 (the
#  "License"); you may not use this file except in compliance
#  with the License.  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
# limitations under the License.
################################################################################

set -euo pipefail

IMAGE="${PAIMON_NATIVE_IO_DOCKER_IMAGE:-monster830/paimon-plus:spark344-java8}"
DOCKER_PLATFORM="${PAIMON_NATIVE_IO_DOCKER_PLATFORM:-}"

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" >/dev/null && pwd)"
PROJECT_ROOT="$(cd "${SCRIPT_DIR}/../.." >/dev/null && pwd)"
PROJECT_NAME="$(basename "${PROJECT_ROOT}")"
OUTPUT_DIR="${PROJECT_ROOT}/native-io"

copy_jar() {
    local module_dir="$1"
    local jar_name="$2"
    local label="$3"
    local target_dir="${PROJECT_ROOT}/${module_dir}/target"
    local candidates=()

    if [ ! -d "${target_dir}" ]; then
        echo "Missing target directory for ${label}: ${target_dir}" >&2
        exit 1
    fi

    while IFS= read -r -d '' jar; do
        candidates+=("${jar}")
    done < <(
        find "${target_dir}" \
            -maxdepth 1 \
            -type f \
            -name "${jar_name}" \
            ! -name "original-*" \
            ! -name "*-sources.jar" \
            ! -name "*-javadoc.jar" \
            ! -name "*-tests.jar" \
            -print0
    )

    if [ "${#candidates[@]}" -ne 1 ]; then
        echo "Expected exactly one ${label} jar, found ${#candidates[@]}:" >&2
        printf '  %s\n' "${candidates[@]}" >&2
        exit 1
    fi

    cp "${candidates[0]}" "${OUTPUT_DIR}/"
    echo "Copied ${label}: $(basename "${candidates[0]}")"
}

verify_jar_contains() {
    local jar_path="$1"
    local entry="$2"
    local expected="$3"
    local label="$4"

    if ! unzip -p "${jar_path}" "${entry}" | LC_ALL=C grep -a -q "${expected}"; then
        echo "Verification failed for ${label}: ${jar_path}" >&2
        echo "Expected ${entry} to contain: ${expected}" >&2
        echo "This usually means the jar was built from stale classes." >&2
        exit 1
    fi
    echo "Verified ${label}: ${expected}"
}

DOCKER_RUN_ARGS=(--rm --entrypoint /bin/bash)
if [ -n "${DOCKER_PLATFORM}" ]; then
    DOCKER_RUN_ARGS+=(--platform "${DOCKER_PLATFORM}")
fi

docker run "${DOCKER_RUN_ARGS[@]}" \
    -v "${PROJECT_ROOT}:/work/nativeio/${PROJECT_NAME}" \
    -v "${HOME}/.m2:/root/.m2" \
    -w "/work/nativeio/${PROJECT_NAME}" \
    "${IMAGE}" \
    -c 'set -euo pipefail;
        export PATH=/opt/maven/bin:/usr/local/cargo/bin:/opt/spark/bin:/opt/java/openjdk/bin:$PATH;
        mvn -T 1C \
          -pl :paimon-spark-3.4_2.12,:paimon-obs,:paimon-native-io \
          -am \
          -Pfast-build,spark3,native-io \
          -DskipTests \
          -Dscala.binary.version=2.12 \
          clean package'

mkdir -p "${OUTPUT_DIR}"
find "${OUTPUT_DIR}" -maxdepth 1 -type f -name "*.jar" -exec rm -f {} +

copy_jar "paimon-spark/paimon-spark-3.4" "paimon-spark-3.4_2.12-*.jar" "Spark 3.4 connector"
copy_jar "paimon-filesystems/paimon-obs" "paimon-obs-*.jar" "OBS plugin"
copy_jar "paimon-native-io" "paimon-native-io-*.jar" "Native IO"

SPARK_CONNECTOR_JAR="$(find "${OUTPUT_DIR}" -maxdepth 1 -type f -name "paimon-spark-3.4_2.12-*.jar" | head -n 1)"
NATIVE_IO_JAR="$(find "${OUTPUT_DIR}" -maxdepth 1 -type f -name "paimon-native-io-*.jar" | head -n 1)"

verify_jar_contains \
    "${SPARK_CONNECTOR_JAR}" \
    "org/apache/paimon/spark/procedure/ExportParquetProcedure.class" \
    "Native export requested for sys.export_parquet" \
    "Spark connector native export routing"

verify_jar_contains \
    "${SPARK_CONNECTOR_JAR}" \
    "org/apache/paimon/spark/procedure/ExportParquetProcedure.class" \
    "NATIVE_IO_EXPORT_ENABLED" \
    "Spark connector native export option lookup"

verify_jar_contains \
    "${NATIVE_IO_JAR}" \
    "META-INF/services/org.apache.paimon.operation.nativeio.export.NativeExportProviderFactory" \
    "org.apache.paimon.nativeio.export.NativeExportProviderFactoryImpl" \
    "Native export ServiceLoader registration"

echo "Native IO jars are ready under ${OUTPUT_DIR}"
