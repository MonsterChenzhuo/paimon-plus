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

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" >/dev/null && pwd)"
PROJECT_ROOT="$(cd "${SCRIPT_DIR}/../.." >/dev/null && pwd)"

IMAGE="${PAIMON_NATIVE_IO_CENTOS7_DOCKER_IMAGE:-${PAIMON_NATIVE_IO_DOCKER_IMAGE:-paimon-nativeio-centos7:local}}"
PLATFORM="${PAIMON_NATIVE_IO_DOCKER_PLATFORM:-linux/amd64}"
BUILD_IMAGE="${PAIMON_NATIVE_IO_BUILD_IMAGE:-1}"
if [ -n "${PAIMON_NATIVE_IO_SPARK_TGZ:-}" ] || [ -n "${PAIMON_NATIVE_IO_SPARK_DIR:-}" ]; then
    INSTALL_SPARK="${PAIMON_NATIVE_IO_INSTALL_SPARK:-1}"
else
    INSTALL_SPARK="${PAIMON_NATIVE_IO_INSTALL_SPARK:-0}"
fi
DOCKERFILE="${PROJECT_ROOT}/dev/native-io-e2e/Dockerfile.centos7"
CONTEXT_DIR="${PROJECT_ROOT}/dev/native-io-e2e"
BUILD_CONTEXT="${CONTEXT_DIR}"
SPARK_LOCAL_KIND="none"

is_truthy() {
    case "${1}" in
        1 | true | TRUE | yes | YES | y | Y) return 0 ;;
        *) return 1 ;;
    esac
}

prepare_build_context() {
    if [ -n "${PAIMON_NATIVE_IO_SPARK_TGZ:-}" ] && [ -n "${PAIMON_NATIVE_IO_SPARK_DIR:-}" ]; then
        echo "Only one of PAIMON_NATIVE_IO_SPARK_TGZ or PAIMON_NATIVE_IO_SPARK_DIR can be set." >&2
        exit 1
    fi

    if [ -n "${PAIMON_NATIVE_IO_SPARK_TGZ:-}" ]; then
        if [ ! -f "${PAIMON_NATIVE_IO_SPARK_TGZ}" ]; then
            echo "Missing Spark archive: ${PAIMON_NATIVE_IO_SPARK_TGZ}" >&2
            exit 1
        fi
        BUILD_CONTEXT="$(mktemp -d)"
        mkdir -p "${BUILD_CONTEXT}/spark-local"
        cp "${PAIMON_NATIVE_IO_SPARK_TGZ}" "${BUILD_CONTEXT}/spark-local/spark.tgz"
        SPARK_LOCAL_KIND="tgz"
        echo "Prepared temporary Docker build context: ${BUILD_CONTEXT}"
    elif [ -n "${PAIMON_NATIVE_IO_SPARK_DIR:-}" ]; then
        if [ ! -d "${PAIMON_NATIVE_IO_SPARK_DIR}" ]; then
            echo "Missing Spark directory: ${PAIMON_NATIVE_IO_SPARK_DIR}" >&2
            exit 1
        fi
        BUILD_CONTEXT="$(mktemp -d)"
        mkdir -p "${BUILD_CONTEXT}/spark-local/spark"
        cp -R "${PAIMON_NATIVE_IO_SPARK_DIR}/." "${BUILD_CONTEXT}/spark-local/spark/"
        SPARK_LOCAL_KIND="dir"
        echo "Prepared temporary Docker build context: ${BUILD_CONTEXT}"
    fi
}

build_image() {
    local manylinux_image="${PAIMON_NATIVE_IO_MANYLINUX_IMAGE:-}"
    local jdk8_url="${PAIMON_NATIVE_IO_JDK8_URL:-}"

    case "${PLATFORM}" in
        linux/amd64 | linux/amd64/*)
            manylinux_image="${manylinux_image:-quay.io/pypa/manylinux2014_x86_64}"
            jdk8_url="${jdk8_url:-https://api.adoptium.net/v3/binary/latest/8/ga/linux/x64/jdk/hotspot/normal/eclipse}"
            ;;
        linux/arm64 | linux/arm64/*)
            manylinux_image="${manylinux_image:-quay.io/pypa/manylinux2014_aarch64}"
            jdk8_url="${jdk8_url:-https://api.adoptium.net/v3/binary/latest/8/ga/linux/aarch64/jdk/hotspot/normal/eclipse}"
            ;;
        *)
            echo "CentOS 7 compatible Native IO packaging only supports linux/amd64 or linux/arm64, got: ${PLATFORM}" >&2
            exit 1
            ;;
    esac

    local args=(
        --platform "${PLATFORM}"
        -t "${IMAGE}"
        -f "${DOCKERFILE}"
        --build-arg "MANYLINUX_IMAGE=${manylinux_image}"
        --build-arg "JDK8_URL=${jdk8_url}"
        --build-arg "INSTALL_SPARK=${INSTALL_SPARK}"
        --build-arg "SPARK_LOCAL_KIND=${SPARK_LOCAL_KIND}"
    )

    if [ -n "${PAIMON_NATIVE_IO_MAVEN_VERSION:-}" ]; then
        args+=(--build-arg "MAVEN_VERSION=${PAIMON_NATIVE_IO_MAVEN_VERSION}")
    fi
    if [ -n "${PAIMON_NATIVE_IO_SPARK_VERSION:-}" ]; then
        args+=(--build-arg "SPARK_VERSION=${PAIMON_NATIVE_IO_SPARK_VERSION}")
    fi
    if [ -n "${PAIMON_NATIVE_IO_SPARK_TGZ_URL:-}" ]; then
        args+=(--build-arg "SPARK_TGZ_URL=${PAIMON_NATIVE_IO_SPARK_TGZ_URL}")
    fi
    if [ -n "${PAIMON_NATIVE_IO_RUST_TOOLCHAIN:-}" ]; then
        args+=(--build-arg "RUST_TOOLCHAIN=${PAIMON_NATIVE_IO_RUST_TOOLCHAIN}")
    fi

    args+=("${BUILD_CONTEXT}")

    echo "Building CentOS 7 compatible Native IO image: ${IMAGE} (${PLATFORM})"
    if docker buildx version >/dev/null 2>&1; then
        docker buildx build --load "${args[@]}"
    else
        docker build "${args[@]}"
    fi
}

if [ ! -f "${DOCKERFILE}" ]; then
    echo "Missing Dockerfile: ${DOCKERFILE}" >&2
    exit 1
fi

if is_truthy "${BUILD_IMAGE}"; then
    prepare_build_context
    build_image
else
    echo "Skipping image build because PAIMON_NATIVE_IO_BUILD_IMAGE=${BUILD_IMAGE}"
fi

PAIMON_NATIVE_IO_DOCKER_IMAGE="${IMAGE}" \
PAIMON_NATIVE_IO_DOCKER_PLATFORM="${PLATFORM}" \
    "${SCRIPT_DIR}/package-jars.sh"
