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
WORKSPACE_ROOT="$(cd "${PROJECT_ROOT}/.." >/dev/null && pwd)"

IMAGE="${PAIMON_NATIVE_IO_CENTOS7_DOCKER_IMAGE:-${PAIMON_NATIVE_IO_DOCKER_IMAGE:-paimon-nativeio-centos7:local}}"
PLATFORM="${PAIMON_NATIVE_IO_DOCKER_PLATFORM:-linux/amd64}"
BUILD_IMAGE="${PAIMON_NATIVE_IO_BUILD_IMAGE:-1}"
DOCKERFILE="${PROJECT_ROOT}/dev/native-io-e2e/Dockerfile.centos7"
CONTEXT_DIR="${PROJECT_ROOT}/dev/native-io-e2e"

is_truthy() {
    case "${1}" in
        1 | true | TRUE | yes | YES | y | Y) return 0 ;;
        *) return 1 ;;
    esac
}

build_image() {
    local args=(
        --platform "${PLATFORM}"
        -t "${IMAGE}"
        -f "${DOCKERFILE}"
    )

    if [ -n "${PAIMON_NATIVE_IO_JDK8_URL:-}" ]; then
        args+=(--build-arg "JDK8_URL=${PAIMON_NATIVE_IO_JDK8_URL}")
    fi
    if [ -n "${PAIMON_NATIVE_IO_MAVEN_VERSION:-}" ]; then
        args+=(--build-arg "MAVEN_VERSION=${PAIMON_NATIVE_IO_MAVEN_VERSION}")
    fi
    if [ -n "${PAIMON_NATIVE_IO_SPARK_VERSION:-}" ]; then
        args+=(--build-arg "SPARK_VERSION=${PAIMON_NATIVE_IO_SPARK_VERSION}")
    fi
    if [ -n "${PAIMON_NATIVE_IO_RUST_TOOLCHAIN:-}" ]; then
        args+=(--build-arg "RUST_TOOLCHAIN=${PAIMON_NATIVE_IO_RUST_TOOLCHAIN}")
    fi

    args+=("${CONTEXT_DIR}")

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

case "${PLATFORM}" in
    linux/amd64 | linux/amd64/*) ;;
    *)
        echo "CentOS 7 compatible Native IO packaging only supports linux/amd64, got: ${PLATFORM}" >&2
        exit 1
        ;;
esac

if [ ! -d "${WORKSPACE_ROOT}/obs-rust-sdk" ]; then
    echo "Missing required sibling repository: ${WORKSPACE_ROOT}/obs-rust-sdk" >&2
    echo "package-jars.sh expects paimon-plus and obs-rust-sdk under the same parent directory." >&2
    exit 1
fi

if is_truthy "${BUILD_IMAGE}"; then
    build_image
else
    echo "Skipping image build because PAIMON_NATIVE_IO_BUILD_IMAGE=${BUILD_IMAGE}"
fi

PAIMON_NATIVE_IO_DOCKER_IMAGE="${IMAGE}" \
PAIMON_NATIVE_IO_DOCKER_PLATFORM="${PLATFORM}" \
    "${SCRIPT_DIR}/package-jars.sh"
