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

IMAGE="${PAIMON_NATIVE_IO_CENTOS7_DOCKER_IMAGE:-monster830/paimon-plus:paimon-nativeio-centos7-java8}"
PLATFORMS="${PAIMON_NATIVE_IO_DOCKER_PLATFORMS:-linux/amd64,linux/arm64}"
PUSH_IMAGE="${PAIMON_NATIVE_IO_PUSH_IMAGE:-0}"
if [ -n "${PAIMON_NATIVE_IO_SPARK_TGZ:-}" ] || [ -n "${PAIMON_NATIVE_IO_SPARK_DIR:-}" ]; then
    INSTALL_SPARK="${PAIMON_NATIVE_IO_INSTALL_SPARK:-1}"
else
    INSTALL_SPARK="${PAIMON_NATIVE_IO_INSTALL_SPARK:-0}"
fi
DOCKERFILE="${PROJECT_ROOT}/dev/native-io-e2e/Dockerfile.centos7"
CONTEXT_DIR="${PROJECT_ROOT}/dev/native-io-e2e"
BUILD_CONTEXT="${CONTEXT_DIR}"
SPARK_LOCAL_KIND="none"

usage() {
    cat <<EOF
Usage: tools/native-io/build-centos7-image.sh

Environment variables:
  PAIMON_NATIVE_IO_CENTOS7_DOCKER_IMAGE  Image tag to build. Default: ${IMAGE}
  PAIMON_NATIVE_IO_DOCKER_PLATFORMS      Comma-separated platforms. Default: ${PLATFORMS}
  PAIMON_NATIVE_IO_PUSH_IMAGE            Set to 1 to push and create a multi-arch manifest.
  PAIMON_NATIVE_IO_SPARK_TGZ             Optional local spark-*-bin-hadoop3.tgz path.
  PAIMON_NATIVE_IO_SPARK_DIR             Optional local unpacked Spark directory.
  PAIMON_NATIVE_IO_INSTALL_SPARK         Set to 1 to download Spark when no local path is set.
EOF
}

is_truthy() {
    case "${1}" in
        1 | true | TRUE | yes | YES | y | Y) return 0 ;;
        *) return 1 ;;
    esac
}

case "${1:-}" in
    -h | --help)
        usage
        exit 0
        ;;
    "")
        ;;
    *)
        echo "Unknown argument: ${1}" >&2
        usage >&2
        exit 1
        ;;
esac

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
        trap 'rm -rf "${BUILD_CONTEXT}"' EXIT
        mkdir -p "${BUILD_CONTEXT}/spark-local"
        cp "${PAIMON_NATIVE_IO_SPARK_TGZ}" "${BUILD_CONTEXT}/spark-local/spark.tgz"
        SPARK_LOCAL_KIND="tgz"
    elif [ -n "${PAIMON_NATIVE_IO_SPARK_DIR:-}" ]; then
        if [ ! -d "${PAIMON_NATIVE_IO_SPARK_DIR}" ]; then
            echo "Missing Spark directory: ${PAIMON_NATIVE_IO_SPARK_DIR}" >&2
            exit 1
        fi
        BUILD_CONTEXT="$(mktemp -d)"
        trap 'rm -rf "${BUILD_CONTEXT}"' EXIT
        mkdir -p "${BUILD_CONTEXT}/spark-local/spark"
        cp -R "${PAIMON_NATIVE_IO_SPARK_DIR}/." "${BUILD_CONTEXT}/spark-local/spark/"
        SPARK_LOCAL_KIND="dir"
    fi
}

image_repo() {
    case "${IMAGE}" in
        *:*) printf '%s\n' "${IMAGE%:*}" ;;
        *) printf '%s\n' "${IMAGE}" ;;
    esac
}

image_tag() {
    case "${IMAGE}" in
        *:*) printf '%s\n' "${IMAGE##*:}" ;;
        *) printf 'latest\n' ;;
    esac
}

platform_arch() {
    case "${1}" in
        linux/amd64 | linux/amd64/*) printf 'amd64\n' ;;
        linux/arm64 | linux/arm64/*) printf 'arm64\n' ;;
        *)
            echo "Unsupported platform: ${1}" >&2
            exit 1
            ;;
    esac
}

manylinux_image() {
    case "$(platform_arch "${1}")" in
        amd64) printf 'quay.io/pypa/manylinux2014_x86_64\n' ;;
        arm64) printf 'quay.io/pypa/manylinux2014_aarch64\n' ;;
    esac
}

jdk8_url() {
    case "$(platform_arch "${1}")" in
        amd64) printf 'https://api.adoptium.net/v3/binary/latest/8/ga/linux/x64/jdk/hotspot/normal/eclipse\n' ;;
        arm64) printf 'https://api.adoptium.net/v3/binary/latest/8/ga/linux/aarch64/jdk/hotspot/normal/eclipse\n' ;;
    esac
}

set_build_args_for_platform() {
    local platform="$1"
    args=(
        --platform "${platform}"
        -f "${DOCKERFILE}"
        --build-arg "MANYLINUX_IMAGE=$(manylinux_image "${platform}")"
        --build-arg "JDK8_URL=$(jdk8_url "${platform}")"
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
}

if [ ! -f "${DOCKERFILE}" ]; then
    echo "Missing Dockerfile: ${DOCKERFILE}" >&2
    exit 1
fi

prepare_build_context

if ! docker buildx version >/dev/null 2>&1; then
    echo "docker buildx is required to build the Native IO image." >&2
    exit 1
fi

IFS=',' read -r -a platform_list <<<"${PLATFORMS}"

if [ "${#platform_list[@]}" -gt 1 ] && ! is_truthy "${PUSH_IMAGE}"; then
    echo "Multi-architecture build requires PAIMON_NATIVE_IO_PUSH_IMAGE=1." >&2
    echo "For a local single-platform image, set PAIMON_NATIVE_IO_DOCKER_PLATFORMS=linux/amd64." >&2
    exit 1
fi

repo="$(image_repo)"
tag="$(image_tag)"
refs=()
args=()

for platform in "${platform_list[@]}"; do
    arch="$(platform_arch "${platform}")"
    if is_truthy "${PUSH_IMAGE}"; then
        ref="${repo}:${tag}-${arch}"
        refs+=("${ref}")
        set_build_args_for_platform "${platform}"
        echo "Building and pushing ${ref} for ${platform}"
        docker buildx build --push -t "${ref}" "${args[@]}" "${BUILD_CONTEXT}"
    else
        set_build_args_for_platform "${platform}"
        echo "Building and loading ${IMAGE} for ${platform}"
        docker buildx build --load -t "${IMAGE}" "${args[@]}" "${BUILD_CONTEXT}"
    fi
done

if is_truthy "${PUSH_IMAGE}"; then
    echo "Creating multi-architecture manifest ${IMAGE}"
    docker buildx imagetools create -t "${IMAGE}" "${refs[@]}"
    docker buildx imagetools inspect "${IMAGE}"
fi
