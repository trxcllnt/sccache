#! /usr/bin/env bash

set -euo pipefail

print_help() {
    cat <<EOF
Usage: $0 [-b|--build-type <debug|release>] [-c|--clear-cache] [-i|--iterations <num>] [-j|--jobs <num>] [-t|--threads <num>]

Build the SimpleP2P example from the CUDA Samples with sccache (and if configured, sccache-dist).

Options:
  -b|--build-type <debug|release>  If sccache not on PATH, use this build dir's binary
  -c|--clear-cache                 \`rm /tmp/sccache_* ~/.cache/sccache\` before building
  -i|--iterations <num>            Number of total builds to run (default: 100)
  -s|--step       <num>            Iteration step (default: 1)
  -j|--jobs       <num>            Number of builds to run concurrently (default: \`nproc\`, but can be higher)
  -t|--threads    <num>            Number of CUDA device architectures to compile in parallel (default: up to 20)
EOF
}

BUILD_TYPE=debug
JOBS="$(grep -cP 'processor\s+:' /proc/cpuinfo)"
STEP=1
ITERATIONS=100
THREADS=20

eval set -- "$(getopt -n "$0" -o b:chi:j:s:t: --long build-type:,clear-cache,help,iterations:,jobs:,step:,threads -- "$@")"

while true; do
    case "$1" in
        -b|--build-type)
            BUILD_TYPE="$2";
            shift 2;
            ;;
        -c|--clear-cache)
            rm -rf /tmp/sccache_*;
            rm -rf "${SCCACHE_DIR:-"${XDG_CACHE_HOME:-"${HOME}/.cache"}"/sccache}";
            mkdir -p "${SCCACHE_DIR:-"${XDG_CACHE_HOME:-"${HOME}/.cache"}"/sccache}";
            shift 1
            ;;
        -i|--iterations)
            ITERATIONS="$2";
            shift 2;
            ;;
        -j|--jobs)
            JOBS="$2";
            shift 2;
            ;;
        -t|--threads)
            THREADS="$2";
            shift 2;
            ;;
        -s|--step)
            STEP="$2";
            shift 2;
            ;;
        -h|--help)
            print_help;
            exit 0;
            ;;
        --)
            break;
            ;;
        *)
            echo "$@" 2>&1;
            print_help
            exit 1;
            ;;
    esac
done

# Ensure we're in the script dir
cd "$( cd "$( dirname "$(realpath -m "${BASH_SOURCE[0]}")" )" && pwd )";

rm -rf build
mkdir build/

if ! command -v sccache >/dev/null 2>&1; then
    export PATH="$(realpath -m "$(pwd)/../../../target/$(uname -m)-unknown-linux-musl/${BUILD_TYPE:-debug}"):$PATH"
fi

time seq "$((1 - (STEP % 2)))" "$STEP" "$((ITERATIONS - (STEP % 2)))" | xargs -n1 -P"${JOBS}" bash -c "$(cat <<"EOF"
CMD=(sccache nvcc
    -t=$0
    -arch=all
    -DNONCE=$1
    -I ./include
    -c ./src/simpleP2P.cu
    -o ./build/simpleP2P.$1.cu.o
    -MD
    -MT ./build/simpleP2P.$1.cu.o
    -MF ./build/simpleP2P.$1.cu.o.d
)
echo "$1: ${CMD[@]}";
"${CMD[@]}" || exit 255
EOF
)" "$THREADS"
