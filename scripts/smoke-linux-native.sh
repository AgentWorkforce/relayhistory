#!/usr/bin/env bash
# Usage: smoke-linux-native.sh IMAGE local|registry [VERSION]
# DOCKER_DEFAULT_PLATFORM can select amd64/arm64 for local QA.
set -euo pipefail
IMAGE=${1:?container image required}
MODE=${2:?local or registry required}
VERSION=${3:-}
ROOT=$(cd "$(dirname "$0")/.." && pwd)
[[ "$MODE" = local || "$MODE" = registry ]] || exit 2
[[ "$MODE" != registry || -n "$VERSION" ]] || exit 2

RUNTIME=$IMAGE
if [[ "$IMAGE" = ubuntu:* ]]; then
  # Install only runtime dependencies in the requested Ubuntu image. Copy the
  # official Node binary without replacing Ubuntu's libc with Debian's libc.
  RUNTIME="ai-hist-smoke-ubuntu-${IMAGE#ubuntu:}-$$"
  trap 'docker image rm "$RUNTIME" >/dev/null' EXIT
  docker build --quiet --build-arg "BASE=$IMAGE" -t "$RUNTIME" - <<'DOCKERFILE'
ARG BASE
FROM node:22-bookworm-slim AS node
FROM ${BASE}
RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates libstdc++6 && rm -rf /var/lib/apt/lists/*
COPY --from=node /usr/local/ /usr/local/
DOCKERFILE
fi

docker run --rm -v "$ROOT:/work:ro" -e "SMOKE_MODE=$MODE" -e "SMOKE_VERSION=$VERSION" \
  -e "SMOKE_IMAGE=$IMAGE" "$RUNTIME" sh -eu -c '
  node -e '\''console.log(process.env.SMOKE_IMAGE, process.arch, "glibc=" + (process.report.getReport().header.glibcVersionRuntime || "musl"))'\''
  mkdir -p /tmp/smoke
  cd /tmp/smoke
  if [ "$SMOKE_MODE" = registry ]; then
    npm init -y >/dev/null
    node /work/scripts/npm-install-with-registry-retry.mjs "ai-hist@$SMOKE_VERSION"
    node /work/scripts/smoke-native-cli.mjs node_modules/ai-hist/dist/cli.js --prove-rejection
  else
    # A private writable copy keeps the negative control away from the build
    # artifacts, and lets the SDK local native dependency resolve normally.
    mkdir -p crates
    cp -R /work/crates/ai-hist-napi crates/
    cp -R /work/sdk-ts .
    node /work/scripts/smoke-native-cli.mjs sdk-ts/dist/cli.js --prove-rejection
  fi
'
