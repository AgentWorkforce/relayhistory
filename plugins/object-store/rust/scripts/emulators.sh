#!/usr/bin/env bash
# Start the local emulators the conformance suite runs against and print the
# environment for `cargo test --features conformance --test conformance`.
#
#   eval "$(plugins/object-store/rust/scripts/emulators.sh up)"
#   plugins/object-store/rust/scripts/emulators.sh down
#
# Only well-known emulator credentials are used (MinIO root user created here,
# Azurite's published development key). Nothing touches a real cloud.
set -euo pipefail

MINIO_IMAGE=${MINIO_IMAGE:-cgr.dev/chainguard/minio:latest}
# object_store writes through the GCS XML API, which upstream fake-gcs-server
# does not implement for PUT (fsouza/fake-gcs-server#1164); object_store's own
# CI uses this fork for the same reason.
GCS_IMAGE=${GCS_IMAGE:-tustvold/fake-gcs-server:latest}
AZURITE_IMAGE=${AZURITE_IMAGE:-mcr.microsoft.com/azure-storage/azurite:latest}
S3_PORT=${S3_PORT:-19000}
GCS_PORT=${GCS_PORT:-14443}
AZURITE_PORT=${AZURITE_PORT:-10000}
STATE_DIR=${STATE_DIR:-$(cd "$(dirname "$0")/.." && pwd)/target/emulators}

up() {
  mkdir -p "$STATE_DIR"
  docker run -d --rm --name rh-objstore-minio -p "$S3_PORT:9000" \
    -e MINIO_ROOT_USER=rh-conformance -e MINIO_ROOT_PASSWORD=rh-conformance-secret \
    "$MINIO_IMAGE" server /data >/dev/null
  docker run -d --rm --name rh-objstore-gcs -p "$GCS_PORT:4443" \
    "$GCS_IMAGE" -scheme http -backend memory -public-host "localhost:$GCS_PORT" >/dev/null
  docker run -d --rm --name rh-objstore-azurite -p "$AZURITE_PORT:10000" \
    "$AZURITE_IMAGE" azurite-blob --blobHost 0.0.0.0 --skipApiVersionCheck >/dev/null

  # A throwaway RSA key so GCS presigning (V4 signed URLs) can be exercised;
  # fake-gcs-server does not verify the signature or call OAuth.
  local key="$STATE_DIR/gcs-sa.pem"
  openssl genpkey -algorithm RSA -pkeyopt rsa_keygen_bits:2048 -out "$key" 2>/dev/null
  python3 - "$key" "http://localhost:$GCS_PORT" > "$STATE_DIR/gcs-sa.json" <<'PY'
import json, sys
print(json.dumps({
    "type": "service_account",
    "client_email": "conformance@example.iam.gserviceaccount.com",
    "private_key_id": "conformance",
    "private_key": open(sys.argv[1]).read(),
    "gcs_base_url": sys.argv[2],
}))
PY

  for url in "http://localhost:$S3_PORT/minio/health/ready" \
             "http://localhost:$GCS_PORT/_internal/healthcheck" \
             "http://localhost:$AZURITE_PORT/devstoreaccount1?comp=list"; do
    for _ in $(seq 1 60); do
      # Azurite answers the unauthenticated probe with 403, which still means up.
      code=$(curl -s -o /dev/null -w '%{http_code}' "$url" || true)
      [[ "$code" =~ ^(200|403)$ ]] && break
      sleep 1
    done
  done

  cat <<ENV
export RH_OBJSTORE_REQUIRE_EMULATORS=1
export RH_OBJSTORE_S3_ENDPOINT=http://localhost:$S3_PORT
export RH_OBJSTORE_S3_ACCESS_KEY_ID=rh-conformance
export RH_OBJSTORE_S3_SECRET_ACCESS_KEY=rh-conformance-secret
export RH_OBJSTORE_GCS_BASE_URL=http://localhost:$GCS_PORT
export RH_OBJSTORE_GCS_SERVICE_ACCOUNT_KEY_FILE=$STATE_DIR/gcs-sa.json
export RH_OBJSTORE_AZURITE_ENDPOINT=http://127.0.0.1:$AZURITE_PORT/devstoreaccount1
ENV
}

down() {
  docker rm -f rh-objstore-minio rh-objstore-gcs rh-objstore-azurite >/dev/null 2>&1 || true
}

case "${1:-}" in
  up) up ;;
  down) down ;;
  *) echo "usage: $0 up|down" >&2; exit 2 ;;
esac
