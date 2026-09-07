#!/usr/bin/env bash
# Drop a fixture PDF into a tenant's prefix and watch the api pick it up.
#
#   ./scripts/demo_drop.sh tests/fixtures/pdfs/note_simple.pdf tenant-a
#
# Requires the compose stack to be up (`make up`) and the fixtures to exist
# (`make fixtures`).
set -euo pipefail

PDF="${1:-tests/fixtures/pdfs/note_simple.pdf}"
TENANT="${2:-tenant-a}"
KEY="tenants/${TENANT}/claims/$(basename "${PDF%.pdf}")-$(date +%s).pdf"

if [ ! -f "$PDF" ]; then
  echo "no such file: $PDF (run 'make fixtures' first)" >&2
  exit 1
fi

docker compose cp "$PDF" minio:/tmp/drop.pdf
docker compose exec -T minio mc alias set local http://localhost:9000 \
  "${MINIO_ROOT_USER:-minioadmin}" "${MINIO_ROOT_PASSWORD:-minioadmin}" >/dev/null
docker compose exec -T minio mc cp /tmp/drop.pdf "local/claims/${KEY}"

echo "dropped ${KEY}"
docker compose logs --since 30s api
