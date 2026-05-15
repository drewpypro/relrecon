#!/usr/bin/env bash
# Spin up a Trino container with sample data and user/password auth.
#
# Usage:
#   cd examples/trino
#   ./setup.sh
#
# Default credentials: test / test123
#   Override with TRINO_USER and TRINO_PASSWORD env vars.
#
# After setup, run the demo recipe:
#   export TRINO_HOST=localhost TRINO_USER=test TRINO_PASSWORD=test123
#   python3 -m src --recipe examples/trino/sample_recipe.yaml

set -euo pipefail

CONTAINER_NAME="trino-test"
TRINO_PORT="${TRINO_PORT:-8085}"
TRINO_USER="${TRINO_USER:-test}"
TRINO_PASSWORD="${TRINO_PASSWORD:-test123}"
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"

echo "==> Cleaning up old container..."
docker rm -f "$CONTAINER_NAME" 2>/dev/null || true

# --- Build config directory ---
CONF_DIR=$(mktemp -d)

# Self-signed cert (Trino requires HTTPS for password auth)
echo "==> Generating self-signed TLS cert..."
openssl req -x509 -newkey rsa:2048 -sha256 -days 365 \
  -nodes -keyout "$CONF_DIR/trino.key" -out "$CONF_DIR/trino.crt" \
  -subj "/CN=localhost" -addext "subjectAltName=DNS:localhost,IP:127.0.0.1" \
  2>/dev/null
# PKCS12 keystore (what Trino's Java server needs)
openssl pkcs12 -export -in "$CONF_DIR/trino.crt" -inkey "$CONF_DIR/trino.key" \
  -out "$CONF_DIR/keystore.p12" -name trino -passout pass:changeit 2>/dev/null
chmod 644 "$CONF_DIR/keystore.p12"

# Password file (bcrypt)
echo "==> Creating password file for user '$TRINO_USER'..."
HASH=$(docker run --rm python:3.13-slim bash -c \
  "pip install -q bcrypt 2>/dev/null && python3 -c \"
import bcrypt
print(bcrypt.hashpw(b'$TRINO_PASSWORD', bcrypt.gensalt()).decode())
\"")
echo "${TRINO_USER}:${HASH}" > "$CONF_DIR/password.db"

# Trino config
SHARED_SECRET=$(openssl rand -hex 32)
cat > "$CONF_DIR/config.properties" <<EOF
coordinator=true
node-scheduler.include-coordinator=true
discovery.uri=https://localhost:8443
http-server.https.enabled=true
http-server.https.port=8443
http-server.https.keystore.path=/etc/trino/tls/keystore.p12
http-server.https.keystore.key=changeit
http-server.authentication.type=PASSWORD
catalog.management=\${ENV:CATALOG_MANAGEMENT}
internal-communication.shared-secret=$SHARED_SECRET
EOF

cat > "$CONF_DIR/password-authenticator.properties" <<EOF
password-authenticator.name=file
file.password-file=/etc/trino/password.db
EOF

cat > "$CONF_DIR/sample.properties" <<EOF
connector.name=memory
EOF

echo "==> Starting Trino container (port $TRINO_PORT, HTTPS + user/password)..."
docker run -d \
  --name "$CONTAINER_NAME" \
  -p "${TRINO_PORT}:8443" \
  -v "$CONF_DIR/keystore.p12:/etc/trino/tls/keystore.p12:ro" \
  -v "$CONF_DIR/config.properties:/etc/trino/config.properties:ro" \
  -v "$CONF_DIR/password-authenticator.properties:/etc/trino/password-authenticator.properties:ro" \
  -v "$CONF_DIR/password.db:/etc/trino/password.db:ro" \
  -v "$CONF_DIR/sample.properties:/etc/trino/catalog/sample.properties:ro" \
  trinodb/trino:latest

# Internal CLI helper (uses --insecure to skip TLS verification)
trino_exec() {
  docker exec -e TRINO_PASSWORD="$TRINO_PASSWORD" "$CONTAINER_NAME" trino \
    --server https://localhost:8443 \
    --user "$TRINO_USER" --password \
    --insecure \
    --execute "$1"
}

echo "==> Waiting for Trino to start..."
for i in $(seq 1 60); do
  if trino_exec "SELECT 1" &>/dev/null; then
    echo "    Trino ready after ${i}s"
    break
  fi
  if [ "$i" -eq 60 ]; then
    echo "    ERROR: Trino did not start within 60s"
    docker logs "$CONTAINER_NAME" 2>&1 | grep "ERROR" | tail -3
    exit 1
  fi
  sleep 1
done

echo "==> Creating sample schema..."
trino_exec "CREATE SCHEMA IF NOT EXISTS sample.demo"

echo "==> Loading migrated_parts (15 rows)..."
trino_exec "
CREATE TABLE IF NOT EXISTS sample.demo.migrated_parts (
  vendor_id VARCHAR,
  vendor_name VARCHAR,
  part_type VARCHAR,
  brand VARCHAR
)"

INSERT_SQL="INSERT INTO sample.demo.migrated_parts VALUES"
first=true
while IFS=, read -r vid vname ptype brand; do
  [ "$vid" = "vendor_id" ] && continue
  if [ "$first" = true ]; then
    INSERT_SQL="$INSERT_SQL ('$vid', '$vname', '$ptype', '$brand')"
    first=false
  else
    INSERT_SQL="$INSERT_SQL, ('$vid', '$vname', '$ptype', '$brand')"
  fi
done < "$SCRIPT_DIR/migrated_parts.csv"
trino_exec "$INSERT_SQL"

echo "==> Loading trusted_parts (15 rows)..."
trino_exec "
CREATE TABLE IF NOT EXISTS sample.demo.trusted_parts (
  vendor_id VARCHAR,
  vendor_name VARCHAR,
  part_type VARCHAR,
  brand VARCHAR
)"

INSERT_SQL="INSERT INTO sample.demo.trusted_parts VALUES"
first=true
while IFS=, read -r vid vname ptype brand; do
  [ "$vid" = "vendor_id" ] && continue
  if [ "$first" = true ]; then
    INSERT_SQL="$INSERT_SQL ('$vid', '$vname', '$ptype', '$brand')"
    first=false
  else
    INSERT_SQL="$INSERT_SQL, ('$vid', '$vname', '$ptype', '$brand')"
  fi
done < "$SCRIPT_DIR/trusted_parts.csv"
trino_exec "$INSERT_SQL"

echo ""
echo "==> Done! Trino is running on port $TRINO_PORT (HTTPS + user/password)"
echo ""
echo "Credentials: $TRINO_USER / $TRINO_PASSWORD"
echo ""
echo "Available data:"
echo "  tpch.tiny.*                   -- built-in benchmark data"
echo "  sample.demo.migrated_parts    -- 15 rows, mangled names"
echo "  sample.demo.trusted_parts     -- 15 rows, clean reference"
echo ""
echo "Run the demo recipe:"
echo "  export TRINO_HOST=localhost TRINO_USER=$TRINO_USER TRINO_PASSWORD=$TRINO_PASSWORD"
echo "  python3 -m src --recipe examples/trino/sample_recipe.yaml"
echo ""
echo "Cleanup:"
echo "  docker rm -f $CONTAINER_NAME"
