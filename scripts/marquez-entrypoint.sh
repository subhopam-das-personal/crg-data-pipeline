#!/bin/sh
set -e

# Create the 'marquez' schema in PostgreSQL if it doesn't exist.
# Marquez's Flyway migrations will land in this schema, keeping
# the railway database's public schema clean.
echo "[marquez-entrypoint] Ensuring schema 'marquez' exists..."
PGPASSWORD="${MARQUEZ_DB_PASSWORD}" psql \
  -h "${MARQUEZ_DB_HOST:-localhost}" \
  -p "${MARQUEZ_DB_PORT:-5432}" \
  -U "${MARQUEZ_DB_USER:-marquez}" \
  -d "${MARQUEZ_DB_NAME:-marquez}" \
  -c "CREATE SCHEMA IF NOT EXISTS marquez;" \
  && echo "[marquez-entrypoint] Schema ready." \
  || echo "[marquez-entrypoint] Schema creation skipped (may already exist or no permission)."

# Locate the Marquez API JAR.
JAR=$(ls /usr/src/app/marquez-api-*.jar 2>/dev/null | head -1)
if [ -z "$JAR" ]; then
  echo "[marquez-entrypoint] ERROR: Marquez JAR not found at /usr/src/app/" >&2
  exit 1
fi

echo "[marquez-entrypoint] Starting Marquez: $JAR"
exec java -jar "$JAR" server /usr/src/app/marquez.yml
