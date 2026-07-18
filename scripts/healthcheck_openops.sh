#!/usr/bin/env bash
set -u

PROJECT_DIR="/opt/openops/docker"
PG_CONTAINER="openops_postgres"
MINIO_URL="http://127.0.0.1:9000/minio/health/ready"

status=0

check_ok() {
  echo "OK   - $1"
}

check_fail() {
  echo "FAIL - $1"
  status=1
}

cd "$PROJECT_DIR" || {
  echo "FAIL - pasta do Docker Compose nao encontrada: $PROJECT_DIR"
  exit 1
}

echo "== OpenOps Healthcheck =="
echo

if docker compose ps >/dev/null 2>&1; then
  check_ok "docker compose responde"
else
  check_fail "docker compose nao respondeu"
fi

if docker ps --format '{{.Names}}' | grep -qx 'openops_postgres'; then
  check_ok "container openops_postgres esta up"
else
  check_fail "container openops_postgres nao esta up"
fi

if docker ps --format '{{.Names}}' | grep -qx 'minio_datalake'; then
  check_ok "container minio_datalake esta up"
else
  check_fail "container minio_datalake nao esta up"
fi

if curl -fsS "$MINIO_URL" >/dev/null; then
  check_ok "MinIO health ready"
else
  check_fail "MinIO health falhou"
fi

if docker exec "$PG_CONTAINER" pg_isready -U openops -d openops >/dev/null 2>&1; then
  check_ok "PostgreSQL aceita conexao"
else
  check_fail "PostgreSQL nao aceitou conexao"
fi

rows=$(docker exec "$PG_CONTAINER" psql -U openops -d openops -tAc "SELECT count(*) FROM staging.ibge_municipios;" 2>/dev/null || echo "ERROR")
if [[ "$rows" =~ ^[0-9]+$ ]] && [ "$rows" -gt 0 ]; then
  check_ok "staging.ibge_municipios possui $rows linhas"
else
  check_fail "staging.ibge_municipios sem linhas ou inacessivel"
fi

last_run=$(docker exec "$PG_CONTAINER" psql -U openops -d openops -tAc "SELECT id || '|' || status || '|' || COALESCE(rows_read,0) || '|' || COALESCE(rows_written,0) FROM audit.pipeline_runs WHERE pipeline_name = 'ibge_municipios' ORDER BY id DESC LIMIT 1;" 2>/dev/null || echo "ERROR")

if [[ "$last_run" == *"SUCCESS"* ]]; then
  check_ok "ultima execucao ibge_municipios: $last_run"
else
  check_fail "ultima execucao ibge_municipios nao esta SUCCESS: $last_run"
fi

echo
if [ "$status" -eq 0 ]; then
  echo "HEALTHCHECK_OK"
else
  echo "HEALTHCHECK_FAILED"
fi

exit "$status"
