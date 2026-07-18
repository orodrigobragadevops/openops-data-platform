import argparse
import json
import logging
import os
from datetime import datetime, timezone
from pathlib import Path

import boto3
import psycopg2
import requests
from botocore.client import Config
from dotenv import load_dotenv
from psycopg2.extras import Json, execute_values
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

PIPELINE_NAME = "ibge_municipios"
SOURCE_NAME = "IBGE Localidades - Municipios"
SOURCE_URL = "https://servicodados.ibge.gov.br/api/v1/localidades/municipios"
BASE_DIR = Path(__file__).resolve().parent
ENV_FILE = BASE_DIR / ".env"
LOG_DIR = Path("/opt/openops/logs")
LOG_FILE = LOG_DIR / f"{PIPELINE_NAME}.log"

def setup_logging() -> None:
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    logging.basicConfig(
        level=os.getenv("LOG_LEVEL", "INFO").upper(),
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
        handlers=[
            logging.StreamHandler(),
            logging.FileHandler(LOG_FILE, encoding="utf-8"),
        ],
    )

def env_required(name: str) -> str:
    value = os.getenv(name)
    if value is None or value.strip() == "":
        raise RuntimeError(f"Variavel obrigatoria ausente: {name}")
    return value.strip()

def env_bool(name: str, default: str = "false") -> bool:
    return os.getenv(name, default).strip().lower() in {"1", "true", "yes", "y"}

def load_config() -> None:
    if not ENV_FILE.exists():
        raise RuntimeError(f"Arquivo .env nao encontrado: {ENV_FILE}")
    load_dotenv(ENV_FILE, override=True)
    required = [
        "PG_HOST",
        "PG_PORT",
        "PG_DATABASE",
        "PG_USER",
        "MINIO_ENDPOINT",
        "MINIO_ACCESS_KEY",
    ]
    for name in required:
        env_required(name)
    env_required("PG_" + "PASSWORD")
    env_required("MINIO_" + "SECRET_KEY")

def minio_endpoint_url() -> str:
    endpoint = env_required("MINIO_ENDPOINT")
    if endpoint.startswith("http://") or endpoint.startswith("https://"):
        return endpoint
    scheme = "https" if env_bool("MINIO_SECURE") else "http"
    return f"{scheme}://{endpoint}"

def pg_conn():
    return psycopg2.connect(
        host=env_required("PG_HOST"),
        port=env_required("PG_PORT"),
        dbname=env_required("PG_DATABASE"),
        user=env_required("PG_USER"),
        password=env_required("PG_" + "PASSWORD"),
    )

def minio_client():
    return boto3.client(
        "s3",
        endpoint_url=minio_endpoint_url(),
        aws_access_key_id=env_required("MINIO_ACCESS_KEY"),
        aws_secret_access_key=env_required("MINIO_" + "SECRET_KEY"),
        config=Config(signature_version="s3v4"),
        region_name="us-east-1",
    )

def requests_session() -> requests.Session:
    retry = Retry(
        total=3,
        connect=3,
        read=3,
        backoff_factor=1,
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=("GET",),
    )
    adapter = HTTPAdapter(max_retries=retry)
    session = requests.Session()
    session.mount("http://", adapter)
    session.mount("https://", adapter)
    return session

def start_audit(conn) -> int:
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO audit.pipeline_runs
                (pipeline_name, source_name, status)
            VALUES
                (%s, %s, %s)
            RETURNING id;
            """,
            (PIPELINE_NAME, SOURCE_NAME, "RUNNING"),
        )
        run_id = cur.fetchone()[0]
    conn.commit()
    return run_id

def finish_audit(conn, run_id: int, status: str, rows_read: int, rows_written: int, error_message: str | None = None) -> None:
    with conn.cursor() as cur:
        cur.execute(
            """
            UPDATE audit.pipeline_runs
            SET finished_at = now(),
                   status = %s,
                   rows_read = %s,
                   rows_written = %s,
                   error_message = %s
             WHERE id = %s;
            """,
            (status, rows_read, rows_written, error_message, run_id),
        )
    conn.commit()

def ensure_tables(conn) -> None:
    with conn.cursor() as cur:
        cur.execute(
            """
            CREATE SCHEMA IF NOT EXISTS raw;
            CREATE SCHEMA IF NOT EXISTS staging;
            CREATE SCHEMA IF NOT EXISTS audit;

            CREATE TABLE IF NOT EXISTS audit.pipeline_runs (
                id BIGSERIAL PRIMARY KEY,
                pipeline_name TEXT NOT NULL,
                source_name TEXT NOT NULL,
                started_at TIMESTAMPTZ NOT NULL DEFAULT now(),
                finished_at TIMESTAMPTZ,
                status TEXT NOT NULL,
                rows_read BIGINT DEFAULT 0,
                rows_written BIGINT DEFAULT 0,
                error_message TEXT,
                created_at TIMESTAMPTZ NOT NULL DEFAULT now()
            );

            CREATE TABLE IF NOT EXISTS raw.ibge_municipios_payloads (
                run_id BIGINT PRIMARY KEY REFERENCES audit.pipeline_runs(id),
                source_url TEXT NOT NULL,
                object_key TEXT NOT NULL,
                payload JSONB NOT NULL,
                ingested_at TIMESTAMPTZ NOT NULL DEFAULT now()
            );

            CREATE TABLE IF NOT EXISTS staging.ibge_municipios (
                id_municipio INTEGER PRIMARY KEY,
                nome_municipio TEXT NOT NULL,
                id_microrregiao INTEGER,
                nome_microrregiao TEXT,
                id_mesorregiao INTEGER,
                nome_mesorregiao TEXT,
                id_uf INTEGER,
                sigla_uf TEXT,
                nome_uf TEXT,
                id_regiao INTEGER,
                sigla_regiao TEXT,
                nome_regiao TEXT,
                updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
            );
            """
        )
    conn.commit()

def fetch_municipios() -> list[dict]:
    logging.info("Buscando municipios na API do IBGE")
    response = requests_session().get(SOURCE_URL, timeout=60)
    response.raise_for_status()
    data = response.json()
    if not isinstance(data, list) or not data:
        raise ValueError("IBGE retornou payload vazio ou fora do formato esperado")
    logging.info("Payload recebido do IBGE: %s registros", len(data))
    return data

def upload_bronze(payload: list[dict]) -> str:
    now = datetime.now(timezone.utc)
    object_key = f"ibge/municipios/load_date={now:%Y-%m-%d}/municipios_{now:%Y%m%dT%H%M%SZ}.json"
    body = json.dumps(payload, ensure_ascii=False, indent=2).encode("utf-8")
    minio_client().put_object(
        Bucket="bronze",
        Key=object_key,
        Body=body,
        ContentType="application/json; charset=utf-8",
    )
    logging.info("Arquivo bruto salvo no MinIO: bucket=bronze key=%s bytes=%s", object_key, len(body))
    return object_key

def normalize(payload: list[dict]) -> list[tuple]:
    rows = []
    for item in payload:
        micro = item.get("microrregiao") or {}
        meso = micro.get("mesorregiao") or {}
        uf = meso.get("UF") or {}
        regiao = uf.get("regiao") or {}
        rows.append(
            (
                item.get("id"),
                item.get("nome"),
                micro.get("id"),
                micro.get("nome"),
                meso.get("id"),
                meso.get("nome"),
                uf.get("id"),
                uf.get("sigla"),
                uf.get("nome"),
                regiao.get("id"),
                regiao.get("sigla"),
                regiao.get("nome"),
            )
        )
    if any(row[0] is None or row[1] is None for row in rows):
        raise ValueError("Payload possui municipio sem id ou nome")
    logging.info("Payload normalizado: %s linhas", len(rows))
    return rows

def persist(conn, run_id: int, object_key: str, payload: list[dict], rows: list[tuple]) -> int:
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO raw.ibge_municipios_payloads
                (run_id, source_url, object_key, payload)
            VALUES
                (%s, %s, %s, %s);
            """,
            (run_id, SOURCE_URL, object_key, Json(payload)),
        )
        execute_values(
            cur,
            """
            INSERT INTO staging.ibge_municipios (
                id_municipio, nome_municipio,
                id_microrregiao, nome_microrregiao,
                id_mesorregiao, nome_mesorregiao,
                id_uf, sigla_uf, nome_uf,
                id_regiao, sigla_regiao, nome_regiao
            ) VALUES %s
            ON CONFLICT (id_municipio) DO UPDATE SET
                nome_municipio = EXCLUDED.nome_municipio,
                id_microrregiao = EXCLUDED.id_microrregiao,
                nome_microrregiao = EXCLUDED.nome_microrregiao,
                id_mesorregiao = EXCLUDED.id_mesorregiao,
                nome_mesorregiao = EXCLUDED.nome_mesorregiao,
                id_uf = EXCLUDED.id_uf,
                sigla_uf = EXCLUDED.sigla_uf,
                nome_uf = EXCLUDED.nome_uf,
                id_regiao = EXCLUDED.id_regiao,
                sigla_regiao = EXCLUDED.sigla_regiao,
                nome_regiao = EXCLUDED.nome_regiao,
                updated_at = now();
            """,
            rows,
        )
    conn.commit()
    logging.info("Dados persistidos: run_id=%s rows_written=%s", run_id, len(rows))
    return len(rows)

def validate_only() -> None:
    logging.info("Iniciando validacao do ambiente")
    with pg_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT 1;")
            cur.fetchone()
    minio_client().head_bucket(Bucket="bronze")
    logging.info("Validacao concluida: PostgreSQL OK, MinIO bronze OK")
    print("VALIDATION_OK")

def run_pipeline() -> None:
    rows_read = 0
    rows_written = 0
    with pg_conn() as conn:
        ensure_tables(conn)
        run_id = start_audit(conn)
        logging.info("Pipeline iniciado: run_id=%s", run_id)
        try:
            payload = fetch_municipios()
            rows_read = len(payload)
            object_key = upload_bronze(payload)
            rows = normalize(payload)
            rows_written = persist(conn, run_id, object_key, payload, rows)
            finish_audit(conn, run_id, "SUCCESS", rows_read, rows_written)
            logging.info("Pipeline finalizado com sucesso: run_id=%s", run_id)
            print(f"OK run_id={run_id} rows_read={rows_read} rows_written={rows_written} bronze={object_key}")
        except Exception as exc:
            conn.rollback()
            logging.exception("Pipeline falhou: run_id=%s", run_id)
            finish_audit(conn, run_id, "FAILED", rows_read, rows_written, str(exc))
            raise

def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Ingestao IBGE municipios para OpenOps")
    parser.add_argument("--validate-only", action="store_true", help="Valida conexoes e configuracao sem carregar dados")
    return parser.parse_args()

def main() -> None:
    setup_logging()
    load_config()
    args = parse_args()
    if args.validate_only:
        validate_only()
    else:
        run_pipeline()

if __name__ == "__main__":
    main()
