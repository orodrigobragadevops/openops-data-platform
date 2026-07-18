from pathlib import Path
import subprocess

from prefect import flow, task

PIPELINE_SCRIPT = Path("/opt/openops/pipelines/ibge_municipios/ingest_ibge_municipios.py")
PYTHON_BIN = Path("/opt/openops/.venv/bin/python")

@task(name="Validar ambiente IBGE municipios", retries=2, retry_delay_seconds=10)
def validate_environment() -> None:
    subprocess.run(
        [str(PYTHON_BIN), str(PIPELINE_SCRIPT), "--validate-only"],
        check=True,
    )

@task(name="Executar ingestao IBGE municipios", retries=1, retry_delay_seconds=30)
def run_ingestion() -> None:
    subprocess.run(
        [str(PYTHON_BIN), str(PIPELINE_SCRIPT)],
        check=True,
    )

@flow(name="ibge_municipios")
def ibge_municipios_flow() -> None:
    validate_environment()
    run_ingestion()

if __name__ == "__main__":
    ibge_municipios_flow()
