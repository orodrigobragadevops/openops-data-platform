# OpenOps Data Platform

Plataforma local de dados para ingestao, armazenamento bruto, tratamento e auditoria de pipelines.

## Servicos

- PostgreSQL 16: banco analitico, staging e auditoria.
- MinIO: data lake S3 compativel.
- Python pipelines: ingestao e transformacao inicial.

## Camadas

- bronze: dados brutos no MinIO.
- silver: dados tratados e padronizados.
- gold: dados prontos para consumo analitico.
- PostgreSQL schemas: raw, staging, analytics, audit.

## Primeiro pipeline

Pipeline: ibge_municipios

Fluxo:

1. Consome API publica do IBGE.
2. Salva JSON bruto no MinIO bucket bronze.
3. Persiste payload em raw.ibge_municipios_payloads.
4. Carrega dados normalizados em staging.ibge_municipios.
5. Registra execucao em audit.pipeline_runs.

## Comandos uteis

Entrar na pasta do Docker Compose:

    cd /opt/openops/docker
    docker compose ps

Acessar PostgreSQL:

    docker exec -it openops_postgres psql -U openops -d openops

Rodar pipeline IBGE municipios:

    source /opt/openops/.venv/bin/activate
    python /opt/openops/pipelines/ibge_municipios/ingest_ibge_municipios.py

Validar carga:

    docker exec -it openops_postgres psql -U openops -d openops -c "SELECT count(*) FROM staging.ibge_municipios;"

## Status atual

- MinIO criado com buckets bronze, silver, gold e logs.
- PostgreSQL em Docker com volume persistente.
- Schemas raw, staging, analytics e audit criados.
- Pipeline ibge_municipios validado com 5571 municipios.
- Auditoria registrada com status SUCCESS.
