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
