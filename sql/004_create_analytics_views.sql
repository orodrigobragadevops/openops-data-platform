CREATE OR REPLACE VIEW analytics.vw_ibge_municipios_por_uf AS
SELECT
    sigla_uf,
    nome_uf,
    nome_regiao,
    COUNT(*) AS qtd_municipios,
    MAX(updated_at) AS ultima_atualizacao
FROM staging.ibge_municipios
GROUP BY
    sigla_uf,
    nome_uf,
    nome_regiao;
