-- ============================================================
-- Model: stg_bacen (Camada Silver)
-- Descrição: Padroniza os dados brutos da camada Bronze
--            (tabela bacen do BACEN via Airflow)
-- Dataset destino: dados_economicos_silver
-- ============================================================

{{ config(materialized='view') }}

SELECT
    -- Data já vem como DATE do BigQuery
    data,
    -- Valor já vem como FLOAT
    valor,
    -- Padroniza nome do indicador em maiúsculas
    UPPER(TRIM(indicador))          AS indicador,
    -- Série do BACEN para rastreabilidade
    serie,
    -- Timestamp de quando o registro entrou no pipeline
    inserted_at
FROM {{ source('dados_economicos_bronze', 'bacen') }}
