-- ============================================================
-- Model: stg_indicadores (Camada Silver)
-- Descrição: Padroniza os dados brutos da camada Bronze
--            (tabela indicadores do BACEN via Airflow)
-- Dataset destino: dados_economicos_silver
-- ============================================================

SELECT
    -- Converte data de string "01/06/2026" para DATE
    PARSE_DATE('%d/%m/%Y', data)    AS data,

    -- Valor já vem como FLOAT
    valor,

    -- Padroniza nome do indicador em maiúsculas
    UPPER(TRIM(indicador))          AS indicador,

    -- Série do BACEN para rastreabilidade
    serie,

    -- Timestamp de quando o registro entrou no pipeline
    inserted_at

FROM {{ source('dados_economicos_bronze', 'indicadores') }}