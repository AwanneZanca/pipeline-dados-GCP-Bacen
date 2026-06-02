-- ============================================================
-- Model: stg_indicadores
-- Descrição: Padroniza os dados brutos da tabela indicadores
--            vinda do BACEN via Airflow
-- ============================================================

SELECT
    -- Converte data de string "01/06/2026" para DATE
    PARSE_DATE('%d/%m/%Y', data) AS data,

    -- Valor já vem como FLOAT
    valor,

    -- Padroniza nome do indicador
    UPPER(TRIM(indicador)) AS indicador,

    -- Série do BACEN
    serie,

    -- Adiciona timestamp de quando o registro entrou no BigQuery
    CURRENT_TIMESTAMP() AS inserted_at

FROM {{ source('dados_economicos', 'indicadores') }}