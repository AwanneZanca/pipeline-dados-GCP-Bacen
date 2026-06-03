-- ============================================================
-- Model: stg_ibge (Camada Silver)
-- Descrição: Padroniza os dados brutos da camada Bronze
--            (tabela ibge do IBGE SIDRA via Airflow)
-- Dataset destino: dados_economicos_silver
-- ============================================================

{{ config(materialized='view') }}

SELECT
    -- Padroniza o período para DATE (IBGE usa AAAAMM ou AAAATt)
    CASE
        -- Trimestral: ex "20241" → primeiro dia do trimestre
        WHEN LENGTH(periodo) = 5
        THEN DATE(
            CAST(SUBSTR(periodo, 1, 4) AS INT64),
            (CAST(SUBSTR(periodo, 5, 1) AS INT64) - 1) * 3 + 1,
            1
        )
        -- Mensal: ex "202401" → primeiro dia do mês
        WHEN LENGTH(periodo) = 6
        THEN DATE(
            CAST(SUBSTR(periodo, 1, 4) AS INT64),
            CAST(SUBSTR(periodo, 5, 2) AS INT64),
            1
        )
        ELSE NULL
    END                                         AS data,

    -- Descrição legível do período (ex: "1º trimestre 2024", "janeiro 2024")
    periodo_desc,

    -- Valor numérico do indicador
    valor,

    -- Nome padronizado em maiúsculas para consistência com stg_indicadores
    UPPER(TRIM(indicador))                      AS indicador,

    -- Metadados da série IBGE
    tabela,
    variavel,

    -- Localidade
    localidade_cod,
    UPPER(TRIM(localidade_nome))                AS localidade_nome,
    nivel_geo,

    -- Categoria (ex: para IPCA por categoria)
    UPPER(TRIM(classificacao))                  AS classificacao,

    -- Timestamp de quando o registro entrou no pipeline
    CURRENT_TIMESTAMP()                         AS inserted_at

FROM {{ source('dados_economicos_bronze', 'ibge') }}
WHERE valor IS NOT NULL
