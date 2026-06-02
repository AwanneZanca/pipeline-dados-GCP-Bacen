{{ config(
    materialized='table',
    dataset='dados_economicos_gold'
) }}

-- ============================================================
-- Model: mart_indicadores (Camada Gold)
-- Descrição: Dados econômicos prontos para análise e dashboard
--            com classificações, métricas e filtros de data
-- Dataset destino: dados_economicos_gold
-- ============================================================

WITH base AS (
    -- Lê da camada Silver
    SELECT * FROM {{ ref('stg_indicadores') }}
),

classificado AS (
    SELECT
        data,
        valor,
        indicador,
        serie,
        inserted_at,

        CASE indicador
            WHEN 'TAXA SELIC' THEN
                CASE
                    WHEN valor > 10 THEN 'Alta'
                    WHEN valor < 5  THEN 'Baixa'
                    ELSE 'Normal'
                END
            WHEN 'IPCA' THEN
                CASE
                    WHEN valor > 0.5 THEN 'Acima da meta'
                    WHEN valor < 0   THEN 'Deflação'
                    ELSE 'Normal'
                END
            WHEN 'USD/BRL' THEN
                CASE
                    WHEN valor > 5.5 THEN 'Dólar alto'
                    WHEN valor < 4.5 THEN 'Dólar baixo'
                    ELSE 'Normal'
                END
            ELSE 'N/A'
        END AS classificacao,

        FORMAT_DATE('%Y-%m', data) AS ano_mes,
        EXTRACT(YEAR FROM data)    AS ano,
        EXTRACT(MONTH FROM data)   AS mes

    FROM base
)

SELECT * FROM classificado