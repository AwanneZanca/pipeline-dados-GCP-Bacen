-- ============================================================
-- Model: mart_indicadores (Camada Gold)
-- Descrição: Dados econômicos prontos para análise e dashboard
-- Dataset destino: dados_economicos_gold
-- ============================================================

{{ config(materialized='table') }}

WITH base AS (
    SELECT * FROM {{ ref('stg_bacen') }}
),

classificado AS (
    SELECT
        SAFE.PARSE_DATE('%Y-%m-%d', CAST(data AS STRING)) as data,
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
            WHEN 'IGP-M' THEN
                CASE
                    WHEN valor > 0.5 THEN 'Acima da meta'
                    WHEN valor < 0   THEN 'Deflação'
                    ELSE 'Normal'
                END
            WHEN 'DESEMPREGO' THEN
                CASE
                    WHEN valor > 12 THEN 'Alto'
                    WHEN valor < 8  THEN 'Baixo'
                    ELSE 'Normal'
                END
            WHEN 'EUR/BRL' THEN
                CASE
                    WHEN valor > 6.0 THEN 'Euro alto'
                    WHEN valor < 5.0 THEN 'Euro baixo'
                    ELSE 'Normal'
                END
            WHEN 'CREDITO TOTAL' THEN
                CASE
                    WHEN valor > 15 THEN 'Alto'
                    WHEN valor < 10 THEN 'Baixo'
                    ELSE 'Normal'
                END
        END AS classificacao,

        FORMAT_DATE('%Y-%m', SAFE.PARSE_DATE('%Y-%m-%d', CAST(data AS STRING))) AS ano_mes,
        EXTRACT(YEAR FROM SAFE.PARSE_DATE('%Y-%m-%d', CAST(data AS STRING)))    AS ano,
        EXTRACT(MONTH FROM SAFE.PARSE_DATE('%Y-%m-%d', CAST(data AS STRING)))   AS mes

    FROM base
)

SELECT * FROM classificado
