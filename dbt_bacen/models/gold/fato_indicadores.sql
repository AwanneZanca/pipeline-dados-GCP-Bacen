-- ============================================================
-- Model: fato_indicadores (Camada Gold)
-- Descrição: Tabela fato central do Star Schema.
--            Granularidade: 1 linha por indicador por data.
--            Contém valor, variações e médias móveis.
-- Depende de: stg_indicadores, dim_tempo, dim_indicador
-- Partição: mensal (data)
-- Cluster: nome_indicador, categoria
-- Dataset destino: dados_economicos_gold
-- ============================================================

{{ config(
    materialized='table',
    partition_by={
        "field": "data",
        "data_type": "date",
        "granularity": "month"
    },
    cluster_by=["nome_indicador", "categoria"]
) }}

with stg as (
    select * from {{ ref('stg_indicadores') }}
),

dim_t as (
    select * from {{ ref('dim_tempo') }}
),

dim_i as (
    select * from {{ ref('dim_indicador') }}
),

fato as (
    select
        -- Surrogate keys (FKs para as dimensões)
        dt.sk_tempo                             as fk_tempo,
        di.sk_indicador                         as fk_indicador,

        -- Atributos degenerados
        stg.data,
        stg.indicador                           as nome_indicador,
        di.categoria,
        di.unidade_medida,

        -- Métricas
        stg.valor,

        -- Variações
        round(
            stg.valor - lag(stg.valor) over (
                partition by stg.serie
                order by stg.data
            ), 6
        )                                       as variacao_absoluta,

        round(
            safe_divide(
                stg.valor - lag(stg.valor) over (
                    partition by stg.serie
                    order by stg.data
                ),
                lag(stg.valor) over (
                    partition by stg.serie
                    order by stg.data
                )
            ) * 100, 4
        )                                       as variacao_percentual,

        -- Média móvel 30 dias
        round(
            avg(stg.valor) over (
                partition by stg.serie
                order by stg.data
                rows between 29 preceding and current row
            ), 6
        )                                       as media_movel_30d,

        -- Média móvel 90 dias
        round(
            avg(stg.valor) over (
                partition by stg.serie
                order by stg.data
                rows between 89 preceding and current row
            ), 6
        )                                       as media_movel_90d,

        -- Metadados
        stg.serie                               as codigo_serie_bacen,
        current_timestamp()                     as dbt_updated_at

    from stg
    inner join dim_t dt on stg.data  = dt.data
    inner join dim_i di on stg.serie = di.codigo_serie_bacen
)

select * from fato
