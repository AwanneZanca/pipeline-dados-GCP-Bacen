-- ============================================================
-- Model: fato_indicadores (Camada Gold)
-- Depende de: stg_bacen, stg_ibge, dim_tempo, dim_indicador
-- Dataset destino: dados_economicos_gold
-- ============================================================

{{ config(
    materialized='table',
    partition_by={
        "field": "data",
        "data_type": "date",
        "granularity": "month"
    },
    cluster_by=["fonte", "categoria", "nome_indicador"]
) }}

with bacen as (
    select
        SAFE.PARSE_DATE('%Y-%m-%d', CAST(data AS STRING)) as data,
        indicador                              as nome_indicador,
        valor,
        CAST(serie AS STRING)                  as codigo_serie,
        'BACEN'                                as fonte,
        'Brasil'                               as localidade,
        CAST(NULL AS STRING)                   as classificacao
    from {{ ref('stg_bacen') }}
),

ibge as (
    select
        data,
        indicador                              as nome_indicador,
        valor,
        CAST(tabela AS STRING)                 as codigo_serie,
        'IBGE'                                 as fonte,
        localidade_nome                        as localidade,
        classificacao
    from {{ ref('stg_ibge') }}
    where data is not null
),

todos as (
    select * from bacen
    union all
    select * from ibge
),

dim_t as (select * from {{ ref('dim_tempo') }}),
dim_i as (select * from {{ ref('dim_indicador') }}),

fato as (
    select
        dt.sk_tempo                             as fk_tempo,
        di.sk_indicador                         as fk_indicador,
        t.data,
        t.nome_indicador,
        t.fonte,
        t.localidade,
        t.classificacao,
        di.categoria,
        di.unidade_medida,
        t.valor,
        round(
            t.valor - lag(t.valor) over (
                partition by t.codigo_serie, t.localidade, t.classificacao
                order by t.data
            ), 6
        )                                       as variacao_absoluta,
        round(
            safe_divide(
                t.valor - lag(t.valor) over (
                    partition by t.codigo_serie, t.localidade, t.classificacao
                    order by t.data
                ),
                lag(t.valor) over (
                    partition by t.codigo_serie, t.localidade, t.classificacao
                    order by t.data
                )
            ) * 100, 4
        )                                       as variacao_percentual,
        round(
            avg(t.valor) over (
                partition by t.codigo_serie, t.localidade, t.classificacao
                order by t.data
                rows between 29 preceding and current row
            ), 6
        )                                       as media_movel_30d,
        round(
            avg(t.valor) over (
                partition by t.codigo_serie, t.localidade, t.classificacao
                order by t.data
                rows between 89 preceding and current row
            ), 6
        )                                       as media_movel_90d,
        t.codigo_serie,
        current_timestamp()                     as dbt_updated_at
    from todos t
    inner join dim_t dt on t.data = dt.data
    inner join dim_i di on t.nome_indicador = di.nome_indicador
                       and t.localidade = di.localidade
                       and (
                               (t.classificacao is null and di.classificacao is null)
                               or t.classificacao = di.classificacao
                           )
)

select * from fato
