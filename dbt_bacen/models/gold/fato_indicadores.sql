-- ============================================================
-- Model: fato_indicadores (Camada Gold)
-- Descrição: Tabela fato central do Star Schema.
--            Une dados do BACEN e IBGE (SIDRA) em uma única
--            tabela analítica. Granularidade: 1 linha por
--            indicador + localidade + classificação por data.
--            Contém valor, variações e médias móveis.
-- Depende de: stg_indicadores, stg_ibge, dim_tempo, dim_indicador
-- Partição: mensal (data)
-- Cluster: fonte, categoria, nome_indicador
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
        data,
        indicador                              as nome_indicador,
        valor,
        CAST(serie AS STRING)                  as codigo_serie,
        'BACEN'                                as fonte,
        'Brasil'                               as localidade,
        NULL                                   as classificacao
    from {{ ref('stg_indicadores') }}
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

-- Une as duas fontes
todos as (
    select * from bacen
    union all
    select * from ibge
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

        -- Atributos degenerados (evitam JOINs desnecessários no Looker Studio)
        t.data,
        t.nome_indicador,
        t.fonte,
        t.localidade,
        t.classificacao,
        di.categoria,
        di.unidade_medida,

        -- Métricas
        t.valor,

        -- Variação absoluta em relação ao período anterior do mesmo indicador
        round(
            t.valor - lag(t.valor) over (
                partition by t.codigo_serie, t.localidade, t.classificacao
                order by t.data
            ), 6
        )                                       as variacao_absoluta,

        -- Variação percentual em relação ao período anterior
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

        -- Média móvel 30 dias
        round(
            avg(t.valor) over (
                partition by t.codigo_serie, t.localidade, t.classificacao
                order by t.data
                rows between 29 preceding and current row
            ), 6
        )                                       as media_movel_30d,

        -- Média móvel 90 dias
        round(
            avg(t.valor) over (
                partition by t.codigo_serie, t.localidade, t.classificacao
                order by t.data
                rows between 89 preceding and current row
            ), 6
        )                                       as media_movel_90d,

        -- Metadados
        t.codigo_serie,
        current_timestamp()                     as dbt_updated_at

    from todos t
    inner join dim_t dt on t.data              = dt.data
    inner join dim_i di on t.nome_indicador    = di.nome_indicador
                       and t.localidade        = di.localidade
                       and (
                               (t.classificacao is null and di.classificacao is null)
                               or t.classificacao = di.classificacao
                           )
)

select * from fato
