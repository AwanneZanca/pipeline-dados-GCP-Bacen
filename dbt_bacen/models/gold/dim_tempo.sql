-- ============================================================
-- Model: dim_tempo (Camada Gold)
-- Depende de: stg_bacen
-- Dataset destino: dados_economicos_gold
-- ============================================================

{{ config(materialized='table') }}

with datas as (
    select distinct SAFE.PARSE_DATE('%Y-%m-%d', CAST(data AS STRING)) as data
    from {{ ref('stg_bacen') }}
    where data is not null
),

dim as (
    select
        cast(format_date('%Y%m%d', data) as int64)  as sk_tempo,
        data,
        extract(year  from data)                    as ano,
        extract(month from data)                    as mes,
        extract(day   from data)                    as dia,
        format_date('%B', data)                     as nome_mes,
        format_date('%b', data)                     as nome_mes_abrev,
        format_date('%Y-%m', data)                  as ano_mes,
        extract(quarter from data)                  as trimestre,
        concat('T', extract(quarter from data),
               '/', extract(year from data))        as desc_trimestre,
        extract(isoweek from data)                  as semana_iso,
        extract(dayofweek from data)                as dia_semana_num,
        format_date('%A', data)                     as dia_semana_nome,
        case when extract(dayofweek from data) in (1, 7)
             then true else false end               as is_fim_de_semana,
        case when extract(month from data) = 12
              and extract(day from data) = 31
             then true else false end               as is_ultimo_dia_ano,
        case when extract(day from data) =
                  extract(day from date_sub(
                      date_trunc(date_add(data, interval 1 month), month),
                      interval 1 day))
             then true else false end               as is_ultimo_dia_mes
    from datas
)

select * from dim
order by data
