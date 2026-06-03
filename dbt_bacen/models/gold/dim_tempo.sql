-- models/gold/dim_tempo.sql
-- Dimensão Tempo — granularidade diária
-- Gerada a partir do intervalo de datas presente em stg_indicadores

{{ config(
    materialized='table',
    schema='gold'
) }}

with datas as (
    select distinct data_referencia
    from {{ ref('stg_indicadores') }}
),

dim as (
    select
        -- Surrogate key
        cast(format_date('%Y%m%d', data_referencia) as int64)  as sk_tempo,

        -- Atributos naturais
        data_referencia,

        -- Ano / Mês / Dia
        extract(year  from data_referencia)                    as ano,
        extract(month from data_referencia)                    as mes,
        extract(day   from data_referencia)                    as dia,

        -- Descrições
        format_date('%B', data_referencia)                     as nome_mes,      -- Janeiro, Fevereiro…
        format_date('%b', data_referencia)                     as nome_mes_abrev, -- Jan, Fev…
        format_date('%Y-%m', data_referencia)                  as ano_mes,        -- 2024-01

        -- Trimestre
        extract(quarter from data_referencia)                  as trimestre,
        concat('T', extract(quarter from data_referencia),
               '/', extract(year from data_referencia))        as desc_trimestre, -- T1/2024

        -- Semana
        extract(isoweek from data_referencia)                  as semana_iso,
        extract(dayofweek from data_referencia)                as dia_semana_num, -- 1=Dom … 7=Sáb
        format_date('%A', data_referencia)                     as dia_semana_nome,

        -- Flags úteis
        case when extract(dayofweek from data_referencia) in (1, 7)
             then true else false end                          as is_fim_de_semana,

        case when extract(month from data_referencia) = 12
              and extract(day   from data_referencia) = 31
             then true else false end                         as is_ultimo_dia_ano,

        case when extract(day from data_referencia) =
                  extract(day from date_trunc(
                      date_add(date_trunc(data_referencia, month), interval 1 month),
                      month) - interval 1 day)
             then true else false end                         as is_ultimo_dia_mes

    from datas
)

select * from dim
order by data_referencia
