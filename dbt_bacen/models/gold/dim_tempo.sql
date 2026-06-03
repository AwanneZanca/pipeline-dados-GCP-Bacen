-- ============================================================
-- Model: dim_tempo (Camada Gold)
-- Descrição: Dimensão calendário com granularidade diária.
--            Gerada a partir do range de datas presente em
--            stg_indicadores. Contém atributos de ano, mês,
--            trimestre, semana e flags úteis para análise.
-- Depende de: stg_indicadores
-- Dataset destino: dados_economicos_gold
-- ============================================================

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
        format_date('%B', data_referencia)                     as nome_mes,
        format_date('%b', data_referencia)                     as nome_mes_abrev,
        format_date('%Y-%m', data_referencia)                  as ano_mes,

        -- Trimestre
        extract(quarter from data_referencia)                  as trimestre,
        concat('T', extract(quarter from data_referencia),
               '/', extract(year from data_referencia))        as desc_trimestre,

        -- Semana
        extract(isoweek from data_referencia)                  as semana_iso,
        extract(dayofweek from data_referencia)                as dia_semana_num,
        format_date('%A', data_referencia)                     as dia_semana_nome,

        -- Flags úteis
        case when extract(dayofweek from data_referencia) in (1, 7)
             then true else false end                          as is_fim_de_semana,

        case when extract(month from data_referencia) = 12
              and extract(day   from data_referencia) = 31
             then true else false end                         as is_ultimo_dia_ano,

        case when extract(day from data_referencia) =
                  extract(day from date_sub(
                      date_trunc(date_add(data_referencia, interval 1 month), month),
                      interval 1 day))
             then true else false end                         as is_ultimo_dia_mes

    from datas
)

select * from dim
order by data_referencia