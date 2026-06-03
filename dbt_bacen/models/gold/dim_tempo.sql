-- ============================================================
-- Model: dim_tempo (Camada Gold)
-- Descrição: Dimensão calendário com granularidade diária.
--            Gerada a partir do range de datas presente em
--            stg_indicadores. Contém atributos de ano, mês,
--            trimestre, semana e flags úteis para análise.
-- Depende de: stg_indicadores
-- Dataset destino: dados_economicos_gold
-- ============================================================

{{ config(materialized='table') }}

with datas as (
    select distinct data
    from {{ ref('stg_indicadores') }}
),

dim as (
    select
        -- Surrogate key
        cast(format_date('%Y%m%d', data) as int64)  as sk_tempo,

        -- Atributos naturais
        data,

        -- Ano / Mês / Dia
        extract(year  from data)                    as ano,
        extract(month from data)                    as mes,
        extract(day   from data)                    as dia,

        -- Descrições
        format_date('%B', data)                     as nome_mes,
        format_date('%b', data)                     as nome_mes_abrev,
        format_date('%Y-%m', data)                  as ano_mes,

        -- Trimestre
        extract(quarter from data)                  as trimestre,
        concat('T', extract(quarter from data),
               '/', extract(year from data))        as desc_trimestre,

        -- Semana
        extract(isoweek from data)                  as semana_iso,
        extract(dayofweek from data)                as dia_semana_num,
        format_date('%A', data)                     as dia_semana_nome,

        -- Flags úteis
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
