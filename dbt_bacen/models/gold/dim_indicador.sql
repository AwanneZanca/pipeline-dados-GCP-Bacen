-- ============================================================
-- Model: dim_indicador (Camada Gold)
-- Descrição: Dimensão com metadados de todos os indicadores
--            econômicos — BACEN e IBGE (SIDRA).
-- Depende de: stg_bacen, stg_ibge
-- Dataset destino: dados_economicos_gold
-- ============================================================

{{ config(materialized='table') }}

with bacen as (
    select distinct
        indicador                              as nome_indicador,
        CAST(serie AS STRING)                  as codigo_serie,
        'BACEN'                                as fonte,
        'Brasil'                               as localidade,
        CAST(NULL AS STRING)                   as classificacao
    from {{ ref('stg_bacen') }}
),

ibge as (
    select distinct
        indicador                              as nome_indicador,
        CAST(tabela AS STRING)                 as codigo_serie,
        'IBGE'                                 as fonte,
        localidade_nome                        as localidade,
        classificacao
    from {{ ref('stg_ibge') }}
),

todos as (
    select * from bacen
    union all
    select * from ibge
),

dim as (
    select
        row_number() over (
            order by fonte, nome_indicador, localidade
        )                                      as sk_indicador,
        nome_indicador,
        codigo_serie,
        fonte,
        localidade,
        classificacao,
        case
            when nome_indicador = 'TAXA SELIC'             then 'Política Monetária'
            when nome_indicador in ('IPCA', 'IGP-M')       then 'Inflação'
            when nome_indicador like 'IPCA%'               then 'Inflação'
            when nome_indicador in ('USD/BRL', 'EUR/BRL')  then 'Câmbio'
            when nome_indicador like 'DESEMPREGO%'         then 'Mercado de Trabalho'
            when nome_indicador = 'CREDITO TOTAL'          then 'Crédito'
            when nome_indicador = 'PIB TRIMESTRAL'         then 'Atividade Econômica'
            else 'Outros'
        end                                    as categoria,
        case
            when nome_indicador = 'TAXA SELIC'             then '% a.a.'
            when nome_indicador in ('IPCA', 'IGP-M')       then '% a.m.'
            when nome_indicador like 'IPCA%'               then '% a.m.'
            when nome_indicador in ('USD/BRL', 'EUR/BRL')  then 'R$'
            when nome_indicador like 'DESEMPREGO%'         then '%'
            when nome_indicador = 'CREDITO TOTAL'          then 'R$ bilhões'
            when nome_indicador = 'PIB TRIMESTRAL'         then 'Número-índice'
            else 'N/A'
        end                                    as unidade_medida,
        case
            when nome_indicador in ('TAXA SELIC', 'USD/BRL', 'EUR/BRL') then 'Diária'
            when nome_indicador = 'PIB TRIMESTRAL'                       then 'Trimestral'
            else 'Mensal'
        end                                    as frequencia,
        case fonte
            when 'BACEN' then 'Banco Central do Brasil (BCB)'
            when 'IBGE'  then 'Instituto Brasileiro de Geografia e Estatística (IBGE)'
            else fonte
        end                                    as fonte_completa
    from todos
)

select * from dim
order by sk_indicador
