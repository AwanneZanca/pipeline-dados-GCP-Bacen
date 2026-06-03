-- ============================================================
-- Model: dim_indicador (Camada Gold)
-- Descrição: Dimensão com metadados de todos os indicadores
--            econômicos — BACEN e IBGE (SIDRA).
--            Inclui categoria, unidade de medida, frequência,
--            fonte, localidade e classificação.
-- Depende de: stg_indicadores, stg_ibge
-- Dataset destino: dados_economicos_gold
-- ============================================================

{{ config(materialized='table') }}

with bacen as (
    select distinct
        indicador                              as nome_indicador,
        serie                                  as codigo_serie,
        'BACEN'                                as fonte,
        'Brasil'                               as localidade,
        NULL                                   as classificacao
    from {{ ref('stg_indicadores') }}
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
        -- Surrogate key
        row_number() over (
            order by fonte, nome_indicador, localidade
        )                                      as sk_indicador,

        -- Atributos naturais
        nome_indicador,
        codigo_serie,
        fonte,
        localidade,
        classificacao,

        -- Categorização
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

        -- Unidade de medida
        case
            when nome_indicador = 'TAXA SELIC'             then '% a.a.'
            when nome_indicador in ('IPCA', 'IGP-M')       then '% a.m.'
            when nome_indicador like 'IPCA%'               then '% a.m.'
            when nome_indicador in ('USD/BRL', 'EUR/BRL')  then 'R$'
            when nome_indicador like 'DESEMPREGO%'         then '%'
            when nome_indicador = 'CREDITO TOTAL'          then 'R$ bilhões'
            when nome_indicador = 'PIB TRIMESTRAL'         then 'R$ milhões'
            else 'N/A'
        end                                    as unidade_medida,

        -- Frequência de divulgação
        case
            when nome_indicador in ('TAXA SELIC', 'USD/BRL', 'EUR/BRL') then 'Diária'
            when nome_indicador = 'PIB TRIMESTRAL'                       then 'Trimestral'
            else 'Mensal'
        end                                    as frequencia,

        -- Fonte completa
        case fonte
            when 'BACEN' then 'Banco Central do Brasil (BCB)'
            when 'IBGE'  then 'Instituto Brasileiro de Geografia e Estatística (IBGE)'
            else fonte
        end                                    as fonte_completa

    from todos
)

select * from dim
order by sk_indicador
