-- ============================================================
-- Model: dim_indicador (Camada Gold)
-- Descrição: Dimensão com metadados dos 7 indicadores econômicos
--            do BACEN. Inclui categoria, unidade de medida,
--            frequência de divulgação e URL da API.
-- Depende de: stg_indicadores
-- Dataset destino: dados_economicos_gold
-- ============================================================

{{ config(materialized='table') }}

with indicadores_raw as (
    select distinct
        indicador,
        serie
    from {{ ref('stg_indicadores') }}
),

dim as (
    select
        -- Surrogate key
        row_number() over (order by serie)    as sk_indicador,

        -- Atributos naturais
        serie                                  as codigo_serie_bacen,
        indicador                              as nome_indicador,

        -- Categorização manual (enriquecimento)
        case indicador
            when 'TAXA SELIC'    then 'Política Monetária'
            when 'IPCA'          then 'Inflação'
            when 'IGP-M'         then 'Inflação'
            when 'USD/BRL'       then 'Câmbio'
            when 'EUR/BRL'       then 'Câmbio'
            when 'DESEMPREGO'    then 'Mercado de Trabalho'
            when 'CREDITO TOTAL' then 'Crédito'
            else 'Outros'
        end                                    as categoria,

        -- Unidade de medida
        case indicador
            when 'TAXA SELIC'    then '% a.a.'
            when 'IPCA'          then '% a.m.'
            when 'IGP-M'         then '% a.m.'
            when 'USD/BRL'       then 'R$'
            when 'EUR/BRL'       then 'R$'
            when 'DESEMPREGO'    then '%'
            when 'CREDITO TOTAL' then 'R$ bilhões'
            else 'N/A'
        end                                    as unidade_medida,

        -- Frequência de divulgação
        case indicador
            when 'TAXA SELIC'    then 'Diária'
            when 'IPCA'          then 'Mensal'
            when 'IGP-M'         then 'Mensal'
            when 'USD/BRL'       then 'Diária'
            when 'EUR/BRL'       then 'Diária'
            when 'DESEMPREGO'    then 'Mensal'
            when 'CREDITO TOTAL' then 'Mensal'
            else 'Desconhecida'
        end                                    as frequencia,

        -- Fonte
        'Banco Central do Brasil (BCB)'        as fonte,

        -- URL da série no BACEN
        concat(
            'https://api.bcb.gov.br/dados/serie/bcdata.sgs.',
            cast(serie as string),
            '/dados'
        )                                      as url_api_bacen

    from indicadores_raw
)

select * from dim
order by sk_indicador
