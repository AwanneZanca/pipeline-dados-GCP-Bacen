{{ config(
    materialized='table',
    schema='gold'
) }}

-- ============================================================
-- Model: dim_indicador (Camada Gold)
-- Descrição: Dimensão indicador com metadados de cada série econômica.
--            Gerada a partir dos dados presentes em stg_indicadores.
-- Depende de: stg_indicadores
-- Dataset destino: dados_economicos_gold
-- ============================================================

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
            when 'Taxa Selic'    then 'Política Monetária'
            when 'IPCA'          then 'Inflação'
            when 'IGP-M'         then 'Inflação'
            when 'USD/BRL'       then 'Câmbio'
            when 'EUR/BRL'       then 'Câmbio'
            when 'Desemprego'    then 'Mercado de Trabalho'
            when 'Crédito Total' then 'Crédito'
            else 'Outros'
        end                                    as categoria,

        -- Unidade de medida
        case indicador
            when 'Taxa Selic'    then '% a.a.'
            when 'IPCA'          then '% a.m.'
            when 'IGP-M'         then '% a.m.'
            when 'USD/BRL'       then 'R$'
            when 'EUR/BRL'       then 'R$'
            when 'Desemprego'    then '%'
            when 'Crédito Total' then 'R$ bilhões'
            else 'N/A'
        end                                    as unidade_medida,

        -- Frequência de divulgação
        case indicador
            when 'Taxa Selic'    then 'Diária'
            when 'IPCA'          then 'Mensal'
            when 'IGP-M'         then 'Mensal'
            when 'USD/BRL'       then 'Diária'
            when 'EUR/BRL'       then 'Diária'
            when 'Desemprego'    then 'Mensal'
            when 'Crédito Total' then 'Mensal'
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
