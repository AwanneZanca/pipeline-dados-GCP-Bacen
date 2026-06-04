# ============================================================
# DAG: painel_economico_ibge
# Descrição: Busca indicadores macroeconômicos do IBGE (SIDRA)
#            e salva no BigQuery camada Bronze.
#            Complementa o painel_economico_bacen (BACEN).
# Agendamento: semanal (@weekly)
# Autor: Awanne Zanca
# ============================================================

from airflow import DAG
from ibge_operator import IbgeOperator
from datetime import datetime, timedelta

default_args = {
    "owner": "awanne",
    "depends_on_past": False,
    "email_on_failure": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

# ── Indicadores IBGE SIDRA ────────────────────────────────────────────────────
# Tabela 1621 → PIB trimestral (variável 584)
# Tabela 7060 → IPCA por grupo (variável 2265, classificação 315)
#   1904 = Alimentação e bebidas
#   1906 = Habitação
#   1912 = Transportes
# Tabela 6381 → Desemprego por região (variável 4099)
#   Regiões: 1=Norte, 2=Nordeste, 3=Sudeste, 4=Sul, 5=Centro-Oeste

INDICADORES = [
    {
        "task_id": "busca_pib_trimestral",
        "tabela": 1621,
        "variavel": 584,
        "nome": "PIB Trimestral",
        "classificacao_cod": None,
        "classificacao_cat": None,
        "nivel_geo": "1",
        "localidade": "all",
        "periodo": "last 1",
    },
    {
        "task_id": "busca_ipca_alimentacao",
        "tabela": 7060,
        "variavel": 2265,
        "nome": "IPCA Alimentação",
        "classificacao_cod": "315",
        "classificacao_cat": "1904",
        "nivel_geo": "1",
        "localidade": "all",
        "periodo": "last 1",
    },
    {
        "task_id": "busca_ipca_habitacao",
        "tabela": 7060,
        "variavel": 2265,
        "nome": "IPCA Habitação",
        "classificacao_cod": "315",
        "classificacao_cat": "1906",
        "nivel_geo": "1",
        "localidade": "all",
        "periodo": "last 1",
    },
    {
        "task_id": "busca_ipca_transportes",
        "tabela": 7060,
        "variavel": 2265,
        "nome": "IPCA Transportes",
        "classificacao_cod": "315",
        "classificacao_cat": "1912",
        "nivel_geo": "1",
        "localidade": "all",
        "periodo": "last 1",
    },
    {
        "task_id": "busca_desemprego_sudeste",
        "tabela": 6381,
        "variavel": 4099,
        "nome": "Desemprego Sudeste",
        "classificacao_cod": None,
        "classificacao_cat": None,
        "nivel_geo": "2",
        "localidade": "3",
        "periodo": "last 1",
    },
    {
        "task_id": "busca_desemprego_nordeste",
        "tabela": 6381,
        "variavel": 4099,
        "nome": "Desemprego Nordeste",
        "classificacao_cod": None,
        "classificacao_cat": None,
        "nivel_geo": "2",
        "localidade": "2",
        "periodo": "last 1",
    },
    {
        "task_id": "busca_desemprego_norte",
        "tabela": 6381,
        "variavel": 4099,
        "nome": "Desemprego Norte",
        "classificacao_cod": None,
        "classificacao_cat": None,
        "nivel_geo": "2",
        "localidade": "1",
        "periodo": "last 1",
    },
    {
        "task_id": "busca_desemprego_sul",
        "tabela": 6381,
        "variavel": 4099,
        "nome": "Desemprego Sul",
        "classificacao_cod": None,
        "classificacao_cat": None,
        "nivel_geo": "2",
        "localidade": "4",
        "periodo": "last 1",
    },
    {
        "task_id": "busca_desemprego_centro_oeste",
        "tabela": 6381,
        "variavel": 4099,
        "nome": "Desemprego Centro-Oeste",
        "classificacao_cod": None,
        "classificacao_cat": None,
        "nivel_geo": "2",
        "localidade": "5",
        "periodo": "last 1",
    },
]

with DAG(
    dag_id="painel_economico_ibge",
    description="Indicadores macroeconômicos IBGE (SIDRA) → BigQuery Bronze",
    schedule="@weekly",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["ibge", "sidra", "financeiro", "brasil", "painel", "bronze"],
    default_args=default_args,
) as dag:

    for ind in INDICADORES:
        IbgeOperator(
            task_id=ind["task_id"],
            tabela=ind["tabela"],
            variavel=ind["variavel"],
            nome_indicador=ind["nome"],
            classificacao_cod=ind["classificacao_cod"],
            classificacao_cat=ind["classificacao_cat"],
            nivel_geo=ind["nivel_geo"],
            localidade=ind["localidade"],
            modo="incremental",
            periodo=ind["periodo"],
        )
