# ============================================================
# DAG: painel_economico_ibge
# Descrição: Busca indicadores macroeconômicos do IBGE (SIDRA)
#            e salva no BigQuery camada Bronze.
#            Complementa o painel_economico_brasil (BACEN).
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
# Tabela 7060 → IPCA por grupo (variável 63 = variação mensal %, classificação 315)
#   7445 = Alimentação e bebidas
#   7486 = Habitação
#   7625 = Transportes
# Tabela 6381 → Desemprego Brasil - mensal (variável 4099, n1)
# Tabela 6468 → Desemprego por Grandes Regiões - trimestral (variável 4099, n2)
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
        "periodo": "last1",
    },
    {
        "task_id": "busca_ipca_alimentacao",
        "tabela": 7060,
        "variavel": 63,
        "nome": "IPCA Alimentação",
        "classificacao_cod": "315",
        "classificacao_cat": "7445",
        "nivel_geo": "1",
        "localidade": "all",
        "periodo": "last1",
    },
    {
        "task_id": "busca_ipca_habitacao",
        "tabela": 7060,
        "variavel": 63,
        "nome": "IPCA Habitação",
        "classificacao_cod": "315",
        "classificacao_cat": "7486",
        "nivel_geo": "1",
        "localidade": "all",
        "periodo": "last1",
    },
    {
        "task_id": "busca_ipca_transportes",
        "tabela": 7060,
        "variavel": 63,
        "nome": "IPCA Transportes",
        "classificacao_cod": "315",
        "classificacao_cat": "7625",
        "nivel_geo": "1",
        "localidade": "all",
        "periodo": "last1",
    },
    {
        "task_id": "busca_desemprego_brasil",
        "tabela": 6381,
        "variavel": 4099,
        "nome": "Desemprego Brasil",
        "classificacao_cod": None,
        "classificacao_cat": None,
        "nivel_geo": "1",
        "localidade": "all",
        "periodo": "last1",
    },
    {
        "task_id": "busca_desemprego_regioes",
        "tabela": 6468,
        "variavel": 4099,
        "nome": "Desemprego Regiões",
        "classificacao_cod": None,
        "classificacao_cat": None,
        "nivel_geo": "2",
        "localidade": "all",
        "periodo": "last1",
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
            modo="historico",
            periodo=ind["periodo"],
        )