# ============================================================
# DAG: painel_economico_ibge
# Descrição: Busca indicadores macroeconômicos do IBGE (SIDRA)
#            e salva no BigQuery camada Bronze.
#            Complementa o painel_economico_brasil (BACEN).
# Agendamento: semanal (@weekly)
# Autor: Awanne Zanca
# ============================================================

# Importa a classe DAG — define o fluxo completo
from airflow import DAG

# Importa o Operator customizado que criamos
from ibge_operator import IbgeOperator

# Importa datetime para definir a data de início
from datetime import datetime, timedelta

# ── Configurações ────────────────────────────────────────────────────────────
default_args = {
    "owner": "awanne",
    "depends_on_past": False,
    "email_on_failure": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

INDICADORES = [
    {
        "task_id": "busca_pib_trimestral",
        "tabela": 1621,
        "variavel": 584,
        "nome": "PIB Trimestral",
        "classificacao": None,
        "nivel_geo": "1",
        "localidade": "1",
        "periodo": "last 1",
    },
    {
        "task_id": "busca_ipca_alimentacao",
        "tabela": 7060,
        "variavel": 2265,
        "nome": "IPCA Alimentação",
        "classificacao": "315[1904]",
        "nivel_geo": "1",
        "localidade": "1",
        "periodo": "last 1",
    },
    {
        "task_id": "busca_ipca_habitacao",
        "tabela": 7060,
        "variavel": 2265,
        "nome": "IPCA Habitação",
        "classificacao": "315[1906]",
        "nivel_geo": "1",
        "localidade": "1",
        "periodo": "last 1",
    },
    {
        "task_id": "busca_ipca_transportes",
        "tabela": 7060,
        "variavel": 2265,
        "nome": "IPCA Transportes",
        "classificacao": "315[1912]",
        "nivel_geo": "1",
        "localidade": "1",
        "periodo": "last 1",
    },
    {
        "task_id": "busca_desemprego_sudeste",
        "tabela": 6381,
        "variavel": 4099,
        "nome": "Desemprego Sudeste",
        "classificacao": None,
        "nivel_geo": "2",
        "localidade": "3",
        "periodo": "last 1",
    },
    {
        "task_id": "busca_desemprego_nordeste",
        "tabela": 6381,
        "variavel": 4099,
        "nome": "Desemprego Nordeste",
        "classificacao": None,
        "nivel_geo": "2",
        "localidade": "2",
        "periodo": "last 1",
    },
    {
        "task_id": "busca_desemprego_norte",
        "tabela": 6381,
        "variavel": 4099,
        "nome": "Desemprego Norte",
        "classificacao": None,
        "nivel_geo": "2",
        "localidade": "1",
        "periodo": "last 1",
    },
    {
        "task_id": "busca_desemprego_sul",
        "tabela": 6381,
        "variavel": 4099,
        "nome": "Desemprego Sul",
        "classificacao": None,
        "nivel_geo": "2",
        "localidade": "4",
        "periodo": "last 1",
    },
    {
        "task_id": "busca_desemprego_centro_oeste",
        "tabela": 6381,
        "variavel": 4099,
        "nome": "Desemprego Centro-Oeste",
        "classificacao": None,
        "nivel_geo": "2",
        "localidade": "5",
        "periodo": "last 1",
    },
]

# ── DAG ──────────────────────────────────────────────────────────────────────
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
            classificacao=ind["classificacao"],
            nivel_geo=ind["nivel_geo"],
            localidade=ind["localidade"],
            modo="incremental",
            periodo=ind["periodo"],
        )
