# ============================================================
# DAG: painel_economico_brasil
# Descrição: Busca os principais indicadores econômicos do
#            Brasil via API do Banco Central (BACEN)
# Agendamento: diário (@daily)
# Autor: Awanne Zanca
# ============================================================

# Importa a classe DAG — define o fluxo completo
from airflow import DAG

# Importa o Operator customizado que criamos
from bacen_operator import BacenOperator

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
    {"task_id": "busca_selic",      "serie": 11,    "nome": "Taxa Selic"},
    {"task_id": "busca_ipca",       "serie": 433,   "nome": "IPCA"},
    {"task_id": "busca_igpm",       "serie": 189,   "nome": "IGP-M"},
    {"task_id": "busca_dolar",      "serie": 1,     "nome": "USD/BRL"},
    {"task_id": "busca_euro",       "serie": 21619, "nome": "EUR/BRL"},
    {"task_id": "busca_desemprego", "serie": 7326,  "nome": "Desemprego"},
    {"task_id": "busca_credito",    "serie": 4189,  "nome": "Crédito Total"},
]

# ── DAG ──────────────────────────────────────────────────────────────────────
with DAG(
    dag_id="painel_economico_bacen",
    description="Indicadores econômicos do Brasil — API BACEN → BigQuery Bronze",
    schedule="@daily",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["bacen", "financeiro", "brasil", "painel", "bronze"],
    default_args=default_args,
    params={"modo_historico": False},
) as dag:

    for ind in INDICADORES:
        BacenOperator(
            task_id=ind["task_id"],
            serie=ind["serie"],
            nome_indicador=ind["nome"],
            # Modo controlado via DAG params ao disparar manualmente
            modo="incremental",
            registros=1,
        )
