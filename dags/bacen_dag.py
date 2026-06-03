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
from datetime import datetime

# Importa Variable para configurar o modo histórico via Airflow UI
from airflow.models import Variable

# ── Configurações ────────────────────────────────────────────────────────────
MODO_HISTORICO = Variable.get("MODO_HISTORICO", default_var="false").lower() == "true"
 
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
    dag_id="painel_economico_brasil",
    description="Indicadores econômicos do Brasil — API BACEN → BigQuery Bronze",
    schedule="@daily",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["bacen", "financeiro", "brasil", "painel", "bronze"],
    default_args=default_args,
    doc_md=__doc__,
) as dag:
 
    for ind in INDICADORES:
        BacenOperator(
            task_id=ind["task_id"],
            serie=ind["serie"],
            nome_indicador=ind["nome"],
            # Modo histórico: busca 24 meses. Modo incremental: último registro.
            modo="historico" if MODO_HISTORICO else "incremental",
            registros=1,
        )
 
    # Nota: As tasks são independentes (sem dependências entre si).
    # O dbt é acionado via Jenkins após o push, não diretamente aqui.