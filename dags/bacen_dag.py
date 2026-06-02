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

# ============================================================
# Definição dos indicadores que serão buscados
# Cada indicador vira uma task independente na DAG
# ============================================================
INDICADORES = [
    {"task_id": "busca_selic",      "serie": 11,    "nome": "Taxa Selic"},
    {"task_id": "busca_ipca",       "serie": 433,   "nome": "IPCA"},
    {"task_id": "busca_igpm",       "serie": 189,   "nome": "IGP-M"},
    {"task_id": "busca_dolar",      "serie": 1,     "nome": "USD/BRL"},
    {"task_id": "busca_euro",       "serie": 21619, "nome": "EUR/BRL"},
    {"task_id": "busca_desemprego", "serie": 7326,  "nome": "Desemprego"},
    {"task_id": "busca_credito",    "serie": 4189,  "nome": "Credito Total"},
]

# ============================================================
# Definição da DAG
# ============================================================
with DAG(
    dag_id="painel_economico_brasil",   # Nome único da DAG no Airflow
    start_date=datetime(2026, 1, 1),    # Data a partir de quando a DAG é válida
    schedule="@daily",                  # Roda uma vez por dia à meia noite UTC
    catchup=False,                      # Não reprocessa datas passadas
    tags=["bacen", "financeiro",        # Tags para organizar na UI do Airflow
          "painel", "brasil"]
) as dag:

    # Cria uma task para cada indicador dinamicamente
    # Cada task roda de forma independente — se uma falhar, as outras continuam
    tasks = []
    for indicador in INDICADORES:
        task = BacenOperator(
            task_id=indicador["task_id"],       # Nome único da task
            serie=indicador["serie"],           # Código da série no BACEN
            nome_indicador=indicador["nome"],   # Nome amigável pro log
        )
        tasks.append(task)

    # Define a ordem de execução — todas rodam em paralelo
    # Se quiser sequencial seria: tasks[0] >> tasks[1] >> tasks[2] ...
    # Em paralelo não precisa definir dependências — o Airflow roda todas ao mesmo tempo