# ============================================================
# DAG: dbt_pipeline
# Descrição: Roda o dbt após o pipeline do BACEN e IBGE
#            terminarem. Atualiza Silver e Gold automaticamente.
# Agendamento: diário (@daily)
# Autor: Awanne Zanca
# ============================================================

# Importa a classe DAG — define o fluxo completo
from airflow import DAG

# Importa o Operator para rodar comandos bash (usado para rodar dbt)
from airflow.operators.bash import BashOperator

# Importa o Sensor para esperar outra DAG terminar (BACEN e IBGE)
from airflow.sensors.external_task import ExternalTaskSensor

# Importa datetime para definir a data de início
from datetime import datetime

with DAG(
    dag_id="dbt_pipeline",
    start_date=datetime(2026, 1, 1),
    schedule="@daily",
    catchup=False,
    tags=["dbt", "medallion"]
) as dag:

    # Espera o painel_economico_brasil (BACEN) terminar
    aguarda_bacen = ExternalTaskSensor(
        task_id="aguarda_bacen",
        external_dag_id="painel_economico_brasil",
        timeout=3600,  # espera até 1 hora
        poke_interval=30,  # verifica a cada 30 segundos
        mode="poke"
    )

    # Espera o painel_economico_ibge (IBGE) terminar
    aguarda_ibge = ExternalTaskSensor(
        task_id="aguarda_ibge",
        external_dag_id="painel_economico_ibge",
        timeout=3600,  # espera até 1 hora
        poke_interval=30,  # verifica a cada 30 segundos
        mode="poke"
    )

    # Roda todos os models dbt após ambas as DAGs terminarem
    dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command="cd /opt/airflow/dbt_bacen && ~/.local/bin/dbt run --profiles-dir /opt/airflow/dbt_profiles"
    )

    # Roda os testes de qualidade após o dbt run
    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command="cd /opt/airflow/dbt_bacen && ~/.local/bin/dbt test --profiles-dir /opt/airflow/dbt_profiles"
    )

    # Ambas as DAGs precisam terminar antes do dbt rodar
    [aguarda_bacen, aguarda_ibge] >> dbt_run >> dbt_test