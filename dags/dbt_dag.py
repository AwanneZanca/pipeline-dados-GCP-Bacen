# ============================================================
# DAG: dbt_pipeline
# Descrição: Roda o dbt diariamente após o pipeline do BACEN.
#            Atualiza Silver e Gold automaticamente.
# Agendamento: diário às 00:30 (30 min após o BACEN)
# Autor: Awanne Zanca
# ============================================================

from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

with DAG(
    dag_id="dbt_pipeline",
    start_date=datetime(2026, 1, 1),
    schedule="30 0 * * *",  # roda às 00:30 — após o BACEN (00:00)
    catchup=False,
    tags=["dbt", "medallion"]
) as dag:

    # Roda todos os models dbt
    dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command="cd /opt/airflow/dbt_bacen && ~/.local/bin/dbt run --profiles-dir /opt/airflow/dbt_profiles --no-partial-parse"
    )

    # Roda os testes de qualidade após o dbt run
    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command="cd /opt/airflow/dbt_bacen && ~/.local/bin/dbt test --profiles-dir /opt/airflow/dbt_profiles --no-partial-parse"
    )

    dbt_run >> dbt_test