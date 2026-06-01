from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

def busca_taxa_selic():
    import urllib.request
    import json
    url = "https://api.bcb.gov.br/dados/serie/bcdata.sgs.11/dados/ultimos/1?formato=json"
    with urllib.request.urlopen(url) as response:
        data = json.loads(response.read())
        print(f"Taxa Selic: {data[0]['valor']}%")
        return data

with DAG(
    dag_id="bacen_selic",
    start_date=datetime(2026, 1, 1),
    schedule="@daily",
    catchup=False,
    tags=["bacen", "financeiro"]
) as dag:

    busca_selic = PythonOperator(
        task_id="busca_taxa_selic",
        python_callable=busca_taxa_selic
    )
    