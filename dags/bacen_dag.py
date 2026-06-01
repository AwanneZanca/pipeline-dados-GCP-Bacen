# ============================================================
# DAG: bacen_selic
# Descrição: Busca a taxa Selic mais recente da API do Banco
#            Central do Brasil (BACEN) e imprime no log
# Agendamento: diário (@daily)
# Autor: Awanne Zanca
# ============================================================

# Importa a classe DAG — é ela que define o fluxo completo
from airflow import DAG

# Importa o PythonOperator — permite executar funções Python como tasks
from airflow.operators.python import PythonOperator

# Importa datetime para definir a data de início da DAG
from datetime import datetime


def busca_taxa_selic():
    """
    Função que busca a taxa Selic mais recente na API do BACEN.

    A API do Banco Central disponibiliza séries históricas de dados.
    A série 11 corresponde à Taxa Selic.
    O parâmetro 'ultimos/1' retorna apenas o último registro disponível.

    Retorna:
        list: Lista com o último registro da taxa Selic
              Exemplo: [{"data": "01/06/2026", "valor": "10.50"}]
    """
    # urllib.request é a biblioteca nativa do Python para fazer requisições HTTP
    import urllib.request

    # json é a biblioteca nativa para converter texto JSON em dicionário Python
    import json

    # URL da API do BACEN — série 11 = Taxa Selic, últimos 1 registro
    url = "https://api.bcb.gov.br/dados/serie/bcdata.sgs.11/dados/ultimos/1?formato=json"

    # Faz a requisição HTTP e lê a resposta
    with urllib.request.urlopen(url) as response:

        # Converte o JSON retornado em lista de dicionários Python
        data = json.loads(response.read())

        # Imprime o valor no log do Airflow
        print(f"Taxa Selic: {data[0]['valor']}%")

        # Retorna os dados — pode ser usado por outras tasks via XCom
        return data


# Define a DAG — bloco principal que organiza as tasks
with DAG(
    dag_id="bacen_selic",            # Nome único da DAG no Airflow
    start_date=datetime(2026, 1, 1), # Data a partir de quando a DAG é válida
    schedule="@daily",               # Roda uma vez por dia à meia noite UTC
    catchup=False,                   # Não reprocessa datas passadas
    tags=["bacen", "financeiro"]     # Tags para organizar na UI do Airflow
) as dag:

    # PythonOperator → executa qualquer função Python como uma task
    busca_selic = PythonOperator(
        task_id="busca_taxa_selic",       # Nome único da task dentro da DAG
        python_callable=busca_taxa_selic  # Função que será executada
    )

    # Quando tiver mais tasks, a ordem seria:
    # busca_selic >> transforma_dados >> salva_no_bigquery