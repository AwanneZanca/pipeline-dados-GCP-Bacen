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