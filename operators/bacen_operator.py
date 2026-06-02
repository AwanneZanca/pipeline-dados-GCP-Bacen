# ============================================================
# Operator: BacenOperator
# Descrição: Busca dados do BACEN e salva no BigQuery
# Autor: Awanne Zanca
# ============================================================

from airflow.models import BaseOperator
from bacen_hook import BacenHook
from google.cloud import bigquery
from google.oauth2 import service_account
import logging
import os

log = logging.getLogger(__name__)

class BacenOperator(BaseOperator):
    """
    Operator que busca indicadores do BACEN e salva no BigQuery.
    """

    def __init__(self, serie: int, nome_indicador: str, registros: int = 1, **kwargs):
        super().__init__(**kwargs)
        self.serie = serie
        self.nome_indicador = nome_indicador
        self.registros = registros

    def execute(self, context):
        # Busca dados via Hook
        hook = BacenHook(serie=self.serie, registros=self.registros)
        dados = hook.get_dados()

        for registro in dados:
            log.info(f"{self.nome_indicador}: {registro['valor']} ({registro['data']})")

        # Salva no BigQuery
        credentials = service_account.Credentials.from_service_account_file(
            '/opt/airflow/gcp_credentials.json'
        )

        client = bigquery.Client(
            credentials=credentials,
            project='voltaic-reducer-396401'
        )

        # Prepara os dados para inserção
        rows = [{
            "data": registro['data'],
            "valor": float(registro['valor'].replace(',', '.')),
            "indicador": self.nome_indicador,
            "serie": self.serie,
            "inserted_at": datetime.now(timezone.utc).isoformat()
        } for registro in dados]

        # Insere no BigQuery
        table_id = "voltaic-reducer-396401.dados_economicos_bronze.indicadores"
        errors = client.insert_rows_json(table_id, rows)

        if errors:
            log.error(f"Erros ao inserir no BigQuery: {errors}")
        else:
            log.info(f"✅ {len(rows)} registro(s) inserido(s) no BigQuery!")

        return dados