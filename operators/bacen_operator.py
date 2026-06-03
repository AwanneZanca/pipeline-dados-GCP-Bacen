# ============================================================
# Operator: BacenOperator
# Descrição: Busca dados do BACEN e salva no BigQuery
# Autor: Awanne Zanca
# ============================================================

# datetime: para converter datas e calcular o intervalo de 24 meses
# timedelta: para subtrair dias da data atual (usado em calcular_data_inicio_24_meses)
from datetime import datetime, timedelta

# BaseOperator: classe base do Airflow — toda task customizada herda dela
from airflow.models import BaseOperator

# Cliente oficial do Google para interagir com o BigQuery
from google.cloud import bigquery

# Hook customizado que faz a chamada à API do BACEN
from bacen_hook import BacenHook

# Logging padrão do Python — os logs aparecem na UI do Airflow (task logs)
import logging

# Cria um logger com o nome do módulo atual (ex: "operators.bacen_operator")
logger = logging.getLogger(__name__)

class BacenOperator(BaseOperator):
    """
    Operator que busca dados do BACEN e salva na camada Bronze do BigQuery.

    Parâmetros
    ----------
    serie : int
        Código da série temporal no BACEN.
    nome_indicador : str
        Nome legível do indicador (ex: "Taxa Selic").
    dataset_id : str
        Dataset do BigQuery (default: dados_economicos_bronze).
    table_id : str
        Tabela do BigQuery (default: indicadores).
    modo : str
        'incremental' → últimos N registros (padrão: 1).
        'historico'   → busca 24 meses completos.
    registros : int
        Usado apenas no modo incremental.
    """

    template_fields = ("serie", "nome_indicador")

    def __init__(
        self,
        serie: int,
        nome_indicador: str,
        dataset_id: str = "dados_economicos_bronze",
        table_id: str = "indicadores",
        modo: str = "incremental",
        registros: int = 1,
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.serie = serie
        self.nome_indicador = nome_indicador
        self.dataset_id = dataset_id
        self.table_id = table_id
        self.modo = modo
        self.registros = registros

    def execute(self, context):
        logger.info(
            f"[BacenOperator] Iniciando '{self.nome_indicador}' "
            f"(série {self.serie}) — modo: {self.modo}"
        )

        # Instancia hook conforme modo
        if self.modo == "historico":
            data_inicio = BacenHook.calcular_data_inicio_24_meses()
            hook = BacenHook(
                serie=self.serie,
                data_inicio=data_inicio,
            )
        else:
            hook = BacenHook(serie=self.serie, registros=self.registros)

        dados = hook.get_dados()

        if not dados:
            logger.warning(f"[BacenOperator] Nenhum dado retornado para série {self.serie}")
            return

        # Monta rows para BigQuery
        rows = []
        for item in dados:
            rows.append(
                {
                    "data_referencia": self._parse_data(item["data"]),
                    "valor": float(item["valor"]),
                    "indicador": self.nome_indicador,
                    "serie": self.serie,
                    "ingestao_ts": datetime.utcnow().isoformat(),
                }
            )

        # Envia para BigQuery
        client = bigquery.Client()
        table_ref = f"{client.project}.{self.dataset_id}.{self.table_id}"

        errors = client.insert_rows_json(table_ref, rows)
        if errors:
            raise ValueError(f"[BacenOperator] Erro ao inserir no BigQuery: {errors}")

        logger.info(
            f"[BacenOperator] {len(rows)} registros inseridos em {table_ref} "
            f"para '{self.nome_indicador}'"
        )
        return len(rows)

    @staticmethod
    def _parse_data(data_str: str) -> str:
        """Converte 'DD/MM/AAAA' → 'AAAA-MM-DD' (formato BigQuery DATE)."""
        try:
            return datetime.strptime(data_str, "%d/%m/%Y").strftime("%Y-%m-%d")
        except ValueError:
            return data_str