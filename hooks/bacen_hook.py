# ============================================================
# Hook: BacenHook
# Descrição: Gerencia a conexão com a API pública do Banco
#            Central do Brasil (BACEN)
# Documentação: https://dadosabertos.bcb.gov.br/
# Autor: Awanne Zanca
# ============================================================

# datetime: para converter o formato de data DD/MM/AAAA → AAAA-MM-DD
# timedelta: para calcular a data de 24 meses atrás
from datetime import datetime, timedelta

# BaseHook: classe base do Airflow — todo hook customizado herda dela
from airflow.sdk.bases.hook import BaseHook

# Logging padrão do Python — os logs aparecem na UI do Airflow (task logs)
import logging

# Cria um logger com o nome do módulo atual
logger = logging.getLogger(__name__)


class BacenHook(BaseHook):
    """
    Hook para consumir a API de Séries Temporais do Banco Central do Brasil.
    """

    conn_name_attr = "bacen_default"
    default_conn_name = "bacen_default"
    conn_type = "http"
    hook_name = "BACEN API"

    BASE_URL = "https://api.bcb.gov.br/dados/serie/bcdata.sgs.{serie}/dados"

    def __init__(
        self,
        serie: int,
        registros: int = 1,
        data_inicio: str = None,
        data_fim: str = None,
    ):
        super().__init__()
        self.serie = serie
        self.registros = registros
        self.data_inicio = data_inicio
        self.data_fim = data_fim or datetime.today().strftime("%d/%m/%Y")

    def get_dados(self) -> list[dict]:
        """Busca os dados da série temporal."""
        # Importa requests dentro do método para evitar timeout no import da DAG
        import requests

        if self.data_inicio:
            url = self.BASE_URL.format(serie=self.serie)
            params = {
                "formato": "json",
                "dataInicial": self.data_inicio,
                "dataFinal": self.data_fim,
            }
            logger.info(f"[BacenHook] Buscando série {self.serie} de {self.data_inicio} até {self.data_fim}")
        else:
            url = self.BASE_URL.format(serie=self.serie) + f"/ultimos/{self.registros}"
            params = {"formato": "json"}
            logger.info(f"[BacenHook] Buscando últimos {self.registros} registros da série {self.serie}")

        response = requests.get(url, params=params, timeout=30)
        response.raise_for_status()

        dados = response.json()
        logger.info(f"[BacenHook] {len(dados)} registros recebidos para série {self.serie}")
        return dados

    @staticmethod
    def calcular_data_inicio_12_meses() -> str:
        """Retorna a data de 12 meses atrás no formato DD/MM/AAAA."""
        data = datetime.today() - timedelta(days=365)
        return data.strftime("%d/%m/%Y")
