# ============================================================
# Hook: BacenHook
# Descrição: Gerencia a conexão com a API pública do Banco
#            Central do Brasil (BACEN)
# Documentação: https://dadosabertos.bcb.gov.br/
# Autor: Awanne Zanca
# ============================================================

# BaseHook é a classe base do Airflow para todos os hooks
from airflow.hooks.base import BaseHook

# urllib.request para fazer requisições HTTP sem dependências externas
import urllib.request

# json para converter a resposta da API em dicionário Python
import json


class BacenHook(BaseHook):
    """
    Hook para conexão com a API pública do Banco Central do Brasil.

    A API do BACEN disponibiliza séries históricas de indicadores
    econômicos gratuitamente, sem necessidade de autenticação.

    Séries disponíveis:
        11    → Taxa Selic
        433   → IPCA (inflação oficial)
        189   → IGP-M (inflação mercado)
        1     → USD/BRL (dólar comercial)
        21619 → EUR/BRL (euro)
        7326  → Desemprego
        4189  → Crédito total
    """

    # URL base da API do BACEN
    BASE_URL = "https://api.bcb.gov.br/dados/serie/bcdata.sgs.{serie}/dados/ultimos/{registros}?formato=json"

    def __init__(self, serie: int, registros: int = 1):
        """
        Inicializa o hook com a série e quantidade de registros.

        Args:
            serie: Código da série histórica do BACEN
            registros: Quantidade de registros a buscar (padrão: 1)
        """
        super().__init__()
        self.serie = serie
        self.registros = registros

    def get_dados(self) -> list:
        """
        Busca os dados da série na API do BACEN.

        Returns:
            list: Lista de registros com 'data' e 'valor'
            Exemplo: [{"data": "01/06/2026", "valor": "10.50"}]
        """
        # Monta a URL com a série e quantidade de registros
        url = self.BASE_URL.format(
            serie=self.serie,
            registros=self.registros
        )

        # Faz a requisição e retorna os dados
        with urllib.request.urlopen(url) as response:
            data = json.loads(response.read())
            return data