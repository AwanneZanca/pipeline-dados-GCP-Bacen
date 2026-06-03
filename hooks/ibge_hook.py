# ============================================================
# Hook: IbgeHook
# Descrição: Gerencia a conexão com a API pública do IBGE
#            (SIDRA — Sistema IBGE de Recuperação Automática)
# Documentação: https://apisidra.ibge.gov.br/
# Autor: Awanne Zanca
# ============================================================

# datetime: para converter o formato de data e calcular intervalos
# timedelta: para calcular a data de 24 meses atrás
from datetime import datetime, timedelta

# BaseHook: classe base do Airflow — todo hook customizado herda dela
from airflow.hooks.base import BaseHook

# Logging padrão do Python — os logs aparecem na UI do Airflow (task logs)
import logging

# Cria um logger com o nome do módulo atual
logger = logging.getLogger(__name__)


class IbgeHook(BaseHook):
    """
    Hook para consumir a API SIDRA do IBGE.
    """

    conn_name_attr = "ibge_default"
    default_conn_name = "ibge_default"
    conn_type = "http"
    hook_name = "IBGE SIDRA API"

    BASE_URL = "https://apisidra.ibge.gov.br/values"

    def __init__(
        self,
        tabela: int,
        variavel: int,
        nome_indicador: str,
        classificacao: str = None,
        periodo: str = "last 8",
        nivel_geo: str = "1",
        localidade: str = "1",
    ):
        super().__init__()
        self.tabela = tabela
        self.variavel = variavel
        self.nome_indicador = nome_indicador
        self.classificacao = classificacao
        self.periodo = periodo
        self.nivel_geo = nivel_geo
        self.localidade = localidade

    def get_dados(self) -> list[dict]:
        """Busca os dados da tabela SIDRA."""
        # Importa requests dentro do método para evitar timeout no import da DAG
        import requests

        url = f"{self.BASE_URL}/t/{self.tabela}/n{self.nivel_geo}/{self.localidade}/v/{self.variavel}/p/{self.periodo}"

        if self.classificacao:
            url += f"/c{self.classificacao}"

        url += "?formato=json"

        logger.info(f"[IbgeHook] Buscando tabela {self.tabela} — {self.nome_indicador}")

        response = requests.get(url, timeout=30)
        response.raise_for_status()

        raw = response.json()

        # Remove a primeira linha que é o cabeçalho da API SIDRA
        dados = raw[1:] if raw else []

        logger.info(f"[IbgeHook] {len(dados)} registros recebidos para '{self.nome_indicador}'")
        return dados

    @staticmethod
    def calcular_periodo_24_meses() -> str:
        """Retorna o período dos últimos 24 meses no formato IBGE."""
        hoje = datetime.today()
        inicio = hoje - timedelta(days=730)
        return f"{inicio.strftime('%Y%m')}-{hoje.strftime('%Y%m')}"

    @staticmethod
    def calcular_periodo_trimestral_24_meses() -> str:
        """Retorna os últimos 8 trimestres no formato IBGE."""
        return "last 8"
