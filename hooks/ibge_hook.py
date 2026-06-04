# ============================================================
# Hook: IbgeHook
# Descrição: Gerencia a conexão com a API pública do IBGE
#            (SIDRA — Sistema IBGE de Recuperação Automática)
# Documentação: https://apisidra.ibge.gov.br/
# Autor: Awanne Zanca
# ============================================================

from datetime import datetime, timedelta
from airflow.hooks.base import BaseHook
import logging

logger = logging.getLogger(__name__)


class IbgeHook(BaseHook):
    """
    Hook para consumir a API SIDRA do IBGE.
    
    Parâmetros
    ----------
    tabela : int
        Código da tabela SIDRA
    variavel : int
        Código da variável dentro da tabela
    nome_indicador : str
        Nome legível do indicador
    classificacao_cod : str, opcional
        Código da classificação (ex: "315")
    classificacao_cat : str, opcional
        Código da categoria (ex: "1906")
    periodo : str
        Período no formato IBGE (ex: "last 1")
    nivel_geo : str
        Nível geográfico: "1" = Brasil, "2" = Região
    localidade : str
        Código da localidade
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
        classificacao_cod: str = None,
        classificacao_cat: str = None,
        periodo: str = "last 1",
        nivel_geo: str = "1",
        localidade: str = "all",
    ):
        super().__init__()
        self.tabela = tabela
        self.variavel = variavel
        self.nome_indicador = nome_indicador
        self.classificacao_cod = classificacao_cod
        self.classificacao_cat = classificacao_cat
        self.periodo = periodo
        self.nivel_geo = nivel_geo
        self.localidade = localidade

    def get_dados(self) -> list[dict]:
        """Busca os dados da tabela SIDRA."""
        # Importa requests dentro do método para evitar timeout no import da DAG
        import requests

        # Monta URL no padrão SIDRA: /t/{tabela}/n{nivel}/{localidade}/v/{variavel}/p/{periodo}
        url = (
            f"{self.BASE_URL}"
            f"/t/{self.tabela}"
            f"/n{self.nivel_geo}/{self.localidade}"
            f"/v/{self.variavel}"
            f"/p/{self.periodo}"
        )

        # Adiciona classificação se informada: /c{cod}/{cat}
        if self.classificacao_cod and self.classificacao_cat:
            url += f"/c{self.classificacao_cod}/{self.classificacao_cat}"

        url += "?formato=json"

        logger.info(f"[IbgeHook] Buscando tabela {self.tabela} — {self.nome_indicador}")
        logger.info(f"[IbgeHook] URL: {url}")

        response = requests.get(url, timeout=30)
        response.raise_for_status()

        raw = response.json()

        # Remove a primeira linha que é o cabeçalho da API SIDRA
        dados = raw[1:] if raw else []

        logger.info(f"[IbgeHook] {len(dados)} registros recebidos para '{self.nome_indicador}'")
        return dados

    @staticmethod
    def calcular_periodo_12_meses() -> str:
        """Retorna o período dos últimos 12 meses no formato IBGE."""
        hoje = datetime.today()
        inicio = hoje - timedelta(days=365)
        return f"{inicio.strftime('%Y%m')}-{hoje.strftime('%Y%m')}"

    @staticmethod
    def calcular_periodo_trimestral_12_meses() -> str:
        """Retorna os últimos 4 trimestres no formato IBGE."""
        return "last4"
