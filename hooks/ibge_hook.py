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
# Integra com o sistema de Connections do Airflow (Admin → Connections)
from airflow.hooks.base import BaseHook

# requests: biblioteca HTTP para fazer as chamadas à API REST do IBGE
import requests

# Logging padrão do Python — os logs aparecem na UI do Airflow (task logs)
import logging

# Cria um logger com o nome do módulo atual (ex: "hooks.ibge_hook")
logger = logging.getLogger(__name__)


class IbgeHook(BaseHook):
    """
    Hook para consumir a API SIDRA do IBGE.

    Parâmetros
    ----------
    tabela : int
        Código da tabela SIDRA (ex: 1621 = PIB, 7060 = IPCA por categoria)
    variavel : int
        Código da variável dentro da tabela
    classificacao : str, opcional
        Filtro de classificação no formato "cod[val1,val2]" (ex: "315[all]")
    periodo : str
        Período no formato IBGE (ex: "202001-202412" ou "last 8")
    nivel_geo : str
        Nível geográfico: "1" = Brasil, "2" = Região, "3" = UF
    localidade : str
        Código da localidade (ex: "1" = Brasil, "1,2,3,4,5" = todas regiões)
    nome_indicador : str
        Nome legível do indicador para rastreabilidade
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
        """
        Busca os dados da tabela SIDRA.

        Retorna lista de dicts padronizados com chaves:
        'periodo', 'valor', 'indicador', 'tabela', 'localidade', 'nivel_geo'
        """
        # Monta URL no padrão SIDRA
        # Exemplo: /t/1621/n1/1/v/584/p/last 8/d/v584 2
        url = f"{self.BASE_URL}/t/{self.tabela}/n{self.nivel_geo}/{self.localidade}/v/{self.variavel}/p/{self.periodo}"

        if self.classificacao:
            url += f"/c{self.classificacao}"

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
    def calcular_periodo_24_meses() -> str:
        """Retorna o período dos últimos 24 meses no formato IBGE (AAAAMM-AAAAMM)."""
        hoje = datetime.today()
        inicio = hoje - timedelta(days=730)
        return f"{inicio.strftime('%Y%m')}-{hoje.strftime('%Y%m')}"

    @staticmethod
    def calcular_periodo_trimestral_24_meses() -> str:
        """Retorna os últimos 8 trimestres (2 anos) no formato IBGE."""
        return "last 8"
