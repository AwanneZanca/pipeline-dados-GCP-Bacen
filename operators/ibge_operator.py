# ============================================================
# Operator: IbgeOperator
# Descrição: Busca dados do IBGE (SIDRA) e salva no BigQuery
# Autor: Awanne Zanca
# ============================================================

from datetime import datetime, timedelta
from airflow.sdk import BaseOperator
from ibge_hook import IbgeHook
import logging

logger = logging.getLogger(__name__)


class IbgeOperator(BaseOperator):
    """
    Operator que busca dados do IBGE (SIDRA) e salva na camada Bronze do BigQuery.

    Parâmetros
    ----------
    tabela : int
        Código da tabela SIDRA.
    variavel : int
        Código da variável dentro da tabela.
    nome_indicador : str
        Nome legível do indicador.
    classificacao_cod : str, opcional
        Código da classificação SIDRA (ex: "315" para grupos do IPCA).
    classificacao_cat : str, opcional
        Código da categoria (ex: "1906" para Habitação).
    periodo : str
        Período no formato IBGE (ex: "last 1").
    nivel_geo : str
        Nível geográfico: "1" = Brasil, "2" = Região.
    localidade : str
        Código da localidade.
    dataset_id : str
        Dataset do BigQuery.
    table_id : str
        Tabela do BigQuery.
    modo : str
        'incremental' → último período. 'historico' → 24 meses.
    """

    template_fields = ("tabela", "nome_indicador")

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
        dataset_id: str = "dados_economicos_bronze",
        table_id: str = "ibge",
        modo: str = "incremental",
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.tabela = tabela
        self.variavel = variavel
        self.nome_indicador = nome_indicador
        self.classificacao_cod = classificacao_cod
        self.classificacao_cat = classificacao_cat
        self.periodo = periodo
        self.nivel_geo = nivel_geo
        self.localidade = localidade
        self.dataset_id = dataset_id
        self.table_id = table_id
        self.modo = modo

    def execute(self, context):
        from google.cloud import bigquery

        logger.info(
            f"[IbgeOperator] Iniciando '{self.nome_indicador}' "
            f"(tabela {self.tabela}) — modo: {self.modo}"
        )

        # Define período conforme modo
        if self.modo == "historico":
            periodo = (
                IbgeHook.calcular_periodo_trimestral_12_meses()
                if self.tabela in (1621, 6468)
                else IbgeHook.calcular_periodo_12_meses()
            )
        else:
            periodo = self.periodo

        hook = IbgeHook(
            tabela=self.tabela,
            variavel=self.variavel,
            nome_indicador=self.nome_indicador,
            classificacao_cod=self.classificacao_cod,
            classificacao_cat=self.classificacao_cat,
            periodo=periodo,
            nivel_geo=self.nivel_geo,
            localidade=self.localidade,
        )

        dados = hook.get_dados()

        if not dados:
            logger.warning(f"[IbgeOperator] Nenhum dado retornado para '{self.nome_indicador}'")
            return

        rows = []
        for item in dados:
            valor_raw = item.get("V", "")

            if not valor_raw or valor_raw in ["...", "-", "X"]:
                continue

            try:
                valor = float(valor_raw.replace(",", "."))
            except ValueError:
                logger.warning(f"[IbgeOperator] Valor inválido ignorado: {valor_raw}")
                continue

            rows.append(
                {
                    "periodo": item.get("D3C", ""),                
                    "periodo_desc": item.get("D3N", ""),
                    "valor": valor,
                    "indicador": self.nome_indicador,
                    "tabela": self.tabela,
                    "variavel": self.variavel,
                    "localidade_cod": item.get("D1C", "1"),
                    "localidade_nome": item.get("D1N", "Brasil"),
                    "nivel_geo": self.nivel_geo,
                    "classificacao": item.get("D4N", ""),
                    "inserted_at": datetime.utcnow().isoformat(),
                }
            )

        if not rows:
            logger.warning(f"[IbgeOperator] Nenhuma linha válida para '{self.nome_indicador}'")
            return

        client = bigquery.Client()
        table_ref = f"{client.project}.{self.dataset_id}.{self.table_id}"

        errors = client.insert_rows_json(table_ref, rows)
        if errors:
            raise ValueError(f"[IbgeOperator] Erro ao inserir no BigQuery: {errors}")

        logger.info(
            f"[IbgeOperator] {len(rows)} registros inseridos em {table_ref} "
            f"para '{self.nome_indicador}'"
        )
        return len(rows)
