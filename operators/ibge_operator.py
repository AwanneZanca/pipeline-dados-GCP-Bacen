# ============================================================
# Operator: IbgeOperator
# Descrição: Busca dados do IBGE (SIDRA) e salva no BigQuery
# Autor: Awanne Zanca
# ============================================================

# datetime: para converter datas e calcular o intervalo de 24 meses
# timedelta: para subtrair dias da data atual
from datetime import datetime, timedelta

# BaseOperator: classe base do Airflow — toda task customizada herda dela
from airflow.sdk import BaseOperator

# Hook customizado que faz a chamada à API do IBGE SIDRA
from ibge_hook import IbgeHook

# Logging padrão do Python — os logs aparecem na UI do Airflow (task logs)
import logging

# Cria um logger com o nome do módulo atual (ex: "operators.ibge_operator")
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
        Nome legível do indicador (ex: "PIB Trimestral").
    classificacao : str, opcional
        Filtro de classificação no formato SIDRA (ex: "315[all]").
    periodo : str
        Período no formato IBGE (ex: "last 8").
    nivel_geo : str
        Nível geográfico: "1" = Brasil, "2" = Região, "3" = UF.
    localidade : str
        Código da localidade (ex: "1" = Brasil).
    dataset_id : str
        Dataset do BigQuery (default: dados_economicos_bronze).
    table_id : str
        Tabela do BigQuery (default: ibge).
    modo : str
        'incremental' → últimos N períodos.
        'historico'   → busca 24 meses/trimestres completos.
    """

    template_fields = ("tabela", "nome_indicador")

    def __init__(
        self,
        tabela: int,
        variavel: int,
        nome_indicador: str,
        classificacao: str = None,
        periodo: str = "last 1",
        nivel_geo: str = "1",
        localidade: str = "1",
        dataset_id: str = "dados_economicos_bronze",
        table_id: str = "ibge",
        modo: str = "incremental",
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.tabela = tabela
        self.variavel = variavel
        self.nome_indicador = nome_indicador
        self.classificacao = classificacao
        self.periodo = periodo
        self.nivel_geo = nivel_geo
        self.localidade = localidade
        self.dataset_id = dataset_id
        self.table_id = table_id
        self.modo = modo

    def execute(self, context):
        # Importa BigQuery dentro do execute para evitar timeout no import da DAG
        from google.cloud import bigquery

        logger.info(
            f"[IbgeOperator] Iniciando '{self.nome_indicador}' "
            f"(tabela {self.tabela}) — modo: {self.modo}"
        )

        # Define período conforme modo
        if self.modo == "historico":
            # PIB usa trimestres, demais usam meses
            if self.tabela == 1621:
                periodo = IbgeHook.calcular_periodo_trimestral_24_meses()
            else:
                periodo = IbgeHook.calcular_periodo_24_meses()
        else:
            periodo = self.periodo

        # Instancia o hook com o período calculado
        hook = IbgeHook(
            tabela=self.tabela,
            variavel=self.variavel,
            nome_indicador=self.nome_indicador,
            classificacao=self.classificacao,
            periodo=periodo,
            nivel_geo=self.nivel_geo,
            localidade=self.localidade,
        )

        dados = hook.get_dados()

        if not dados:
            logger.warning(f"[IbgeOperator] Nenhum dado retornado para '{self.nome_indicador}'")
            return

        # Monta rows para BigQuery
        rows = []
        for item in dados:
            valor_raw = item.get("V", "")

            # Ignora linhas sem valor numérico (ex: cabeçalhos residuais)
            if not valor_raw or valor_raw in ["...", "-", "X"]:
                continue

            try:
                valor = float(valor_raw.replace(",", "."))
            except ValueError:
                logger.warning(f"[IbgeOperator] Valor inválido ignorado: {valor_raw}")
                continue

            rows.append(
                {
                    "periodo": item.get("D2C", item.get("D3C", "")),
                    "periodo_desc": item.get("D2N", item.get("D3N", "")),
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

        # Envia para BigQuery
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
