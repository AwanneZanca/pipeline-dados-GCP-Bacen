# ============================================================
# Operator: BacenOperator
# Descrição: Operator customizado que usa o BacenHook para
#            buscar indicadores econômicos do BACEN
# Autor: Awanne Zanca
# ============================================================

# BaseOperator é a classe base do Airflow para todos os operators
from airflow.models import BaseOperator

# Importa o Hook que criamos — ele sabe se conectar ao BACEN
from hooks.bacen_hook import BacenHook

# Logging para registrar informações no log do Airflow
import logging

# Instancia o logger para registrar mensagens
log = logging.getLogger(__name__)


class BacenOperator(BaseOperator):
    """
    Operator customizado para buscar indicadores econômicos do BACEN.

    Encapsula o BacenHook numa task reutilizável — qualquer DAG
    pode usar esse operator sem precisar conhecer os detalhes
    de como a conexão com o BACEN funciona.

    Exemplo de uso na DAG:
        busca_selic = BacenOperator(
            task_id="busca_selic",
            serie=11,
            nome_indicador="Taxa Selic"
        )
    """

    def __init__(self, serie: int, nome_indicador: str, registros: int = 1, **kwargs):
        """
        Inicializa o operator.

        Args:
            serie: Código da série histórica do BACEN
            nome_indicador: Nome amigável do indicador (para o log)
            registros: Quantidade de registros a buscar (padrão: 1)
        """
        # Chama o construtor da classe pai (BaseOperator)
        super().__init__(**kwargs)
        self.serie = serie
        self.nome_indicador = nome_indicador
        self.registros = registros

    def execute(self, context):
        """
        Método principal executado pelo Airflow quando a task roda.

        O Airflow sempre chama o método execute() de um Operator.
        Aqui instanciamos o Hook e buscamos os dados.

        Args:
            context: Contexto da execução (data, run_id, etc.)

        Returns:
            list: Dados retornados pela API do BACEN
        """
        # Instancia o Hook com a série e quantidade de registros
        hook = BacenHook(serie=self.serie, registros=self.registros)

        # Busca os dados via Hook
        dados = hook.get_dados()

        # Registra o resultado no log do Airflow
        for registro in dados:
            log.info(f"{self.nome_indicador}: {registro['valor']} ({registro['data']})")

        # Retorna os dados — ficam disponíveis para outras tasks via XCom
        return dados