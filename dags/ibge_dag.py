# ============================================================
# DAG: painel_economico_ibge
# Descrição: Busca indicadores macroeconômicos do IBGE (SIDRA)
#            e salva no BigQuery camada Bronze.
#            Complementa o painel_economico_brasil (BACEN).
# Agendamento: semanal (@weekly) — dados IBGE têm menor frequência
# Autor: Awanne Zanca
# ============================================================

# Importa a classe DAG — define o fluxo completo
from airflow import DAG

# Importa o Operator customizado que criamos
from ibge_operator import IbgeOperator

# Importa datetime para definir a data de início
from datetime import datetime, timedelta

# Importa Variable para configurar o modo histórico via Airflow UI
from airflow.models import Variable

# ── Configurações ────────────────────────────────────────────────────────────

# Controla se a DAG busca histórico completo ou apenas o período mais recente
# Para carregar histórico: Admin → Variables → MODO_HISTORICO_IBGE = true
MODO_HISTORICO = Variable.get("MODO_HISTORICO_IBGE", default_var="false").lower() == "true"

default_args = {
    "owner": "awanne",
    "depends_on_past": False,
    "email_on_failure": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

# ── Indicadores IBGE SIDRA ────────────────────────────────────────────────────
# Tabela 1621 → PIB trimestral (variável 584 = Valor em R$ milhões)
# Tabela 7060 → IPCA por categoria (variável 2265 = variação %)
# Tabela 6381 → Desemprego por região (variável 4099 = taxa desocupação %)

INDICADORES = [
    {
        "task_id": "busca_pib_trimestral",
        "tabela": 1621,
        "variavel": 584,
        "nome": "PIB Trimestral",
        "classificacao": None,
        "nivel_geo": "1",       # Brasil
        "localidade": "1",
        "periodo_incremental": "last 1",
    },
    {
        "task_id": "busca_ipca_alimentacao",
        "tabela": 7060,
        "variavel": 2265,
        "nome": "IPCA Alimentação",
        "classificacao": "315[1904]",   # 1904 = Alimentação e bebidas
        "nivel_geo": "1",
        "localidade": "1",
        "periodo_incremental": "last 1",
    },
    {
        "task_id": "busca_ipca_habitacao",
        "tabela": 7060,
        "variavel": 2265,
        "nome": "IPCA Habitação",
        "classificacao": "315[1906]",   # 1906 = Habitação
        "nivel_geo": "1",
        "localidade": "1",
        "periodo_incremental": "last 1",
    },
    {
        "task_id": "busca_ipca_transportes",
        "tabela": 7060,
        "variavel": 2265,
        "nome": "IPCA Transportes",
        "classificacao": "315[1912]",   # 1912 = Transportes
        "nivel_geo": "1",
        "localidade": "1",
        "periodo_incremental": "last 1",
    },
    {
        "task_id": "busca_desemprego_sudeste",
        "tabela": 6381,
        "variavel": 4099,
        "nome": "Desemprego Sudeste",
        "classificacao": None,
        "nivel_geo": "2",       # Região
        "localidade": "3",      # 3 = Sudeste
        "periodo_incremental": "last 1",
    },
    {
        "task_id": "busca_desemprego_nordeste",
        "tabela": 6381,
        "variavel": 4099,
        "nome": "Desemprego Nordeste",
        "classificacao": None,
        "nivel_geo": "2",
        "localidade": "2",      # 2 = Nordeste
        "periodo_incremental": "last 1",
    },
    {
        "task_id": "busca_desemprego_norte",
        "tabela": 6381,
        "variavel": 4099,
        "nome": "Desemprego Norte",
        "classificacao": None,
        "nivel_geo": "2",
        "localidade": "1",      # 1 = Norte
        "periodo_incremental": "last 1",
    },
    {
        "task_id": "busca_desemprego_sul",
        "tabela": 6381,
        "variavel": 4099,
        "nome": "Desemprego Sul",
        "classificacao": None,
        "nivel_geo": "2",
        "localidade": "4",      # 4 = Sul
        "periodo_incremental": "last 1",
    },
    {
        "task_id": "busca_desemprego_centro_oeste",
        "tabela": 6381,
        "variavel": 4099,
        "nome": "Desemprego Centro-Oeste",
        "classificacao": None,
        "nivel_geo": "2",
        "localidade": "5",      # 5 = Centro-Oeste
        "periodo_incremental": "last 1",
    },
]

# ── DAG ──────────────────────────────────────────────────────────────────────
with DAG(
    dag_id="painel_economico_ibge",
    description="Indicadores macroeconômicos IBGE (SIDRA) → BigQuery Bronze",
    schedule="@weekly",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["ibge", "sidra", "financeiro", "brasil", "painel", "bronze"],
    default_args=default_args,
) as dag:

    for ind in INDICADORES:
        IbgeOperator(
            task_id=ind["task_id"],
            tabela=ind["tabela"],
            variavel=ind["variavel"],
            nome_indicador=ind["nome"],
            classificacao=ind["classificacao"],
            nivel_geo=ind["nivel_geo"],
            localidade=ind["localidade"],
            # Modo histórico: busca 24 meses. Modo incremental: último período.
            modo="historico" if MODO_HISTORICO else "incremental",
            periodo=ind["periodo_incremental"],
        )

    # Nota: As tasks são independentes (sem dependências entre si).
    # O dbt é acionado via Jenkins após o push, não diretamente aqui.
