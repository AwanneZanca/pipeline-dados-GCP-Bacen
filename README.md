# 📊 Portfolio de Dados — End-to-End Pipeline na GCP

Pipeline de dados completo rodando na GCP com orquestração, transformação, modelagem e visualização — usando ferramentas reais de mercado.

---

## 🏗️ Arquitetura

```
API Pública (BACEN — Banco Central do Brasil)
        ↓
Apache Airflow (orquestração)
  └── BacenHook → BacenOperator → 7 indicadores econômicos
        ↓
BigQuery — Medallion Architecture
  ├── Bronze → dados brutos (Airflow)
  ├── Silver → dados limpos (dbt)
  └── Gold   → dados analíticos (dbt) — Star Schema
        ↓
Looker Studio (dashboards com gráficos de linha — 24 meses)

CI/CD: GitHub → Jenkins → validação → deploy automático
```

---

## 🛠️ Stack

| Camada | Tecnologia |
|---|---|
| Orquestração | Apache Airflow 3.2 |
| Hooks & Operators | Python customizado |
| Data Warehouse | BigQuery (GCP) |
| Transformação | dbt Core 1.11 |
| CI/CD | Jenkins |
| Infraestrutura | Docker + GCP (VM e2-medium) |
| Versionamento | GitHub + Webhook |

---

## 📂 Estrutura do Projeto

```
pipeline-dados-GCP-Bacen/
  ├── dags/               → DAGs do Airflow
  ├── hooks/              → BacenHook — conexão com API BACEN
  ├── operators/          → BacenOperator — task customizada
  ├── tests/              → Testes automatizados
  ├── dbt_bacen/          → Projeto dbt
  │   ├── macros/         → generate_schema_name (Medallion)
  │   ├── models/
  │   │   ├── silver/     → stg_indicadores (padronização)
  │   │   └── gold/       → mart_indicadores (análise)
  │   │                   → dim_tempo       (Star Schema)
  │   │                   → dim_indicador   (Star Schema)
  │   │                   → fato_indicadores (Star Schema)
  │   └── dbt_project.yml
  ├── Jenkinsfile         → Pipeline CI/CD
  └── docker-compose.yaml → Infraestrutura local
```

---

## 🚀 DAGs

### painel_economico_brasil
Busca os principais indicadores econômicos do Brasil via API do Banco Central e salva no BigQuery (camada Bronze).

Suporta dois modos controlados pela variável Airflow `MODO_HISTORICO`:

| Modo | Comportamento |
|---|---|
| `false` (padrão) | Busca o último registro diário de cada indicador |
| `true` | Busca histórico completo de 24 meses (backfill) |

| Task | Indicador | Série BACEN |
|---|---|---|
| busca_selic | Taxa Selic | 11 |
| busca_ipca | IPCA | 433 |
| busca_igpm | IGP-M | 189 |
| busca_dolar | USD/BRL | 1 |
| busca_euro | EUR/BRL | 21619 |
| busca_desemprego | Desemprego | 7326 |
| busca_credito | Crédito Total | 4189 |

- **Agendamento:** Diário (`@daily`)
- **Tags:** `bacen`, `financeiro`, `brasil`, `painel`

---

## 🔌 Hook & Operator Customizados

**BacenHook** — gerencia a conexão com a API do BACEN:
```python
# Modo incremental — último registro
hook = BacenHook(serie=11, registros=1)

# Modo histórico — range de datas explícito
hook = BacenHook(serie=11, data_inicio="01/06/2024")

dados = hook.get_dados()
# → [{"data": "01/06/2026", "valor": "0.0534"}]
```

**BacenOperator** — executa o hook como task do Airflow e salva no BigQuery Bronze:
```python
busca_selic = BacenOperator(
    task_id="busca_selic",
    serie=11,
    nome_indicador="Taxa Selic",
    modo="incremental"  # ou "historico"
)
```

---

## 🧱 Medallion Architecture com dbt

```
Bronze → dados_economicos_bronze.indicadores
  └── dados brutos ingeridos pelo Airflow via API BACEN

Silver → dados_economicos_silver.stg_indicadores
  └── padronização de datas, valores e nomes (VIEW)

Gold → dados_economicos_gold
  ├── mart_indicadores   → classificações e métricas (TABLE)
  ├── dim_tempo          → dimensão calendário diária (TABLE)
  ├── dim_indicador      → metadados dos indicadores  (TABLE)
  └── fato_indicadores   → tabela fato — Star Schema  (TABLE)
                            particionada por mês
                            clusterizada por indicador/categoria
```

**Testes de qualidade (15 testes):**
- `not_null` e `unique` nas surrogate keys das dimensões
- `accepted_values` em categoria, mês e trimestre
- `relationships` — integridade referencial FK → PK entre fato e dimensões
- `accepted_values` nos nomes dos indicadores

---

## ⭐ Star Schema

```
        dim_tempo
            |
            | fk_tempo
            |
dim_indicador — fk_indicador — fato_indicadores
```

A `fato_indicadores` contém por linha: `valor`, `variacao_absoluta`, `variacao_percentual`, `media_movel_30d` e `media_movel_90d`.

---

## ⚙️ CI/CD com Jenkins

Todo push no GitHub dispara automaticamente o Jenkins via **webhook**:

```
git push
    ↓
GitHub notifica Jenkins (webhook)
    ↓
Jenkins valida:
  ✅ Sintaxe das DAGs
  ✅ Sintaxe dos Hooks
  ✅ Sintaxe dos Operators
    ↓
Deploy automático pro Airflow
```

---

## 🔧 Infraestrutura

- **Cloud:** Google Cloud Platform (GCP)
- **VM:** e2-medium (2 vCPU, 4GB RAM + 4GB Swap)
- **SO:** Ubuntu 22.04 LTS
- **Containers:** Docker + Docker Compose
- **IP:** Estático (não muda ao reiniciar)

---

## 👩‍💻 Autora

**Awanne Beatriz Zanca**

Analista de Dados & BI com foco em Analytics Engineering — 4+ anos de experiência em Stone, Raízen, Bosch e IBM.

- LinkedIn: [awanne-zanca](https://linkedin.com/in/awanne-zanca)
- GitHub: [AwanneZanca](https://github.com/AwanneZanca)
