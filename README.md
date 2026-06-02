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
BigQuery (data warehouse)
        ↓
dbt (modelagem, testes e documentação)
  └── staging → marts
        ↓
Looker Studio / Power BI (dashboards)

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
portfolio-dados/
  ├── dags/               → DAGs do Airflow
  ├── hooks/              → BacenHook — conexão com API BACEN
  ├── operators/          → BacenOperator — task customizada
  ├── tests/              → Testes automatizados
  ├── dbt_bacen/          → Projeto dbt
  │   ├── models/
  │   │   ├── staging/    → stg_indicadores (padronização)
  │   │   └── marts/      → mart_indicadores (análise)
  │   └── dbt_project.yml
  ├── Jenkinsfile         → Pipeline CI/CD
  └── docker-compose.yaml → Infraestrutura local
```

---

## 🚀 DAGs

### painel_economico_brasil
Busca os principais indicadores econômicos do Brasil via API do Banco Central, salva no BigQuery e aciona o dbt para modelagem.

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
hook = BacenHook(serie=11, registros=1)
dados = hook.get_dados()
# → [{"data": "01/06/2026", "valor": "0.0534"}]
```

**BacenOperator** — executa o hook como task do Airflow e salva no BigQuery:
```python
busca_selic = BacenOperator(
    task_id="busca_selic",
    serie=11,
    nome_indicador="Taxa Selic"
)
```

---

## 🧱 Modelos dbt

```
dados_economicos/
  ├── indicadores        → tabela bruta (Airflow)
  ├── stg_indicadores    → view staging (dbt)
  │     └── padronização de datas, valores e nomes
  └── mart_indicadores   → view marts (dbt)
        └── classificações e métricas por indicador
```

**Testes de qualidade (5 testes):**
- `not_null` em data, valor, indicador e serie
- `accepted_values` nos nomes dos indicadores

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
