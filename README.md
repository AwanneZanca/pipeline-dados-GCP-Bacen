# 📊 Portfolio de Dados — End-to-End Pipeline

Pipeline de dados completo rodando na GCP com orquestração, transformação, armazenamento e visualização.

---

## 🏗️ Arquitetura

```
API Pública (BACEN, Yahoo Finance...)
        ↓
Apache Airflow (orquestração)
        ↓
Databricks + PySpark (processamento)
        ↓
dbt (modelagem e qualidade)
        ↓
BigQuery (data warehouse)
        ↓
Power BI / Looker Studio (dashboards)

CI/CD: GitHub → Jenkins → deploy automático
```

---

## 🛠️ Stack

| Camada | Tecnologia |
|---|---|
| Orquestração | Apache Airflow 3.2 |
| Processamento | Databricks + PySpark |
| Transformação | dbt |
| Data Warehouse | BigQuery (GCP) |
| Visualização | Power BI, Looker Studio |
| CI/CD | Jenkins |
| Infraestrutura | Docker + GCP |
| Versionamento | GitHub |

---

## 📂 Estrutura do Projeto

```
portfolio-dados/
  ├── dags/               → DAGs do Airflow
  ├── hooks/              → Conexões com serviços externos
  ├── operators/          → Operators customizados
  ├── tests/              → Testes automatizados
  ├── Jenkinsfile         → Pipeline CI/CD
  └── docker-compose.yaml → Infraestrutura local
```

---

## 🚀 DAGs

### bacen_selic
Busca a taxa Selic mais recente da API do Banco Central do Brasil.

- **Fonte:** API BACEN — Série 11 (Taxa Selic)
- **Agendamento:** Diário (`@daily`)
- **Tags:** `bacen`, `financeiro`

---

## ⚙️ CI/CD com Jenkins

Todo push no GitHub dispara automaticamente o Jenkins que:

1. Valida sintaxe de todas as DAGs
2. Valida hooks e operators
3. Faz deploy automático pro Airflow se tudo passar

---

## 🔧 Infraestrutura

- **Cloud:** Google Cloud Platform (GCP)
- **VM:** e2-medium (2 vCPU, 4GB RAM)
- **SO:** Ubuntu 22.04 LTS
- **Containers:** Docker + Docker Compose

---

## 👩‍💻 Autora

**Awanne Beatriz Zanca**
- LinkedIn: [awanne-zanca](https://linkedin.com/in/awanne-zanca)
- GitHub: [AwanneZanca](https://github.com/AwanneZanca)
