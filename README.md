# 🏦 Bank ETL Project

An automated data pipeline that ingests, processes, and transforms bank marketing data from a source database into a star-schema data warehouse, built with Apache Airflow, PostgreSQL, and Docker.

---

## 📋 Table of Contents

- [Overview](#overview)
- [Technology Stack](#technology-stack)
- [Architecture](#architecture)
- [Project Structure](#project-structure)
- [Prerequisites](#prerequisites)
- [Quick Start](#quick-start)
- [Docker Services](#docker-services)
- [DAG — Pipeline Tasks](#dag--pipeline-tasks)
- [SQL File Descriptions](#sql-file-descriptions)
- [Environment Variables](#environment-variables)
- [Known Issues & Fixes](#known-issues--fixes)
- [Production Recommendations](#production-recommendations)

---

## Overview

The Bank ETL Pipeline automates the full ETL lifecycle for bank marketing data:

- **Extract** raw data from a CSV file into a PostgreSQL staging table
- **Transform** it into normalised source tables and a star-schema data warehouse
- **Load** the final data into dimension and fact tables for analytics and reporting
- **Orchestrate** every step with Apache Airflow, fully containerised with Docker

---

## Technology Stack

| Technology | Version | Role |
|---|---|---|
| Apache Airflow | 3.2.2 | Pipeline orchestration — scheduling & monitoring |
| PostgreSQL | 16 | Source database (`bank_source`) & data warehouse (`bank_dw`) |
| Redis | 7.2 | Message broker for Celery Executor |
| Docker / Docker Compose | Latest | Containerisation of the full stack |
| Python | 3.13 | DAG authoring and data transfer logic |
| pandas | Latest | DataFrame-based data transfer between databases |
| SQLAlchemy | Latest | Database engine / connection management |

---

## Architecture

```
bank_source (port 5443)           bank_dw (port 5444)
  raw_bank          Python/pandas    dw.dim_customer
  source_customer  ─────────────►   dw.dim_campaign
  source_campaign                   dw.fact_marketing
  source_marketing_fact
```

All containers communicate over a shared Docker internal network. Airflow containers reference Postgres containers by their **container name** (`bank_source` / `bank_dw`), never by `localhost`.

---

## Project Structure

```
bank-etl-project/
├── config/                  ← airflow.cfg configuration
├── dags/
│   ├── sql/                 ← SQL scripts for each pipeline step
│   │   ├── 01_create_raw.sql
│   │   ├── 02_load_raw.sql
│   │   ├── 03_create_source.sql
│   │   ├── 04_create_dw.sql
│   │   └── 05_load_dw.sql
│   └── bank_etl_dag.py      ← Main Airflow DAG
├── plugins/                 ← Custom Airflow plugins (reserved)
├── .env                     ← Environment variables (do not commit)
├── docker-compose.yaml      ← Full stack definition
├── source_tables.sql        ← pg_dump of source schema
└── bank-additional-full.csv ← Raw input data (41,188 rows)
```

---

## Prerequisites

- Docker Desktop with Docker Compose v2
- Minimum 8 GB RAM for the full Airflow stack
- Ports `5443`, `5444`, `6379`, and `8081` free on the host

---

## Quick Start

### Step 1 — Start the stack

```bash
docker compose up -d
```

Wait ~60 seconds for all services to become healthy. The `airflow-init` container will exit with code `0` — this is expected and confirms successful initialisation.

### Step 2 — Copy the CSV into the source container

PostgreSQL's `COPY` command reads from the **server** filesystem, not the host. Run this once after every `docker compose up`:

```bash
docker cp data/bank-additional-full.csv bank_source:/tmp/
```

### Step 3 — Configure Airflow connections

Open the Airflow UI at **http://localhost:8081** (login: `airflow` / `airflow`), go to **Admin → Connections**, and create two connections:

| Field | `postgres_source` | `postgres_dw` |
|---|---|---|
| Conn Type | Postgres | Postgres |
| Host | `bank_source` | `bank_dw` |
| Database | `bank` | `bank_dw` |
| Login | `postgres` | `postgres` |
| Password | `postgres123` | `postgres123` |
| Port | `5432` | `5432` |

> ⚠️ **Use container names as hostnames**, not `localhost` or `127.0.0.1`.

### Step 4 — Trigger the DAG

In the Airflow UI, find the `bank_etl_pipeline` DAG, toggle it **ON**, then click the **Trigger** button. Monitor progress in the Grid or Graph view.

---

## Docker Services

| Service | Image | Port | Purpose |
|---|---|---|---|
| `bank_source` | `postgres:16` | `5443:5432` | Source PostgreSQL database |
| `bank_dw` | `postgres:16` | `5444:5432` | Data warehouse PostgreSQL database |
| `redis` | `redis:7.2-bookworm` | `6379` (internal) | Celery message broker |
| `airflow-init` | `apache/airflow:3.2.2` | — | DB migration & admin user creation |
| `airflow-apiserver` | `apache/airflow:3.2.2` | `8081:8080` | Airflow web UI |
| `airflow-scheduler` | `apache/airflow:3.2.2` | — | DAG scheduling |
| `airflow-worker` | `apache/airflow:3.2.2` | — | Task execution (Celery) |
| `airflow-triggerer` | `apache/airflow:3.2.2` | — | Deferred task handling |
| `airflow-dag-processor` | `apache/airflow:3.2.2` | — | DAG file parsing & loading |

---

## DAG — Pipeline Tasks

**DAG ID:** `bank_etl_pipeline` | **Execution:** sequential (linear chain)

| Task ID | Operator | Target DB | Description |
|---|---|---|---|
| `load_raw` | `SQLExecuteQueryOperator` | `bank_source` | Runs `01_create_raw.sql` + `02_load_raw.sql`; loads 41,188 rows from CSV |
| `create_source` | `SQLExecuteQueryOperator` | `bank_source` | Runs `03_create_source.sql`; creates 3 normalised source tables |
| `transfer_data_to_dw` | `PythonOperator` | Both | Reads source tables via pandas; writes to `bank_dw` public schema |
| `create_dw` | `SQLExecuteQueryOperator` | `bank_dw` | Runs `04_create_dw.sql`; creates `dw` schema with star-schema tables |
| `load_dw` | `SQLExecuteQueryOperator` | `bank_dw` | Runs `05_load_dw.sql`; INSERTs data from source into dim/fact tables |

---

## SQL File Descriptions

| File | Description |
|---|---|
| `01_create_raw.sql` | Creates the `raw_bank` staging table with `IF NOT EXISTS` guard |
| `02_load_raw.sql` | Loads the CSV into `raw_bank` via PostgreSQL `COPY` (delimiter `;`, with header) |
| `03_create_source.sql` | Splits `raw_bank` into `source_customer`, `source_campaign`, `source_marketing_fact` using `DROP … CREATE TABLE AS SELECT` for idempotency |
| `04_create_dw.sql` | Creates `dw` schema and three star-schema tables; drops in FK-safe order (fact before dims) |
| `05_load_dw.sql` | `INSERT INTO` all three DW tables from the corresponding source tables |

---

## Environment Variables

Create a `.env` file in the project root:

```env
_AIRFLOW_WWW_USER_USERNAME=airflow
_AIRFLOW_WWW_USER_PASSWORD=airflow
FERNET_KEY=<your-fernet-key>
AIRFLOW_IMAGE_NAME=apache/airflow:3.2.2
AIRFLOW_PROJ_DIR=.
AIRFLOW_UID=50000
_PIP_ADDITIONAL_REQUIREMENTS=apache-airflow-providers-postgres apache-airflow-providers-common-sql pandas sqlalchemy
```

> ⚠️ Add `.env` to `.gitignore` — never commit secrets to version control.

---

## Known Issues & Fixes

### `ModuleNotFoundError` — PostgresOperator removed
`apache-airflow-providers-postgres` v6+ removed `PostgresOperator`. Use `SQLExecuteQueryOperator` from `apache-airflow-providers-common-sql` instead.

```python
# ❌ Old
from airflow.providers.postgres.operators.postgres import PostgresOperator

# ✅ New
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
```

### `UnicodeDecodeError` — UTF-16 SQL files on Windows
SQL files saved with Windows Notepad default to UTF-16 LE. Airflow requires UTF-8. Resave all `.sql` files using VS Code: click the encoding indicator in the status bar → **Save with Encoding → UTF-8**.

### `UndefinedFile` — CSV not found in container
`COPY` reads from the server filesystem. After every `docker compose up`, run:
```bash
docker cp data/bank-additional-full.csv bank_source:/tmp/
```

### Connection refused — `localhost` vs container name
Airflow connections must use container names as hostnames (`bank_source`, `bank_dw`), not `localhost` or `127.0.0.1`.

### Duplicate key in `.env`
Having two `_PIP_ADDITIONAL_REQUIREMENTS` lines causes the first to be silently ignored. Keep only one line with all packages listed.

---

## Production Recommendations

- **Custom Dockerfile** — bake packages into the image instead of using `_PIP_ADDITIONAL_REQUIREMENTS` to avoid reinstalling on every container start
- **Volume mount for CSV** — add `./data:/tmp/data` to the `bank_source` service so the CSV persists across restarts
- **Data quality checks** — add row count and null-value validation after each task
- **Code-managed connections** — manage Airflow connections via environment variables or a secrets backend instead of the UI
- **Alerting** — add failure notifications via email or Slack using Airflow callbacks
- **Secrets rotation** — rotate the Fernet key and all database passwords before going to production

---

*Bank ETL Project — Apache Airflow • PostgreSQL • Docker — June 2026*
