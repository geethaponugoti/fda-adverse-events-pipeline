# FDA Adverse Events Pipeline

Regulatory and pharmacovigilance teams need to know within hours, not weeks, when adverse drug event data stops flowing, drifts in shape, or silently loses volume. Most portfolio pipelines only prove they can move data — this one proves it can catch itself breaking.

This is a production-style ELT pipeline that ingests real FDA adverse event reports, enforces data quality at the boundary, transforms them through a layered warehouse, and ships with a self-monitoring layer plus a controlled failure-injection harness so the monitoring itself can be verified rather than taken on faith.

![Python](https://img.shields.io/badge/Python-3.11-3776AB?logo=python&logoColor=white)
![Airflow](https://img.shields.io/badge/Airflow-2.8-017CEE?logo=apacheairflow&logoColor=white)
![dbt](https://img.shields.io/badge/dbt-1.8-FF694B?logo=dbt&logoColor=white)
![PostgreSQL](https://img.shields.io/badge/PostgreSQL-15-4169E1?logo=postgresql&logoColor=white)
![AWS S3](https://img.shields.io/badge/AWS-S3-232F3E?logo=amazons3&logoColor=white)
![Streamlit](https://img.shields.io/badge/Streamlit-Dashboard-FF4B4B?logo=streamlit&logoColor=white)
![Docker](https://img.shields.io/badge/Docker-Compose-2496ED?logo=docker&logoColor=white)

## The problem

Data pipelines fail quietly. A source API renames a field, an upstream job dies mid-write and only loads 40% of a batch, or a scheduler silently stops firing — and none of it shows up until someone downstream notices a dashboard looks wrong. Catching these failures requires purpose-built instrumentation, and that instrumentation is only trustworthy if it's been tested against real failure modes, not just the happy path.

This project treats that as a first-class requirement, not an afterthought: every pipeline run is logged, every schema is snapshotted, every load is checked for freshness and volume — and four realistic failure scenarios can be injected on demand to prove the monitoring actually catches them.

## Architecture

```
openFDA API
     │
     ▼
┌────────────────────────────────────────────────────────────┐
│  Airflow DAG (fda_pipeline_dag)                             │
│                                                               │
│  check_failure_injection → extract_fda_data →                │
│    validate_raw_schema → load_to_postgres → trigger_dbt_run  │
└────────────────────────────────────────────────────────────┘
     │                    │                        │
     ▼                    ▼                        ▼
  AWS S3            PostgreSQL (raw)          dbt (staging → mart)
 (raw CSVs)          idempotent upsert         cleaned + aggregated
                            │
                            ▼
                   Streamlit dashboard
              (KPIs, drug/reaction analytics,
                 pipeline run history)
```

Every task writes a start/finish record to a `monitoring` schema — status, row counts, error messages, schema snapshots, freshness checks — so pipeline health is queryable, not just visible in logs.

## Tech stack

| Layer | Technology | Role |
|---|---|---|
| Orchestration | Apache Airflow 2.8 | Schedules and sequences the 5-task DAG |
| Ingestion | Python, `requests` | Pulls paginated adverse event reports from the openFDA API |
| Object storage | AWS S3 | Landing zone for raw CSV extracts |
| Warehouse | PostgreSQL 15 | Raw, staging, mart, and monitoring schemas |
| Transformation | dbt | Raw → staging (cleaned, typed) → mart (aggregated) |
| Visualization | Streamlit | Pipeline health and drug/reaction analytics dashboard |
| Runtime | Docker Compose | Reproducible local environment for the full stack |

## Quick start

**Prerequisites:** Docker Desktop, an AWS account with an S3 bucket named `pipeline-fda` (or override `S3_BUCKET` in `dags/fda_pipeline_dag.py`), and AWS credentials available at `~/.aws/credentials` on the host — this is mounted read-only into the containers.

```bash
git clone <repo-url>
cd fda-adverse-events-pipeline

docker compose up -d --build
```

This builds and starts Postgres, the Airflow webserver and scheduler, and the Streamlit dashboard.

```
Airflow UI:  http://localhost:8080   (admin / admin)
Dashboard:   http://localhost:8501
```

`fda_pipeline_dag` runs every 15 minutes and unpauses itself on deploy. To trigger a run immediately:

```bash
docker exec fda_pipeline-airflow-scheduler-1 airflow dags trigger fda_pipeline_dag
```

## Pipeline

Five tasks, run in sequence:

1. **`check_failure_injection`** — reads the `failure_type` Airflow Variable and, if set, injects the corresponding failure into `raw.fda_adverse_events` before resetting the Variable back to `none`.
2. **`extract_fda_data`** — checks whether today's data is already loaded; if not, pages through the openFDA API for the configured date range and writes the result to S3 as CSV.
3. **`validate_raw_schema`** — pulls the CSV back from S3 and asserts the expected columns are present before anything touches the warehouse.
4. **`load_to_postgres`** — upserts into `raw.fda_adverse_events` on `report_id`, so re-running the pipeline never creates duplicates.
5. **`trigger_dbt_run`** — runs dbt to rebuild the staging and mart layers from the freshly loaded raw data.

## Data model

- **`raw.fda_adverse_events`** — one row per adverse event report, loaded as-is from the API.
- **`staging.stg_fda_adverse_events`** — typed and cleaned: dates parsed, boolean flags derived (`is_serious`, `is_fatal`), text fields normalized.
- **`mart.mart_drug_reactions`** — report volume by drug/reaction pair, with serious and fatal counts.
- **`mart.mart_serious_events`** — serious-only reports ranked by volume within each drug, for surfacing the most significant drug/outcome combinations.

## Data

Real FDA adverse event reports pulled live from the [openFDA API](https://open.fda.gov/apis/drug/event/), covering reports received April 2025 through April 2026. Drug names, reactions, patient demographics, and outcomes are all real values as reported to the FDA — nothing here is synthetic.

## Monitoring and failure injection

Every task logs to `monitoring.pipeline_runs` (status, row counts, error messages). Every successful `validate_raw_schema` run records a column-level snapshot to `monitoring.schema_snapshots`, and every load records a freshness check to `monitoring.freshness_checks`. This gives four scenarios worth testing against — and a way to actually test them:

| Scenario | What it simulates | What it does |
|---|---|---|
| `schema_drift` | Upstream API renames a field between versions | Renames a column on `raw.fda_adverse_events`, breaking the next load until it's caught |
| `volume_anomaly` | A partial or truncated upstream write | Deletes 60% of rows at random from `raw.fda_adverse_events` |
| `data_quality` | A bad batch lands with missing values | Sets `drug_name` to `NULL` on 10% of rows |
| `freshness_failure` | The pipeline silently stops running | Pushes `loaded_at` back 3 days on existing rows |

Trigger a scenario by setting the Airflow Variable:

```bash
docker exec fda_pipeline-airflow-scheduler-1 airflow variables set failure_type schema_drift
```

The next DAG run injects it, then resets the variable to `none` automatically. `scripts/inject_failure.py` provides the same four scenarios as a standalone CLI for testing outside Airflow, and includes an automatic snapshot/restore so any injected failure can be cleanly undone. `scripts/monitoring_loop.py` runs the same row-count, freshness, and schema-drift checks independently on an interval, emitting structured alerts to stdout.

## Dashboard

The Streamlit dashboard (`scripts/dashboard.py`) surfaces:

- KPI cards: reports loaded today, serious reports, fatal reports, unique drugs
- Top 10 drugs by adverse report volume
- Most common reactions, with serious/fatal breakdowns
- Serious events ranked by drug and outcome
- A filterable raw data preview (by drug name and date range)
- Pipeline run history, with expandable error detail on any failed run

## Project structure

```
fda-adverse-events-pipeline/
├── dags/
│   └── fda_pipeline_dag.py          # Airflow DAG: 5-task extract/validate/load/transform pipeline
├── dbt_project/
│   ├── models/staging/               # Cleaned, typed source data
│   └── models/mart/                  # Aggregated, analytics-ready tables
├── scripts/
│   ├── dashboard.py                  # Streamlit monitoring + analytics dashboard
│   ├── inject_failure.py             # Standalone CLI for failure injection / restore
│   ├── monitoring_loop.py            # Independent anomaly-detection loop
│   └── init_db.sql                   # Schema and role bootstrap
├── docker-compose.yml                 # Postgres, Airflow, Streamlit services
└── README.md
```
