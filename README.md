# FDA Adverse Events Pipeline

Regulatory and pharmacovigilance teams need to know within hours, not weeks, when adverse drug event data stops flowing, drifts in shape, or silently loses volume. Most portfolio pipelines only prove they can move data — this one proves it can catch itself breaking, and fix itself when it does.

This is a production-style ELT pipeline that ingests real FDA adverse event reports, enforces data quality at the boundary, transforms them through a layered warehouse, and ships with two additional layers most portfolio projects skip: a controlled failure-injection harness so monitoring can be verified rather than taken on faith, and a self-healing LangGraph agent that diagnoses real failures, proposes a fix (mechanically, from memory, or via an LLM), and either applies it automatically or routes it to a human in Slack — depending on how safe the specific fix actually is, not how safe an LLM claims it is.

![Python](https://img.shields.io/badge/Python-3.11-3776AB?logo=python&logoColor=white)
![Airflow](https://img.shields.io/badge/Airflow-2.8-017CEE?logo=apacheairflow&logoColor=white)
![dbt](https://img.shields.io/badge/dbt-1.8-FF694B?logo=dbt&logoColor=white)
![PostgreSQL](https://img.shields.io/badge/PostgreSQL-15-4169E1?logo=postgresql&logoColor=white)
![AWS S3](https://img.shields.io/badge/AWS-S3-232F3E?logo=amazons3&logoColor=white)
![AWS RDS](https://img.shields.io/badge/AWS-RDS-232F3E?logo=amazonrds&logoColor=white)
![Streamlit](https://img.shields.io/badge/Streamlit-Dashboard-FF4B4B?logo=streamlit&logoColor=white)
![Docker](https://img.shields.io/badge/Docker-Compose-2496ED?logo=docker&logoColor=white)
![LangGraph](https://img.shields.io/badge/LangGraph-Self--Healing_Agent-1C3C3C?logo=langchain&logoColor=white)
![OpenAI](https://img.shields.io/badge/OpenAI-Fix_Generation-412991?logo=openai&logoColor=white)
![Qdrant](https://img.shields.io/badge/Qdrant-Incident_Memory-DC244C?logo=qdrant&logoColor=white)
![Slack](https://img.shields.io/badge/Slack-Human_Approval-4A154B?logo=slack&logoColor=white)

## The problem

Data pipelines fail quietly. A source API renames a field, an upstream job dies mid-write and only loads 40% of a batch, or a scheduler silently stops firing — and none of it shows up until someone downstream notices a dashboard looks wrong. Catching these failures requires purpose-built instrumentation, and that instrumentation is only trustworthy if it's been tested against real failure modes, not just the happy path. Catching them is also only half the job — someone still has to context-switch, diagnose, and fix it, usually hours after it started.

This project treats both halves as first-class requirements. Every pipeline run is logged, every schema is snapshotted, every load is checked for freshness and volume, and four realistic failure scenarios can be injected on demand to prove the monitoring actually catches them. On top of that, a self-healing agent watches for failures in real time, investigates the actual root cause against the live database, and resolves what it safely can — with every fix, safe or not, subject to a structural safety gate that never trusts an LLM's own opinion of its risk.

## Architecture

```
openFDA API
     │
     ▼
┌────────────────────────────────────────────────────────────┐
│  Airflow DAG (fda_pipeline_dag)               EC2 #1       │
│                                                            │
│  check_failure_injection → extract_fda_data →              │
│    validate_raw_schema → load_to_postgres → trigger_dbt_run│
└────────────────────────────────────────────────────────────┘
     │                    │                        │      │
     ▼                    ▼                        ▼      │ on_failure_callback
  AWS S3          RDS PostgreSQL (raw)       dbt (staging  │      │
 (raw CSVs)         idempotent upsert         → mart)      │      ▼
                            │                         ┌─────────────────────── ┐
                            ▼                         │  Self-Healing Agent    │
                   Streamlit dashboard                │  (LangGraph)  EC2 #2   │
              (KPIs, drug/reaction analytics,         │  ingest → classify →   │
                 pipeline run history)                │  investigate → fix →   │
                                                      │  approve/escalate →    │
                                                      │  postmortem            │
                                                      └──────┬──────┬──────────┘
                                                                 │      │
                                                    Slack approval  Qdrant Cloud
                                                    (safety-gated)  (incident memory)
```

Every task writes a start/finish record to a `monitoring` schema — status, row counts, error messages, schema snapshots, freshness checks — so pipeline health is queryable, not just visible in logs. The agent runs on its own EC2 instance (separate from Airflow/Streamlit, for memory headroom — it loads a local sentence-transformer model) and talks to the same RDS instance over the network, since it's no longer a sibling Docker container.

## Tech stack

| Layer | Technology | Role |
|---|---|---|
| Orchestration | Apache Airflow 2.8 | Schedules and sequences the 5-task DAG |
| Ingestion | Python, `requests` | Pulls paginated adverse event reports from the openFDA API |
| Object storage | AWS S3 | Landing zone for raw CSV extracts |
| Warehouse | AWS RDS PostgreSQL 15 | Raw, staging, mart, and monitoring schemas |
| Transformation | dbt | Raw → staging (cleaned, typed) → mart (aggregated) |
| Visualization | Streamlit | Pipeline health and drug/reaction analytics dashboard |
| Runtime | Docker Compose | Reproducible environment, split across two EC2 instances |
| Agent orchestration | LangGraph, FastAPI | State machine driving diagnosis → fix → approval → postmortem |
| Fix generation | OpenAI (structured output) | Proposes a fix given live schema + investigation context |
| Fix safety | `sqlparse` (custom classifier) | Structurally classifies proposed SQL — never trusts self-reported risk |
| Incident memory | Qdrant Cloud, `sentence-transformers` | Semantic search over past incidents; reuses fixes that actually worked |
| Human approval | Slack (FastMCP server) | Approve/Reject buttons, HMAC-verified interactive callback |
| Escalation | APScheduler | Time-based ladder: P1 auto-applies, P0 emails then re-confirms pause |

## Quick start

**Prerequisites:** Docker Desktop, an AWS account with an S3 bucket named `pipeline-fda` (or override `S3_BUCKET` in `dags/fda_pipeline_dag.py`), AWS credentials available at `~/.aws/credentials` on the host, and an RDS PostgreSQL instance. Copy `.env.example` to `.env` and fill in real values — see that file's comments for which variables each EC2 instance actually needs.

```bash
git clone <repo-url>
cd fda-adverse-events-pipeline

docker compose up -d --build
```

This builds and starts Airflow (webserver + scheduler) and the Streamlit dashboard, both talking to RDS.

```
Airflow UI:  http://localhost:8080   (admin / admin)
Dashboard:   http://localhost:8501
```

`fda_pipeline_dag` runs every 15 minutes and unpauses itself on deploy. To trigger a run immediately:

```bash
docker exec fda-adverse-events-pipeline-airflow-scheduler-1 airflow dags trigger fda_pipeline_dag
```

The self-healing agent runs separately (`docker-compose.agent.yml`, typically on its own instance) and needs `OPENAI_API_KEY`, `SLACK_BOT_TOKEN`, and `QDRANT_URL`/`QDRANT_API_KEY` in its `.env`, in addition to the shared RDS and Airflow API credentials:

```bash
docker compose -f docker-compose.agent.yml up -d --build
```

```
Agent health check: http://<agent-host>:8000/health
```

## Pipeline

Five tasks, run in sequence:

1. **`check_failure_injection`** — reads the `failure_type` Airflow Variable and, if set, injects the corresponding failure into `raw.fda_adverse_events` before resetting the Variable back to `none`.
2. **`extract_fda_data`** — checks whether today's data is already loaded; if not, pages through the openFDA API for the configured date range and writes the result to S3 as CSV.
3. **`validate_raw_schema`** — pulls the CSV back from S3 and asserts the expected columns are present before anything touches the warehouse.
4. **`load_to_postgres`** — upserts into `raw.fda_adverse_events` on `report_id`, so re-running the pipeline never creates duplicates.
5. **`trigger_dbt_run`** — runs dbt to rebuild the staging and mart layers from the freshly loaded raw data.

On failure, any task's `on_failure_callback` posts the error to the self-healing agent's `/alert` endpoint — see below.

## Data model

- **`raw.fda_adverse_events`** — one row per adverse event report, loaded as-is from the API.
- **`staging.stg_fda_adverse_events`** — typed and cleaned: dates parsed, boolean flags derived (`is_serious`, `is_fatal`), text fields normalized.
- **`mart.mart_drug_reactions`** — report volume by drug/reaction pair, with serious and fatal counts.
- **`mart.mart_serious_events`** — serious-only reports ranked by volume within each drug, for surfacing the most significant drug/outcome combinations.

## Data

Real FDA adverse event reports pulled live from the openFDA API. Drug names, reactions, patient demographics, and outcomes are all real values as reported to the FDA — nothing here is synthetic.
The pipeline automatically manages the openFDA API's 25,000 record limit by shifting to the previous year's date range when the current range is exhausted, ensuring continuous data collection. Data coverage spans from 2004 to present day.

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
docker exec fda-adverse-events-pipeline-airflow-scheduler-1 airflow variables set failure_type schema_drift
```

The next DAG run injects it, then resets the variable to `none` automatically. `scripts/inject_failure.py` provides the same four scenarios as a standalone CLI for testing outside Airflow, and includes an automatic snapshot/restore so any injected failure can be cleanly undone. `scripts/monitoring_loop.py` runs the same row-count, freshness, and schema-drift checks independently on an interval, emitting structured alerts to stdout.

## Self-healing agent

A LangGraph state machine (`agent/graph.py`) that turns a raw task failure into either an automatic fix, a Slack approval request, or an escalation — never a silent SQL execution nobody reviewed.

```
ingest → classify → investigate → fix → approve_or_escalate → postmortem
```

- **`ingest`** — parses the failure (Airflow's exception text, or the last `error_message` logged to `monitoring.pipeline_runs`) into a clean error message and a heuristic error-type hint.
- **`classify`** — an LLM call assigns a severity: **P0** (data corruption — pause the DAG immediately, escalate), **P1** (schema drift / volume anomaly — fix and auto-apply on a timeout if unanswered), **P2** (data quality / freshness — same fix logic, no auto-timeout), **P3** (upstream/infra — log only, no fix attempted).
- **`investigate`** — runs a targeted check against the live database (a null-rate check, a row-count comparison, a schema diff) and searches Qdrant for similar past incidents, so the fix step has real evidence instead of just the error string.
- **`fix`** — proposes a fix in order of increasing cost and decreasing certainty: (1) a mechanical, `information_schema`-verified column rename when the drift is unambiguous; (2) a semantically similar prior incident's fix, reused from Qdrant, if one actually worked; (3) a fresh OpenAI call given the live schema, the last snapshot, and the investigation findings, only if neither of the above applies.
- **`approve_or_escalate`** — routes the proposed fix through `agent/tools/sql_validator.py`, which structurally parses it (via `sqlparse`, not keyword matching) into one of three tiers, **regardless of what the LLM itself claimed about its risk**:

  | Tier | Examples | What happens |
  |---|---|---|
  | `REJECTED` | `DROP`, `TRUNCATE`, `DELETE` without `WHERE`, multi-statement payloads, tables outside an explicit allowlist, anything unparseable | Never executed, never offered for approval — surfaced to Slack as a rejection so it's not silently dropped |
  | `NEEDS_APPROVAL` | `UPDATE`, `INSERT`, `DELETE` with `WHERE`, any `ALTER TABLE` beyond a plain rename | Posted to Slack with the exact SQL shown; P1 auto-applies after a timeout if unanswered, P2 waits indefinitely for a human |
  | `AUTO_EXECUTABLE` | `ALTER TABLE ... RENAME COLUMN` only | Applied automatically — but even here, the literal proposed text is never trusted directly; the rename is re-derived and re-verified against live `information_schema` before anything runs |

- **`postmortem`** — writes a structured incident record to both `monitoring.incident_reports` (relational audit trail) and Qdrant (embedded, for future semantic reuse). A fix is only cached for reuse if it's confirmed to have actually executed — a P1 that fell back to a plain retry doesn't pollute the memory with a fix that never ran.

Human approval, when needed, happens in Slack: `agent/mcp_servers/slack_server.py` posts an Approve/Reject message (showing the exact proposed SQL), verifies the button-click callback's HMAC signature, and executes the fix — re-validated through the same safety gate — only on explicit approval. `agent/escalation_scheduler.py` polls for approvals still pending past their deadline and enforces the timeout ladder: P1 auto-applies, P0 sends an urgent email and, if still unanswered, re-confirms the DAG is paused.

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
│   └── fda_pipeline_dag.py            # Airflow DAG: 5-task extract/validate/load/transform pipeline
├── dbt_project/
│   ├── models/staging/                 # Cleaned, typed source data
│   └── models/mart/                    # Aggregated, analytics-ready tables
├── agent/
│   ├── main.py                         # FastAPI app: /alert, /health, /slack/actions
│   ├── graph.py                        # LangGraph state machine (ingest → ... → postmortem)
│   ├── state.py                        # Shared AgentState schema
│   ├── postmortem.py                   # Writes incident records to Postgres + Qdrant
│   ├── escalation_scheduler.py         # Timeout ladder for pending Slack approvals
│   ├── mcp_servers/
│   │   └── slack_server.py             # Slack notifications + approval webhook (FastMCP)
│   └── tools/
│       ├── airflow_client.py           # Airflow REST API: retry, pause, status
│       ├── schema_inspector.py         # Live schema reads, safe-rename detection/verification
│       ├── sql_validator.py            # Structural SQL safety classifier
│       ├── memory_lookup.py            # Qdrant incident search + fix reuse
│       ├── log_parser.py               # Error message / error-type extraction
│       ├── lineage_tracer.py           # dbt manifest lineage lookup
│       └── email_notifier.py           # SMTP escalation emails
├── scripts/
│   ├── dashboard.py                    # Streamlit monitoring + analytics dashboard
│   ├── inject_failure.py               # Standalone CLI for failure injection / restore
│   ├── monitoring_loop.py              # Independent anomaly-detection loop
│   └── init_db.sql                     # Schema and role bootstrap
├── docker-compose.yml                   # Airflow, Streamlit services (EC2 #1)
├── docker-compose.agent.yml             # Self-healing agent (EC2 #2)
├── .env.example                         # All environment variables, annotated by which host needs them
└── README.md
```
