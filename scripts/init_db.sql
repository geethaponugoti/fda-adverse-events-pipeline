-- Airflow database
CREATE USER airflow WITH PASSWORD 'airflow';
CREATE DATABASE airflow OWNER airflow;

-- Pipeline database
CREATE USER pipeline WITH PASSWORD 'pipeline';
CREATE DATABASE fda_pipeline OWNER pipeline;

\connect fda_pipeline

GRANT ALL ON SCHEMA public TO pipeline;

-- Raw schema
CREATE SCHEMA raw AUTHORIZATION pipeline;

CREATE TABLE raw.fda_adverse_events (
    report_id           VARCHAR(50),
    received_date       VARCHAR(20),
    serious             VARCHAR(5),
    serious_death       VARCHAR(5),
    serious_hosp        VARCHAR(5),
    serious_life        VARCHAR(5),
    patient_age         NUMERIC,
    patient_age_unit    VARCHAR(20),
    patient_sex         VARCHAR(5),
    drug_name           VARCHAR(500),
    drug_indication     VARCHAR(500),
    reaction            VARCHAR(500),
    outcome             VARCHAR(100),
    country             VARCHAR(100),
    loaded_at           TIMESTAMP DEFAULT NOW(),
    load_date           DATE
);

-- Staging schema
CREATE SCHEMA staging AUTHORIZATION pipeline;

-- Mart schema
CREATE SCHEMA mart AUTHORIZATION pipeline;

-- Monitoring schema
CREATE SCHEMA monitoring AUTHORIZATION pipeline;

CREATE TABLE monitoring.row_count_baselines (
    id              SERIAL PRIMARY KEY,
    table_name      VARCHAR(100) NOT NULL,
    schema_name     VARCHAR(50)  NOT NULL,
    baseline_count  BIGINT       NOT NULL,
    recorded_at     TIMESTAMP    DEFAULT NOW()
);

CREATE TABLE monitoring.schema_snapshots (
    id             SERIAL PRIMARY KEY,
    table_name     VARCHAR(100) NOT NULL,
    schema_name    VARCHAR(50)  NOT NULL,
    column_name    VARCHAR(100) NOT NULL,
    data_type      VARCHAR(100) NOT NULL,
    is_nullable    VARCHAR(5),
    snapshotted_at TIMESTAMP    DEFAULT NOW()
);

CREATE TABLE monitoring.pipeline_runs (
    id             SERIAL PRIMARY KEY,
    dag_id         VARCHAR(200) NOT NULL,
    run_id         VARCHAR(200) NOT NULL,
    task_id        VARCHAR(200),
    status         VARCHAR(50)  NOT NULL,
    rows_processed BIGINT,
    error_message  TEXT,
    started_at     TIMESTAMP    DEFAULT NOW(),
    finished_at    TIMESTAMP
);

CREATE TABLE monitoring.freshness_checks (
    id           SERIAL PRIMARY KEY,
    table_name   VARCHAR(100) NOT NULL,
    schema_name  VARCHAR(50)  NOT NULL,
    last_loaded  TIMESTAMP,
    checked_at   TIMESTAMP    DEFAULT NOW(),
    is_stale     BOOLEAN      DEFAULT FALSE,
    -- 'live' (default) or 'cached' — set when extract_fda_data falls
    -- back to the most recent S3 file after the FDA API is unreachable.
    data_source  VARCHAR(20)  DEFAULT 'live'
);

-- Matches what agent/postmortem.py actually inserts — see that file's
-- _insert_incident_report() for the column mapping.
CREATE TABLE monitoring.incident_reports (
    id             SERIAL PRIMARY KEY,
    dag_id         VARCHAR(200),
    run_id         VARCHAR(200),
    task_id        VARCHAR(200),
    error_type     VARCHAR(50),
    error_message  TEXT,
    fix_attempted  TEXT,
    fix_result     VARCHAR(50),
    approved       BOOLEAN,
    created_at     TIMESTAMP DEFAULT NOW()
);

-- Normally created on demand by agent/mcp_servers/slack_server.py's
-- _ensure_approvals_table(); included here too so a fresh install's
-- schema matches what the running agent expects without waiting for
-- the first Slack approval request to create it.
CREATE TABLE monitoring.agent_approvals (
    incident_id               VARCHAR(64) PRIMARY KEY,
    dag_id                    VARCHAR(200),
    run_id                    VARCHAR(200),
    task_id                   VARCHAR(200),
    summary                   TEXT,
    risk_level                VARCHAR(20),
    severity                  VARCHAR(10),
    status                    VARCHAR(20) NOT NULL DEFAULT 'pending',
    created_at                TIMESTAMP DEFAULT NOW(),
    resolved_at               TIMESTAMP,
    resolved_by               VARCHAR(200),
    escalation_email_sent_at  TIMESTAMP,
    dag_paused_at             TIMESTAMP
);

GRANT ALL ON ALL TABLES    IN SCHEMA raw        TO pipeline;
GRANT ALL ON ALL TABLES    IN SCHEMA staging    TO pipeline;
GRANT ALL ON ALL TABLES    IN SCHEMA mart       TO pipeline;
GRANT ALL ON ALL TABLES    IN SCHEMA monitoring TO pipeline;
GRANT ALL ON ALL SEQUENCES IN SCHEMA monitoring TO pipeline;
GRANT USAGE ON SCHEMA raw, staging, mart, monitoring TO pipeline;