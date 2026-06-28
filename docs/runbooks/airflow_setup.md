# Runbook: one-time Airflow local setup

**When to use**: you want to run the `oss_pulse_pipeline` DAG via a
scheduler (UI + retries + scheduling) instead of triggering the
shell commands by hand.

This is local dev only. Production lives on Astronomer or Databricks
Workflows in the project plan (Sprint 5+).

## Steps

### 1. Pick an `AIRFLOW_HOME` outside the project

Airflow writes a SQLite metadata DB, logs, and config — keep it out of
the repo:

```bash
export AIRFLOW_HOME=$HOME/.airflow-oss-pulse
mkdir -p $AIRFLOW_HOME
```

### 2. Configure Airflow to read this project's DAGs and skip examples

```bash
export AIRFLOW__CORE__DAGS_FOLDER=/Users/peter/Desktop/oss-pulse/airflow/dags
export AIRFLOW__CORE__LOAD_EXAMPLES=False
```

### 3. Initialize the metadata DB

```bash
cd /Users/peter/Desktop/oss-pulse
.venv/bin/airflow db init
```

### 4. Create the local admin user

```bash
.venv/bin/airflow users create \
  --username peter --firstname Peter --lastname Wang \
  --role Admin --email shuweipeter1618@gmail.com --password peter
```

### 5. Run `airflow standalone`

`standalone` is the lightest setup — single process that runs
scheduler + webserver + triggerer. Good for local; not for
production.

```bash
.venv/bin/airflow standalone
```

You'll see logs and a generated admin password. The UI lives at
http://localhost:8080.

### 6. Trigger the DAG

```bash
.venv/bin/airflow dags trigger oss_pulse_pipeline \
  --conf '{"start_hour": "2025-01-15-12", "end_hour": "2025-01-15-12"}'
```

Or click "Trigger DAG" in the UI.

## Common issues

| Symptom | Cause | Fix |
|---------|-------|-----|
| DAG doesn't appear in UI | `AIRFLOW__CORE__DAGS_FOLDER` not set, or set after init | re-export, restart standalone |
| DAG import error | `apache-airflow` not installed in `.venv` | `uv add apache-airflow==2.10.4` |
| Task `ingest_bronze` fails with Java error | `JAVA_HOME` not in the BashOperator env | the DAG sets it via `COMMON_ENV` — confirm Java 17 is installed at the expected path; otherwise set `OSS_PULSE_JAVA_HOME` env var before `airflow standalone` |
| Task `gate_*` fails with OOM | driver heap | the DAG sets `--driver-memory 4g` via `PYSPARK_SUBMIT_ARGS` — confirm this propagates into the task env |

## Tear down

```bash
# Stop airflow standalone with Ctrl-C, then:
rm -rf $AIRFLOW_HOME
```

The project itself is unaffected — Airflow only stored metadata
under `$AIRFLOW_HOME`.
