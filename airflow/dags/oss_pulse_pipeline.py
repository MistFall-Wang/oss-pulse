"""OSS Pulse end-to-end pipeline DAG.

Downloads one GH Archive hour, ingests to Bronze, runs DQ gates, builds
Silver via dbt, gates Silver, builds Gold via dbt, gates Gold, runs the
cross-mart consistency gate, then runs the full dbt test suite.

Parameterized for arbitrary-date backfill via `params.start_hour` /
`params.end_hour` (both `YYYY-MM-DD-HH` strings, inclusive). The default
config runs a single sample hour.

Idempotency: every task is safe to re-run. Bronze MERGE on event_id and
Silver/Gold MERGE on their respective keys (ADR-0002) guarantee no
duplicates on re-execution.

Gate semantics: every `gate_*` task fails the DAG if any data-quality
check returns non-zero. Downstream tasks therefore do NOT run on bad
data, which is the whole point of the gate-style integration (see
quality/checks.py for the rationale on the lightweight gate approach
vs full Great Expectations).
"""

from __future__ import annotations

from datetime import datetime, timedelta
import os
from pathlib import Path

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator


# Resolve the repo root from this file's location:
#   <repo>/airflow/dags/oss_pulse_pipeline.py  →  parents[2] = <repo>
# Override with OSS_PULSE_ROOT when the DAG is symlinked / packaged.
PROJECT_ROOT = os.environ.get(
    "OSS_PULSE_ROOT",
    str(Path(__file__).resolve().parents[2]),
)

# Java 17 location (Spark 3.5 breaks on Java 18+). Override on systems
# where Corretto 17 lives elsewhere.
JAVA_HOME = os.environ.get(
    "OSS_PULSE_JAVA_HOME",
    "/Library/Java/JavaVirtualMachines/amazon-corretto-17.jdk/Contents/Home",
)

# Common env every BashOperator task needs: Java 17 (Spark 3.5 breaks on
# Java 18+), driver heap headroom for the QA suites on Bronze.
COMMON_ENV = {
    "JAVA_HOME": JAVA_HOME,
    "PATH": f"{JAVA_HOME}/bin:" + os.environ.get("PATH", ""),
    "PYSPARK_DRIVER_MEMORY": "4g",
    "PYSPARK_SUBMIT_ARGS": "--driver-memory 4g pyspark-shell",
}


def hour_range(start: str, end: str) -> list[str]:
    """Inclusive hour strings between two YYYY-MM-DD-HH stamps."""
    fmt = "%Y-%m-%d-%H"
    s = datetime.strptime(start, fmt)
    e = datetime.strptime(end, fmt)
    out: list[str] = []
    cur = s
    while cur <= e:
        out.append(cur.strftime(fmt))
        cur += timedelta(hours=1)
    return out


def expand_ingest_commands(**context) -> str:
    """Build a single bash command that downloads + ingests every hour
    in the requested range. Emits a shell script string the BashOperator
    can run as one task — keeps the DAG topology tiny and the per-hour
    ingest output co-located in logs.
    """
    params = context["params"]
    hours = hour_range(params["start_hour"], params["end_hour"])
    cmds: list[str] = ["set -euo pipefail"]
    for h in hours:
        target = f"{PROJECT_ROOT}/data/raw/{h}.json.gz"
        cmds.append(
            f"if [ ! -f {target} ]; then "
            f"  curl -sf -o {target} https://data.gharchive.org/{h}.json.gz; "
            f"fi"
        )
        cmds.append(
            f"cd {PROJECT_ROOT} && "
            f".venv/bin/python -m spark.jobs.bronze_ingest "
            f"--source data/raw/{h}.json.gz "
            f"--bronze-path data/bronze/events"
        )
    script = "\n".join(cmds)
    context["ti"].xcom_push(key="ingest_script", value=script)
    return script


with DAG(
    dag_id="oss_pulse_pipeline",
    description="GH Archive → Bronze → Silver → Gold with DQ gates",
    start_date=datetime(2025, 1, 15),
    schedule=None,  # triggered manually or by external scheduler
    catchup=False,
    max_active_runs=1,
    default_args={
        "owner": "peter",
        "retries": 1,
        "retry_delay": timedelta(minutes=5),
    },
    params={
        "start_hour": "2025-01-15-12",
        "end_hour": "2025-01-15-12",
    },
    tags=["oss-pulse", "medallion"],
) as dag:
    plan_ingest = PythonOperator(
        task_id="plan_ingest_range",
        python_callable=expand_ingest_commands,
    )

    ingest_bronze = BashOperator(
        task_id="ingest_bronze",
        bash_command="{{ ti.xcom_pull(task_ids='plan_ingest_range', key='ingest_script') }}",
        env=COMMON_ENV,
        append_env=True,
    )

    gate_bronze = BashOperator(
        task_id="gate_bronze",
        bash_command=(
            f"cd {PROJECT_ROOT} && .venv/bin/python -m quality.runner --layer bronze"
        ),
        env=COMMON_ENV,
        append_env=True,
    )

    build_silver = BashOperator(
        task_id="build_silver",
        bash_command=(f"cd {PROJECT_ROOT}/dbt && ../.venv/bin/dbt run --select silver"),
        env=COMMON_ENV,
        append_env=True,
    )

    gate_silver = BashOperator(
        task_id="gate_silver",
        bash_command=(
            f"cd {PROJECT_ROOT} && .venv/bin/python -m quality.runner --layer silver"
        ),
        env=COMMON_ENV,
        append_env=True,
    )

    build_gold = BashOperator(
        task_id="build_gold",
        bash_command=(f"cd {PROJECT_ROOT}/dbt && ../.venv/bin/dbt run --select gold"),
        env=COMMON_ENV,
        append_env=True,
    )

    gate_gold = BashOperator(
        task_id="gate_gold",
        bash_command=(
            f"cd {PROJECT_ROOT} && .venv/bin/python -m quality.runner --layer gold"
        ),
        env=COMMON_ENV,
        append_env=True,
    )

    gate_cross_mart = BashOperator(
        task_id="gate_cross_mart",
        bash_command=(
            f"cd {PROJECT_ROOT} && "
            f".venv/bin/python -m quality.runner --layer cross_mart"
        ),
        env=COMMON_ENV,
        append_env=True,
    )

    dbt_test_all = BashOperator(
        task_id="dbt_test_all",
        bash_command=(f"cd {PROJECT_ROOT}/dbt && ../.venv/bin/dbt test"),
        env=COMMON_ENV,
        append_env=True,
    )

    (
        plan_ingest
        >> ingest_bronze
        >> gate_bronze
        >> build_silver
        >> gate_silver
        >> build_gold
        >> gate_gold
        >> gate_cross_mart
        >> dbt_test_all
    )
