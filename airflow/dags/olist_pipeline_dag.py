from __future__ import annotations

import glob
import json
import os
import re
import subprocess
from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import BranchPythonOperator, PythonOperator
from airflow.utils.trigger_rule import TriggerRule


DAG_ID = "olist_pipeline"
SCHEDULE = "*/30 * * * *"

PROJECT_ROOT = Path(os.getenv("PROJECT_ROOT", Path(__file__).resolve().parents[2]))

INCOMING_DIR = PROJECT_ROOT / "data" / "raw" / "olist" / "incoming"
CHECKPOINT_PATH = PROJECT_ROOT / "data" / "checkpoints" / "bronze_ingest_state.json"

MARKER_GLOB = str(INCOMING_DIR / "*.ready")
MARKER_PATTERN = re.compile(r"_BATCH_(?P<batch_id>.+)\.ready$", re.IGNORECASE)

SCRIPTS_DIR = PROJECT_ROOT / "scripts"
PHASE2 = SCRIPTS_DIR / "run_phase2.sh"
PHASE3 = SCRIPTS_DIR / "run_phase3.sh"
PHASE4 = SCRIPTS_DIR / "run_phase4.sh"

BASH = os.getenv("BASH_BIN", "bash")
XCOM_KEY_BATCH_ID = "batch_id"


def _find_markers_sorted() -> list[Path]:
    markers = [Path(p) for p in glob.glob(MARKER_GLOB)]
    markers = [m for m in markers if m.is_file()]
    markers.sort(key=lambda p: p.stat().st_mtime, reverse=True)
    return markers


def _extract_batch_id_from_marker(marker: Path) -> Optional[str]:
    m = MARKER_PATTERN.search(marker.name)
    return m.group("batch_id").strip() if m else None


def _read_last_processed_batch_id() -> Optional[str]:
    if not CHECKPOINT_PATH.exists():
        return None
    with open(CHECKPOINT_PATH, "r", encoding="utf-8") as f:
        data = json.load(f)
    val = data.get("last_batch_id")
    return val.strip() if isinstance(val, str) and val.strip() else None


def choose_branch(**context) -> str:
    ti = context["ti"]

    markers = _find_markers_sorted()
    if not markers:
        ti.xcom_push(key=XCOM_KEY_BATCH_ID, value=None)
        return "t_noop"

    latest_marker = markers[0]
    batch_id = _extract_batch_id_from_marker(latest_marker)
    if not batch_id:
        raise RuntimeError(f"Marker filename not parseable: {latest_marker.name}")

    last_batch_id = _read_last_processed_batch_id()
    ti.xcom_push(key=XCOM_KEY_BATCH_ID, value=batch_id)

    if last_batch_id and batch_id == last_batch_id:
        return "t_noop"

    return "t_phase2_bronze"


def run_bash(script_path: Path, script_args: list[str] | None = None) -> None:
    script_args = script_args or []
    if not script_path.exists():
        raise FileNotFoundError(f"Script not found: {script_path}")

    cmd = [BASH, str(script_path), *script_args]
    proc = subprocess.run(
        cmd,
        cwd=str(PROJECT_ROOT),
        capture_output=True,
        text=True,
        env=os.environ.copy(),
    )

    if proc.stdout:
        print(proc.stdout)
    if proc.stderr:
        print(proc.stderr)

    if proc.returncode != 0:
        raise RuntimeError(f"Script failed: {script_path.name} | exit_code={proc.returncode}")


def run_phase3(**context) -> None:
    ti = context["ti"]
    batch_id = ti.xcom_pull(task_ids="t_branch_on_marker_and_checkpoint", key=XCOM_KEY_BATCH_ID)
    if not batch_id or not isinstance(batch_id, str):
        raise RuntimeError("Missing batch_id in XCom. Check choose_branch().")
    run_bash(script_path=PHASE3, script_args=[batch_id])


def notify_success(**context) -> None:
    markers = _find_markers_sorted()
    if not markers:
        print("SUCCESS (NO-OP): no marker in incoming/")
        return
    latest_marker = markers[0]
    batch_id = _extract_batch_id_from_marker(latest_marker) or "<unknown>"
    last_batch_id = _read_last_processed_batch_id()
    print("SUCCESS")
    print(f"marker={latest_marker.name}")
    print(f"batch_id={batch_id}")
    print(f"checkpoint_last_batch_id={last_batch_id}")


def notify_failure(**context) -> None:
    print("FAILURE: at least one phase failed. check task logs.")


default_args = {
    "owner": "you",
    "depends_on_past": False,
    "retries": 0,
    "retry_delay": timedelta(minutes=1),
}

with DAG(
    dag_id=DAG_ID,
    default_args=default_args,
    start_date=datetime(2025, 1, 1),
    schedule=SCHEDULE,
    catchup=False,
    max_active_runs=1,
    tags=["olist", "uplift"],
) as dag:
    t_start = EmptyOperator(task_id="t_start")

    t_branch = BranchPythonOperator(
        task_id="t_branch_on_marker_and_checkpoint",
        python_callable=choose_branch,
    )

    t_noop = EmptyOperator(task_id="t_noop")

    t_phase2 = PythonOperator(
        task_id="t_phase2_bronze",
        python_callable=run_bash,
        op_kwargs={"script_path": PHASE2, "script_args": []},
    )

    t_phase3 = PythonOperator(
        task_id="t_phase3_silver",
        python_callable=run_phase3,
    )

    t_phase4 = PythonOperator(
        task_id="t_phase4_gold",
        python_callable=run_bash,
        op_kwargs={"script_path": PHASE4, "script_args": []},
    )

    t_join = EmptyOperator(
        task_id="t_join",
        trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS,
    )

    t_notify_success = PythonOperator(
        task_id="t_notify_success",
        python_callable=notify_success,
        trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS,
    )

    t_notify_failure = PythonOperator(
        task_id="t_notify_failure",
        python_callable=notify_failure,
        trigger_rule=TriggerRule.ONE_FAILED,
    )

    t_end = EmptyOperator(task_id="t_end", trigger_rule=TriggerRule.ALL_DONE)

    t_start >> t_branch
    t_branch >> t_noop >> t_join
    t_branch >> t_phase2 >> t_phase3 >> t_phase4 >> t_join
    t_join >> t_notify_success
    [t_phase2, t_phase3, t_phase4] >> t_notify_failure
    [t_notify_success, t_notify_failure] >> t_end
