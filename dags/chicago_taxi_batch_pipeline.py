"""One manual batch DAG; business logic lives in the standalone modules."""
import os
from datetime import datetime, timedelta, timezone

from airflow.sdk import DAG, Param
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.empty import EmptyOperator


with DAG(
    dag_id="chicago_taxi_batch_pipeline",
    start_date=datetime(2023, 1, 1, tzinfo=timezone.utc),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    params={"processing_date": Param("2023-06-01", type="string", format="date",
                                     pattern=r"^\d{4}-\d{2}-\d{2}$")},
    default_args={"retries": 1, "retry_delay": timedelta(minutes=1),
                  "retry_exponential_backoff": True, "max_retry_delay": timedelta(minutes=5),
                  "execution_timeout": timedelta(hours=2)},
    tags=["chicago-taxi", "batch"],
) as dag:
    start = EmptyOperator(task_id="start")
    previous = start
    commands = {
        "bronze": "python -m scripts.run_stage --layer bronze",
        "validate_bronze": "python -m src.dataops --layer bronze",
        "silver": "python -m scripts.run_stage --layer silver",
        "quality_gate": "python -m src.dataops --layer silver",
        "gold": "python -m scripts.run_stage --layer gold",
        "validate_gold": "python -m src.dataops --layer gold",
    }
    for task_id, command in commands.items():
        task = BashOperator(
            task_id=task_id,
            bash_command=command + ' --processing-date "$PROCESSING_DATE" --config "$PIPELINE_CONFIG"',
            cwd=os.environ.get("CHICAGO_PROJECT_ROOT", "/opt/chicago"),
            env={"PROCESSING_DATE": "{{ params.processing_date }}",
                 "PIPELINE_CONFIG": os.environ.get("CHICAGO_CONFIG", "config/chicago_taxi.yml")},
            append_env=True,
            do_xcom_push=False,
        )
        previous >> task
        previous = task
    end = EmptyOperator(task_id="end")
    previous >> end
