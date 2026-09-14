"""Manual daily and range DAGs; business logic lives in standalone modules."""
import os
from datetime import datetime, timedelta, timezone

from airflow.sdk import DAG, Param, task
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.standard.operators.hitl import ApprovalOperator
from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator

from src.date_range import processing_dates


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
        stage_task = BashOperator(
            task_id=task_id,
            bash_command=command + ' --processing-date "$PROCESSING_DATE" --config "$PIPELINE_CONFIG"',
            cwd=os.environ.get("CHICAGO_PROJECT_ROOT", "/opt/chicago"),
            env={"PROCESSING_DATE": "{{ params.processing_date }}",
                 "PIPELINE_CONFIG": os.environ.get("CHICAGO_CONFIG", "config/chicago_taxi.yml")},
            append_env=True,
            do_xcom_push=False,
        )
        previous >> stage_task
        previous = stage_task
        if task_id == "quality_gate":
            human_approval = ApprovalOperator(
                task_id="approve_data_and_business_quality",
                subject="Approve data and business quality - {{ params.processing_date }}",
                body=(
                    "Automated Bronze and Silver checks passed. Review the validate_bronze "
                    "and quality_gate task outputs, including row reconciliation, rejection "
                    "rate and business consistency. Approve to build and publish Gold; "
                    "Reject stops downstream tasks."
                ),
                fail_on_reject=False,
                response_timeout=timedelta(days=7),
                execution_timeout=timedelta(days=7),
            )
            previous >> human_approval
            previous = human_approval
    end = EmptyOperator(task_id="end")
    previous >> end


with DAG(
    dag_id="chicago_taxi_range_pipeline",
    start_date=datetime(2023, 1, 1, tzinfo=timezone.utc),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    params={
        "start_date": Param("2023-06-01", type="string", format="date"),
        "end_date": Param("2023-06-07", type="string", format="date"),
    },
    tags=["chicago-taxi", "range"],
) as range_dag:
    @task
    def build_daily_configs(start_date, end_date):
        return [{"processing_date": day} for day in processing_dates(start_date, end_date)]

    daily_configs = build_daily_configs("{{ params.start_date }}", "{{ params.end_date }}")
    TriggerDagRunOperator.partial(
        task_id="run_daily_partition",
        trigger_dag_id="chicago_taxi_batch_pipeline",
        trigger_run_id="range__{{ ts_nodash }}__{{ ti.map_index }}",
        wait_for_completion=True,
        poke_interval=30,
        deferrable=True,
        fail_when_dag_is_paused=True,
    ).expand(conf=daily_configs)
