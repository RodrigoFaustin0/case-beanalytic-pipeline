from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

with DAG(
    dag_id="mobilidade_bh_pipeline",
    start_date=datetime(2026, 1, 2),
    schedule="@daily",
    catchup=False,
    tags=["case", "mobilidade"],
) as dag:

    run_pipeline = BashOperator(
        task_id="run_pipeline",
        bash_command="cd /opt/project && python run_pipeline.py",
    )


    run_pipeline
