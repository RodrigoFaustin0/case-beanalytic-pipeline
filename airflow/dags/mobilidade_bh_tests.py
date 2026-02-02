from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

with DAG(
    dag_id="mobilidade_bh_testes",
    start_date=datetime(2026, 1, 2),
    schedule="@daily",
    catchup=False,
    tags=["case", "mobilidade","tests"],
) as dag:

    run_tests = BashOperator(
        task_id="run_tests",
        bash_command="cd /opt/project && pytest -q",
    )

    run_tests
