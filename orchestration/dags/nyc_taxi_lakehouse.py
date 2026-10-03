from datetime import datetime

from airflow import DAG
from airflow.operators.bash import BashOperator

PROJECT = "/opt/lakehouse-dq-agent"
MONTH = (
    "{{ (data_interval_start - macros.dateutil.relativedelta.relativedelta(months=2)).strftime('%Y-%m') }}"
)

with DAG(
    dag_id="nyc_taxi_lakehouse",
    start_date=datetime(2024, 3, 1),
    schedule="@monthly",
    catchup=False,
    max_active_runs=1,
    default_args={"retries": 2},
    tags=["lakehouse", "data-quality"],
) as dag:
    load = BashOperator(task_id="bronze_silver_quality", bash_command=f"cd {PROJECT} && lakehouse {MONTH}")
    marts = BashOperator(
        task_id="dbt_build",
        bash_command=f"cd {PROJECT}/dbt && dbt build --profiles-dir .",
    )
    agent = BashOperator(task_id="dq_agent", bash_command=f"cd {PROJECT} && python -m lakehouse.agent")
    regression = BashOperator(
        task_id="dbt_test_agent_rules", bash_command=f"cd {PROJECT}/dbt && dbt test --profiles-dir ."
    )

    load >> marts >> agent >> regression
