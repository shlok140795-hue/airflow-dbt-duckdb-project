from datetime import datetime

from airflow.sdk import DAG # type: ignore
from airflow.providers.standard.operators.bash import BashOperator # type: ignore

DBT_PROJECT_DIR = "/opt/airflow/dbt_project"
DBT_PROFILES_DIR = "/opt/airflow/dbt_project/profiles"

with DAG(
    dag_id="dbt_sales_pipeline",
    description="Run dbt models and tests using DuckDB",
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
    tags=["dbt", "duckdb", "sales"],
) as dag:

    run_dbt_model = BashOperator(
        task_id="run_dbt_model",
        bash_command=(
            f"dbt run --project-dir {DBT_PROJECT_DIR} "
            f"--profiles-dir {DBT_PROFILES_DIR}"
        ),
    )

    test_dbt_model = BashOperator(
        task_id="test_dbt_model",
        bash_command=(
            f"dbt test --project-dir {DBT_PROJECT_DIR} "
            f"--profiles-dir {DBT_PROFILES_DIR}"
        ),
    )

    run_dbt_model >> test_dbt_model