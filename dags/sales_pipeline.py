from datetime import datetime

from airflow.sdk import DAG # type: ignore
from airflow.providers.standard.operators.empty import EmptyOperator # type: ignore
from airflow.providers.standard.operators.bash import BashOperator # type: ignore

DBT_PROJECT = "/opt/airflow/dbt_project"
DBT_PROFILES = "/opt/airflow/dbt_project/profiles"

with DAG(
    dag_id="sales_pipeline",
    description="Load CSV, transform sales with dbt, and run tests",
    start_date=datetime(2025, 1, 1),
    schedule="@daily",
    catchup=False,
    max_active_runs=1,
    tags=["sales", "dbt", "duckdb"],
) as dag:

    start = EmptyOperator(task_id="start")

    load_csv = BashOperator(
        task_id="load_csv",
        bash_command="""
python - <<'PY'
import duckdb

con = duckdb.connect('/opt/airflow/data/sales.duckdb')

con.execute('CREATE SCHEMA IF NOT EXISTS public')

con.execute('''
CREATE OR REPLACE TABLE public.sales_raw (
    order_id INTEGER,
    customer VARCHAR,
    city VARCHAR,
    product VARCHAR,
    quantity INTEGER,
    price DECIMAL(10,2)
)
''')

con.execute('''
INSERT INTO public.sales_raw
SELECT order_id, customer, city, product, quantity, price
FROM read_csv_auto('/opt/airflow/data/customer_sales.csv')
''')

print('Loaded rows:', con.execute(
    'SELECT COUNT(*) FROM public.sales_raw'
).fetchone()[0])

con.close()
PY
""",
    )

    dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command=(
            f"dbt run --select +customer_sales "
            f"--project-dir {DBT_PROJECT} "
            f"--profiles-dir {DBT_PROFILES}"
        ),
    )

    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command=(
            f"dbt test --select stg_sales "
            f"--project-dir {DBT_PROJECT} "
            f"--profiles-dir {DBT_PROFILES}"
        ),
    )

    end = EmptyOperator(task_id="end")

    start >> load_csv >> dbt_run >> dbt_test >> end