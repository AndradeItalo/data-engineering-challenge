from datetime import datetime, timedelta

from airflow import DAG
from airflow.models import Variable
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

PROJECT_DIR = "/opt/airflow/project"
DBT_DIR = f"{PROJECT_DIR}/dbt/urban_mobility"


def resolve_competency(**context):
    conf = context["dag_run"].conf or {}
    if "year" in conf and "month" in conf:
        year, month = int(conf["year"]), int(conf["month"])
    else:
        try:
            year = int(Variable.get("uma_year"))
            month = int(Variable.get("uma_month"))
        except KeyError:
            logical_date = context["logical_date"]
            previous_month = logical_date.replace(day=1) - timedelta(days=1)
            year, month = previous_month.year, previous_month.month

    context["ti"].xcom_push(key="year", value=year)
    context["ti"].xcom_push(key="month", value=month)
    context["ti"].xcom_push(key="year_month", value=f"{year:04d}-{month:02d}")
    print(f"Competência resolvida: {year:04d}-{month:02d}")


def refresh_materialized_view(**context):
    import psycopg2

    from src.common.settings import PostgresConfig

    pg = PostgresConfig.from_env()
    conn = psycopg2.connect(host=pg.host, port=pg.port, dbname=pg.database, user=pg.user, password=pg.password)
    try:
        with conn.cursor() as cur:
            cur.execute("REFRESH MATERIALIZED VIEW gold.mv_monthly_indicators")
        conn.commit()
    finally:
        conn.close()


default_args = {
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="urban_mobility_pipeline",
    schedule="@weekly",
    start_date=datetime(2025, 1, 1),
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
) as dag:

    year_expr = "{{ ti.xcom_pull(task_ids='resolve_competency', key='year') }}"
    month_expr = "{{ ti.xcom_pull(task_ids='resolve_competency', key='month') }}"
    year_month_expr = '{{ ti.xcom_pull(task_ids="resolve_competency", key="year_month") }}'

    t_resolve = PythonOperator(
        task_id="resolve_competency",
        python_callable=resolve_competency,
    )

    t_download = BashOperator(
        task_id="download_bronze",
        bash_command=f"cd {PROJECT_DIR} && python -m src.bronze.download_tlc_data --year {year_expr} --month {month_expr}",
    )

    t_silver = BashOperator(
        task_id="process_silver",
        bash_command=f"cd {PROJECT_DIR} && python -m src.silver.silver_job --year {year_expr} --month {month_expr}",
    )

    t_dbt_deps = BashOperator(
        task_id="dbt_deps",
        bash_command=f"cd {DBT_DIR} && dbt deps",
    )

    t_dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command=f"cd {DBT_DIR} && dbt run --vars '{{\"year_month\": \"{year_month_expr}\"}}'",
    )

    t_refresh_mv = PythonOperator(
        task_id="refresh_mv",
        python_callable=refresh_materialized_view,
    )

    t_dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command=f"cd {DBT_DIR} && dbt test",
    )

    t_resolve >> t_download >> t_silver >> t_dbt_deps >> t_dbt_run >> t_refresh_mv >> t_dbt_test