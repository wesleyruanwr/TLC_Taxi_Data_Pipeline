import os
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.models.param import Param

# parametros padrao: uma execucao processa pelo menos 6 meses de 2025
default_params = {
    "year": Param("2025", type="string", description="ano do processamento (ex: 2025)"),
    "start_month": Param("1", type="string", description="mes inicial do processamento (1 a 12)"),
    "end_month": Param("6", type="string", description="mes final do processamento (1 a 12)")
}

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2025, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'tlc_yellow_taxi_pipeline',
    default_args=default_args,
    description='pipeline para analisar corridas de taxi da TLC',
    schedule_interval='@weekly', # como solicitado no pdf
    catchup=False,
    params=default_params,
    max_active_runs=1
) as dag:

    year = "{{ params.year }}"
    start_month = "{{ params.start_month }}"
    end_month = "{{ params.end_month }}"

    download_bronze = BashOperator(
        task_id='download_bronze',
        bash_command=(
            "set -e; "
            f"for month in $(seq {start_month} {end_month}); do "
            f"python /opt/airflow/scripts/download_data.py {year} $month; "
            "done"
        )
    )

    # processa e aplica regras de qualidade no spark (silver)
    process_silver = BashOperator(
        task_id='process_silver',
        bash_command=(
            "set -e; "
            f"for month in $(seq {start_month} {end_month}); do "
            f"python /opt/airflow/scripts/process_silver.py {year} $month; "
            "done"
        )
    )

    # rodar transformacoes no dbt (gold)
    dbt_run = BashOperator(
        task_id='dbt_run',
        bash_command=(
            "cd /opt/airflow/dbt_project && "
            "dbt run --profiles-dir . --project-dir ."
        )
    )

    # testes de qualidade dbt
    dbt_test = BashOperator(
        task_id='dbt_test',
        bash_command=(
            "cd /opt/airflow/dbt_project && "
            "dbt test --profiles-dir . --project-dir ."
        )
    )

    download_bronze >> process_silver >> dbt_run >> dbt_test
