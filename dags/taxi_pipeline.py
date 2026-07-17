import os
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.models.param import Param

# parametros padrao para rodar manualmente para qualquer mess de 2025
default_params = {
    "year": Param("2025", type="string", description="ano do processamento (ex: 2025)"),
    "month": Param("1", type="string", description="mes do processamento (1 a 12)")
}

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2026, 1, 1),
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


    # pega o ano e mes dos parametros se rodado manualmente ou do contexto se rodado agendado
    download_bronze = BashOperator(
        task_id='download_bronze',
        bash_command=(
            "python /opt/airflow/scripts/download_data.py "
            "{{ params.year }} {{ params.month }}"
        )
    )

    # processa e aplica regras de qualidade no spark (silver)
    process_silver = BashOperator(
        task_id='process_silver',
        bash_command=(
            "python /opt/airflow/scripts/process_silver.py "
            "{{ params.year }} {{ params.month }}"
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
