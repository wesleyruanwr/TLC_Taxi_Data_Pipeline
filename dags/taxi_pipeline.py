import os
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.models.param import Param

# parametros para execucao manual — quando acionado via trigger usa esses valores
# quando rodado automaticamente pelo scheduler o ano/mes sao derivados da data logica de execucao
default_params = {
    "year": Param("2025", type="string", description="ano do processamento (ex: 2025) — usado apenas em trigger manual"),
    "month": Param("1", type="string", description="mes do processamento (1 a 12) — usado apenas em trigger manual")
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
    schedule_interval='@weekly',  # como solicitado no pdf
    catchup=False,
    params=default_params,
    max_active_runs=1
) as dag:

    # se  for disparada manualmente (external_trigger=True) usa os params informados
    # se rodar pelo scheduler automaticamente deriva o ano/mes da data logica de execucao
    _year  = "{{ params.year  if dag_run.external_trigger else logical_date.strftime('%Y') }}"
    _month = "{{ params.month if dag_run.external_trigger else logical_date.strftime('%-m') }}"

    download_bronze = BashOperator(
        task_id='download_bronze',
        bash_command=(
            f"python /opt/airflow/scripts/download_data.py {_year} {_month}"
        )
    )

    # processa e aplica regras de qualidade no spark (silver)
    process_silver = BashOperator(
        task_id='process_silver',
        bash_command=(
            f"python /opt/airflow/scripts/process_silver.py {_year} {_month}"
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
