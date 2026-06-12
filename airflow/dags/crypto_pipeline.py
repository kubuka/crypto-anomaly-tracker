from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta
import sys

sys.path.append("/opt/airflow/scripts")  # Dodajemy ścieżkę do folderu ze skryptami
from pull import pull
from silver_transform import silver_transform

with DAG(
    dag_id="crypto_data_pipeline_v2",
    start_date=datetime(2026, 6, 11),
    schedule_interval=None,
    catchup=False,
    default_args={
        "owner": "Jakub",
        "retries": 1,
        "retry_delay": timedelta(minutes=0.5),
    },
) as dag:

    task_pull = PythonOperator(task_id="pyspark_api_ingestion", python_callable=pull)

    task_silver = PythonOperator(
        task_id="pyspark_silver_transform", python_callable=silver_transform
    )

    task_dbt_run = BashOperator(
        task_id="dbt_run", bash_command="cd /opt/airflow/dbt_crypto && dbt run"
    )

    task_pull >> task_silver >> task_dbt_run
