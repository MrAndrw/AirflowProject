from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2025, 7, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'AirflowLogsClean',
    default_args=default_args,
    description='Безопасная очистка логов Airflow старше 7 дней',
    schedule_interval='@daily',  # запуск каждый день
    catchup=False,
) as dag:

    clean_logs = BashOperator(
        task_id='remove_old_logs',
        bash_command='find /opt/airflow/logs/ -type f -mtime +7 -exec rm -f {} \;',
    )
    clean_logs

