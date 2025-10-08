from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2025, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
        dag_id='DockerClean',
        default_args=default_args,
        description='Безопасная очистка завершенных Docker-ресурсов',
        schedule_interval='0 3 * * *',  # ежедневно в 3:00
        catchup=False,
        tags=['maintenance'],
) as dag:
    clean_containers = BashOperator(
        task_id='prune_exited_containers',
        bash_command='docker container prune -f',
    )

    clean_images = BashOperator(
        task_id='prune_unused_images',
        bash_command='docker image prune -af',
    )

    clean_volumes = BashOperator(
        task_id='prune_unused_volumes',
        bash_command='docker volume prune -f',
    )

    clean_networks = BashOperator(
        task_id='prune_unused_networks',
        bash_command='docker network prune -f',
    )

    clean_containers >> clean_images >> clean_volumes >> clean_networks
