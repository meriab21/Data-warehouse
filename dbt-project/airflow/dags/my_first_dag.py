from sched import scheduler
from signal import default_int_handler
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator


default_args = {
    'owner': 'coder2j',
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
}

with DAG(
    dag_id="my_first_dag",
    default_args=default_args,
    description='This is my first dag',
    start_date=datetime(2022, 7, 23, 2),
    schedule_interval='@daily',
) as dag:
    task1 = BashOperator(
        task_id='first_task',
        bash_command='echo "Hello World" this is my first task whoohoo'
    )

    task1
