from datetime import datetime
from airflow.models import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.postgres_operator import PostgresOperator
import pandas as pd
import psycopg2
import random

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 3, 29),
    'retries': 1
}

dag = DAG(
    'data_loading_dag',
    default_args=default_args,
    description='DAG for loading data from CSV files to a database',
    schedule_interval=None,
)

def generate_and_square_number():
    num = random.randint(1, 100)
    squared_num = num ** 2
    print(f"Generated number: {num}")
    print(f"Squared number: {squared_num}")

create_table_task = PythonOperator(
    task_id='create_table',
    python_callable=generate_and_square_number,
    dag=dag
)

load_files_task = PythonOperator(
    task_id='load_files',
    python_callable=generate_and_square_number,
    dag=dag,
)

create_table_task >> load_files_task 
