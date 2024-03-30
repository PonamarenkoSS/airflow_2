from datetime import datetime
from airflow.models import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.postgres_operator import PostgresOperator
import pandas as pd
import psycopg2

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

def load_transform_files():
    conn = psycopg2.connect(
        dbname="airflow_db", 
        user="admin", 
        password="17071997", 
        host="localhost",
        port="5432")
    booking = pd.read_csv('booking.csv')
    client = pd.read_csv('client.csv')
    hotel = pd.read_csv('hotel.csv')
    df = client.merge(booking, on='client_id', how='outer')
    df = df.merge(hotel, on='hotel_id', how='outer')
    df = df.dropna()
    df.rename(columns={'name_x': 'name_client', 'name_y': 'name_hotel'}, inplace=True)
    del df['client_id']
    del df['hotel_id']
    df['booking_date'] = pd.to_datetime(df['booking_date'])
    df.to_sql("booking", conn, if_exists="append", index=False)

def create_table():
    create_table=PostgresOperator(
        task_id='create_table',
        postgress_conn_id='my_postgress_conn',
        sql='''
        CREATE TABLE IF NOT EXISTS booking (
        age INTEGER,
        name_client TEXT,
        type TEXT,
        booking_date DATE,
        room_type TEXT,
        booking_cost INTEGER,
        currency TEXT,
        name_hotel TEXT,
        address TEXT
        );
        '''
)

create_table_task = PythonOperator(
    task_id='create_table',
    python_callable=create_table,
    dag=dag
)

load_files_task = PythonOperator(
    task_id='load_files',
    python_callable=load_transform_files,
    dag=dag,
)

create_table_task >> load_files_task 
