from datetime import datetime
from airflow import DAG
from airflow.operators.python import BranchPythonOperator, PythonOperator
from airflow.providers.http.operators.http import HttpOperator
import json
from airflow.models import Variable

API_key = Variable.get('openweather_key')
url = f"/data/2.5/weather?q=Samara,ru&ecxlude=current&appid={API_key}&units=metric"

def get_weather(ti):
    current_temp = ti.xcom_pull(task_ids='get_weather')
    if current_temp > 15:
        return 'warm_branch'
    return 'cold_branch'

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 3, 23),
    'retries': 1,
}

dag = DAG(
    'OpenWeatherApi_http',
    default_args=default_args,
    description='DAG to fetch and display temperature from OpenWeather API',
    schedule_interval=None,
)

get_response = HttpOperator(
    task_id='get_weather',
    method='GET',
    http_conn_id='openweather',
    endpoint=url,
    response_filter=lambda response: response.json()["main"]["temp"],
    headers={},
    dag=dag
)

check_weather_branch = BranchPythonOperator(
    task_id='check_branch',
    python_callable=get_weather,
    dag=dag,
)

warm_branch = PythonOperator(
    task_id='warm_branch',
    python_callable=lambda ti: print(f'ТЕПЛО: {ti.xcom_pull(task_ids="check_branch")}°C'),
    dag=dag
)

cold_branch = PythonOperator(
    task_id='cold_branch',
    python_callable=lambda ti: print(f'ХОЛОДНО: {ti.xcom_pull(task_ids="check_branch")}°C'),
    dag=dag
)

get_response >> check_weather_branch
check_weather_branch >> warm_branch
check_weather_branch >> cold_branch

