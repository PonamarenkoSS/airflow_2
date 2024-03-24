import requests
from datetime import datetime
from airflow import DAG
from airflow.operators.python import BranchPythonOperator, PythonOperator
import json
from airflow.models import Variable

def get_weather_temperature():
    url = f"https://api.openweathermap.org/data/2.5/weather?lat=53.12&lon=50.06&appid=64114b2d324939c9758a6ed91666ef73&units=metric"
    response = requests.get(url)
    data = response.json()
    temperature = data['main']['temp']
    if temperature > 15:
        return 'warm_branch'
    return 'cold_branch'


default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 3, 23),
    'retries': 1,
}

dag = DAG(
    'OpenWeatherApi_request_py',
    default_args=default_args,
    description='DAG to fetch and display temperature from OpenWeather API',
    schedule_interval=None,
)

check_weather_branch = BranchPythonOperator(
    task_id='check_branch',
    python_callable=get_weather_temperature,
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


check_weather_branch >> warm_branch
check_weather_branch >> cold_branch

