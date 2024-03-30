from datetime import datetime
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.http_operator import SimpleHttpOperator
from airflow.operators.telegram_operator import TelegramOperator
import json
from airflow.models import Variable

API_key = Variable.get('openweather_key')
url = f"/data/2.5/weather?lat=53.12&lon=50.06&appid={API_key}&units=metric"

def get_weather_info():
    response = SimpleHttpOperator(
        task_id='get_weather_data',
        http_conn_id='openweather',
        endpoint=url,
        method='GET'
    )
    task_instance = response.execute(None)
    weather_data = json.loads(task_instance.content)

    TelegramOperator(
        task_id='send_telegram_message',
        telegram_conn_id='telegram_connection',
        message=f'Weather data for city: {weather_data}',
        channel='+stvHKIZUplZjM2Iy'
    )

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 3, 30),
    'retries': 1
}

dag = DAG(
    'weather_telegram_dag',
    default_args=default_args,
    description='DAG to get weather data and send to Telegram',
    schedule_interval=None,
)

get_weather_task = PythonOperator(
    task_id='get_weather_info_task',
    python_callable=get_weather_info,
    dag=dag,
)

get_weather_task