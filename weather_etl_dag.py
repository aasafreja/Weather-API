# weather_etl_dag.py

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
from weather_etl_functions import main_task  # Import the main_task function

# Load environment variables from .env file
from dotenv import load_dotenv
import os
load_dotenv()

EMAIL = os.getenv("EMAIL")


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email': EMAIL,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'weather_etl_pipeline',
    default_args=default_args,
    description='A DAG to fetch weather data and load into a PostgreSQL database',
    schedule_interval=timedelta(hours=6),  # Run every 6 hours
    start_date=datetime(2024, 8, 1),
    catchup=False,
)

t1_main_task = PythonOperator(
    task_id='run_main',
    python_callable=main_task,
    dag=dag,
)

t1_main_task
