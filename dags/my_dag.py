# dags/my_dag.py
import sys

sys.path.append('/home/alex/Desktop/DataEngineering/')

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from scripts.extract_data import extract_data
from scripts.clean_data import clean_data
from scripts.load_data import load_data


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'my_dag',
    default_args=default_args,
    description='A simple Airflow DAG',
    schedule_interval=timedelta(days=1),
)

extract_task = PythonOperator(
    task_id='extract_data',
    python_callable=extract_data,
    op_args=["/home/alex/Desktop/DataEngineering/Data in csv/source_data.csv"],
    dag=dag,
)

clean_task = PythonOperator(
    task_id='clean_data',
    python_callable=clean_data,
    op_args=["/home/alex/Desktop/DataEngineering/Data in csv/source_data.csv"],
    dag=dag,
)

load_task = PythonOperator(
    task_id='load_data',
    python_callable=load_data,
    provide_context=True,  # Change to True to access context
    op_args=[],
    dag=dag,
)

extract_task >> clean_task >> load_task
