from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2021, 1, 1),
}

dag = DAG(
    'print_ds_variable',
    default_args=default_args,
    schedule_interval='@daily',
)

def print_ds(ds, **kwargs):
    print(f"The value of ds is {ds}")

print_ds_task = PythonOperator(
    task_id='print_ds',
    python_callable=print_ds,
    op_kwargs={'ds': '{{ ds }}'},
    dag=dag,
)

print_ds_task