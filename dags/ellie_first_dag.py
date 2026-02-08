from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator

def hello():
    print("Hello, Airflow!")

with DAG(
    dag_id="ellie_first_dag",
    start_date=datetime(2026, 2, 8),
    schedule_interval="@daily",  # daily run
    catchup=False
) as dag:

    task1 = PythonOperator(
        task_id="say_hello",
        python_callable=hello
    )