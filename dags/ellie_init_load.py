from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
import ellie_01_load_dim_date

ellie_01_load_dim_date.ellie_load_dim_date()

with DAG(
    dag_id="ellie_init_load",
    start_date=datetime(2026, 2, 8),
    schedule_interval="@daily",  # daily run
    catchup=False
) as dag:

    task1 = PythonOperator(
        task_id="ellie_01_load_dim_date",
        python_callable=ellie_load_dim_date
    )