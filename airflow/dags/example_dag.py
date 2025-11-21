"""
Example Airflow DAG demonstrating basic tasks and dependencies.
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}


def _print_hello():
    print("Hello from Airflow!")
    return "Hello task completed"


def _process_data(**context):
    print(f"Processing data for execution date: {context['ds']}")
    return "Data processed"


with DAG(
    dag_id="example_dag",
    default_args=default_args,
    description="A simple example DAG",
    schedule="@daily",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["example"],
) as dag:

    start = EmptyOperator(task_id="start")

    hello_task = PythonOperator(
        task_id="hello_task",
        python_callable=_print_hello,
    )

    process_task = PythonOperator(
        task_id="process_data",
        python_callable=_process_data,
    )

    end = EmptyOperator(task_id="end")

    start >> hello_task >> process_task >> end
