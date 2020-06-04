from datetime import datetime

from airflow.models import DAG
from airflow.utils.dates import days_ago
from airflow.operators.python_operator import PythonOperator


default_args = {
    'owner': 'Fredrik Bakken',
    'start_date': datetime(2018, 5, 9),
}

dag = DAG(
    dag_id='step-3_iot-23-dataset',
    default_args=default_args,
    description='DAG for putting the IoT-23 dataset into Postgres tables.',
    schedule_interval="00 07 * * *",
)


# ########################################################################


def hello_world(**kwargs):
    print("Hello world!")
    print("Hello Norway!")


def how_are_you(**kwargs):
    print("How are you?")
    print("I am doing well!")


# ########################################################################


task_hello_world = PythonOperator(
    dag=dag,
    task_id='hello_world',
    python_callable=hello_world,
    provide_context=True,
)

task_how_are_you = PythonOperator(
    dag=dag,
    task_id='how_are_you',
    python_callable=how_are_you,
    provide_context=True,
)


# ########################################################################


task_hello_world >> task_how_are_you
