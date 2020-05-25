import os
import pyspark

from airflow.models import DAG
from airflow.utils.dates import days_ago
from airflow.operators.python_operator import PythonOperator


default_args = {
    'owner': 'Fredrik Bakken',
    # 'depends_on_past': False,
    'start_date': days_ago(1),
    # 'email': ['email@email.com'],
    # 'email_on_failure': False,
    # 'email_on_retry': False,
    # 'retries': 1,
    # 'retry_delay': timedelta(minutes=5),
    # 'queue': 'bash_queue',
    # 'pool': 'backfill',
    # 'priority_weight': 10,
    # 'end_date': datetime(2016, 1, 1),
    # 'wait_for_downstream': False,
    # 'dag': dag,
    # 'sla': timedelta(hours=2),
    # 'execution_timeout': timedelta(seconds=300),
    # 'on_failure_callback': some_function,
    # 'on_success_callback': some_other_function,
    # 'on_retry_callback': another_function,
    # 'sla_miss_callback': yet_another_function,
    # 'trigger_rule': 'all_success'
    # 'provide_context': True,
}

dag = DAG(
    dag_id='step-1_data-cleaner',
    default_args=default_args,
    description='A simple DAG for cleaning and preparing the IoT-23 dataset.',
    schedule_interval=None,
)


# ########################################################################


def check_if_cleaned(**kwargs):
    print("Checking if the IoT-23 dataset has cleaned...")

    path_dataset = os.path.exists("/usr/local/airflow/datasets/iot-23")
    path_raw_dataset = os.path.exists("/usr/local/airflow/datasets/opt")

    if (path_dataset is True and path_raw_dataset is False):
        print("Good state: Cleaned dataset exists and raw dataset has been removed")
        kwargs['ti'].xcom_push(key='status_code', value=1)
    elif (path_dataset is True and path_raw_dataset is True):
        print("Bad state: Cleaned dataset exists and raw dataset exists")
        kwargs['ti'].xcom_push(key='status_code', value=-1)
    else:
        print("Bad state: Cleaned dataset does not exist and raw dataset exists")
        kwargs['ti'].xcom_push(key='status_code', value=0)

    print("Checking if the IoT-23 dataset has been cleaned completed!")


def clean_the_dataset(**kwargs):
    print("Clean the dataset")
    # Using Apache Spark
    
    print(kwargs)
    ti = kwargs['ti']
    print(ti)
    status_code = ti.xcom_pull(key=None, task_ids='check_if_cleaned')
    print(status_code)



# ########################################################################


task_check_if_cleaned = PythonOperator(
    dag=dag,
    task_id='check_if_cleaned',
    python_callable=check_if_cleaned,
    provide_context=True,
)

task_clean_the_dataset = PythonOperator(
    dag=dag,
    task_id='clean_the_dataset',
    python_callable=clean_the_dataset,
    provide_context=True,
)


# ########################################################################


task_check_if_cleaned >> task_clean_the_dataset
