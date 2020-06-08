import pyspark
import pyspark.sql.functions as F

from datetime import datetime

from pyspark.sql import SparkSession
from pyspark.sql.types import *

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


path_cleaned_dataset = "/usr/local/airflow/datasets/iot-23"


# ########################################################################


# Confirm that dataset has been cleaned...


def hello_world(**kwargs):
    ts = kwargs["execution_date"]
    print(ts)

    print("Getting or creating a Spark Session")
    spark = SparkSession \
        .builder \
        .appName("IoT-23 Dataset Inserter") \
        .config("spark.executor.memory", "4g") \
        .config("spark.driver.memory","4g") \
        .getOrCreate()

    df = spark \
        .read \
        .parquet("file:///usr/local/airflow/datasets/iot-23/year=2018/month=5/day=9")

    df.show(10)

def how_are_you(**kwargs):
    print("Placeholder...")


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
