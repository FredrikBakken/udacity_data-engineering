import pyspark
import pyspark.sql.functions as F

from datetime import datetime

from pyspark.sql import SparkSession
from pyspark.sql.types import *

from airflow.models import DAG
from airflow.utils.dates import days_ago
from airflow.operators.python_operator import PythonOperator

from helpers.dag_config import get_arguments
from helpers.db_config import *
from helpers.spark_config import *


dag = DAG(
    dag_id='step-3_iot-23-dataset',
    default_args=get_arguments(datetime(2018, 5, 9)),
    description='DAG for putting the IoT-23 dataset into Postgres tables.',
    schedule_interval="00 07 * * *",
)


# ########################################################################


def check_if_dataset_exist(**kwargs):
    print("Checking...")


def create_packets_table(**kwargs):
    print("Creating the IoT-23 Packets table...")

    print("Step 1 | Opening connection to the database")
    connection, cursor = establish_connection()

    print("Step 2 | Executing create table query")
    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS packets (
            timestamp VARCHAR,
            uid VARCHAR,
            originate_network_id VARCHAR,
            response_network_id VARCHAR,
            protocol VARCHAR,
            service VARCHAR,
            duration VARCHAR,
            connection_state VARCHAR,
            local_originate VARCHAR,
            local_response VARCHAR,
            missed_bytes VARCHAR,
            history VARCHAR,
            tunnel_parents VARCHAR,
            label VARCHAR,
            detailed_label VARCHAR,
            insert_date DATE,
            PRIMARY KEY (uid, originate_network_id, response_network_id, insert_date)
        ) PARTITION BY RANGE (insert_date);
        """
    )

    print("Step 3 | Closing connection")
    connection.close()
    cursor.close()

    print("Table has been successfully created!")


def create_originate_packets_table(**kwargs):
    print("Creating...")


def create_response_packets_table(**kwargs):
    print("Creating...")


def insert_packets_into_table(**kwargs):
    print("Read, structure, and insert")


def insert_originate_packets_into_table(**kwargs):
    print("Read, structure, and insert")


def insert_response_packets_into_table(**kwargs):
    print("Read, structure, and insert")


def hello_world(**kwargs):
    ts = kwargs["execution_date"]
    print(ts)

    '''
    print("Getting or creating a Spark Session")
    spark = get_spark_session("IoT-23 Dataset Inserter")

    df = spark \
        .read \
        .parquet("file:///usr/local/airflow/datasets/iot-23/year=2018/month=5/day=9")

    df.show(10)
    '''


def how_are_you(**kwargs):
    print("Placeholder...")



# ########################################################################


task_check_if_dataset_exist = PythonOperator(
    dag=dag,
    task_id='check_if_dataset_exist',
    python_callable=check_if_dataset_exist,
    provide_context=True,
)

task_create_packets_table = PythonOperator(
    dag=dag,
    task_id='create_packets_table',
    python_callable=create_packets_table,
    provide_context=True,
)

task_create_originate_packets_table = PythonOperator(
    dag=dag,
    task_id='create_originate_packets_table',
    python_callable=create_originate_packets_table,
    provide_context=True,
)

task_create_response_packets_table = PythonOperator(
    dag=dag,
    task_id='create_response_packets_table',
    python_callable=create_response_packets_table,
    provide_context=True,
)

task_insert_packets_into_table = PythonOperator(
    dag=dag,
    task_id='insert_packets_into_table',
    python_callable=insert_packets_into_table,
    provide_context=True,
)

task_insert_originate_packets_into_table = PythonOperator(
    dag=dag,
    task_id='insert_originate_packets_into_table',
    python_callable=insert_originate_packets_into_table,
    provide_context=True,
)

task_insert_response_packets_into_table = PythonOperator(
    dag=dag,
    task_id='insert_response_packets_into_table',
    python_callable=insert_response_packets_into_table,
    provide_context=True,
)

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


task_check_if_dataset_exist >> task_create_packets_table
task_check_if_dataset_exist >> task_create_originate_packets_table
task_check_if_dataset_exist >> task_create_response_packets_table
task_create_packets_table >> task_insert_packets_into_table
task_create_originate_packets_table >> task_insert_originate_packets_into_table
task_create_response_packets_table >> task_insert_response_packets_into_table

task_hello_world >> task_how_are_you
