import os
import pyspark
import pyspark.sql.functions as F

from shutil import rmtree

from pyspark.sql import SparkSession
from pyspark.sql.types import *

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
    'dags_are_paused_at_creation': False,
}

dag = DAG(
    dag_id='step-1_data-cleaner',
    default_args=default_args,
    description='A simple DAG for cleaning and preparing the IoT-23 dataset.',
    schedule_interval=None,
)


# ########################################################################


# Fixed variables shared over multiple tasks
path_raw_dataset = "/usr/local/airflow/datasets/opt"
path_cleaned_dataset = "/usr/local/airflow/datasets/iot-23"


def check_if_cleaned(**kwargs):
    print("Checking if the IoT-23 dataset has cleaned...")

    exists_cleaned_dataset = os.path.exists(path_cleaned_dataset)
    exists_raw_dataset = os.path.exists(path_raw_dataset)

    if (exists_cleaned_dataset is True and exists_raw_dataset is False):
        print("Good state: Cleaned dataset exists and raw dataset has been removed")
        kwargs['ti'].xcom_push(key='status_code', value=1)
    elif (exists_cleaned_dataset is True and exists_raw_dataset is True):
        print("Bad state: Cleaned dataset exists and raw dataset exists")
        kwargs['ti'].xcom_push(key='status_code', value=-1)
    else:
        print("Bad state: Cleaned dataset does not exist and raw dataset exists")
        kwargs['ti'].xcom_push(key='status_code', value=0)

    print("Checking if the IoT-23 dataset has been cleaned completed!")


def remove_partially_cleaned(**kwargs):
    ti = kwargs['ti']
    status_code = ti.xcom_pull(key=None, task_ids='check_if_cleaned')

    if (status_code != -1):
        print("Partially clean dataset does not exist.")
        return 0
    
    print("Removing the partially cleaned dataset...")
    rmtree(path_cleaned_dataset)
    print("Removing the partially cleaned dataset completed!")


def remove_honeypot_captures(**kwargs):
    ti = kwargs['ti']
    status_code = ti.xcom_pull(key=None, task_ids='check_if_cleaned')

    if (status_code == 1):
        print("The raw dataset has already been cleaned!")
        return 0

    print("Removing the honeypot captures from the raw dataset...")
    path_captures = os.path.join(path_raw_dataset, "Malware-Project/BigDataset/IoTScenarios")
    path_honeypot_capture_4_1 = os.path.join(path_captures, "CTU-Honeypot-Capture-4-1")
    path_honeypot_capture_5_1 = os.path.join(path_captures, "CTU-Honeypot-Capture-5-1")
    path_honeypot_capture_7_1 = os.path.join(path_captures, "CTU-Honeypot-Capture-7-1")

    rmtree(path_honeypot_capture_4_1)
    rmtree(path_honeypot_capture_5_1)
    rmtree(path_honeypot_capture_7_1)

    print("Removing the honeypot captures from the raw dataset completed!")


def remove_commented_lines(**kwargs):
    ti = kwargs['ti']
    status_code = ti.xcom_pull(key=None, task_ids='check_if_cleaned')

    if (status_code == 1):
        print("The raw dataset has already been cleaned!")
        return 0

    print("Removing commented lines in the raw dataset files...")

    for root, dirs, files in os.walk(path_raw_dataset):
        for filename in files:
            absolute_path = os.path.abspath(os.path.join(root, filename))
            bash_command = "sed -i '/^#/d' " + absolute_path
            os.system(bash_command)

    print("Removing commented lines in the raw dataset files completed!")


def clean_the_dataset(**kwargs):
    ti = kwargs['ti']
    status_code = ti.xcom_pull(key=None, task_ids='check_if_cleaned')

    if (status_code == 1):
        print("The raw dataset has already been cleaned!")
        return 0

    print("Cleaning the dataset...")

    packet_schema = StructType([
        StructField("ts", StringType(), False),
        StructField("uid", StringType(), False),
        StructField("orig_host", StringType(), False),
        StructField("orig_port", IntegerType(), False),
        StructField("resp_host", StringType(), False),
        StructField("resp_port", IntegerType(), False),
        StructField("protocol", StringType(), True),
        StructField("service", StringType(), True),
        StructField("duration", StringType(), True),
        StructField("orig_bytes", StringType(), True),
        StructField("resp_bytes", StringType(), True),
        StructField("conn_state", StringType(), True),
        StructField("local_orig", StringType(), True),
        StructField("local_resp", StringType(), True),
        StructField("missed_bytes", IntegerType(), True),
        StructField("history", StringType(), True),
        StructField("orig_pkts", IntegerType(), True),
        StructField("orig_ip_bytes", IntegerType(), True),
        StructField("resp_pkts", IntegerType(), True),
        StructField("resp_ip_bytes", IntegerType(), True),
        StructField("tunnel_parents", StringType(), True),
    ])

    print("Getting or creating a Spark Session")
    spark = SparkSession \
        .builder \
        .appName("Dataset Cleaner") \
        .getOrCreate()
    
    for root, dirs, files in os.walk(path_raw_dataset):
        for filename in files:
            absolute_path = os.path.abspath(os.path.join(root, filename))
            print("Starting the cleaning process of file: " + absolute_path)

            df = spark \
                .read \
                .option("delimiter", "\t") \
                .schema(packet_schema) \
                .csv(absolute_path)

            df.printSchema()

            # Get label information from tunnel_parents
            df = df.withColumn("label", F.split(df.tunnel_parents, "[ ]{3,}").getItem(1)) \
                .withColumn("detailed_label", F.split(df.tunnel_parents, "[ ]{3,}").getItem(2)) \
                .withColumn("tunnel_parents", F.split(df.tunnel_parents, "[ ]{3,}").getItem(0)) \
            
            # Get necessary date values from ts
            df = df.withColumn("timestamp", F.split(df.ts, "\.").getItem(0))
            df = df.withColumn("date", F.to_date(F.from_unixtime(df.timestamp)))
            df = df.withColumn("year", F.year(df.date)) \
                .withColumn("month", F.month(df.date)) \
                .withColumn("day", F.dayofmonth(df.date)) \
                .drop("date")

            df.show(10, truncate = False)

            print("Saving the cleaned dataset")
            df.repartition(df.year, df.month, df.day) \
                .write \
                .mode('append') \
                .partitionBy("year", "month", "day") \
                .parquet(path_cleaned_dataset)

            print("Removing the raw dataset file: " + absolute_path)
            os.remove(absolute_path)

    print("Cleaning the dataset completed!")


def remove_raw_dataset(**kwargs):
    ti = kwargs['ti']
    status_code = ti.xcom_pull(key=None, task_ids='check_if_cleaned')

    if (status_code == 1):
        print("The raw dataset has already been cleaned!")
        return 0

    print("Removing the raw dataset...")
    rmtree(path_raw_dataset)
    print("Removing the raw dataset completed!")


# ########################################################################


task_check_if_cleaned = PythonOperator(
    dag=dag,
    task_id='check_if_cleaned',
    python_callable=check_if_cleaned,
    provide_context=True,
)

task_remove_partially_cleaned = PythonOperator(
    dag=dag,
    task_id='remove_partially_cleaned',
    python_callable=remove_partially_cleaned,
    provide_context=True,
)

task_remove_honeypot_captures = PythonOperator(
    dag=dag,
    task_id='remove_honeypot_captures',
    python_callable=remove_honeypot_captures,
    provide_context=True,
)

task_remove_commented_lines = PythonOperator(
    dag=dag,
    task_id='remove_commented_lines',
    python_callable=remove_commented_lines,
    provide_context=True,
)

task_clean_the_dataset = PythonOperator(
    dag=dag,
    task_id='clean_the_dataset',
    python_callable=clean_the_dataset,
    provide_context=True,
)

task_remove_raw_dataset = PythonOperator(
    dag=dag,
    task_id='remove_raw_dataset',
    python_callable=remove_raw_dataset,
    provide_context=True,
)


# ########################################################################


task_check_if_cleaned >> [task_remove_partially_cleaned, task_remove_honeypot_captures]
[task_remove_partially_cleaned, task_remove_honeypot_captures] >> task_remove_commented_lines
task_remove_commented_lines >> task_clean_the_dataset
task_clean_the_dataset >> task_remove_raw_dataset
