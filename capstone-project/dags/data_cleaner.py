import os
import pyspark
import pyspark.sql.functions as F

from shutil import rmtree

from pyspark.sql import SparkSession

from airflow.models import DAG
from airflow.utils.dates import days_ago
from airflow.operators.python_operator import PythonOperator

from helpers.dag_config import get_arguments
from helpers.data_paths import *
from helpers.spark_config import *


dag = DAG(
    dag_id='step-1_data-cleaner',
    default_args=get_arguments(days_ago(0)),
    description='A simple DAG for cleaning and preparing the IoT-23 dataset.',
    schedule_interval=None,
)


# ########################################################################


def check_if_cleaned(**kwargs):
    print("Checking if the IoT-23 dataset has cleaned...")

    exists_cleaned_dataset = os.path.exists(path_cleaned_dataset)
    exists_raw_dataset = os.path.exists(path_raw_dataset)

    if (exists_cleaned_dataset is True and exists_raw_dataset is False):
        print("Good state: Cleaned dataset exists and raw dataset has been removed")
        kwargs['ti'].xcom_push(key='status_code', value=1)
    else:
        print("Bad state: Cleaning is not executed/completed")
        kwargs['ti'].xcom_push(key='status_code', value=0)

    print("Checking if the IoT-23 dataset has been cleaned completed!")


def remove_honeypot_captures(**kwargs):
    ti = kwargs['ti']
    status_code = ti.xcom_pull(key=None, task_ids='check_if_cleaned')

    if (status_code == 1):
        print("The raw dataset has already been cleaned!")
        return 0

    print("Removing the honeypot captures from the raw dataset...")
    try:
        rmtree(path_honeypot_capture_4_1)
    except:
        print(path_honeypot_capture_4_1 + " already deleted.")

    try:
        rmtree(path_honeypot_capture_5_1)
    except:
        print(path_honeypot_capture_5_1 + " already deleted.")

    try:
        rmtree(path_honeypot_capture_7_1)
    except: 
        print(path_honeypot_capture_7_1 + " already deleted.")

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

    print("Getting or creating a Spark Session")
    spark = get_spark_session("Dataset Cleaner")
    
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


task_check_if_cleaned >> task_remove_honeypot_captures
task_remove_honeypot_captures >> task_remove_commented_lines
task_remove_commented_lines >> task_clean_the_dataset
task_clean_the_dataset >> task_remove_raw_dataset
