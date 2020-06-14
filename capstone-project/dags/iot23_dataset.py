import os
import pyspark
import pyspark.sql.functions as F

from shutil import rmtree
from datetime import datetime, timedelta

from pyspark.sql import SparkSession
from pyspark.sql.types import *

from airflow.models import DAG
from airflow.utils.dates import days_ago
from airflow.operators.python_operator import PythonOperator

from helpers.dag_config import *
from helpers.data_paths import *
from helpers.db_config import *
from helpers.spark_config import *
from helpers.sqls import *


dag = DAG(
    dag_id='step-3_iot-23-dataset',
    default_args=get_arguments(datetime(2018, 5, 9)),
    description='DAG for putting the IoT-23 dataset into Postgres tables.',
    schedule_interval="59 23 * * *",
    end_date=datetime(2019, 9, 22)
)


# ########################################################################


def check_if_dataset_exist(**kwargs):
    print("Checking if the cleaned IoT-23 dataset exist...")

    iot23_exists = os.path.exists(path_cleaned_dataset)

    if (not iot23_exists):
        raise ValueError("ERROR! The cleaned IoT-23 dataset does not exist...")

    print("Checking if the cleaned IoT-23 dataset exist completed!")


def create_originate_packets_table(**kwargs):
    print("Creating the IoT-23 Originate Packets table...")

    print("Step 1 | Opening connection to the database")
    connection, cursor = establish_connection()

    print("Step 2 | Executing create table query")
    cursor.execute(create_table_originate_packets)
    connection.commit()

    print("Step 3 | Closing connection")
    connection.close()
    cursor.close()

    print("Table has been successfully created!")


def create_response_packets_table(**kwargs):
    print("Creating the IoT-23 Response Packets table...")

    print("Step 1 | Opening connection to the database")
    connection, cursor = establish_connection()

    print("Step 2 | Executing create table query")
    cursor.execute(create_table_response_packets)
    connection.commit()

    print("Step 3 | Closing connection")
    connection.close()
    cursor.close()

    print("Table has been successfully created!")


def create_packets_table(**kwargs):
    print("Creating the IoT-23 Packets table...")

    print("Step 1 | Opening connection to the database")
    connection, cursor = establish_connection()

    print("Step 2 | Executing create table query")
    cursor.execute(create_table_packets)
    connection.commit()

    print("Step 3 | Closing connection")
    connection.close()
    cursor.close()

    print("Table has been successfully created!")


def create_originate_packets_partition_table(**kwargs):
    print("Creating the IoT-23 Originate Packets table...")

    print("Step 1 | Get the year, month, and day for the current interval")
    ds = kwargs["ds"]
    year, month, day = get_ds_time(ds)

    print("Step 2 | Get and check path of current dataset partition")
    path_iot23_partition = get_iot23_partition(year, month, day)
    iot23_partition_exists = os.path.exists(path_iot23_partition)

    if (not iot23_partition_exists):
        print("This partition does not exist!")
    else:
        print("Step 1 | Handle the date formatting")
        current_date = datetime(int(year), int(month), int(day))
        next_date = current_date + timedelta(days=1)

        print("Step 2 | Opening connection to the database")
        connection, cursor = establish_connection()

        print("Step 3 | Executing create table query")
        cursor.execute(
            create_patition_table.format(
                "originate_packets_" + year + "_" + month + "_" + day,
                "originate_packets",
                current_date.strftime("%Y-%m-%d"),
                next_date.strftime("%Y-%m-%d")
            )
        )
        connection.commit()

        print("Step 4 | Closing connection")
        connection.close()
        cursor.close()

        print("Table has been successfully created!")


def create_response_packets_partition_table(**kwargs):
    print("Creating the IoT-23 Response Packets table...")

    print("Step 1 | Get the year, month, and day for the current interval")
    ds = kwargs["ds"]
    year, month, day = get_ds_time(ds)

    print("Step 2 | Get and check path of current dataset partition")
    path_iot23_partition = get_iot23_partition(year, month, day)
    iot23_partition_exists = os.path.exists(path_iot23_partition)

    if (not iot23_partition_exists):
        print("This partition does not exist!")
    else:
        print("Step 1 | Handle the date formatting")
        current_date = datetime(int(year), int(month), int(day))
        next_date = current_date + timedelta(days=1)

        print("Step 2 | Opening connection to the database")
        connection, cursor = establish_connection()

        print("Step 3 | Executing create table query")
        cursor.execute(
            create_patition_table.format(
                "response_packets_" + year + "_" + month + "_" + day,
                "response_packets",
                current_date.strftime("%Y-%m-%d"),
                next_date.strftime("%Y-%m-%d")
            )
        )
        connection.commit()

        print("Step 4 | Closing connection")
        connection.close()
        cursor.close()

        print("Table has been successfully created!")


def create_packets_partition_table(**kwargs):
    print("Creating the IoT-23 Packets table...")

    print("Step 1 | Get the year, month, and day for the current interval")
    ds = kwargs["ds"]
    year, month, day = get_ds_time(ds)

    print("Step 2 | Get and check path of current dataset partition")
    path_iot23_partition = get_iot23_partition(year, month, day)
    iot23_partition_exists = os.path.exists(path_iot23_partition)

    if (not iot23_partition_exists):
        print("This partition does not exist!")
    else:
        print("Step 1 | Handle the date formatting")
        current_date = datetime(int(year), int(month), int(day))
        next_date = current_date + timedelta(days=1)

        print("Step 2 | Opening connection to the database")
        connection, cursor = establish_connection()

        print("Step 3 | Executing create table query")
        cursor.execute(
            create_patition_table.format(
                "packets_" + year + "_" + month + "_" + day,
                "packets",
                current_date.strftime("%Y-%m-%d"),
                next_date.strftime("%Y-%m-%d")
            )
        )
        connection.commit()

        print("Step 4 | Closing connection")
        connection.close()
        cursor.close()

        print("Table has been successfully created!")


def insert_originate_packets_into_table(**kwargs):
    print("Inserting data into the IoT-23 Originate Packets table...")
    
    print("Step 1 | Get the year, month, and day for the current interval")
    ds = kwargs["ds"]
    year, month, day = get_ds_time(ds)

    print("Step 2 | Get and check path of current dataset partition")
    path_iot23_partition = get_iot23_partition(year, month, day)
    iot23_partition_exists = os.path.exists(path_iot23_partition)

    if (not iot23_partition_exists):
        print("This partition does not exist!")
        kwargs['ti'].xcom_push(key='originate_packets', value=0)
    else:
        print("Step 3 | Get or create a Spark Session")
        spark = get_spark_session("IoT-23 Dataset Inserter")

        print("Step 4 | Extract the current partition of the IoT-23 dataset")
        df = spark \
            .read \
            .parquet("file://{}".format(path_iot23_partition))

        print("Step 5 | Add the missing column")
        df = df.withColumn("insert_date", F.to_date(F.lit(ds), "yyyy-MM-dd"))

        print("Step 6 | Select and rename the columns for the current table")
        df = df.selectExpr(
            "uid as uid",
            "originate_host as host",
            "originate_port as port",
            "originate_bytes as bytes",
            "local_originate as local",
            "originate_packets as packets",
            "originate_ip_bytes as ip_bytes",
            "insert_date as insert_date"
        )

        df.printSchema()
        df.show(10, truncate = False)

        print("Step 7 | Log the count of the DataFrame for data quality checks")
        kwargs['ti'].xcom_push(key='originate_packets_count', value=df.count())

        print("Step 8 | Load the dataset data to the Originate Packets table.")
        write_to_db(df, "append", "originate_packets")

        print("Inserting data into the IoT-23 Originate Packets table completed!")


def insert_response_packets_into_table(**kwargs):
    print("Inserting data into the IoT-23 Response Packets table...")
    
    print("Step 1 | Get the year, month, and day for the current interval")
    ds = kwargs["ds"]
    year, month, day = get_ds_time(ds)

    print("Step 2 | Get and check path of current dataset partition")
    path_iot23_partition = get_iot23_partition(year, month, day)
    iot23_partition_exists = os.path.exists(path_iot23_partition)

    if (not iot23_partition_exists):
        print("This partition does not exist!")
        kwargs['ti'].xcom_push(key='response_packets_count', value=0)
    else:
        print("Step 3 | Get or create a Spark Session")
        spark = get_spark_session("IoT-23 Dataset Inserter")

        print("Step 4 | Extract the current partition of the IoT-23 dataset")
        df = spark \
            .read \
            .parquet("file://{}".format(path_iot23_partition))

        print("Step 5 | Add the missing column")
        df = df.withColumn("insert_date", F.to_date(F.lit(ds), "yyyy-MM-dd"))

        print("Step 6 | Select and rename the columns for the current table")
        df = df.selectExpr(
            "uid as uid",
            "response_host as host",
            "response_port as port",
            "response_bytes as bytes",
            "local_response as local",
            "response_packets as packets",
            "response_ip_bytes as ip_bytes",
            "insert_date as insert_date"
        )

        df.printSchema()
        df.show(10, truncate = False)

        print("Step 7 | Log the count of the DataFrame for data quality checks")
        kwargs['ti'].xcom_push(key='response_packets_count', value=df.count())

        print("Step 8 | Load the dataset data to the Response Packets table.")
        write_to_db(df, "append", "response_packets")

        print("Inserting data into the IoT-23 Response Packets table completed!")


def insert_packets_into_table(**kwargs):
    print("Inserting data into the IoT-23 Packets table...")
    
    print("Step 1 | Get the year, month, and day for the current interval")
    ds = kwargs["ds"]
    year, month, day = get_ds_time(ds)

    print("Step 2 | Get and check path of current dataset partition")
    path_iot23_partition = get_iot23_partition(year, month, day)
    iot23_partition_exists = os.path.exists(path_iot23_partition)

    if (not iot23_partition_exists):
        print("This partition does not exist!")
        kwargs['ti'].xcom_push(key='packets_count', value=0)
    else:
        print("Step 3 | Get or create a Spark Session")
        spark = get_spark_session("IoT-23 Dataset Inserter")

        print("Step 4 | Extract the current partition of the IoT-23 dataset")
        df = spark \
            .read \
            .parquet("file://{}".format(path_iot23_partition))

        print("Step 5 | Add the missing columns")
        df = df.withColumn("originate_network_id", F.concat(
                F.split(df.originate_host, "\.").getItem(0),
                F.lit("."),
                F.split(df.originate_host, "\.").getItem(1),
                F.lit("."),
                F.split(df.originate_host, "\.").getItem(2))
            ).withColumn("response_network_id", F.concat(
                F.split(df.response_host, "\.").getItem(0),
                F.lit("."), 
                F.split(df.response_host, "\.").getItem(1),
                F.lit("."),
                F.split(df.response_host, "\.").getItem(2))
            ).withColumn("insert_date", F.to_date(F.lit(ds), "yyyy-MM-dd"))

        print("Step 6 | Select and rename the columns for the current table")
        df = df.selectExpr(
            "timestamp as timestamp",
            "uid as uid",
            "originate_network_id as originate_network_id",
            "response_network_id as response_network_id",
            "protocol as protocol",
            "service as service",
            "duration as duration",
            "connection_state as connection_state",
            "missed_bytes as missed_bytes",
            "history as history",
            "tunnel_parents as tunnel_parents",
            "label as label",
            "detailed_label as detailed_label",
            "insert_date as insert_date"
        )

        df.printSchema()
        df.show(10, truncate = False)

        print("Step 7 | Log the count of the DataFrame for data quality checks")
        kwargs['ti'].xcom_push(key='packets_count', value=df.count())

        print("Step 8 | Load the dataset data to the Packets table.")
        write_to_db(df, "append", "packets")

        print("Inserting data into the IoT-23 Packets table completed!")


def check_data_quality(**kwargs):
    print("Check the data quality...")

    print("Step 1 | Get the year, month, and day for the current interval")
    ds = kwargs["ds"]
    year, month, day = get_ds_time(ds)

    print("Step 2 | Get and check path of current dataset partition")
    path_iot23_partition = get_iot23_partition(year, month, day)
    iot23_partition_exists = os.path.exists(path_iot23_partition)

    if (not iot23_partition_exists):
        print("This partition does not exist!")
    else:
        print("Step 3 | Getting dataframe counts")
        ti = kwargs['ti']
        df_originate_packets_count = ti.xcom_pull(key=None, task_ids='insert_originate_packets_into_table')
        df_response_packets_count = ti.xcom_pull(key=None, task_ids='insert_response_packets_into_table')
        df_packets_count = ti.xcom_pull(key=None, task_ids='insert_packets_into_table')

        print("Step 4 | Opening connection to the database")
        connection, cursor = establish_connection()

        print("Step 5 | Checking number of rows from each table")
        cursor.execute(count_select_iot23.format("originate_packets", ds))
        db_originate_packets_count = cursor.fetchone()

        cursor.execute(count_select_iot23.format("response_packets", ds))
        db_response_packets_count = cursor.fetchone()

        cursor.execute(count_select_iot23.format("packets", ds))
        db_packets_count = cursor.fetchone()

        print("Step 6 | Confirm data quality")
        if (df_originate_packets_count != db_originate_packets_count[0] or df_response_packets_count != db_response_packets_count[0] or df_packets_count != db_packets_count[0]):
            raise ValueError('ERROR! Rows in the dataframes are not the same as in the database tables...')

    print("SUCCESS! Checking the data quality completed!")


def delete_partition_file(**kwargs):
    print("Deleting the current cleaned partition file...")

    print("Step 1 | Get the year, month, and day for the current interval")
    ds = kwargs["ds"]
    year, month, day = get_ds_time(ds)

    print("Step 2 | Get and check path of current dataset partition")
    path_iot23_partition = get_iot23_partition(year, month, day)
    iot23_partition_exists = os.path.exists(path_iot23_partition)

    if (iot23_partition_exists):
        print("Step 3 | Deleting the current cleaned partition file")
        rmtree(path_iot23_partition)

    print("Deleting the current cleaned partition file completed!")


# ########################################################################


task_check_if_dataset_exist = PythonOperator(
    dag=dag,
    task_id='check_if_dataset_exist',
    python_callable=check_if_dataset_exist,
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

task_create_packets_table = PythonOperator(
    dag=dag,
    task_id='create_packets_table',
    python_callable=create_packets_table,
    provide_context=True,
)

task_create_originate_packets_partition_table = PythonOperator(
    dag=dag,
    task_id='create_originate_packets_partition_table',
    python_callable=create_originate_packets_partition_table,
    provide_context=True,
)

task_create_response_packets_partition_table = PythonOperator(
    dag=dag,
    task_id='create_response_packets_partition_table',
    python_callable=create_response_packets_partition_table,
    provide_context=True,
)

task_create_packets_partition_table = PythonOperator(
    dag=dag,
    task_id='create_packets_partition_table',
    python_callable=create_packets_partition_table,
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

task_check_data_quality = PythonOperator(
    dag=dag,
    task_id='check_data_quality',
    python_callable=check_data_quality,
    provide_context=True,
)

task_delete_partition_file = PythonOperator(
    dag=dag,
    task_id='delete_partition_file',
    python_callable=delete_partition_file,
    provide_context=True,
)


# ########################################################################


task_check_if_dataset_exist >> task_create_originate_packets_table
task_check_if_dataset_exist >> task_create_response_packets_table
task_check_if_dataset_exist >> task_create_packets_table

task_create_originate_packets_table >> task_create_originate_packets_partition_table
task_create_response_packets_table >> task_create_response_packets_partition_table
task_create_packets_table >> task_create_packets_partition_table

task_create_originate_packets_partition_table >> task_insert_originate_packets_into_table
task_create_response_packets_partition_table >> task_insert_response_packets_into_table
task_create_packets_partition_table >> task_insert_packets_into_table

task_insert_originate_packets_into_table >> task_check_data_quality
task_insert_response_packets_into_table >> task_check_data_quality
task_insert_packets_into_table >> task_check_data_quality

task_check_data_quality >> task_delete_partition_file
