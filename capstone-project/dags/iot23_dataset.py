import os
import pyspark
import pyspark.sql.functions as F

from datetime import datetime

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
)


# ########################################################################


def check_if_dataset_exist(**kwargs):
    print("Checking if the cleaned IoT-23 dataset exist...")

    iot23_exists = os.path.exists(path_cleaned_dataset)

    #if (not iot23_exists):
    #    raise ValueError("ERROR! The cleaned IoT-23 dataset does not exist...")

    print("Checking if the cleaned IoT-23 dataset exist completed!")


def create_originate_packets_table(**kwargs):
    print("Creating the IoT-23 Originate Packets table...")

    print("Step 1 | Opening connection to the database")
    connection, cursor = establish_connection()

    print("Step 2 | Executing create table query")
    cursor.execute(
        create_table.format("originate_packets",
            """
            uid         VARCHAR NOT NULL UNIQUE,
            host        VARCHAR,
            port        INTEGER,
            bytes       VARCHAR,
            local       VARCHAR,
            packets     INTEGER,
            ip_bytes    INTEGER,
            insert_date VARCHAR NOT NULL,
            PRIMARY KEY (uid)
            """
        )
    )
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
    cursor.execute(
        create_table.format("response_packets",
            """
            uid         VARCHAR NOT NULL UNIQUE,
            host        VARCHAR,
            port        INTEGER,
            bytes       VARCHAR,
            local       VARCHAR,
            packets     INTEGER,
            ip_bytes    INTEGER,
            insert_date VARCHAR NOT NULL,
            PRIMARY KEY (uid)
            """
        )
    )
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
    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS packets (
            timestamp VARCHAR,
            uid VARCHAR NOT NULL UNIQUE,
            originate_network_id VARCHAR,
            response_network_id VARCHAR,
            protocol VARCHAR,
            service VARCHAR,
            duration VARCHAR,
            connection_state VARCHAR,
            missed_bytes INTEGER,
            history VARCHAR,
            tunnel_parents VARCHAR,
            label VARCHAR,
            detailed_label VARCHAR,
            insert_date VARCHAR NOT NULL,
            PRIMARY KEY (uid)
        );
        """
    )
    connection.commit()

    print("Step 3 | Closing connection")
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
        df = df.withColumn("insert_date", F.lit(ds))

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
        df = df.withColumn("insert_date", F.lit(ds))

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
            ).withColumn("insert_date", F.lit(ds))

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

    print("Step 1 | Getting dataframe counts")
    ti = kwargs['ti']
    df_originate_packets_count = ti.xcom_pull(key=None, task_ids='insert_originate_packets_into_table')
    df_response_packets_count = ti.xcom_pull(key=None, task_ids='insert_response_packets_into_table')
    df_packets_count = ti.xcom_pull(key=None, task_ids='insert_packets_into_table')

    ds = kwargs["ds"]

    print("Step 2 | Opening connection to the database")
    connection, cursor = establish_connection()

    print("Step 3 | Checking number of rows from each table")
    q1 = "SELECT count(*) FROM originate_packets WHERE insert_date = '{}';".format(ds)
    print(q1)
    cursor.execute(q1)
    db_originate_packets_count = cursor.fetchone()

    q2 = "SELECT count(*) FROM response_packets WHERE insert_date = '{}';".format(ds)
    print(q2)
    cursor.execute(q2)
    db_response_packets_count = cursor.fetchone()

    q3 = "SELECT count(*) FROM packets WHERE insert_date = '{}';".format(ds)
    print(q3)
    cursor.execute(q3)
    db_packets_count = cursor.fetchone()

    print(df_originate_packets_count)
    print(db_originate_packets_count)
    print(df_response_packets_count)
    print(db_response_packets_count)
    print(df_packets_count)
    print(db_packets_count)

    print("Step 4 | Confirm data quality")
    if (df_originate_packets_count != db_originate_packets_count[0] or df_response_packets_count != db_response_packets_count[0] or df_packets_count != db_packets_count[0]):
        raise ValueError('ERROR! Rows in the dataframes are not the same as in the database tables...')

    print("SUCCESS! Checking the data quality completed!")


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


# ########################################################################


task_check_if_dataset_exist >> task_create_originate_packets_table
task_check_if_dataset_exist >> task_create_response_packets_table
task_check_if_dataset_exist >> task_create_packets_table

task_create_originate_packets_table >> task_insert_originate_packets_into_table
task_create_response_packets_table >> task_insert_response_packets_into_table
task_create_packets_table >> task_insert_packets_into_table

task_insert_originate_packets_into_table >> task_check_data_quality
task_insert_response_packets_into_table >> task_check_data_quality
task_insert_packets_into_table >> task_check_data_quality
