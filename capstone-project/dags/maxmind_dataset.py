import os
import psycopg2
import pyspark.sql.functions as F

from pyspark.sql import SparkSession

from airflow.models import DAG
from airflow.utils.dates import days_ago
from airflow.operators.python_operator import PythonOperator

from helpers.dag_config import get_arguments
from helpers.data_paths import *
from helpers.db_config import *
from helpers.spark_config import *
from helpers.sqls import *


dag = DAG(
    dag_id='step-2_maxmind-dataset',
    default_args=get_arguments(days_ago(0)),
    description='DAG for putting the MaxMind datasets into Postgres tables.',
    schedule_interval=None,
)


# ########################################################################


def check_if_datasets_exist(**kwargs):
    print("Checking if the MaxMind datasets exist...")

    asn_blocks_exists = os.path.exists(path_asn_blocks)
    city_blocks_exists = os.path.exists(path_city_blocks)
    city_locations_exists = os.path.exists(path_city_locations)

    if (not asn_blocks_exists and not city_blocks_exists and not city_locations_exists):
        raise ValueError("ERROR! One or more of the MaxMind dataset files were not found...")

    print("Checking if the MaxMind datasets exist completed!")


def drop_asn_table(**kwargs):
    print("Dropping the current MaxMind ASN table...")

    print("Step 1 | Opening connection to the database")
    connection, cursor = establish_connection()

    print("Step 2 | Executing drop table query")
    cursor.execute(drop_table.format("asn"))

    print("Step 3 | Closing connection")
    connection.close()
    cursor.close()

    print("Table has been successfully dropped!")


def drop_city_blocks_table(**kwargs):
    print("Dropping the current MaxMind City Blocks table...")

    print("Step 1 | Opening connection to the database")
    connection, cursor = establish_connection()

    print("Step 2 | Executing drop table queries")
    cursor.execute(drop_table.format("city_blocks"))

    print("Step 3 | Closing connection")
    connection.close()
    cursor.close()

    print("Table has been successfully dropped!")


def drop_city_locations_table(**kwargs):
    print("Dropping the current MaxMind City Locations table...")

    print("Step 1 | Opening connection to the database")
    connection, cursor = establish_connection()

    print("Step 2 | Executing drop table queries")
    cursor.execute(drop_table.format("city_locations"))

    print("Step 3 | Closing connection")
    connection.close()
    cursor.close()

    print("Table has been successfully dropped!")


def create_asn_table(**kwargs):
    print("Creating the current MaxMind ASN table...")

    print("Step 1 | Opening connection to the database")
    connection, cursor = establish_connection()

    print("Step 2 | Executing create table query")
    cursor.execute(
        create_table.format("asn",
            """
            network_id                      VARCHAR NOT NULL UNIQUE,
            network                         VARCHAR,
            autonomous_system_number        INTEGER,
            autonomous_system_organization  VARCHAR,
            PRIMARY KEY (network_id)
            """
        )
    )
    connection.commit()

    print("Step 3 | Closing connection")
    connection.close()
    cursor.close()

    print("Table has been successfully created!")


def create_city_locations_table(**kwargs):
    print("Creating the current MaxMind City Locations table...")

    print("Step 1 | Opening connection to the database")
    connection, cursor = establish_connection()

    print("Step 2 | Executing create table query")
    cursor.execute(
        create_table.format("city_locations",
            """
            geoname_id                      INTEGER NOT NULL UNIQUE,
            locale_code                     VARCHAR,
            continent_code                  VARCHAR,
            continent_name                  VARCHAR,
            country_iso_code                VARCHAR,
            country_name                    VARCHAR,
            subdivision_1_iso_code          VARCHAR,
            subdivision_1_name              VARCHAR,
            subdivision_2_iso_code          VARCHAR,
            subdivision_2_name              VARCHAR,
            city_name                       VARCHAR,
            metro_code                      VARCHAR,
            time_zone                       VARCHAR,
            is_in_european_union            INTEGER,
            PRIMARY KEY (geoname_id)
            """
        )
    )
    connection.commit()

    print("Step 3 | Closing connection")
    connection.close()
    cursor.close()

    print("Table has been successfully created!")


def create_city_blocks_table(**kwargs):
    print("Creating the current MaxMind City Blocks table...")

    print("Step 1 | Opening connection to the database")
    connection, cursor = establish_connection()

    print("Step 2 | Executing create table query")
    cursor.execute(
        create_table.format("city_blocks",
            """
            network_id                      VARCHAR NOT NULL UNIQUE,
            network                         VARCHAR,
            geoname_id                      INTEGER,
            registered_country_geoname_id   INTEGER,
            represented_country_geoname_id  INTEGER,
            is_anonymous_proxy              INTEGER,
            is_satellite_provider           INTEGER,
            postal_code                     VARCHAR,
            latitude                        FLOAT,
            longitude                       FLOAT,
            accuracy_radius                 INTEGER,
            PRIMARY KEY (network_id)
            """
        )
    )
    connection.commit()

    print("Step 3 | Closing connection")
    connection.close()
    cursor.close()

    print("Table has been successfully created!")


def insert_asn_into_table(**kwargs):
    print("Inserting data into the ASN table...")

    asn_schema = StructType([
        StructField("network", StringType(), False),
        StructField("autonomous_system_number", IntegerType(), False),
        StructField("autonomous_system_organization", StringType(), False),
    ])

    spark = get_spark_session("MaxMind ASN Dataset Inserter")

    df = spark \
        .read \
        .option("delimiter", ",") \
        .option("header", "true") \
        .schema(asn_schema) \
        .csv(path_asn_blocks)

    df = df.withColumn("network_id", F.split(df.network, ".0\/").getItem(0))

    df.printSchema()
    df.show(10, truncate = False)
    kwargs['ti'].xcom_push(key='asn_count', value=df.count())
    
    print("Writing the dataset data to the ASN table.")
    write_to_db(df, "overwrite", "asn")

    print("Inserting data into the ASN table completed!")


def insert_city_blocks_into_table(**kwargs):
    print("Inserting data into the City Blocks table...")

    spark = get_spark_session("MaxMind City Blocks Dataset Inserter")

    df = spark \
        .read \
        .option("delimiter", ",") \
        .option("header", "true") \
        .schema(city_blocks_schema) \
        .csv(path_city_blocks)

    df = df.withColumn("network_id", F.split(df.network, ".0\/").getItem(0))

    df.printSchema()
    df.show(10, truncate = False)
    kwargs['ti'].xcom_push(key='city_blocks_count', value=df.count())
    
    print("Writing the dataset data to the City Blocks table.")
    write_to_db(df, "overwrite", "city_blocks")

    print("Inserting data into the City Blocks table completed!")


def insert_city_locations_into_table(**kwargs):
    print("Inserting data into the City Locations table...")

    spark = get_spark_session("MaxMind City Locations Dataset Inserter")

    df = spark \
        .read \
        .option("delimiter", ",") \
        .option("header", "true") \
        .schema(city_locations_schema) \
        .csv(path_city_locations)

    df.printSchema()
    df.show(10, truncate = False)
    kwargs['ti'].xcom_push(key='city_locations_count', value=df.count())
    
    print("Writing the dataset data to the City Locations table.")
    write_to_db(df, "overwrite", "city_locations")

    print("Inserting data into the City Locations table completed!")


def check_data_quality(**kwargs):
    print("Check the data quality...")

    print("Step 1 | Getting dataframe counts")
    ti = kwargs['ti']
    df_asn_count = ti.xcom_pull(key=None, task_ids='insert_asn_into_table')
    df_city_blocks_count = ti.xcom_pull(key=None, task_ids='insert_city_blocks_into_table')
    df_city_locations_count = ti.xcom_pull(key=None, task_ids='insert_city_locations_into_table')

    print("Step 2 | Opening connection to the database")
    connection, cursor = establish_connection()

    print("Step 3 | Checking number of rows from each table")
    cursor.execute(count_select.format("asn"))
    db_asn_count = cursor.fetchone()

    cursor.execute(count_select.format("city_blocks"))
    db_city_blocks_count = cursor.fetchone()

    cursor.execute(count_select.format("city_locations"))
    db_city_locations_count = cursor.fetchone()

    print("Step 4 | Confirm data quality")
    if (df_asn_count != db_asn_count[0] or df_city_blocks_count != db_city_blocks_count[0] or df_city_locations_count != db_city_locations_count[0]):
        raise ValueError('ERROR! Rows in the dataframes are not the same as in the database tables...')

    print("SUCCESS! Checking the data quality completed!")


# ########################################################################


task_check_if_datasets_exist = PythonOperator(
    dag=dag,
    task_id='check_if_datasets_exist',
    python_callable=check_if_datasets_exist,
    provide_context=True,
)

task_drop_asn_table = PythonOperator(
    dag=dag,
    task_id='drop_asn_table',
    python_callable=drop_asn_table,
    provide_context=True,
)

task_drop_city_blocks_table = PythonOperator(
    dag=dag,
    task_id='drop_city_blocks_table',
    python_callable=drop_city_blocks_table,
    provide_context=True,
)

task_drop_city_locations_table = PythonOperator(
    dag=dag,
    task_id='drop_city_locations_table',
    python_callable=drop_city_locations_table,
    provide_context=True,
)

task_create_asn_table = PythonOperator(
    dag=dag,
    task_id='create_asn_table',
    python_callable=create_asn_table,
    provide_context=True,
)

task_create_city_locations_table = PythonOperator(
    dag=dag,
    task_id='create_city_locations_table',
    python_callable=create_city_locations_table,
    provide_context=True,
)

task_create_city_blocks_table = PythonOperator(
    dag=dag,
    task_id='create_city_blocks_table',
    python_callable=create_city_blocks_table,
    provide_context=True,
)

task_insert_asn_into_table = PythonOperator(
    dag=dag,
    task_id='insert_asn_into_table',
    python_callable=insert_asn_into_table,
    provide_context=True,
)

task_insert_city_blocks_into_table = PythonOperator(
    dag=dag,
    task_id='insert_city_blocks_into_table',
    python_callable=insert_city_blocks_into_table,
    provide_context=True,
)

task_insert_city_locations_into_table = PythonOperator(
    dag=dag,
    task_id='insert_city_locations_into_table',
    python_callable=insert_city_locations_into_table,
    provide_context=True,
)

task_check_data_quality = PythonOperator(
    dag=dag,
    task_id='check_data_quality',
    python_callable=check_data_quality,
    provide_context=True,
)


# ########################################################################


task_check_if_datasets_exist >> task_drop_asn_table
task_drop_asn_table >> task_create_asn_table
task_create_asn_table >> task_insert_asn_into_table
task_insert_asn_into_table >> task_check_data_quality

task_check_if_datasets_exist >> task_drop_city_blocks_table
task_drop_city_blocks_table >> task_drop_city_locations_table
task_drop_city_locations_table >> task_create_city_locations_table
task_create_city_locations_table >> task_create_city_blocks_table
task_create_city_blocks_table >> task_insert_city_blocks_into_table
task_create_city_blocks_table >> task_insert_city_locations_into_table
task_insert_city_blocks_into_table >> task_check_data_quality
task_insert_city_locations_into_table >> task_check_data_quality
