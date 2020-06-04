import os
import pyspark
import pyspark.sql.functions as F
import psycopg2

from pyspark.sql import SparkSession
from pyspark.sql.types import *

from airflow.models import DAG
from airflow.utils.dates import days_ago
from airflow.operators.python_operator import PythonOperator


default_args = {
    'owner': 'Fredrik Bakken',
    'start_date': days_ago(0),
}

dag = DAG(
    dag_id='step-2_maxmind-dataset',
    default_args=default_args,
    description='DAG for putting the MaxMind datasets into Postgres tables.',
    schedule_interval=None,
)


# ########################################################################


# Relative paths to dataset directories
path_asn_dataset = "/usr/local/airflow/datasets/GeoLite2-ASN-CSV_20200519"
path_city_dataset = "/usr/local/airflow/datasets/GeoLite2-City-CSV_20200519"

# Paths to the dataset files
path_asn_blocks = os.path.join(path_asn_dataset, "GeoLite2-ASN-Blocks-IPv4.csv")
path_city_blocks = os.path.join(path_city_dataset, "GeoLite2-City-Blocks-IPv4.csv")
path_city_locations = os.path.join(path_city_dataset, "GeoLite2-City-Locations-en.csv")

# Postgres connection variables
database = "capstone_project"
user = "udacity"
password = "udacity"
host = "172.28.1.2"
port = "5432"

# Postgres connection and cursor
def establish_connection():
    connection = psycopg2.connect(
        database = database,
        user = user,
        password = password,
        host = host,
        port = port,
    )
    cursor = connection.cursor()

    return connection, cursor

# SQL query statements
drop_table = "DROP TABLE IF EXISTS {};"
create_table = "CREATE TABLE IF NOT EXISTS {} ({});"

# Get or create Spark session
def get_spark_session(appName):
    spark = SparkSession \
        .builder \
        .appName(appName) \
        .config("spark.executor.memory", "4g") \
        .config("spark.driver.memory","4g") \
        .config("spark.jars", "/usr/local/airflow/config/postgresql-42.2.13.jar") \
        .getOrCreate()
    
    return spark

# Spark-Postgres connection parameters
mode = "overwrite"
url = "jdbc:postgresql://{}:{}/{}".format(host, port, database)
properties = {
    "user": user,
    "password": password,
    "driver": "org.postgresql.Driver"
}


# ########################################################################


def check_if_datasets_exist(**kwargs):
    print("Checking if the MaxMind datasets exist...")

    asn_blocks_exists = os.path.exists(path_asn_blocks)
    city_blocks_exists = os.path.exists(path_city_blocks)
    city_locations_exists = os.path.exists(path_city_locations)

    if (not asn_blocks_exists and not city_blocks_exists and not city_locations_exists):
        print("The MaxMind dataset files were not found!")
        return 0
    
    print("Checking if the MaxMind datasets exist completed!")
    return 1


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


def drop_city_tables(**kwargs):
    print("Dropping the current MaxMind City tables...")

    print("Step 1 | Opening connection to the database")
    connection, cursor = establish_connection()

    print("Step 2 | Executing drop table queries")
    cursor.execute(drop_table.format("city_blocks"))
    cursor.execute(drop_table.format("city_locations"))

    print("Step 3 | Closing connection")
    connection.close()
    cursor.close()

    print("Tables has been successfully dropped!")


def create_asn_table(**kwargs):
    print("Creating the current MaxMind ASN table...")

    print("Step 1 | Opening connection to the database")
    connection, cursor = establish_connection()

    print("Step 2 | Executing create table query")
    cursor.execute(
        create_table.format("asn",
            """
            network                         VARCHAR,
            autonomous_system_number        INTEGER,
            autonomous_system_organization  VARCHAR
            """
        )
    )

    print("Step 3 | Closing connection")
    connection.close()
    cursor.close()

    print("Table has been successfully created!")


def create_city_tables(**kwargs):
    print("Creating the current MaxMind City tables...")

    print("Step 1 | Opening connection to the database")
    connection, cursor = establish_connection()

    print("Step 2 | Executing create table query")
    cursor.execute(
        create_table.format("city_blocks",
            """
            network                         VARCHAR,
            geoname_id                      INTEGER,
            registered_country_geoname_id   INTEGER,
            represented_country_geoname_id  INTEGER,
            is_anonymous_proxy              INTEGER,
            is_satellite_provider           INTEGER,
            postal_code                     VARCHAR,
            latitude                        FLOAT,
            longitude                       FLOAT,
            accuracy_radius                 INTEGER
            """
        )
    )

    cursor.execute(
        create_table.format("city_locations",
            """
            geoname_id                      INTEGER,
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
            is_in_european_union            INTEGER
            """
        )
    )

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

    df.printSchema()
    df.show(10, truncate = False)
    print(df.count())
    
    print("Writing the dataset data to the ASN table.")
    df.write \
        .jdbc(
            url = url,
            table = "asn",
            mode = mode,
            properties = properties
        )

    print("Inserting data into the ASN table completed!")


def insert_city_blocks_into_table(**kwargs):
    print("Inserting data into the City Blocks table...")

    city_blocks_schema = StructType([
        StructField("network", StringType(), False),
        StructField("geoname_id", IntegerType(), False),
        StructField("registered_country_geoname_id", IntegerType(), False),
        StructField("represented_country_geoname_id", IntegerType(), False),
        StructField("is_anonymous_proxy", IntegerType(), False),
        StructField("is_satellite_provider", IntegerType(), False),
        StructField("postal_code", StringType(), False),
        StructField("latitude", FloatType(), False),
        StructField("longitude", FloatType(), False),
        StructField("accuracy_radius", IntegerType(), False),
    ])

    spark = get_spark_session("MaxMind City Blocks Dataset Inserter")

    df = spark \
        .read \
        .option("delimiter", ",") \
        .option("header", "true") \
        .schema(city_blocks_schema) \
        .csv(path_city_blocks)

    df.printSchema()
    df.show(10, truncate = False)
    print(df.count())
    
    print("Writing the dataset data to the City Blocks table.")
    df.write \
        .jdbc(
            url = url,
            table = "city_blocks",
            mode = mode,
            properties = properties
        )

    print("Inserting data into the City Blocks table completed!")


def insert_city_locations_into_table(**kwargs):
    print("Inserting data into the City Locations table...")

    city_locations_schema = StructType([
        StructField("geoname_id", IntegerType(), False),
        StructField("locale_code", StringType(), False),
        StructField("continent_code", StringType(), False),
        StructField("continent_name", StringType(), False),
        StructField("country_iso_code", StringType(), False),
        StructField("country_name", StringType(), False),
        StructField("subdivision_1_iso_code", StringType(), False),
        StructField("subdivision_1_name", StringType(), False),
        StructField("subdivision_2_iso_code", StringType(), False),
        StructField("subdivision_2_name", StringType(), False),
        StructField("city_name", StringType(), False),
        StructField("metro_code", StringType(), False),
        StructField("time_zone", StringType(), False),
        StructField("is_in_european_union", IntegerType(), False),
    ])

    spark = get_spark_session("MaxMind City Locations Dataset Inserter")

    df = spark \
        .read \
        .option("delimiter", ",") \
        .option("header", "true") \
        .schema(city_locations_schema) \
        .csv(path_city_locations)

    df.printSchema()
    df.show(10, truncate = False)
    print(df.count())
    
    print("Writing the dataset data to the City Locations table.")
    df.write \
        .jdbc(
            url = url,
            table = "city_locations",
            mode = mode,
            properties = properties
        )

    print("Inserting data into the City Locations table completed!")


def check_data_quality(**kwargs):
    print("TODO!")


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

task_drop_city_tables = PythonOperator(
    dag=dag,
    task_id='drop_city_tables',
    python_callable=drop_city_tables,
    provide_context=True,
)

task_create_asn_table = PythonOperator(
    dag=dag,
    task_id='create_asn_table',
    python_callable=create_asn_table,
    provide_context=True,
)

task_create_city_tables = PythonOperator(
    dag=dag,
    task_id='create_city_tables',
    python_callable=create_city_tables,
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

task_check_if_datasets_exist >> task_drop_city_tables
task_drop_city_tables >> task_create_city_tables
task_create_city_tables >> task_insert_city_blocks_into_table
task_create_city_tables >> task_insert_city_locations_into_table
task_insert_city_blocks_into_table >> task_check_data_quality
task_insert_city_locations_into_table >> task_check_data_quality
