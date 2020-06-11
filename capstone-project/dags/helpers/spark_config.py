from pyspark.sql import SparkSession
from pyspark.sql.types import *


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


# Define schema for the IoT-23 dataset
packet_schema = StructType([
    StructField("ts", StringType(), False),
    StructField("uid", StringType(), False),
    StructField("originate_host", StringType(), False),
    StructField("originate_port", IntegerType(), False),
    StructField("response_host", StringType(), False),
    StructField("response_port", IntegerType(), False),
    StructField("protocol", StringType(), True),
    StructField("service", StringType(), True),
    StructField("duration", StringType(), True),
    StructField("originate_bytes", StringType(), True),
    StructField("response_bytes", StringType(), True),
    StructField("connection_state", StringType(), True),
    StructField("local_originate", StringType(), True),
    StructField("local_response", StringType(), True),
    StructField("missed_bytes", IntegerType(), True),
    StructField("history", StringType(), True),
    StructField("originate_packets", IntegerType(), True),
    StructField("originate_ip_bytes", IntegerType(), True),
    StructField("response_packets", IntegerType(), True),
    StructField("response_ip_bytes", IntegerType(), True),
    StructField("tunnel_parents", StringType(), True),
])


# Define schema for the MaxMind City Blocks dataset
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


# Define schema for the MaxMind Locations dataset
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
