from pyspark.sql import SparkSession

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
