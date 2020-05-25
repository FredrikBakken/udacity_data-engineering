from pyspark.sql import SparkSession


def create_spark_session():
    spark = SparkSession \
        .builder \
        .appName("PySpark Docker Test") \
        .getOrCreate()
    
    return spark


def main():
    print("Starting application")
    spark = create_spark_session()
    print("Spark session has been created.")

    df = spark.read.csv("info.txt")
    df.show(5)

    print("Completed!")
    print("Testing 2")

if __name__ == "__main__":
    main()
