import psycopg2

# Postgres connection variables
database = "capstone_project"
user = "udacity"
password = "udacity"
host = "172.28.1.2"
port = "5432"


# Spark-Postgres connection parameters
url = "jdbc:postgresql://{}:{}/{}".format(host, port, database)
properties = {
    "user": user,
    "password": password,
    "driver": "org.postgresql.Driver"
}


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
