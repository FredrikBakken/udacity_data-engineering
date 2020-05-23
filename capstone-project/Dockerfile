FROM puckel/docker-airflow:1.10.9

USER root

RUN update-ca-certificates -f \
  && apt-get update \
  && apt-get upgrade -y \
  && apt-get install -y \
    wget \
    git \
    libatlas3-base \
    libopenblas-base \
  && apt-get clean

# Java
COPY ./config/jdk-8u251-linux-x64.tar.gz /opt/jdk-8.tar.gz
RUN cd /opt/ \
    && tar xzf jdk-8.tar.gz \
    && rm jdk-8.tar.gz \
    && update-alternatives --install /usr/bin/java java /opt/jdk1.8.0_251/bin/java 100 \
    && update-alternatives --install /usr/bin/jar jar /opt/jdk1.8.0_251/bin/jar 100 \
    && update-alternatives --install /usr/bin/javac javac /opt/jdk1.8.0_251/bin/javac 100

# SPARK
RUN cd /usr/ \
    && wget "https://apache.uib.no/spark/spark-2.4.5/spark-2.4.5-bin-hadoop2.7.tgz" \
    && tar xzf spark-2.4.5-bin-hadoop2.7.tgz \
    && rm spark-2.4.5-bin-hadoop2.7.tgz \
    && mv spark-2.4.5-bin-hadoop2.7 spark

ENV SPARK_HOME /usr/spark
ENV PATH="/usr/spark/bin:${PATH}"
ENV SPARK_MAJOR_VERSION 2
ENV PYTHONPATH=$SPARK_HOME/python/lib/py4j-0.10.7-src.zip:$SPARK_HOME/python/:$PYTHONPATH

RUN mkdir -p /usr/spark/work/ \
    && chmod -R 777 /usr/spark/work/

ENV SPARK_MASTER_PORT 7077

# Postgres
RUN pip install --upgrade pip \
    && pip install psycopg2-binary

# Add DAGs to Airflow UI
ADD ./dags/ /usr/local/airflow/dags

# Add datasets to the project
RUN mkdir /usr/local/airflow/datasets
ADD ./datasets/GeoLite2-ASN-CSV_20200519 /usr/local/airflow/datasets/GeoLite2-ASN-CSV_20200519
ADD ./datasets/GeoLite2-City-CSV_20200519 /usr/local/airflow/datasets/GeoLite2-City-CSV_20200519
ADD ./datasets/GeoLite2-Country-CSV_20200519 /usr/local/airflow/datasets/GeoLite2-Country-CSV_20200519
RUN cd /usr/local/airflow/datasets \
    && wget --no-check-certificate "https://mcfp.felk.cvut.cz/publicDatasets/IoT-23-Dataset/iot_23_datasets_small.tar.gz" \
    && tar xzf iot_23_datasets_small.tar.gz \
    && rm iot_23_datasets_small.tar.gz

USER airflow
