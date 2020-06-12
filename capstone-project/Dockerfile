FROM puckel/docker-airflow:1.10.9

ARG JAVA_VERSION=251

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
COPY ./config/jdk-8u${JAVA_VERSION}-linux-x64.tar.gz /opt/jdk-8.tar.gz
RUN cd /opt/ \
    && tar xzf jdk-8.tar.gz \
    && rm jdk-8.tar.gz \
    && update-alternatives --install /usr/bin/java java /opt/jdk1.8.0_${JAVA_VERSION}/bin/java 100 \
    && update-alternatives --install /usr/bin/jar jar /opt/jdk1.8.0_${JAVA_VERSION}/bin/jar 100 \
    && update-alternatives --install /usr/bin/javac javac /opt/jdk1.8.0_${JAVA_VERSION}/bin/javac 100

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

# Install necessary Python libraries
RUN pip install --upgrade pip \
    && pip install psycopg2-binary \
    && pip install gdown

# Add Postgres JAR
ADD ./config/ /usr/local/airflow/config

# Add DAGs to Airflow UI
ADD ./dags/ /usr/local/airflow/dags

# TODO: Remove
#ADD ./datasets /usr/local/airflow/datasets

# Add datasets to the project
RUN mkdir /usr/local/airflow/datasets \
    && cd /usr/local/airflow/datasets \
    && gdown "https://drive.google.com/uc?id=1vcBa2iFGgSA4Gf3wCk2a7vHCphu03xDl" \
    && tar xf GeoLite2-ASN-CSV_20200519.tar.xz \
    && rm GeoLite2-ASN-CSV_20200519.tar.xz \
    && gdown "https://drive.google.com/uc?id=1ufDqmL3L5SK3d_2c8gDS6yh6Z-VFFOYw" \
    && tar xf GeoLite2-City-CSV_20200519.tar.xz \
    && rm GeoLite2-City-CSV_20200519.tar.xz \
    && wget --no-check-certificate "https://mcfp.felk.cvut.cz/publicDatasets/IoT-23-Dataset/iot_23_datasets_small.tar.gz" \
    && tar xzf iot_23_datasets_small.tar.gz \
    && rm iot_23_datasets_small.tar.gz

USER root
