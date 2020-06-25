# :trophy: Data Engineering Capstone Project

# Purpose
The purpose of the data engineering capstone project is to give you a chance to combine what you've learned throughout the program. This project will be an important part of your portfolio that will help you achieve your data engineering-related career goals.

In this project, you can choose to complete the project provided for you, or define the scope and data for a project of your own design. Either way, you'll be expected to go through the same steps outlined below.

# Step 1 | Scope the Project and Gather Data

## Scope of the Project
The capstone project for my journey throughout the [Udacity Data Engineering](https://www.udacity.com/course/data-engineer-nanodegree--nd027) course looks into creating a data pipeline by using the IoT-23 dataset by Stratosphere Laboratory and enriching the dataset with network related information from three other datasets, *ASN* ([autonomous system number](https://en.wikipedia.org/wiki/Autonomous_system_(Internet))), *City Blocks*, and *City Locations* by MaxMind.

It is to be built by using a [Docker](https://www.docker.com/) container environment for simple scaling and deployment to *any* platform. The following containers will be constructed for handling each part of the project:

- **Data Engineering** | This container will handle the ETL-processes of extracting, transforming, and loading the cleaned data in the data warehouse. It will be designed to use [Apache Airflow](https://airflow.apache.org/) for managing the workflows and [Apache Spark](https://spark.apache.org/) for handling the datasets. The container will be built upon the [puckel/docker-airflow:1.10.9](https://hub.docker.com/r/puckel/docker-airflow/dockerfile) docker container and then extended with the installation of Apache Spark and to download of the datasets required in the project.
- **Data Warehouse** | This container will function as the storage for the cleaned data extracted from the datasets, by using [PostgreSQL](https://www.postgresql.org/). It is to be handled by the [postgres:latest](https://hub.docker.com/layers/postgres/library/postgres/latest/images/sha256-45bbfe24861756fa1406f8f7942892510b66374e1e25ae758eea49e6ab7725a3) docker container.
- **Visual Analytics** | This container will function as the quality assurance and analytics container for the project. It will be designed to handle queries towards the data warehouse and visualize the results for the user in [Jupyter Notebooks](https://jupyter.org/). The container that is to be used is the [jupyter/datascience-notebook](https://hub.docker.com/r/jupyter/datascience-notebook).

## Datasets
This project looks into engineering data from the [IoT-23 dataset](https://www.stratosphereips.org/datasets-iot23), *a labeled dataset with malicious and benign IoT network traffic*, and three support datasets from MaxMind's [GeoIP2 databases](https://dev.maxmind.com/geoip/geoip2/geolite2/), with correlated IP network information about geographical locations and ASNs.

1. The IoT-23 dataset is used as the baseline for this project, which includes PCAP-data with attached labels that describes the corresponding malware or benign information. In total the dataset contains 325,307,990 packets that has been captured between 9th of May 2018 to 21st of September 2019.

> "IoT-23 is a new dataset of network traffic from Internet of Things (IoT) devices. It has 20 malware captures executed in IoT devices, and 3 captures for benign IoT devices traffic. It was first published in January 2020, with captures ranging from 2018 to 2019. This IoT network traffic was captured in the Stratosphere Laboratory, AIC group, FEL, CTU University, Czech Republic. Its goal is to offer a large dataset of real and labeled IoT malware infections and IoT benign traffic for researchers to develop machine learning algorithms. This dataset and its research is funded by Avast Software, Prague." - [https://www.stratosphereips.org/datasets-iot23](https://www.stratosphereips.org/datasets-iot23)

2. The [MaxMind ASN](https://dev.maxmind.com/geoip/geoip2/geolite2-asn-csv-database/) database is used to enrich the IoT-23 dataset with information about organizations potentially associated with the IP-addresses found.

3. The [MaxMind City](https://dev.maxmind.com/geoip/geoip2/geolite2/) database is used to enrich the IoT-23 dataset with information about geolocation information such as country and city associated with the IP-addresses found.

# Step 2 | Explore and Assess the Data

## Data Exploration
In order to explore the contents of the datasets, they have to be downloaded locally and opened. Download links for each of the datasets can be found below:

- IoT-23: [https://mcfp.felk.cvut.cz/publicDatasets/IoT-23-Dataset/iot_23_datasets_small.tar.gz](https://mcfp.felk.cvut.cz/publicDatasets/IoT-23-Dataset/iot_23_datasets_small.tar.gz)
- MaxMind ASN: [https://drive.google.com/open?id=1vcBa2iFGgSA4Gf3wCk2a7vHCphu03xDl](https://drive.google.com/open?id=1vcBa2iFGgSA4Gf3wCk2a7vHCphu03xDl)
- MaxMind City: [https://drive.google.com/open?id=1ufDqmL3L5SK3d_2c8gDS6yh6Z-VFFOYw](https://drive.google.com/open?id=1ufDqmL3L5SK3d_2c8gDS6yh6Z-VFFOYw)

After exploring the contents of the datasets, we can delete them since the Docker container is configured to take care of downloading and extracting all the necessary files automatically.

### IoT-23
The **IoT-23 dataset** contains 23 sub-directories with network packages, categorized as either *Honeypot* or *Malware* captures. This project will focus on only looking into the network packages found in the *Malware* directories. The files also includes some commented lines (starting with #) at the start and one commented out line in the end, as illustrated below:

```
#separator \x09
#set_separator	,
#empty_field	(empty)
#unset_field	-
#path	conn
#open	2019-03-15-14-50-49
#fields	ts	uid	id.orig_h	id.orig_p	id.resp_h	id.resp_p	proto	service	duration	orig_bytes	resp_bytes	conn_state	local_orig	local_resp	missed_bytes	history	orig_pkts	orig_ip_bytes	resp_pkts	resp_ip_bytes	tunnel_parents   label   detailed-label
#types	time	string	addr	port	addr	port	enum	string	interval	count	count	string	bool	bool	count	string	count	count	count	count	set[string]   string   string
1545403816.962094	CrDn63WjJEmrWGjqf	192.168.1.195	41040	185.244.25.235	80	tcp	-	3.139211	0	0	S0	-	-	0	S	3	180	0	0	-   Benign   -
1545403824.181240	CY9lJW3gh1Eje4usP6	192.168.1.195	41040	185.244.25.235	80	tcp	-	-	-	-	S0	-	-	0	S	1	60	0	0	-   Benign   -
...
1545490181.542213	CuXpFN3fWesWBXUhq1	192.168.1.195	123	82.113.53.40	123	udp	-	-	-	-	S0	-	-	0	D	1	76	0	0	-   Benign   -
1545490198.459568	Ct2Yhy4d33oL3yyZY9	192.168.1.195	123	89.221.210.188	123	udp	-	-	-	-	S0	-	-	0	D	1	76	0	0	-   Benign   -
#close	2019-03-15-14-50-54
```

By studying the structured lines of the files, it is possible to find that the delimiter is `\t`, while the last column can be split into new columns by the three spaces. We are also given the header name for each column and the data type from the commented lines above the data.

### MaxMind ASN
The **MaxMind ASN dataset** contains a CSV file with information about network (IPv4) and the corresponding ASN and organization. It only includes a descriptive header and three columns with data, which will be used to enrich the IoT-23 dataset.

### MaxMind City
The **MaxMind City dataset** contains two CSV files, the first containing the IPv4-blocks and geolocations, and the second containing support information such as continent, country, city name, and time zone. They contain a descriptive header and multiple structured columns with data, which will be used to enrich the IoT-23 dataset.

## Data Cleaning
From the data exploration phase, it is clear that the data found in the IoT-23 dataset needs to be cleaned by removing the commented lines. Since this dataset also includes network captures from IoT devices over a time periode, it is possible to use the timestamp value found in the `ts` column to partition the data by day. This makes it possible for us to simulate a summary of daily packets, which can be processed by a daily schedule using Apache Airflow.

# Step 3 | Define the Data Model

## Data Model
The data model that is used for this project can be represented by a [snowflake schema](https://en.wikipedia.org/wiki/Snowflake_schema), since *City Blocks* contains two geoname IDs for each row towards different locations. Had there only been a single geoname ID, then a [star schema](https://en.wikipedia.org/wiki/Star_schema) would have been the best approach.

![Data Model](https://raw.githubusercontent.com/FredrikBakken/udacity_data-engineering/master/assets/imgs/data-model.png)

### Data Dictionary

| Table             | Term                           | Data Type  | Example                    |
| ----------------- | ------------------------------ | ---------- | -------------------------- |
| Packets           | timestamp                      | VARCHAR    | 1526774401                 |
| Packets           | uid                            | VARCHAR PK | C5e1a23koG8ZxeqDf4         |
| Packets           | originate_network_id           | VARCHAR PK | 192.168.2                  |
| Packets           | response_network_id            | VARCHAR PK | 31.202.212                 |
| Packets           | protocol                       | VARCHAR    | tcp                        |
| Packets           | service                        | VARCHAR    | -                          |
| Packets           | duration                       | VARCHAR    | 2.999712                   |
| Packets           | connection_state               | VARCHAR    | S0                         |
| Packets           | missed_bytes                   | INTEGER    | 0                          |
| Packets           | history                        | VARCHAR    | S                          |
| Packets           | tunnel_parents                 | VARCHAR    | (empty)                    |
| Packets           | label                          | VARCHAR    | Malicious                  |
| Packets           | detailed_label                 | VARCHAR    | PartOfAHorizontalPortScan  |
| Packets           | insert_date                    | DATE       | datetime.date(2018, 7, 25) |
| Originate Packets | uid                            | VARCHAR PK | C5e1a23koG8ZxeqDf4         |
| Originate Packets | host                           | VARCHAR    | 195.189.68.85              |
| Originate Packets | port                           | INTEGER    | 81                         |
| Originate Packets | bytes                          | VARCHAR    | -                          |
| Originate Packets | local                          | VARCHAR    | -                          |
| Originate Packets | packets                        | INTEGER    | 0                          |
| Originate Packets | ip_bytes                       | INTEGER    | 0                          |
| Originate Packets | insert_date                    | DATE       | datetime.date(2018, 7, 25) |
| Response Packets  | uid                            | VARCHAR PK | C5e1a23koG8ZxeqDf4         |
| Response Packets  | host                           | VARCHAR    | 31.202.212.253             |
| Response Packets  | port                           | INTEGER    | 22                         |
| Response Packets  | bytes                          | VARCHAR    | -                          |
| Response Packets  | local                          | VARCHAR    | -                          |
| Response Packets  | packets                        | INTEGER    | 0                          |
| Response Packets  | ip_bytes                       | INTEGER    | 0                          |
| Response Packets  | insert_date                    | DATE       | datetime.date(2018, 7, 25) |
| ASN               | network_id                     | VARCHAR PK | 1.0.0                      |
| ASN               | network                        | VARCHAR    | 1.0.0.0/24                 |
| ASN               | autonomous_system_number       | INTEGER    | 13335                      |
| ASN               | autonomous_system_organization | VARCHAR    | CLOUDFLARENET              |
| City Blocks       | network_id                     | VARCHAR PK | 195.189.66                 |
| City Blocks       | network                        | VARCHAR    | 195.189.66.0/23            |
| City Blocks       | geoname_id                     | INTEGER    | 2970275                    |
| City Blocks       | registered_country_geoname_id  | INTEGER    | 3017382                    |
| City Blocks       | represented_country_geoname_id | INTEGER    | NaN                        |
| City Blocks       | is_anonymous_proxy             | INTEGER    | 0                          |
| City Blocks       | is_satellite_provider          | INTEGER    | 0                          |
| City Blocks       | postal_code                    | VARCHAR    | 42340                      |
| City Blocks       | latitude                       | FLOAT      | 45.5629                    |
| City Blocks       | longitude                      | FLOAT      | 4.2903                     |
| City Blocks       | accuracy_radius                | INTEGER    | 50                         |
| City Locations    | geoname_id                     | INTEGER PK | 5819                       |
| City Locations    | locale_code                    | VARCHAR    | en                         |
| City Locations    | continent_code                 | VARCHAR    | EU                         |
| City Locations    | continent_name                 | VARCHAR    | Europe                     |
| City Locations    | country_iso_code               | VARCHAR    | CY                         |
| City Locations    | country_name                   | VARCHAR    | Cyprus                     |
| City Locations    | subdivision_1_iso_code         | VARCHAR    | 02                         |
| City Locations    | subdivision_1_name             | VARCHAR    | Limassol                   |
| City Locations    | subdivision_2_iso_code         | VARCHAR    | None                       |
| City Locations    | subdivision_2_name             | VARCHAR    | None                       |
| City Locations    | city_name                      | VARCHAR    | Souni                      |
| City Locations    | metro_code                     | VARCHAR    | None                       |
| City Locations    | time_zone                      | VARCHAR    | Asia/Nicosia               |
| City Locations    | is_in_european_union           | INTEGER    | 1                          |

## Data Pipelines
Two data pipelines are defined for this project and each of them has its own Apache Airflow DAG.

### MaxMind Dataset
![Pipeline: MaxMind Dataset](https://raw.githubusercontent.com/FredrikBakken/udacity_data-engineering/master/assets/imgs/pipeline_maxmind.png)

### IoT-23 Dataset
![Pipeline: IoT-23 Dataset](https://raw.githubusercontent.com/FredrikBakken/udacity_data-engineering/master/assets/imgs/pipeline_iot23-dataset.png)

# Step 4 | Run ETL to Model the Data

# Get Started

## Hardware Requirements
The following hardware requirements has to be fulfilled to run this project:
 - 250GB free disk space
 - 16GB of RAM

## Preliminary Steps
This capstone project is designed to be executable on all platforms by taking advantage of Docker containers and thus requires the user to follow these prelimiary steps:

1. **Docker** | Make sure that your system has Docker installed by running `docker` in the terminal. If Docker is not installed on your system, follow the instructions here: [https://docs.docker.com/desktop/](https://docs.docker.com/desktop/) for Mac/Windows installations. For Linux machines, run the following commands in the terminal:
```
 >> sudo apt install docker.io
 >> sudo apt install docker-compose
 >> sudo apt update
```

2. **Java JDK** | The Java JDK file `jdk-8uXXX-linux-x64.tar.gz` (XXX represents the version) is necessary for the installation of Apache Spark within the Docker container. Download this file and place it within the `/config` directory in this project. You can find the latest Java JDK 8 here: [https://www.oracle.com/java/technologies/javase/javase-jdk8-downloads.html]().

3. **Dockerfile** | After placing the JAVA JDK 8 (from the previous step) into the `/config` directory, update the `Dockerfile` on line 3 with the correct JDK version (illustrated by the XXX in step 2). Java JDK 8u251 was used during the development of this project, but a newer version might be found on the [Oracle pages](https://www.oracle.com/java/technologies/javase/javase-jdk8-downloads.html).

## Launch the Project
After completing all the prelimiary steps described above, you are finally ready to launch the project environment. This process will take some time (about 30-60min) depending on your internet connection, since it will download and install all the docker containers and then download and extract the datasets.

**WARNING:** This command triggers the download and execution process for the three Docker containers configured for this project, which means that there has to be a minimum of ~50GB of available disk space on the user's machine.

```
 >> docker-compose up
```

Once everything is successfully downloaded and the Docker containers are up and running, you can find all the active containers by running `docker ps`. This command will give you something like this (may look different on your machine):

| CONTAINER ID | IMAGE                              | COMMAND                | CREATED        | STATUS       | PORTS                        | NAMES            |
| ------------ | ---------------------------------- | ---------------------- | -------------- | ------------ | ---------------------------- | ---------------- |
| 776a40a69ac3 | capstone-project_airflow-spark245  | "/entrypoint.sh webs…" | 15 minutes ago | Up 6 minutes | 5555/tcp, 8080/tcp, 8793/tcp | data-engineering |
| bf864eca62af | jupyter/datascience-notebook       | "tini -g -- start-no…" | 15 minutes ago | Up 6 minutes | 0.0.0.0:8888->8888/tcp       | visual-analytics |
| e41372b43651 | postgres:latest                    | "docker-entrypoint.s…" | 15 minutes ago | Up 6 minutes | 0.0.0.0:5432->5432/tcp       | data-warehouse   |

## Access the Applications
Okei! If you have gotten to this stage, then the project is successfully configured and three containers are up and running, but *what now*? The next step is to actually launch the applications found in this project.

### Apache Airflow
Docker containers are spun up to run in their own [network environments](https://docker-curriculum.com/#docker-network), which can be accessed by using the exposed ports. You can find the network ID of the current docker-compose project by running the following command in the terminal `docker network ls`, this will give you something like this (may look different on your machine):

| NETWORK ID   | NAME                             | DRIVER | SCOPE |
| ------------ | -------------------------------- | ------ | ----- |
| ab818bc6e69c | bridge                           | bridge | local |
| 66372c0553ca | capstone-project_udacity_network | bridge | local |

After finding the *NETWORK ID* for the **capstone-project_udacity_network**, use it to find the IPv4 hosts for the three containers in the project by running `docker network inspect <network ID>` (in this example the command is `docker network inspect 66372c0553ca`). The output in the terminal is a JSON-formatted description of the Docker container network, where the interesting part can be found within the "Containers"-section:

```
"Containers": {
    "29994cc51a391ee1015e15d578e91e3e979c687b16752b7d35d232738a8d5028": {
        "Name": "data-engineering",
        "EndpointID": "3c14b33b801a46c4559068e5750ff8ee256fe2b67f980a13e69d6bc98b7720f9",
        "MacAddress": "02:42:ac:1c:01:01",
        "IPv4Address": "172.28.1.1/16",
        "IPv6Address": ""
    },
    "4b65e64ba58c80a882d5cf3083fc67910cfc645e3849e56d262d1a0bbb55f796": {
        "Name": "visual-analytics",
        "EndpointID": "9173c03c507c5b1a115a800c3dc9394bf5849fa34afe2486c40a733e77bc12f3",
        "MacAddress": "02:42:ac:1c:01:03",
        "IPv4Address": "172.28.1.3/16",
        "IPv6Address": ""
    },
    "64d2d684fb043454f345bd1241545c911b2ba6299bb6dde522a91f37fd47d9ad": {
        "Name": "data-warehouse",
        "EndpointID": "df2e060e2999d5bb4fc437bfccdf3b2a378bd9d975361cbd37bfcb4d8cb847f1",
        "MacAddress": "02:42:ac:1c:01:02",
        "IPv4Address": "172.28.1.2/16",
        "IPv6Address": ""
    }
}
```

The first application is the Apache Airflow, which controls the engineering processes in the project. This application is accessible through the 8080 port, where the host can be found in the *IPv4Address*-field of the **data-engineering** container. In this case, the Apache Airflow application can be access by opening your favorite browser on the URL [http://172.28.1.1:8080/](http://172.28.1.1:8080/).

### Jupyter Notebook
The second application to open is the Jupyter Notebook, which is used to analyze the output results and for checking the data quality. In order to open this application, open your favorite browser on the URL [http://localhost:8888/tree?token=capstone](http://localhost:8888/tree?token=capstone). The notebooks can be found in the `notebooks` directory.

## Running the Applications

### Data Engineering
Open the Apache Airflow UI by going to your browser and accessing the URL [http://172.28.1.1:8080/](http://172.28.1.1:8080/). This page will provide you with the following two DAGs *maxmind-dataset*, and *iot-23-dataset*.

#### MaxMind Dataset
The MaxMind Dataset DAG is designed to drop, create, and insert data into the MaxMind tables in the PostgreSQL Data Warehouse. It uses an ETL-process for extracting data from the dataset files, transforming the data to fit the storage tables, and then loads the data into the database. The image below gives a graph view of how the DAG is constructed:

![MaxMind Dataset Graph View](https://raw.githubusercontent.com/FredrikBakken/udacity_data-engineering/master/assets/imgs/maxmind-dataset.png)

#### IoT-23 Dataset
The IoT-23 Dataset DAG is designed to clean, prepare, drop, create, and insert data into the IoT-23 packet tables in the PostgreSQL Data Warehouse. It uses an ETL-process for extracting data from the dataset files, transforming the data to fit the storage tables, and then loads the data into the database. The image below gives a graph view of how the DAG is constructed:

![IoT-23 Dataset Graph View](https://raw.githubusercontent.com/FredrikBakken/udacity_data-engineering/master/assets/imgs/iot23_graph-view.png)

The DAG is running within a selected timeframe with the start date set to 09.05.2018 and an end date set to 22.09.2019. It is also following a daily schedule, where it runs the DAG at 23:59 every day. The image below gives a tree view of how the DAG is being executed:

![IoT-23 Dataset Tree View](https://raw.githubusercontent.com/FredrikBakken/udacity_data-engineering/master/assets/imgs/iot23_tree-view.png)

### Data Analytics
Open the Jupyter Notebook UI by going to your browser and accessing the URL [http://localhost:8888/tree?token=capstone](http://localhost:8888/tree?token=capstone). This page will provide you with two directories. Go the the `notebooks` directory to find the `Data Analysis` and `Data Quality` notebooks.

#### Data Quality
The [Data Quality Notebook](https://github.com/FredrikBakken/udacity_data-engineering/blob/master/capstone-project/notebooks/Data%20Quality.ipynb) is used to run simple queries towards the packet tables to ensure that the partitions exist, the same number of rows exist, and that JOIN aggregations are possible.

#### Data Analysis
The [Data Analysis Notebook](https://github.com/FredrikBakken/udacity_data-engineering/blob/master/capstone-project/notebooks/Data%20Analysis.ipynb) is used to analyse the data found in the `28_5_20`-partition (other options are also available). It JOINs data from all the tables in order to visualize the origin of all the packets sent from the response network column, as illustrated below:

![World Map | All Packets](https://raw.githubusercontent.com/FredrikBakken/udacity_data-engineering/master/assets/imgs/world-map_all-packets.png)

It also filters on all packets categorized as "Attack" in order to visualize the origin of all the attack locations from the response network column, as illustrated below:

![World Map | Attack Packets](https://raw.githubusercontent.com/FredrikBakken/udacity_data-engineering/master/assets/imgs/world-map_attack-packets.png)

Finally, it shows the autonomous system organization count for the "Attack"-labeled packets.

## Cleaning Up
Once the project has been successfully ran, one can clean up the cluttered files on the system by running the following commands in the terminal.

**WARNING:** This will delete *ALL* Docker images and running containers on your system. DO NOT run these commands if there already exist Docker images/containers on your system which you want to keep.

```
docker stop $(docker ps -aq)
docker rm -vf $(docker ps -a -q)
docker rmi -f $(docker images -a -q)
```

# Step 5 | Complete Project Write Up

## Questions & Answers
1. **What's the goal?**<br/>The goal of the project is to showcase what I have learned during the Udacity Data Engineering course by using my creativity to explore ways to engineer and analyze data from different sources. I also wanted to design the project so that I could get hands-on experience on as many of the technologies as possible from the course. In addition - it was more or less about playing with data and trying to see what would be possible.<br/><br/>
2. **What queries will you want to run?**<br/>I wanted to run queries that confirmed that data quality such as finding the name of the partition tables, the number of rows in each table, and confirm that JOINs were possible. For the analysis part, I wanted to execute queries that would use the network information together with location data and asn data.</br></br>During this testing I found that JOIN-aggregations required a lot of disk space if they were executed directly towards the database with SQL. My solution was therefore to load the data into Pandas dataframes and merge the dataframes directly, since this would use the memory instead of actual disk space on my local machine.<br/><br/>
3. **How would Spark or Airflow be incorporated?**<br/>Apache Spark and Apache Airflow are already incorporated into the project.<br/><br/>
4. **Why did you choose the model you chose?**<br/>I chose to use a snowflake schema approach for the database tables since the city_blocks table included multiple geoname_id columns. This could potentially result in there having to be multiple rows of the city_locations table added to the city_blocks table.<br/><br/>
5. **State the rationale for the choice of tools and technologies for the project.**<br/>The selection of tools and technologies for the project were based on the requirements set for the project. It requires daily processing, where Apache Airflow is perfect for settings up scheduled workflow. Apache Spark (with Python) is perfect for running ETL-processes on large amounts of data (both in cluster and local mode). As an alternative, Scala could have been used instead of Python since Apache Spark is developed in Scala ([to achieve native performance](https://www.simplilearn.com/scala-vs-python-article)).</br></br>PostgreSQL was selected as the database technology, since it is easy to scale with larger amounts of data by dynamically adding partitions. With even larger amounts of data, database technologies such as [Apache Hive](https://hive.apache.org/) (for [OLAP](https://en.wikipedia.org/wiki/Online_analytical_processing) cases) or [Apache HBase](https://hbase.apache.org/) (for [OLTP](https://en.wikipedia.org/wiki/Online_transaction_processing) cases) could be used.</br></br>Jupyter Notebook was selected for the data quality and data analysis parts since it is a practical tool for performing visualization of the results.</br></br>I also added the use of Docker, since I wanted to learn and get hands-on experience with how docker containers work.<br/><br/>
6. **How often the data should be updated and why?**<br/>The current project application updates the data on a daily basis for the IoT-23 dataset by fetching the new data partition that is generated for each day. It could also include a weekly scheduled update of the MaxMind data, since these are updated every Wednesday by MaxMind.

## Scenarios
How would you approach the problem differently under the following scenarios?

1. **If the data was increased by 100x.**<br/>In case where the total amount of data is increased by 100x, it would be necessary to increase the total amount of disk space and increase the number of workers. This can be done by running the project in a cluster environment (e.g. AWS, Azure, Google Cloud, or a self-configured Hadoop cluster) with multiple worker nodes and more disks for storage.<br/><br/>
2. **If the pipelines were run on a daily basis by 7am.**<br/>The current IoT-23 pipeline is configured to run on a daily scheduled interval at 23:59. This pipeline just has to be updated to run at 07:00 instead.<br/><br/>
3. **If the database needed to be accessed by 100+ people.**<br/>In the case where the database has to be accessed by 100+ people, it would require that the **Data Warehouse** container is depolyed in a shared environment which can be accessed by multiple people. This can be done by e.g. having a shared Redshift data warehouse.
