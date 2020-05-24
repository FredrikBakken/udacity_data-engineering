# :trophy: Data Engineering Capstone Project

# Purpose
The purpose of the data engineering capstone project is to give you a chance to combine what you've learned throughout the program. This project will be an important part of your portfolio that will help you achieve your data engineering-related career goals.

In this project, you can choose to complete the project provided for you, or define the scope and data for a project of your own design. Either way, you'll be expected to go through the same steps outlined below.

# Step 1 | Scope the Project and Gather Data

## Scope of the Project
The capstone project for my journey through the Udacity Data Engineering course looks into creating a data pipeline by using the IoT-23 dataset by Stratosphere Laboratory and enriching the dataset with IP related information from two other datasets, *ASN* ([autonomous system number](https://en.wikipedia.org/wiki/Autonomous_system_(Internet))) and *City*, by MaxMind. It is built using a Docker container environment for simple scaling and deployment to *any* platform.

## Datasets
This project looks into engineering data from the [IoT-23 dataset](https://www.stratosphereips.org/datasets-iot23), *a labeled dataset with malicious and benign IoT network traffic*, and two support datasets from MaxMind's [GeoIP2 databases](https://dev.maxmind.com/geoip/geoip2/geolite2/), with correlated IP-information about geographical locations and ASNs.

1. The IoT-23 dataset is used as the baseline for this project, which includes PCAP-data with attached labels that describes the corresponding malware or benign information. In total the dataset contains 764,308,000 packets that has been captured between 2018 to 2019.

> "IoT-23 is a new dataset of network traffic from Internet of Things (IoT) devices. It has 20 malware captures executed in IoT devices, and 3 captures for benign IoT devices traffic. It was first published in January 2020, with captures ranging from 2018 to 2019. This IoT network traffic was captured in the Stratosphere Laboratory, AIC group, FEL, CTU University, Czech Republic. Its goal is to offer a large dataset of real and labeled IoT malware infections and IoT benign traffic for researchers to develop machine learning algorithms. This dataset and its research is funded by Avast Software, Prague." - https://www.stratosphereips.org/datasets-iot23

2. The [MaxMind ASN](https://dev.maxmind.com/geoip/geoip2/geolite2-asn-csv-database/) database is used to enrich the IoT-23 dataset with information about organizations potentially associated with the IP-addresses found.

3. The [MaxMind City](https://dev.maxmind.com/geoip/geoip2/geolite2/) database is used to enrich the IoT-23 dataset with information about geolocation information such as country and city associated with the IP-addresses found.

# Step 2 | Explore and Assess the Data

## Data Exploration
In order to explore the contents of the datasets, they have to be downloaded locally and opened. Download links for each of the datasets can be found below:

- IoT-23: https://mcfp.felk.cvut.cz/publicDatasets/IoT-23-Dataset/iot_23_datasets_small.tar.gz
- MaxMind ASN: https://drive.google.com/open?id=1vcBa2iFGgSA4Gf3wCk2a7vHCphu03xDl
- MaxMind City: https://drive.google.com/open?id=1ufDqmL3L5SK3d_2c8gDS6yh6Z-VFFOYw

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
From the data exploration phase, it is clear that the data found in the IoT-23 dataset needs to be cleaned by removing the commented lines. Since this dataset also includes network captures from IoT devices over a time periode, it is possible to use the timestamp value found in the `time` column to split the dataset by days. This makes it possible for us to simulate a summary of daily packets, which can be processed by a daily schedule using Apache Airflow.

The data found in the MaxMind datasets does not need further cleaning.

# Step 3 | Define the Data Model

## Data Model
...

## Data Pipeline
...

# Step 4 | Run ETL to Model the Data
...

# Get Started

## Preliminary Steps
This capstone project is designed to be executable on all platforms by taking advantage of Docker containers and thus requires the user to follow these prelimiary steps:

1. **Docker** | Make sure that your system has Docker installed by running `docker` in the terminal. If Docker is not installed on your system, follow the instructions here: https://docs.docker.com/desktop/ for Mac/Windows installations. For Linux machines, run the following commands in the terminal:
```
 >> sudo apt install docker.io
 >> sudo apt install docker-compose
 >> sudo apt update
```

2. **Java JDK** | The Java JDK file `jdk-8uXXX-linux-x64.tar.gz` (XXX represents the version) is necessary for the installation of Apache Spark within the Docker container. Download this file and place it within the `/config` directory in this project. You can find the latest Java JDK 8 here: https://www.oracle.com/java/technologies/javase/javase-jdk8-downloads.html.

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

### Jupyter Notebook
The first application to open is the Jupyter Notebook, which is used to analyze the output results and for checking the data quality. This application URL can be found in the terminal window where the `docker-compose up` command was ran, e.g.:

```
visual-analytics        | [I 10:46:28.366 NotebookApp] The Jupyter Notebook is running at:
visual-analytics        | [I 10:46:28.366 NotebookApp] http://bf864eca62af:8888/?token=aec84158afbad9a31c63cadfa9e2cc78e67b04231a60ed4b
visual-analytics        | [I 10:46:28.366 NotebookApp]  or http://127.0.0.1:8888/?token=aec84158afbad9a31c63cadfa9e2cc78e67b04231a60ed4b
```

Copy the URL found (in this case: http://127.0.0.1:8888/?token=aec84158afbad9a31c63cadfa9e2cc78e67b04231a60ed4b) into your favorite browser to open the Jupyter Notebook.

### Apache Airflow
Docker containers are spun up to run in their own [network environments](https://docker-curriculum.com/#docker-network), which can be accessed by using the exposed ports. You can find the network ID of the current docker-compose project by running the following command in the terminal `docker network ls`, this will give you something like this (may look different on your machine):

| NETWORK ID   | NAME                     | DRIVER | SCOPE |
| ------------ | ------------------------ | ------ | ----- |
| ab818bc6e69c | bridge                   | bridge | local |
| 66372c0553ca | capstone-project_default | bridge | local |

After finding the *NETWORK ID* for the **capstone-project_default**, use it to find the IPv4 hosts for the three containers in the project by running `docker network inspect <network ID>` (in this example the command is `docker network inspect 66372c0553ca`). The output in the terminal is a JSON-formatted description of the Docker container network, where the interesting part can be found within the "Containers"-section:

```
"Containers": {
    "776a40a69ac33eb81f3b2cb7c8396d0109f8cf81c08d758c50f5cf81b799bfbe": {
        "Name": "data-engineering",
        "EndpointID": "2cdace886f0688dc08fd2c9136795cbf0dd7116c6d786a3d5148cc12b9503bb8",
        "MacAddress": "02:42:ac:12:00:04",
        "IPv4Address": "172.18.0.4/16",
        "IPv6Address": ""
    },
    "bf864eca62afa50d9de4a067f15360cc5cd7360e50d9b59cf38393cd5b851dd8": {
        "Name": "visual-analytics",
        "EndpointID": "15f826bd93faf95ecfd27020f78466171c8e90e98a7100c0a5f0d52349a8203e",
        "MacAddress": "02:42:ac:12:00:03",
        "IPv4Address": "172.18.0.3/16",
        "IPv6Address": ""
    },
    "e41372b436510388dd80fc963c865671e3fe4d4d0902cd6b6d69fa4c13c3d0da": {
        "Name": "data-warehouse",
        "EndpointID": "891c84c87dcb8dd6b2670a0dfcfd808b149f5abef837d27edb05bd518841a908",
        "MacAddress": "02:42:ac:12:00:02",
        "IPv4Address": "172.18.0.2/16",
        "IPv6Address": ""
    }
}
```

The second application is the Apache Airflow, which controls the engineering processes in the project. This application accessible through the port 8080, where the host can be found in the *IPv4Address*-field of the **data-engineering** container. In this case, the Apache Airflow application can be access by opening your favorite browser on the URL http://172.18.0.4:8080/.


# Cleaning Up
Once the project has been successfully ran, one can clean up the cluttered files on the system by running the following commands in the terminal.

**WARNING:** This will delete *ALL* Docker images and running containers on your system. DO NOT run these commands if there already exists Docker images/containers on your system which you want to keep.

```
docker stop $(docker ps -aq)
docker rm -vf $(docker ps -a -q)
docker rmi -f $(docker images -a -q)
```

# Step 5 | Complete Project Write Up

## Questions & Answers
1. **What's the goal?**<br/><br/>...<br/><br/><br/>
2. **What queries will you want to run?**<br/><br/>...<br/><br/><br/>
3. **How would Spark or Airflow be incorporated?**<br/><br/>...<br/><br/><br/>
4. **Why did you choose the model you chose?**<br/><br/>...<br/><br/><br/>
5. **State the rationale for the choice of tools and technologies for the project.**<br/><br/>...<br/><br/><br/>
6. **How often the data should be updated and why?**<br/><br/>...<br/><br/><br/>

## Scenarios
How would you approach the problem differently under the following scenarios?

1. **If the data was increased by 100x.**<br/><br/>...<br/><br/><br/>
2. **If the pipelines were run on a daily basis by 7am.**<br/><br/>...<br/><br/><br/>
3. **If the database needed to be accessed by 100+ people.**<br/><br/>...<br/><br/><br/>
