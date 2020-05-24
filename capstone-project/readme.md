# :trophy: Data Engineering Capstone Project
description...

# Purpose
The purpose of the data engineering capstone project is to give you a chance to combine what you've learned throughout the program. This project will be an important part of your portfolio that will help you achieve your data engineering-related career goals.

In this project, you can choose to complete the project provided for you, or define the scope and data for a project of your own design. Either way, you'll be expected to go through the same steps outlined below.

# Step 1 | Scope the Project and Gather Data

## Scope of the Project
...

## Datasets
This project looks into engineering data from the [IoT-23 dataset](https://www.stratosphereips.org/datasets-iot23), *a labeled dataset with malicious and benign IoT network traffic*, and two support datasets from MaxMind's [GeoIP2 databases](https://dev.maxmind.com/geoip/geoip2/geolite2/), with correlated IP-information about geographical locations and ASNs.

1. ...

> "IoT-23 is a new dataset of network traffic from Internet of Things (IoT) devices. It has 20 malware captures executed in IoT devices, and 3 captures for benign IoT devices traffic. It was first published in January 2020, with captures ranging from 2018 to 2019. This IoT network traffic was captured in the Stratosphere Laboratory, AIC group, FEL, CTU University, Czech Republic. Its goal is to offer a large dataset of real and labeled IoT malware infections and IoT benign traffic for researchers to develop machine learning algorithms. This dataset and its research is funded by Avast Software, Prague." - https://www.stratosphereips.org/datasets-iot23

2. ...

# Step 2 | Explore and Assess the Data
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

After finding the *NETWORK ID* for the **capstone-project_default**, use it to find the IPv4 hosts for the three containers in the project by running `docker network inspect <network ID>` (in this example the command is `docker network inspect 66372c0553ca`). The output in the terminal is a JSON-formatted description of the Docker container network, where the interesting part can be found within the "Containers"-part:

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
