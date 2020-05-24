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

# Getting Started
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
After all the prelimiary steps above are completed, the project can be started by running the following command in the terminal:

```
 >> docker-compose up
```

**WARNING:** This command triggers the download and execution process for the three Docker containers configured for this project, which means that there has to be a minimum of ~50GB of available disk space on the user's machine.

# Cleaning Up
Once the project has been successfully ran, one can clean up the cluttered files on the system by running the following commands in the terminal.

**WARNING:** This will delete *ALL* Docker images and running containers on your system. DO NOT run these commands if there already exists Docker images/containers on your system which you want to keep.

```
docker stop $(docker ps -aq)
docker rm -vf $(docker ps -a -q)
docker rmi -f $(docker images -a -q)
```
