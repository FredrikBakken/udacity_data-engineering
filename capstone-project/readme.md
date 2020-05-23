# :trophy: Capstone Project
description...

# Purpose
The purpose of the data engineering capstone project is to give you a chance to combine what you've learned throughout the program. This project will be an important part of your portfolio that will help you achieve your data engineering-related career goals.

In this project, you can choose to complete the project provided for you, or define the scope and data for a project of your own design. Either way, you'll be expected to go through the same steps outlined below.

# Getting Started
This capstone project is designed to be executable on all platforms by taking advantage of Docker containers and thus requires the user to follow these prelimiary steps:

1. **Docker** | Make sure that your system has Docker installed by running `docker` in the terminal. If Docker is not installed on your system, follow the instructions here: https://docs.docker.com/desktop/ for Mac/Windows installations. For Linux machines, run the following commands in the terminal:
```
sudo apt install docker.io
sudo apt install docker-compose
sudo apt update
```

2. **Java JDK** | The Java JDK file `jdk-8uXXX-linux-x64.tar.gz` (XXX represents the version) is necessary for the installation of Apache Spark within the Docker container. Download this file and place it within the `/config` directory in this project. You can find the latest Java JDK 8 here: https://www.oracle.com/java/technologies/javase/javase-jdk8-downloads.html.

3. **Dockerfile** | After placing the JAVA JDK 8 in the previous step into the `/config` directory, update the `Dockerfile` on line 3 with the correct JDK version (illustrated by the XXX in step 2). Java JDK 8u251 was used during the development of this project, but a newer version might be found on the Oracle pages.

4. **Datasets** | todo...

# Cleaning Up
Once the project has been successfully ran, one can clean up the cluttered files on the system by running the following commands in the terminal.

**WARNING:** Be aware that this will delete *ALL* Docker images and running containers on your system. DO NOT run these commands if there already exists Docker images/containers on your system which you want to keep.

```
docker stop $(docker ps -aq)
docker rm -vf $(docker ps -a -q)
docker rmi -f $(docker images -a -q)
```
