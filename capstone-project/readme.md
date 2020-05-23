# :trophy: Capstone Project
description...

# Purpose
The purpose of the data engineering capstone project is to give you a chance to combine what you've learned throughout the program. This project will be an important part of your portfolio that will help you achieve your data engineering-related career goals.

In this project, you can choose to complete the project provided for you, or define the scope and data for a project of your own design. Either way, you'll be expected to go through the same steps outlined below.

# Getting Started


# Cleaning Up
Once the project has been successfully ran, one can clean up the cluttered files on the system by running the following commands in the terminal.

**WARNING:** Be aware that this will delete *ALL* Docker images and running containers on your system. DO NOT run these commands if there already exists Docker images/containers on your system which you want to keep.

```
docker stop $(docker ps -aq)
docker rm -vf $(docker ps -a -q)
docker rmi -f $(docker images -a -q)
```
