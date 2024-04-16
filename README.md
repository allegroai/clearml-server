# ClearML API Server Setup Guide
## Introduction

## This guide provides instructions for setting up ClearML API Server either with Docker or without Docker.
## Requirements

    Python 3.x
    Docker (optional)

## Setup With Docker

Clone the Repository:

```bash
git clone https://github.com/Nuva-Org/clearml-server.git
cd clearml-server
```
Build the Docker Image:
```bash
docker build -t solytics-apiserver .
```
Change the image name to solytics-apiserver in docker file in fileserver , webserver, apiserver services 
Run the Docker compose:
```bash
    docker compose up -d 
```
# Setup Without Docker (apiserver)


Clone the Repository:

```bash
git clone https://github.com/Nuva-Org/clearml-server.git
cd clearml-server
```
Install Dependencies:
    make a virtualenv and then install requirements using following cmd
```bash
pip install -r requirements.txt
```
Start the API Server using following cmd:
```bash 
python -m  apiserver.server
```
