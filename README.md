# TRAINS Server
##  Magic Version Control & Experiment Manager for AI

## Introduction

The **trains-server** is the infrastructure behind [trains](https://github.com/allegroai/trains).

The server provides:

 * UI (single-page webapp) for experiment management and browsing
 * REST interface for documenting and logging experiment information, statistics and results
 * REST interface for querying experiments history, logs and results
 * Locally-hosted fileserver, for storing images and models to be easily accessible from the UI

The server is designed to allow multiple users to collaborate and manage their experiments.
The server’s code is freely available [here](https://github.com/allegroai/trains-server).
We've also pre-built a docker image to allow **trains** users to quickly set up their own server.

## System diagram

<pre>
 TRAINS-server
 +--------------------------------------------------------------------+
 |                                                                    |
 |   Server Docker                   Elastic Docker     Mongo Docker  |
 |  +-------------------------+     +---------------+  +------------+ |
 |  |     Pythonic Server     |     |               |  |            | |
 |  |   +-----------------+   |     | ElasticSearch |  |  MongoDB   | |
 |  |   |   WEB server    |   |     |               |  |            | |
 |  |   |   Port 8080     |   |     |               |  |            | |
 |  |   +--------+--------+   |     |               |  |            | |
 |  |            |            |     |               |  |            | |
 |  |   +--------+--------+   |     |               |  |            | |
 |  |   |   API server    +----------------------------+            | |
 |  |   |   Port 8008     +---------+               |  |            | |
 |  |   +-----------------+   |     +-------+-------+  +-----+------+ |
 |  |                         |             |                |        |
 |  |   +-----------------+   |         +---+----------------+------+ |
 |  |   |   File Server   +-------+     |    Host Storage           | |
 |  |   |   Port 8081     |   |   +-----+                           | |
 |  |   +-----------------+   |         +---------------------------+ |
 |  +------------+------------+                                       |
 +---------------|----------------------------------------------------+
                 |HTTP
                 +--------+
 GPU Machine              |
 +------------------------|-------------------------------------------+
 |     +------------------|--------------+                            |
 |     |  Training        |              |    +---------------------+ |
 |     |  Code        +---+------------+ |    | trains configuration| |
 |     |              | TRAINS         | |    | ~/trains.conf       | |
 |     |              |                +------+                     | |
 |     |              +----------------+ |    +---------------------+ |
 |     +---------------------------------+                            |
 +--------------------------------------------------------------------+
</pre>

## Installation

In order to install and run the pre-built **trains-server**, you must be logged in as a user with sudo privileges.

### Setup

In order to run the pre-packaged **trains-server**, you'll need to install **docker**.

#### Install docker

```bash
sudo apt-get install docker
```

#### Setup docker daemon
In order to run the ElasticSearch docker container, you'll need to change some of the default values in the Docker configuration file.

For systems with an `/etc/sysconfig/docker` file, add the options in quotes to the available arguments in `OPTIONS`:

```bash
OPTIONS="--default-ulimit nofile=1024:65536 --default-ulimit memlock=-1:-1"
```

For systems with an `/etc/docker/daemon.json` file, add the section in curly brackets to `default-ulimits`:

```json
"default-ulimits": {
    "nofile": {
            "name": "nofile",
            "hard": 65536,
            "soft": 1024
    },
    "memlock":
    {
            "name": "memlock",
            "soft": -1,
            "hard": -1
    }
}
```

Following this configuration change, you will have to restart the docker daemon:

```bash
sudo service docker stop
sudo service docker start
```

#### vm.max_map_count

The `vm.max_map_count` kernel setting must be at least 262144.

The following example was tested with CentOS 7, Ubuntu 16.04, Mint 18.3, Ubuntu 18.04 and Mint 19:

```bash
sudo echo "vm.max_map_count=262144" > /tmp/99-trains.conf
sudo mv /tmp/99-trains.conf /etc/sysctl.d/99-trains.conf
sudo sysctl -w vm.max_map_count=262144
```

For additional information about setting this parameter on other systems, see the [elastic](https://www.elastic.co/guide/en/elasticsearch/reference/current/docker.html#docker-cli-run-prod-mode) documentation.

#### Choose a data folder

You will need to choose a directory on your system in which all data maintained by **trains-server** will be stored (among others, this includes database, uploaded files and logs).

The following instructions assume the directory is `/opt/trains`.

Issue the following commands:

```bash
sudo mkdir -p /opt/trains/data/elastic && sudo chown -R 1000:1000 /opt/trains
```

### Launching docker images


To launch the docker images, issue the following commands:


```bash
sudo docker run -d --restart="always" --name="trains-elastic" -e "ES_JAVA_OPTS=-Xms2g -Xmx2g" -e "bootstrap.memory_lock=true" -e "cluster.name=trains" -e "discovery.zen.minimum_master_nodes=1" -e "node.name=trains" -e "script.inline=true" -e "script.update=true" -e "thread_pool.bulk.queue_size=2000" -e "thread_pool.search.queue_size=10000" -e "xpack.security.enabled=false" -e "xpack.monitoring.enabled=false" -e "cluster.routing.allocation.node_initial_primaries_recoveries=500" -e "node.ingest=true" -e "http.compression_level=7" -e "reindex.remote.whitelist=*.*" -e "script.painless.regex.enabled=true" --network="host" -v /opt/trains/data/elastic:/usr/share/elasticsearch/data docker.elastic.co/elasticsearch/elasticsearch:5.6.16
```

```bash
sudo docker run -d --restart="always" --name="trains-mongo" -v /opt/trains/data/mongo/db:/data/db -v /opt/trains/data/mongo/configdb:/data/configdb --network="host" mongo:3.6.5
```

```bash
sudo docker run -d --restart="always" --name="trains-fileserver" --network="host" -v /opt/trains/logs:/var/log/trains -v /opt/trains/data/fileserver:/mnt/fileserver allegroai/trains:latest fileserver
```

```bash
sudo docker run -d --restart="always" --name="trains-apiserver" --network="host" -v /opt/trains/logs:/var/log/trains allegroai/trains:latest apiserver
```

```bash
sudo docker run -d --restart="always" --name="trains-webserver" --network="host" -v /opt/trains/logs:/var/log/trains allegroai/trains:latest webserver
```

Once the **trains-server** dockers are up, the following are available:

* API server on port `8008`
* Web server on port `8080`
* File server on port `8081`

## Upgrade

We are constantly updating and adding stuff.
When we release a new version, we’ll include a new pre-built docker image.
Once a new release is out, you can simply:

1. Shut down and remove your docker instances. Each instance can be shut down and removed using the following commands:
    ```bash
    sudo docker stop <docker-name>
    sudo docker rm -v <docker-name>
    ```
    The docker names are (see [Launching docker images](#Launching-docker-images)):
    * `trains-elastic`
    * `trains-mongo`
    * `trains-fileserver`
    * `trains-apiserver`
    * `trains-webserver`

2. Back up your data folder (recommended!). A simple way to do that is using this command:
    ```bash
    sudo tar czvf ~/trains_backup.tgz /opt/trains/data
    ```
    Which will back up all data to an archive in your home folder. Restoring such a backup can be done using these commands:
    ```bash
    sudo rm -R /opt/trains/data
    sudo tar -xzf ~/trains_backup.tgz -C /opt/trains/data
    ```
3. Launch the newly released docker image (see [Launching docker images](#Launching-docker-images))

## License

[Server Side Public License v1.0](https://github.com/mongodb/mongo/blob/master/LICENSE-Community.txt)

**trains-server** relies *heavily* on both [MongoDB](https://github.com/mongodb/mongo) and [ElasticSearch](https://github.com/elastic/elasticsearch).
With the recent changes in both MongoDB's and ElasticSearch's OSS license, we feel it is our job as a community to support the projects we love and cherish.
We feel the cause for the license change in both cases is more than just, and chose [SSPL](https://www.mongodb.com/licensing/server-side-public-license) because it is the more restrictive of the two.

This is our way to say - we support you guys!
