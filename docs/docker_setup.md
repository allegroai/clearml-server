# TRAINS-server: Using Docker Pre-Built Images

The pre-built Docker image for the **trains-server** is the quickest way to get started with your own **TRAINS** server.

You can also build the entire **trains-server** architecture using the code available in the [trains-server](https://github.com/allegroai/trains-server) repository.

**Note**: We tested this pre-built Docker image with Linux, only. For Windows users, we recommend installing the pre-built image on a Linux virtual machine.

## Prerequisites

* You must be logged in as a user with sudo privileges
* Use `bash` for all command-line instructions in this installation

## Setup Docker

### Step 1: Install Docker CE

You must first install Docker. For instructions about installing Docker, see [Supported platforms](https://docs.docker.com/install//#support) in the Docker documentation.

For example, to [install in Ubuntu](https://docs.docker.com/install/linux/docker-ce/ubuntu/) / Mint (x86_64/amd64):

```bash
sudo apt-get install -y apt-transport-https ca-certificates curl software-properties-common
curl -fsSL https://download.docker.com/linux/ubuntu/gpg | sudo apt-key add -
. /etc/os-release
sudo add-apt-repository "deb [arch=amd64] https://download.docker.com/linux/ubuntu $UBUNTU_CODENAME stable"
sudo apt-get update
sudo apt-get install -y docker-ce
```

### Step 2: Set the Maximum Number of Memory Map Areas

Elastic requires that the `vm.max_map_count` kernel setting, which is the maximum number of memory map areas a process can use, is set to at least 262144.

For CentOS 7, Ubuntu 16.04, Mint 18.3, Ubuntu 18.04 and Mint 19.x, we tested the following commands to set `vm.max_map_count`:

```bash
echo "vm.max_map_count=262144" > /tmp/99-trains.conf
sudo mv /tmp/99-trains.conf /etc/sysctl.d/99-trains.conf
sudo sysctl -w vm.max_map_count=262144
```

For information about setting this parameter on other systems, see the [elastic](https://www.elastic.co/guide/en/elasticsearch/reference/current/docker.html#docker-cli-run-prod-mode) documentation.

### Step 3: Restart the Docker daemon

Restart the Docker daemon.

```bash
sudo service docker restart
```

### Step 4: Choose a Data Directory

Choose a directory on your system in which all data maintained by the **trains-server** is stored.
Create this directory, and set its owner and group to `uid` 1000. The data stored in this directory includes the database, uploaded files and logs.

For example, if your data directory is `/opt/trains`, then use the following command:

```bash
sudo mkdir -p /opt/trains/data/elastic
sudo mkdir -p /opt/trains/data/mongo/db
sudo mkdir -p /opt/trains/data/mongo/configdb
sudo mkdir -p /opt/trains/data/redis
sudo mkdir -p /opt/trains/logs
sudo mkdir -p /opt/trains/data/fileserver
sudo mkdir -p /opt/trains/config

sudo chown -R 1000:1000 /opt/trains
```

## TRAINS-server: Manually Launching Docker Containers <a name="launch"></a>

You can manually launch the Docker containers using the following commands.

If your data directory is not `/opt/trains`, then in the five `docker run` commands below, you must replace all occurrences of `/opt/trains` with your data directory path.

1. Launch the **trains-elastic** Docker container.

        sudo docker run -d --restart="always" --name="trains-elastic" -e "bootstrap.memory_lock=true" --ulimit memlock=-1:-1 -e "ES_JAVA_OPTS=-Xms2g -Xmx2g" -e "bootstrap.memory_lock=true" -e "cluster.name=trains" -e "discovery.zen.minimum_master_nodes=1" -e "node.name=trains" -e "script.inline=true" -e "script.update=true" -e "thread_pool.bulk.queue_size=2000" -e "thread_pool.search.queue_size=10000" -e "xpack.security.enabled=false" -e "xpack.monitoring.enabled=false" -e "cluster.routing.allocation.node_initial_primaries_recoveries=500" -e "node.ingest=true" -e "http.compression_level=7" -e "reindex.remote.whitelist=*.*" -e "script.painless.regex.enabled=true" --network="host" -v /opt/trains/data/elastic:/usr/share/elasticsearch/data docker.elastic.co/elasticsearch/elasticsearch:5.6.16

1. Launch the **trains-mongo** Docker container.

        sudo docker run -d --restart="always" --name="trains-mongo" -v /opt/trains/data/mongo/db:/data/db -v /opt/trains/data/mongo/configdb:/data/configdb --network="host" mongo:3.6.5

1. Launch the **trains-redis** Docker container.

        sudo docker run -d --restart="always" --name="trains-redis" -v /opt/trains/data/redis:/data --network="host" redis:5.0

1. Launch the **trains-fileserver** Docker container.

        sudo docker run -d --restart="always" --name="trains-fileserver" --network="host" -v /opt/trains/logs:/var/log/trains -v /opt/trains/data/fileserver:/mnt/fileserver allegroai/trains:latest fileserver

1. Launch the **trains-apiserver** Docker container.

        sudo docker run -d --restart="always" --name="trains-apiserver" --network="host" -v /opt/trains/logs:/var/log/trains -v /opt/trains/config:/opt/trains/config allegroai/trains:latest apiserver

1. Launch the **trains-webserver** Docker container.

        sudo docker run -d --restart="always" --name="trains-webserver" -p 8080:80 allegroai/trains:latest webserver

1. Your server is now running on [http://localhost:8080](http://localhost:8080) and the following ports are available:

    * API server on port `8008`
    * Web server on port `8080`
    * File server on port `8081`

## Manually Upgrading TRAINS-server Containers <a name="upgrade"></a>

We are constantly updating, improving and adding to the **trains-server**.
New releases will include new pre-built Docker images.
When we release a new version and include a new pre-built Docker image for it, upgrade as follows:

1. Shut down and remove each of your Docker instances using the following commands:
    
    ```bash
    $ sudo docker stop <docker-name>
    $ sudo docker rm -v <docker-name>
    ```

    The Docker names are (see [Launching Docker Containers](#launch-docker)):

    * `trains-elastic`
    * `trains-mongo`
    * `trains-redis`
    * `trains-fileserver`
    * `trains-apiserver`
    * `trains-webserver`

2. We highly recommend backing up your data directory!. A simple way to do that is using `tar`:

    For example, if your data directory is `/opt/trains`, use the following command:

    ```bash
    $ sudo tar czvf ~/trains_backup.tgz /opt/trains/data
    ```
    This backups all data to an archive in your home directory.

    To restore this example backup, use the following command:
    ```bash
    $ sudo rm -R /opt/trains/data
    $ sudo tar -xzf ~/trains_backup.tgz -C /opt/trains/data
    ```

3. Pull the new **trains-server** docker image using the following command:

    ```bash
    $ sudo docker pull allegroai/trains:latest
    ```

    If you wish to pull a different version, replace `latest` with the required version number, for example:
    ```bash
    $ sudo docker pull allegroai/trains:0.11.0
     ```

4. Launch the newly released Docker image (see [Launching Docker Containers](#trains-server-manually-launching-docker-containers-)).


#### Common Docker Upgrade Errors 

* In case of a docker error: "... The container name "/trains-???" is already in use by ..."      
    Try removing deprecated images with:
    ```bash
    $ docker rm -f $(docker ps -a -q)
    ```
  