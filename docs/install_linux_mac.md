# Launching the **trains-server** Docker in Linux or macOS

## **NOTE**: These instructions are deprecated. See the [ClearML documentation](https://clear.ml/docs/latest/docs/deploying_clearml/clearml_server) for up-to-date deployment instructions 

For Linux or macOS, use our pre-built Docker image for easy deployment. The latest Docker images can be found [here](https://hub.docker.com/r/allegroai/trains).

For Linux users:

* You must be logged in as a user with sudo privileges.
* Use `bash` for all command-line instructions in this installation.

To launch **trains-server** on Linux or macOS:

1. Install Docker.

    * Linux - see [Docker for Ubuntu](https://docs.docker.com/install/linux/docker-ce/ubuntu/).
    * macOS - see [Docker for macOS](https://docs.docker.com/docker-for-mac/install/).

1. Verify the Docker CE installation. Execute the command:

        docker run hello-world

    The expected is output is:

        Hello from Docker!
        This message shows that your installation appears to be working correctly.
        To generate this message, Docker took the following steps:

        1. The Docker client contacted the Docker daemon.
        2. The Docker daemon pulled the "hello-world" image from the Docker Hub. (amd64)
        3. The Docker daemon created a new container from that image which runs the executable that produces the output you are currently reading.
        4. The Docker daemon streamed that output to the Docker client, which sent it to your terminal.

1. For Linux only, install `docker-compose`. Execute the following commands (for more information, see [Install Docker Compose](https://docs.docker.com/compose/install/) in the Docker documentation):

        sudo curl -L "https://github.com/docker/compose/releases/download/1.24.1/docker-compose-$(uname -s)-$(uname -m)" -o /usr/local/bin/docker-compose
        sudo chmod +x /usr/local/bin/docker-compose

1. Increase `vm.max_map_count` for ElasticSearch docker.

    Linux:

        echo "vm.max_map_count=262144" > /tmp/99-trains.conf
        sudo mv /tmp/99-trains.conf /etc/sysctl.d/99-trains.conf
        sudo sysctl -w vm.max_map_count=262144
        sudo service docker restart

    macOS:

        screen ~/Library/Containers/com.docker.docker/Data/vms/0/tty
        sysctl -w vm.max_map_count=262144


1. Remove any previous installation of **trains-server**.

    **WARNING**: This clears all existing **Trains** databases.

        sudo rm -R /opt/trains/

1. Create local directories for the databases and storage.

        sudo mkdir -p /opt/trains/data/elastic
        sudo mkdir -p /opt/trains/data/mongo/db
        sudo mkdir -p /opt/trains/data/mongo/configdb
        sudo mkdir -p /opt/trains/data/redis
        sudo mkdir -p /opt/trains/logs
        sudo mkdir -p /opt/trains/config
        sudo mkdir -p /opt/trains/data/fileserver

1. For macOS only, open the Docker app, select **Preferences**, and then on the **File Sharing** tab, add `/opt/trains`.

1. Grant access to the Dockers.

    Linux:

        sudo chown -R 1000:1000 /opt/trains

    macOS:

        sudo chown -R $(whoami):staff /opt/trains

1. Download the **trains-server** docker-compose YAML file.

        cd /opt/trains
        curl https://raw.githubusercontent.com/allegroai/trains-server/master/docker-compose.yml -o docker-compose.yml

1. Run `docker-compose` with the downloaded configuration file.

        docker-compose -f docker-compose.yml up

    Your server is now running on [http://localhost:8080](http://localhost:8080) and the following ports are available:

    * Web server on port `8080`
    * API server on port `8008`
    * File server on port `8081`

## Next Step

Configure the [Trains client for trains-server](https://github.com/allegroai/trains/blob/master/README.md#configuration).
