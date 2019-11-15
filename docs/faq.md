# TRAINS-server FAQ

* [Deploying trains-server on Kubernetes clusters](#kubernetes)

* [Creating a Helm Chart for trains-server Kubernetes deployment](#helm)

* [Running trains-server on Mac OS X](#mac-osx)

* [Running trains-server on Windows 10](#docker_compose_win10)

* [Installing trains-server on stand alone Linux Ubuntu  systems ](#ubuntu)

* [Resolving port conflicts preventing fixed users mode authentication and login](#port-conflict)

* [Configuring trains-server for sub-domains and load balancers](#sub-domains)


### Deploying trains-server on Kubernetes clusters <a name="kubernetes"></a>

**trains-server** supports Kubernetes. See [trains-server-k8s](https://github.com/allegroai/trains-server-k8s)
which contains the YAML files describing the required services and detailed instructions for deploying
**trains-server** to a Kubernetes clusters.

### Creating a Helm Chart for trains-server Kubernetes deployment <a name="helm"></a>

**trains-server** supports creating a Helm chart for Kubernetes deployment. See [trains-server-helm](https://github.com/allegroai/trains-server-helm)
which you can use to create a Helm chart for **trains-server** and contains detailed instructions for deploying
**trains-server** to a Kubernetes clusters using Helm.

### Running trains-server on Mac OS X <a name="mac-osx"></a>

To install and configure **trains-server** on Mac OS X, follow the steps below.

1. Install [docker for OS X](https://docs.docker.com/docker-for-mac/install/).

1. Configure [Docker](https://www.elastic.co/guide/en/elasticsearch/reference/current/docker.html#docker-cli-run-prod-mode).

        $ screen ~/Library/Containers/com.docker.docker/Data/vms/0/tty
        $ sysctl -w vm.max_map_count=262144

1. Create local directories for the databases and storage.

        $ sudo mkdir -p /opt/trains/data/elastic
        $ sudo mkdir -p /opt/trains/data/mongo/db
        $ sudo mkdir -p /opt/trains/data/mongo/configdb
        $ sudo mkdir -p /opt/trains/data/redis
        $ sudo mkdir -p /opt/trains/logs
        $ sudo mkdir -p /opt/trains/config
        $ sudo mkdir -p /opt/trains/data/fileserver
        $ sudo chown -R $(whoami):staff /opt/trains

1. Open the Docker app, select **Preferences**, and then on the **File Sharing** tab, add `/opt/trains`.

1. Clone the [trains-server](https://github.com/allegroai/trains-server) repository and change directories to the new **trains-server** directory.

        $ git clone https://github.com/allegroai/trains-server.git
        $ cd trains-server

1. Run `docker-compose` with the unified docker image.

        $ docker-compose -f docker-compose-unified.yml up

    Your server is now running on [http://localhost:8080](http://localhost:8080)

### Running trains-server on Windows 10 <a name="docker_compose_win10"></a>

You can run **trains-server** on Windows 10 using Docker Desktop for Windows (see the Docker [System Requirements](https://docs.docker.com/docker-for-windows/install/#system-requirements)).

To run **trains-server** on Windows 10, follow the steps below.

1. Install the Docker Desktop for Windows application by either:

    * following the [Install Docker Desktop on Windows](https://docs.docker.com/docker-for-windows/install/) instructions.
    * running the Docker installation [wizard](https://hub.docker.com/?overlay=onboarding).

1. Increase the memory allocation in Docker Desktop to `4GB`.

    1. In your Windows notification area (system tray), right click the Docker icon.
    
    1. Click *Settings*, *Advanced*, and then set the memory to at least `4096`. 
    
    1. Click *Apply*.

1. Create local directories for data and logs. Open PowerShell and execute the following commands:

        mkdir c:\opt\trains\logs
        mkdir c:\opt\trains\config
        mkdir c:\opt\trains\data
        mkdir c:\opt\trains\data\elastic
        mkdir c:\opt\trains\data\redis
        mkdir c:\opt\trains\data\fileserver

1. Save the **trains-server** docker-compose YAML file [docker-compose-win10.yml](https://raw.githubusercontent.com/allegroai/trains-server/master/docker-compose-win10.yml) as `c:\opt\trains\docker-compose.yml`.

1. Run `docker-compose`. In PowerShell, execute the following commands:

        cd c:\opt\trains\
        docker-compose up

    Your server is now running on [http://localhost:8080](http://localhost:8080)

### Installing trains-server on stand alone Linux Ubuntu systems <a name="ubuntu"></a>

To install **trains-server** on a stand alone Linux Ubuntu, follow the steps belows.

1. Install [docker for Ubuntu](https://docs.docker.com/install/linux/docker-ce/ubuntu/).

1. Install `docker-compose` using the following commands (for more detailed information, see the [Install Docker Compose](https://docs.docker.com/compose/install/) in the Docker documentation):

        sudo curl -L "https://github.com/docker/compose/releases/download/1.24.1/docker-compose-$(uname -s)-$(uname -m)" -o /usr/local/bin/docker-compose
        sudo chmod +x /usr/local/bin/docker-compose

1. Remove the previous installation of **trains-server**.

    **WARNING**: This clears all existing **TRAINS** databases.

        $ sudo rm -R /opt/trains/

1. Create local directories for the databases and storage.

        $ sudo mkdir -p /opt/trains/data/elastic
        $ sudo mkdir -p /opt/trains/data/mongo/db
        $ sudo mkdir -p /opt/trains/data/mongo/configdb
        $ sudo mkdir -p /opt/trains/logs
        $ sudo mkdir -p /opt/trains/config
        $ sudo mkdir -p /opt/trains/data/fileserver
        $ sudo chown -R 1000:1000 /opt/trains

1. Clone the [trains-server](https://github.com/allegroai/trains-server) repository and change directories to the new **trains-server** directory.

        $ git clone https://github.com/allegroai/trains-server.git
        $ cd trains-server

1. Run `docker-compose`

        $ /usr/local/bin/docker-compose -f docker-compose.yml up

    Your server is now running on [http://localhost:8080](http://localhost:8080)

### Resolving port conflicts preventing fixed users mode authentication and login <a name="port-conflict"></a>

A port conflict may occur between the **trains-server** MongoDB and Elastic instances and other
instances running on your system. **trains-server** uses the following default ports which may be in conflict with other instances:

* MongoDB port `27017`
* Elastic port `9200`

You can check for port conflicts in the logs in `/opt/trains/log`.

If a port conflict occurs, first change the port in your **trains-server** `/opt/trains/server/config/default/hosts.conf` file to the new port and then
run the `docker run` command with the `port` option specifying the new port to restart the **trains-server** instance.

For example, to resolve a MongoDB port conflict change port `27017` to `27018`:

1. Modify `/opt/trains/server/config/default/hosts.conf` changing the ports in the `mongo` section:

        elastic {
          events {
            hosts: [{host: "127.0.0.1", port: 9200}]
            args {
              timeout: 60
              dead_timeout: 10
              max_retries: 5
              retry_on_timeout: true
            }
            index_version: "1"
          }
        }

        mongo {
          backend {
            host: "mongodb://127.0.0.1:27018/backend"
          }
          auth {
            host: "mongodb://127.0.0.1:27018/auth"
          }
        }

2. Start the **trains-server** MongoDB container using `--port 27018`.

        sudo docker run -d --restart="always" --name="trains-mongo" -v /opt/trains/data/mongo/db:/data/db -v /opt/trains/data/mongo/configdb:/data/configdb --network="host" mongo:3.6.5 mongod --port 27018

In a future version of **trains-server**, to start the API server, environment variables will be available to use instead of modifying the configuration file (instead of Step 1 above).
The environment variables will be available to set different ports for both MongoDB and Elastic instances:

* `MONGODB_SERVICE_PORT` (e.g., `MONGODB_SERVICE_PORT=27018`)
* `ELASTIC_SERVICE_POST` (e.g., `ELASTIC_SERVICE_POST=9201`)

### Configuring trains-server for sub-domains and load balancers <a name="sub-domains"></a>

You can configure **trains-server** for sub-domains and a load balancer.

For example, if your domain is `trains.mydomain.com` and your sub-domains are `app` and `api`, then do the following:

1. If you are not using the current **trains-server** version, [upgrade](https://github.com/allegroai/trains-server#upgrade) **trains-server**.

1. Add the following to `/opt/trains/config/apiserver.conf`:

        auth {
          cookies {
            httponly: true
            secure: true
            domain: ".trains.mydomain.com"
            max_age: 99999999999
          }
        }

1. Use the following load balancer configuration:

    * Listeners:
        * Optional: HTTP listener, that redirects all traffic to HTTPS.
        * HTTPS listener for `app.` forwarded to `AppTargetGroup`
        * HTTPS listener for `api.` forwarded to `ApiTargetGroup`
        * HTTPS listener for `files.` forwarded to `FilesTargetGroup`
    * Target groups:
        * `AppTargetGroup`: HTTP based target group, port `8080`
        * `ApiTargetGroup`: HTTP based target group, port `8008`
        * `FilesTargetGroup`: HTTP based target group, port `8081`
    * Security and routing:
        * Load balancer: make sure the load balancers are able to receive traffic from the relevant IP addresses (Security groups and Subnets definitions).
        * Instances: make sure the load balancers are able to access the instances, using the relevant ports (Security groups definitions).

1. Run the Docker containers with our updated `docker run` commands (see [Launching Docker Containers](#https://github.com/allegroai/trains-server#launching-docker-containers)).

