# trains-server FAQ


## **NOTE**: This page's information is deprecated. See the [ClearML documentation](https://clear.ml/docs/latest/docs/deploying_clearml/clearml_server) for up-to-date deployment instructions 

Launching **trains-server**

* How do I launch **trains-server** on:

    * [Stand alone Linux Ubuntu systems?](#ubuntu)
    
    * [macOS?](#mac-osx)
    
    * [Windows 10?](#docker_compose_win10)

* [How do I restart trains-server?](#restart)

Kubernetes

* [Can I deploy trains-server on Kubernetes clusters?](#kubernetes)

* [Can I create a Helm Chart for trains-server Kubernetes deployment?](#helm)

Configuration

* [How do I configure trains-server for sub-domains and load balancers?](#sub-domains)

* [Can I add web login authentication to trains-server?](#web-auth)

* [Can I modify the non-responsive experiment watchdog settings?](#watchdog)

Troubleshooting

* [How do I fix Docker upgrade errors?](#common-docker-upgrade-errors)

* [Why is web login authentication not working?](#port-conflict)

## Launching **trains-server**

### How do I launch trains-server on stand alone Linux Ubuntu systems? <a name="ubuntu"></a>

To launch **trains-server** on a stand alone Linux Ubuntu:

1. Install [docker for Ubuntu](https://docs.docker.com/install/linux/docker-ce/ubuntu/).

1. Install `docker-compose` using the following commands (for more detailed information, see the [Install Docker Compose](https://docs.docker.com/compose/install/) in the Docker documentation):

        sudo curl -L "https://github.com/docker/compose/releases/download/1.24.1/docker-compose-$(uname -s)-$(uname -m)" -o /usr/local/bin/docker-compose
        sudo chmod +x /usr/local/bin/docker-compose

1. Remove the previous installation of **trains-server**.

    **WARNING**: This clears all existing **Trains** databases.

        sudo rm -R /opt/trains/

1. Create local directories for the databases and storage.

        sudo mkdir -p /opt/trains/data/elastic
        sudo mkdir -p /opt/trains/data/mongo/db
        sudo mkdir -p /opt/trains/data/mongo/configdb
        sudo mkdir -p /opt/trains/logs
        sudo mkdir -p /opt/trains/config
        sudo mkdir -p /opt/trains/data/fileserver
        sudo chown -R 1000:1000 /opt/trains

1. Clone the [trains-server](https://github.com/allegroai/trains-server) repository and change directories to the new **trains-server** directory.

        git clone https://github.com/allegroai/trains-server.git
        cd trains-server

1. Run `docker-compose`

        /usr/local/bin/docker-compose -f docker-compose.yml up

    Your server is now running on [http://localhost:8080](http://localhost:8080)
    
### How do I launch trains-server on macOS? <a name="mac-osx"></a>

To launch **trains-server** on macOS:

1. Install [docker for macOS](https://docs.docker.com/docker-for-mac/install/).

1. Configure [Docker](https://www.elastic.co/guide/en/elasticsearch/reference/current/docker.html#docker-cli-run-prod-mode).

        screen ~/Library/Containers/com.docker.docker/Data/vms/0/tty
        sysctl -w vm.max_map_count=262144

1. Create local directories for the databases and storage.

        sudo mkdir -p /opt/trains/data/elastic
        sudo mkdir -p /opt/trains/data/mongo/db
        sudo mkdir -p /opt/trains/data/mongo/configdb
        sudo mkdir -p /opt/trains/data/redis
        sudo mkdir -p /opt/trains/logs
        sudo mkdir -p /opt/trains/config
        sudo mkdir -p /opt/trains/data/fileserver
        sudo chown -R $(whoami):staff /opt/trains

1. Open the Docker app, select **Preferences**, and then on the **File Sharing** tab, add `/opt/trains`.

1. Clone the [trains-server](https://github.com/allegroai/trains-server) repository and change directories to the new **trains-server** directory.

        git clone https://github.com/allegroai/trains-server.git
        cd trains-server

1. Run `docker-compose` with the docker compose file.

        docker-compose -f docker-compose.yml up

    Your server is now running on [http://localhost:8080](http://localhost:8080)

### How do I launch trains-server on Windows 10? <a name="docker_compose_win10"></a>

You can run **trains-server** on Windows 10 using Docker Desktop for Windows (see the Docker [System Requirements](https://docs.docker.com/docker-for-windows/install/#system-requirements)).

To launch **trains-server** on Windows 10:

1. Install the Docker Desktop for Windows application by either:

    * following the [Install Docker Desktop on Windows](https://docs.docker.com/docker-for-windows/install/) instructions.
    * running the Docker installation [wizard](https://hub.docker.com/?overlay=onboarding).

1. Increase the memory allocation in Docker Desktop to `4GB`.

    1. In your Windows notification area (system tray), right click the Docker icon.
    
    1. Click *Settings*, *Advanced*, and then set the memory to at least `4096`. 
    
    1. Click *Apply*.

1. Create local directories for data and logs. Open PowerShell and execute the following commands:

        cd c:
        mkdir c:\opt\trains\data
        mkdir c:\opt\trains\logs

1. Download the **trains-server** docker-compose YAML file [docker-compose-win10.yml](https://raw.githubusercontent.com/allegroai/trains-server/master/docker-compose-win10.yml) as `c:\opt\trains\docker-compose.yml`.

1. Run `docker-compose`. In PowerShell, execute the following commands:

        docker-compose -f up docker-compose-win10.yml

    Your server is now running on [http://localhost:8080](http://localhost:8080)

### How do I restart trains-server? <a name="restart"></a>

Restart *trains-server* by first stopping the Docker containers and then restarting them.

   ```bash
   docker-compose down
   docker-compose up -f docker-compose.yml
   ```
   
   **Note**: If you are using a different docker-compose YAML file, specify that file.

## Kubernetes

### Can I deploy trains-server on Kubernetes clusters? <a name="kubernetes"></a>

**trains-server** supports Kubernetes. See [trains-server-k8s](https://github.com/allegroai/trains-server-k8s)
which contains the YAML files describing the required services and detailed instructions for deploying
**trains-server** to a Kubernetes clusters.

### Can I create a Helm Chart for trains-server Kubernetes deployment? <a name="helm"></a>

**trains-server** supports creating a Helm chart for Kubernetes deployment. See [trains-server-helm](https://github.com/allegroai/trains-server-helm)
which you can use to create a Helm chart for **trains-server** and contains detailed instructions for deploying
**trains-server** to a Kubernetes clusters using Helm.

## Configuration

### How do I configure trains-server for sub-domains and load balancers? <a name="sub-domains"></a>

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

### Can I add web login authentication to trains-server? <a name="web-auth"></a>

By default, anyone can login to the **trains-server** Web-App.
You can configure the **trains-server** to allow only a specific set of users to access the system.

To add web login authentication to **trains-server**:

1. If you are not using the current **trains-server** version, then [upgrade](https://github.com/allegroai/trains-server#upgrade).

1. In `/opt/trains/config/apiserver.conf`, add the `auth` section and in it specify the users, for example:

    **Note**: A sample `apiserver.conf` configuration file is also available [here](https://github.com/allegroai/trains-server/blob/master/docs/apiserver.conf).

        auth {
            # Fixed users login credentials
            # No other user will be able to login
            fixed_users {
                enabled: true
                users: [
                    {
                        username: "jane"
                        password: "12345678"
                        name: "Jane Doe"
                    },
                    {
                        username: "john"
                        password: "12345678"
                        name: "John Doe"
                    },
                ]
            }
        }

1. Restart **trains-server** (see the [Restarting trains-server](#restart) FAQ).

### Can I modify the experiment watchdog settings? <a name="watchdog"></a>

The non-responsive experiment watchdog monitors experiments that were not updated for a specified period of time
and marks them as `aborted`. The watchdog is always active. 

You can modify the following settings for the watchdog:
 
* the time threshold (in seconds) of experiment inactivity (default value is 7200 seconds (2 hours))
* the time interval (in seconds) between watchdog cycles

To change the watchdog's settings:

1. In `/opt/trains/config`, add the `services.conf` file and in it specify the watchdog settings, for example:

    **Note**: A sample watchdog `services.conf` configuration file is also available [here](https://github.com/allegroai/trains-server/blob/master/docs/services.conf).

        tasks {
            non_responsive_tasks_watchdog {
                # In-progress tasks that haven't been updated for at least 'value' seconds will be stopped by the watchdog
                threshold_sec: 7200
        
                # Watchdog will sleep for this number of seconds after each cycle
                watch_interval_sec: 900
            }
        }

1. Restart **trains-server** (see the [Restarting trains-server](#restart) FAQ).

## Troubleshooting

### How do I fix Docker upgrade errors? <a name="common-docker-upgrade-errors"></a>

To resolve the Docker error "... The container name "/trains-???" is already in use by ...", try removing deprecated images:

    docker rm -f $(docker ps -a -q)

### Why is web login authentication not working?

A port conflict between the **trains-server** MongoDB and / or Elastic instances, and other
instances running on your system may prevent web login authentication
from working correctly. 

**trains-server** uses the following default ports which may be in conflict with other instances:

* MongoDB port `27017`
* Elastic port `9200`

You can check for port conflicts in the logs in `/opt/trains/log`.

If a port conflict occurs, change the MongoDB and / or Elastic ports in the `docker-compose.yml`,
and then run the Docker compose commands to restart the **trains-server** instance.

To change the MongoDB and / or Elastic ports for **trains-server**:

1. Edit the `docker-compose.yml` file.

1. In the `services/trainsserver/environment` section, add the following environment variable(s):

    * For MongoDB:
    
            MONGODB_SERVICE_PORT: <new-mongodb-port>
        
    * For Elastic:
            
            ELASTIC_SERVICE_PORT: <new-elasticsearch-port> 
        
    For example:
    
        MONGODB_SERVICE_PORT: 27018
        ELASTIC_SERVICE_PORT: 9201
            
1. For MongoDB, in the `services/mongo/ports` section, expose the new MongoDB port:

        <new-mongodb-port>:27017
        
    For example:
    
        20718:27017
        
1. For Elastic, in the `services/elasticsearch/ports` section, expose the new Elastic port:

        <new-elsticsearch-port>:9200
            
    For example:

        9201:9200
    
2. Restart **trains-server** (see the [Restarting trains-server](#restart) FAQ).