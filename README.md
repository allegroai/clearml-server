# TRAINS Server

##  Auto-Magical Experiment Manager & Version Control for AI

[![GitHub license](https://img.shields.io/badge/license-SSPL-green.svg)](https://img.shields.io/badge/license-SSPL-green.svg)
[![Python versions](https://img.shields.io/badge/python-3.6%20%7C%203.7-blue.svg)](https://img.shields.io/badge/python-3.6%20%7C%203.7-blue.svg)
[![GitHub version](https://img.shields.io/github/release-pre/allegroai/trains-server.svg)](https://img.shields.io/github/release-pre/allegroai/trains-server.svg)
[![PyPI status](https://img.shields.io/badge/status-beta-yellow.svg)](https://img.shields.io/badge/status-beta-yellow.svg)

## Introduction

The **trains-server** is the backend service infrastructure for [TRAINS](https://github.com/allegroai/trains).
It allows multiple users to collaborate and manage their experiments.
By default, TRAINS is set up to work with the TRAINS demo server, which is open to anyone and resets periodically.
In order to host your own server, you will need to install **trains-server** and point TRAINS to it.

**trains-server** contains the following components:

* The TRAINS Web-App, a single-page UI for experiment management and browsing
* RESTful API for:
    * Documenting and logging experiment information, statistics and results
    * Querying experiments history, logs and results
* Locally-hosted file server for storing images and models making them easily accessible using the Web-App

You can quickly setup your **trains-server** using:
 - [Docker Installation](#installation)
 - Pre-built Amazon [AWS image](#aws)
 - [Kubernetes Helm](https://github.com/allegroai/trains-server-helm#trains-server-for-kubernetes-clusters-using-helm)
 or manual [Kubernetes installation](https://github.com/allegroai/trains-server-k8s#trains-server-for-kubernetes-clusters)


## System design


![Alt Text](https://github.com/allegroai/trains/blob/master/docs/system_diagram.png?raw=true)

**trains-server** has two supported configurations:
- Single IP (domain) with the following open ports
    - Web application on port 8080
    - API service on port 8008
    - File storage service on port 8081

- Sub-Domain configuration with default http/s ports (80 or 443)
    - Web application on sub-domain: app.\*.\*
    - API service on sub-domain: api.\*.\*
    - File storage service on sub-domain: files.\*.\*

## Install / Upgrade - AWS <a name="aws"></a>

Use one of our pre-installed Amazon Machine Images for easy deployment in AWS.

For details and instructions, see [TRAINS-server: AWS pre-installed images](docs/install_aws.md).

## Docker Installation - Linux, macOS <a name="installation"></a>

Use our pre-built Docker image for easy deployment in Linux and macOS.
For Windows, we recommend installing our pre-built Docker image on a Linux virtual machine.
Latest docker images can be found [here](https://hub.docker.com/r/allegroai/trains).

1. Setup Docker (docker-compose installation details: [Ubuntu](docs/faq.md#ubuntu) / [macOS](docs/faq.md#mac-osx))

    <details>
    <summary>Make sure ports 8080/8081/8008 are available for the TRAINS-server services:</summary>
   
    For example, to see if port `8080` is in use: 

    ```bash
    $ sudo lsof -Pn -i4 | grep :8080 | grep LISTEN
    ```
    
    </details>
    
    Increase vm.max_map_count for `ElasticSearch` docker

    - Linux
        ```bash
        $ echo "vm.max_map_count=262144" > /tmp/99-trains.conf
        $ sudo mv /tmp/99-trains.conf /etc/sysctl.d/99-trains.conf
        $ sudo sysctl -w vm.max_map_count=262144
        $ sudo service docker restart
        ```
      
    - macOS
        ```bash
        $ screen ~/Library/Containers/com.docker.docker/Data/vms/0/tty
        $ sysctl -w vm.max_map_count=262144
        ```    

1. Create local directories for the databases and storage.

    ```bash
    $ sudo mkdir -p /opt/trains/data/elastic
    $ sudo mkdir -p /opt/trains/data/mongo/db
    $ sudo mkdir -p /opt/trains/data/mongo/configdb
    $ sudo mkdir -p /opt/trains/data/redis
    $ sudo mkdir -p /opt/trains/logs
    $ sudo mkdir -p /opt/trains/data/fileserver
    $ sudo mkdir -p /opt/trains/config
    ```

    Set folder permissions
      
    - Linux
      ```bash
      $ sudo chown -R 1000:1000 /opt/trains
      ```
    - macOS
      ```bash
      $ sudo chown -R $(whoami):staff /opt/trains
      ```

1. Download the `docker-compose.yml` file, either download [manually](https://raw.githubusercontent.com/allegroai/trains-server/master/docker-compose.yml) or execute:

    ```bash
    $ curl https://raw.githubusercontent.com/allegroai/trains-server/master/docker-compose.yml -o docker-compose.yml 
    ```

1. Launch the Docker containers <a name="launch-docker"></a>

    ```bash
    $ docker-compose up -f docker-compose.yml
    ```

1. Your server is now running on [http://localhost:8080](http://localhost:8080) and the following ports are available:

    * Web server on port `8080`
    * API server on port `8008`
    * File server on port `8081`

**\* If something went wrong along the way, check our FAQ: [Docker Setup](docs/docker_setup.md#setup-docker), [Ubuntu Support](docs/faq.md#ubuntu), [macOS Support](docs/faq.md#mac-osx)**

## Optional Configuration

The **trains-server** default configuration can be easily overridden using external configuration files. By default, the server will look for these files in `/opt/trains/config`.

In order to apply the new configuration, you must restart the server (see [Restarting trains-server](#restart-server)).

### Adding Web Login Authentication

By default anyone can login to the **trains-server** Web-App.
You can configure the **trains-server** to allow only a specific set of users to access the system.

Enable this feature by placing `apiserver.conf` file under `/opt/trains/config`.


Sample fixed user configuration file `/opt/trains/config/apiserver.conf`:

    auth {
        # Fixed users login credetials
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

To apply the `apiserver.conf` changes, you must restart the *trains-apiserver* (docker) (see [Restarting trains-server](#restart-server)).

### Configuring the Non-Responsive Experiments Watchdog

The non-responsive experiment watchdog, monitors experiments that were not updated for a given period of time,
and marks them as `aborted`. The watchdog is always active with a default of 7200 seconds (2 hours) of inactivity threshold.

To change the watchdog's timeouts, place a `services.conf` file under `/opt/trains/config`.

Sample watchdog configuration file `/opt/trains/config/services.conf`:

    tasks {
        non_responsive_tasks_watchdog {
            # In-progress tasks that haven't been updated for at least 'value' seconds will be stopped by the watchdog
            threshold_sec: 7200

            # Watchdog will sleep for this number of seconds after each cycle
            watch_interval_sec: 900
        }
    }

To apply the `services.conf` changes, you must restart the *trains-apiserver* (docker) (see [Restarting trains-server](#restart-server)).

### Restarting trains-server <a name="restart-server"></a>

To restart the **trains-server**, you must first stop and remove the containers, and then restart.

1. Restarting docker-compose containers.

        $ docker-compose down
        $ docker-compose up

1. Manually restarting dockers [instructions](docs/docker_setup.md#launch).

## Configuring **TRAINS** client

Once you have installed the **trains-server**, make sure to configure **TRAINS** [client](https://github.com/allegroai/trains)
to use your locally installed server (and not the demo server).

- Run the `trains-init` command for an interactive setup

- Or manually edit `~/trains.conf` file, making sure the `api_server` value is configured correctly, for example:

        api {
            # API server on port 8008
            api_server: "http://localhost:8008"

            # web_server on port 8080
            web_server: "http://localhost:8080"

            # file server on port 8081
            files_server: "http://localhost:8081"
        }

* Notice that if you setup **trains-server** in a sub-domain configuration, there is no need to specify a port number,
it will be inferred from the http/s scheme.

See [Installing and Configuring TRAINS](https://github.com/allegroai/trains#configuration) for more details.

## What next?

Now that the **trains-server** is installed, and TRAINS is configured to use it,
you can [use](https://github.com/allegroai/trains#using-trains) TRAINS in your experiments and view them in the web server,
for example http://localhost:8080

## Upgrading <a name="upgrade"></a>

We are constantly updating, improving and adding to the **trains-server**.
New releases will include new pre-built Docker images.
When we release a new version and include a new pre-built Docker image for it, upgrade as follows:

* Shut down the docker containers
```bash
$ docker-compose down
```

* We highly recommend backing up your data directory before upgrading.

    Assuming your data directory is `/opt/trains`, to archive all data into `~/trains_backup.tgz` execute:
    
    ```bash
    $ sudo tar czvf ~/trains_backup.tgz /opt/trains/data
    ```    
    
    <details>
    <summary>Restore instructions:</summary>
    
    To restore this example backup, execute:
    ```bash
    $ sudo rm -R /opt/trains/data
    $ sudo tar -xzf ~/trains_backup.tgz -C /opt/trains/data
    ```
    </details>

* Download the latest `docker-compose.yml` file, either [manually](https://raw.githubusercontent.com/allegroai/trains-server/master/docker-compose.yml) or execute:

    ```bash
    $ curl https://raw.githubusercontent.com/allegroai/trains-server/master/docker-compose.yml -o docker-compose.yml 
    ```

* Spin up the docker containers, it will automatically pull the latest trains-server build    
```bash
$ docker-compose up -f docker-compose.yml
```

**\* If something went wrong along the way, check our FAQ: [Docker Upgrade](docs/docker_setup.md#common-docker-upgrade-errors)**


## Community & Support

If you have any questions, look to the TRAINS-server [FAQ](https://github.com/allegroai/trains-server/blob/master/docs/faq.md), or
tag your questions on [stackoverflow](https://stackoverflow.com/questions/tagged/trains) with '**trains**' tag.

For feature requests or bug reports, please use [GitHub issues](https://github.com/allegroai/trains-server/issues).

Additionally, you can always find us at *trains@allegro.ai*

## License

[Server Side Public License v1.0](https://github.com/mongodb/mongo/blob/master/LICENSE-Community.txt)

**trains-server** relies on both [MongoDB](https://github.com/mongodb/mongo) and [ElasticSearch](https://github.com/elastic/elasticsearch).
With the recent changes in both MongoDB's and ElasticSearch's OSS license, we feel it is our responsibility as a
member of the community to support the projects we love and cherish.
We believe the cause for the license change in both cases is more than just,
and chose [SSPL](https://www.mongodb.com/licensing/server-side-public-license) because it is the more general and flexible of the two licenses.

This is our way to say - we support you guys!
