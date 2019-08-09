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

You can quickly setup your **trains-server** using a pre-built Docker image (see [Installation](#installation)) or pre-installed machine image [AMI](#aws).

When new releases are available, you can upgrade your pre-built Docker image (see [Upgrade](#upgrade)) or simply restart the machine with the [auto-update AMI](docs/install_aws.md#autoupdate).

## System design


![Alt Text](https://github.com/allegroai/trains/blob/master/docs/system_diagram.png?raw=true)

## Install / Upgrade - AWS <a name="aws"></a>

Use one of our pre-installed Amazon Machine Images for easy deployment in AWS. 

For details and instructions, see [TRAINS-server: AWS pre-installed images](docs/install_aws.md).

## Installation - Linux, Mac OS X <a name="installation"></a>

Use our pre-built Docker image for easy deployment in Linux and Mac OS X. 
For Windows, we recommend installing our pre-built Docker image on a Linux virtual machine.

1. Setup Docker ([docker-compose Ubuntu](docs/faq.md#ubuntu), [docker-compose OS X](docs/faq.md#mac-osx), [Setup Docker Service Manually](docs/docker_setup.md#setup-docker))

    Make sure port 8080/8081/8008 are available for the `trains-server` services 
    
    Increase vm.max_map_count for `ElasticSearch` docker
    
    ```bash
    echo "vm.max_map_count=262144" > /tmp/99-trains.conf
    sudo mv /tmp/99-trains.conf /etc/sysctl.d/99-trains.conf
    sudo sysctl -w vm.max_map_count=262144
    
    sudo service docker restart
    ``` 

1. Create local directories for the databases and storage.
    
    ```bash
    sudo mkdir -p /opt/trains/data/elastic
    sudo mkdir -p /opt/trains/data/mongo/db
    sudo mkdir -p /opt/trains/data/mongo/configdb
    sudo mkdir -p /opt/trains/logs
    sudo mkdir -p /opt/trains/data/fileserver
    ``` 

    Linux
    ```bash
    $ sudo chown -R 1000:1000 /opt/trains
    ```
    Mac OS X
    ```bash            
    $ sudo chown -R $(whoami):staff /opt/trains
    ```
        
1. Clone the [trains-server](https://github.com/allegroai/trains-server) repository and change directories to the new **trains-server** directory.
    
    ```bash                    
    $ git clone https://github.com/allegroai/trains-server.git
    $ cd trains-server
    ```
        
1. Launch the Docker containers <a name="launch-docker"></a>

    * Automatically with docker-compose (details: [Linux/Ubuntu](docs/faq.md#ubuntu), [OS X](docs/faq.md#mac-osx))
    
    ```bash                    
    $ docker-compose up
    ```
            
    * Manually, see [TRAINS-server: Launching Docker Containers Manually](docs/docker_setup.md#launch) for instructions.
    
1. Your server is now running on [http://localhost:8080](http://localhost:8080) and the following ports are available:
    
    * Web server on port `8080`
    * API server on port `8008`
    * File server on port `8081`

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

See [Installing and Configuring TRAINS](https://github.com/allegroai/trains#configuration) for more details.

## What next?

Now that the **trains-server** is installed, and TRAINS is configured to use it, 
you can [use](https://github.com/allegroai/trains#using-trains) TRAINS in your experiments and view them in the web server, 
for example http://localhost:8080

## Upgrading <a name="upgrade"></a>

We are constantly updating, improving and adding to the **trains-server**.
New releases will include new pre-built Docker images.
When we release a new version and include a new pre-built Docker image for it, upgrade as follows:

1. Shut down and remove each of your Docker instances using the following commands:

    * Using Docker-Compose
    
        ```bash
        $ docker-compose down
        ```

    * Manual Docker launching 
    
        ```bash
        $ sudo docker stop <docker-name>
        $ sudo docker rm -v <docker-name>
        ```
    
        The Docker names are (see [Launching Docker Containers](#launch-docker)):
    
        * `trains-elastic`
        * `trains-mongo`
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
    $ sudo docker pull allegroai/trains:0.10.1
     ```
        
4. Launch the newly released Docker image (see [Launching Docker Containers](#launch-docker)).

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
