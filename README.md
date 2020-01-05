# Trains Server

##  Auto-Magical Experiment Manager & Version Control for AI

[![GitHub license](https://img.shields.io/badge/license-SSPL-green.svg)](https://img.shields.io/badge/license-SSPL-green.svg)
[![Python versions](https://img.shields.io/badge/python-3.6%20%7C%203.7-blue.svg)](https://img.shields.io/badge/python-3.6%20%7C%203.7-blue.svg)
[![GitHub version](https://img.shields.io/github/release-pre/allegroai/trains-server.svg)](https://img.shields.io/github/release-pre/allegroai/trains-server.svg)
[![PyPI status](https://img.shields.io/badge/status-beta-yellow.svg)](https://img.shields.io/badge/status-beta-yellow.svg)

## Introduction

The **trains-server** is the backend service infrastructure for [Trains](https://github.com/allegroai/trains).
It allows multiple users to collaborate and manage their experiments.
By default, **Trains** is set up to work with the **Trains** demo server, which is open to anyone and resets periodically.
In order to host your own server, you will need to launch **trains-server** and point **Trains** to it.

**trains-server** contains the following components:

* The **Trains** Web-App, a single-page UI for experiment management and browsing
* RESTful API for:
    * Documenting and logging experiment information, statistics and results
    * Querying experiments history, logs and results
* Locally-hosted file server for storing images and models making them easily accessible using the Web-App

You can quickly [deploy](#launching-trains-server)  your **trains-server** using Docker, AWS EC2 AMI, or Kubernetes. 

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
    
## Launching trains-server

### Prerequisites

The ports 8080/8081/8008 must be available for the **trains-server** services.
   
For example, to see if port `8080` is in use:

* Linux or macOS: 
   
        sudo lsof -Pn -i4 | grep :8080 | grep LISTEN

* Windows:

        netstat -an |find /i "8080"
   
### Launching   
    
Launch **trains-server** in any of the following formats:

- Pre-built [AWS EC2 AMI](https://github.com/allegroai/trains-server/blob/master/docs/install_aws.md)
- Pre-built Docker Image
    - [Linux](https://github.com/allegroai/trains-server/blob/master/docs/install_linux_mac.md)
    - [macOS](https://github.com/allegroai/trains-server/blob/master/docs/install_linux_mac.md)
    - [Windows 10](https://github.com/allegroai/trains-server/blob/master/docs/install_win.md)
- Kubernetes    
    - [Kubernetes Helm](https://github.com/allegroai/trains-server-helm#prerequisites)
    - Manual [Kubernetes installation](https://github.com/allegroai/trains-server-k8s#prerequisites)

## Connecting Trains to your trains-server

By default, the **Trains** client is set up to work with the [**Trains** demo server](https://demoapp.trains.allegro.ai/).  
To have the **Trains** client use your **trains-server** instead:
- Run the `trains-init` command for an interactive setup.
- Or manually edit `~/trains.conf` file, making sure the server settings (`api_server`, `web_server`, `file_server`) are configured correctly, for example:

        api {
            # API server on port 8008
            api_server: "http://localhost:8008"

            # web_server on port 8080
            web_server: "http://localhost:8080"

            # file server on port 8081
            files_server: "http://localhost:8081"
        }

**Note**: If you have set up **trains-server** in a sub-domain configuration, then there is no need to specify a port number,
it will be inferred from the http/s scheme.

After launching the **trains-server** and configuring the **Trains** client to use the **trains-server**,
you can [use](https://github.com/allegroai/trains#using-trains) **Trains** in your experiments and view them in your **trains-server** web server,
for example http://localhost:8080.  
For more information about the Trains client, see [**Trains**](https://github.com/allegroai/trains).

## Advanced Functionality

**trains-server** provides a few additional useful features, which can be manually enabled:
 
* [Web login authentication](https://github.com/allegroai/trains-server/blob/master/docs/faq.md#web-auth)
* [Non-responsive experiments watchdog](https://github.com/allegroai/trains-server/blob/master/docs/faq.md#watchdog-the-non-responsive-task-watchdog-settings)  

## Restarting trains-server

To restart the **trains-server**, you must first stop the containers, and then restart them.

   ```bash
   docker-compose down
   docker-compose -f docker-compose.yml up
   ```

## Upgrading <a name="upgrade"></a>

**trains-server** releases are also reflected in the [docker compose configuration file](https://github.com/allegroai/trains-server/blob/master/docker-compose.yml).  
We strongly encourage you to keep your **trains-server** up to date, by keeping up with the current release.

**Note**: The following upgrade instructions use the Linux OS as an example.

To upgrade your existing **trains-server** deployment:

1. Shut down the docker containers
   ```bash
   docker-compose down
   ```

1. We highly recommend backing up your data directory before upgrading.

   Assuming your data directory is `/opt/trains`, to archive all data into `~/trains_backup.tgz` execute:

   ```bash
   sudo tar czvf ~/trains_backup.tgz /opt/trains/data
   ```    

   <details>
   <summary>Restore instructions:</summary>

   To restore this example backup, execute:
   ```bash
   sudo rm -R /opt/trains/data
   sudo tar -xzf ~/trains_backup.tgz -C /opt/trains/data
   ```
   </details>

1. Download the latest `docker-compose.yml` file.

   ```bash
   curl https://raw.githubusercontent.com/allegroai/trains-server/master/docker-compose.yml -o docker-compose.yml 
   ```

1. Spin up the docker containers, it will automatically pull the latest **trains-server** build    
   ```bash
   docker-compose -f docker-compose.yml pull
   docker-compose -f docker-compose.yml up
   ```

**\* If something went wrong along the way, check our FAQ: [Common Docker Upgrade Errors](https://github.com/allegroai/trains-server/blob/master/docs/faq.md#common-docker-upgrade-errors).**


## Community & Support

If you have any questions, look to the Trains server [FAQ](https://github.com/allegroai/trains-server/blob/master/docs/faq.md), or
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
