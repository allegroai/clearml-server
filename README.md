<div align="center">

<img src="docs/clearml_server_logo.png" width="250px">

**ClearML - Auto-Magical Suite of tools to streamline your ML workflow 
</br>Experiment Manager, ML-Ops and Data-Management**

[![GitHub license](https://img.shields.io/badge/license-SSPL-green.svg)](https://img.shields.io/badge/license-SSPL-green.svg)
[![Python versions](https://img.shields.io/badge/python-3.6%20%7C%203.7-blue.svg)](https://img.shields.io/badge/python-3.6%20%7C%203.7-blue.svg)
[![GitHub version](https://img.shields.io/github/release-pre/allegroai/trains-server.svg)](https://img.shields.io/github/release-pre/allegroai/trains-server.svg)
[![Artifact Hub](https://img.shields.io/endpoint?url=https://artifacthub.io/badge/repository/allegroai)](https://artifacthub.io/packages/search?repo=allegroai)

</div>

---
<div align="center">

**Note regarding Apache Log4j2 Remote Code Execution (RCE) Vulnerability - CVE-2021-44228 - ESA-2021-31**

</div>

According to [ElasticSearch's latest report](https://discuss.elastic.co/t/apache-log4j2-remote-code-execution-rce-vulnerability-cve-2021-44228-esa-2021-31/291476), 
supported versions of Elasticsearch (6.8.9+, 7.8+) used with recent versions of the JDK (JDK9+) **are not susceptible to either remote code execution or information leakage**
due to Elasticsearch’s usage of the Java Security Manager.

**As the latest version of ClearML Server uses Elasticsearch 7.10+ with JDK15, it is not affected by these vulnerabilities.**

As a precaution, we've upgraded the ES version to 7.16.2 and added the mitigation recommended by ElasticSearch to our latest [docker-compose.yml](https://github.com/allegroai/clearml-server/blob/cfccbe05c158b75e520581f86e9668291da5c70a/docker/docker-compose.yml#L42) file.

While previous Elasticsearch versions (5.6.11+, 6.4.0+ and 7.0.0+) used by older ClearML Server versions are only susceptible to the information leakage vulnerability
(which in any case **does not permit access to data within the Elasticsearch cluster**), 
we still recommend upgrading to the latest version of ClearML Server. Alternatively, you can apply the mitigation as implemented in our latest 
[docker-compose.yml](https://github.com/allegroai/clearml-server/blob/cfccbe05c158b75e520581f86e9668291da5c70a/docker/docker-compose.yml#L42) file.    

**Update 15 December**: A further vulnerability (CVE-2021-45046) was disclosed on December 14th.
ElasticSearch's guidance for Elasticsearch remains unchanged by this new vulnerability, thus **not affecting ClearML Server**.

**Update 22 December**: To keep with ElasticSearch's recommendations, we've upgraded the ES version to the newly released 7.16.2

---

## ClearML Server
#### *Formerly known as Trains Server*

The **ClearML Server** is the backend service infrastructure for [ClearML](https://github.com/allegroai/clearml).
It allows multiple users to collaborate and manage their experiments.
**ClearML** offers a [free hosted service](https://app.clear.ml/), which is maintained by **ClearML** and open to anyone.
In order to host your own server, you will need to launch the **ClearML Server** and point **ClearML** to it.

The **ClearML Server** contains the following components:

* The **ClearML** Web-App, a single-page UI for experiment management and browsing
* RESTful API for:
    * Documenting and logging experiment information, statistics and results
    * Querying experiments history, logs and results
* Locally-hosted file server for storing images and models making them easily accessible using the Web-App

You can quickly [deploy](#launching-the-clearml-server)  your **ClearML Server** using Docker, AWS EC2 AMI, or Kubernetes. 

## System design


![Alt Text](docs/ClearML_Server_Diagram.png)

The **ClearML Server** has two supported configurations:
- Single IP (domain) with the following open ports
    - Web application on port 8080
    - API service on port 8008
    - File storage service on port 8081

- Sub-Domain configuration with default http/s ports (80 or 443)
    - Web application on sub-domain: app.\*.\*
    - API service on sub-domain: api.\*.\*
    - File storage service on sub-domain: files.\*.\*
    
## Launching The ClearML Server

### Prerequisites

The ports 8080/8081/8008 must be available for the **ClearML Server** services.
   
For example, to see if port `8080` is in use:

* Linux or macOS: 
   
        sudo lsof -Pn -i4 | grep :8080 | grep LISTEN

* Windows:

        netstat -an |find /i "8080"
   
### Launching   
    
Launch The **ClearML Server** in any of the following formats:

- Pre-built [AWS EC2 AMI](https://clear.ml/docs/latest/docs/deploying_clearml/clearml_server_aws_ec2_ami)
- Pre-built [GCP Custom Image](https://clear.ml/docs/latest/docs/deploying_clearml/clearml_server_gcp)
- Pre-built Docker Image
    - [Linux](https://clear.ml/docs/latest/docs/deploying_clearml/clearml_server_linux_mac)
    - [macOS](https://clear.ml/docs/latest/docs/deploying_clearml/clearml_server_linux_mac)
    - [Windows 10](https://clear.ml/docs/latest/docs/deploying_clearml/clearml_server_win)
- Kubernetes    
    - [Kubernetes Helm](https://clear.ml/docs/latest/docs/deploying_clearml/clearml_server_kubernetes_helm)
    - Manual [Kubernetes installation](https://clear.ml/docs/latest/docs/deploying_clearml/clearml_server_kubernetes)

## Connecting ClearML to your ClearML Server

In order to set up the **ClearML** client to work with your **ClearML Server**:
- Run the `clearml-init` command for an interactive setup.
- Or manually edit `~/clearml.conf` file, making sure the server settings (`api_server`, `web_server`, `file_server`) are configured correctly, for example:

        api {
            # API server on port 8008
            api_server: "http://localhost:8008"

            # web_server on port 8080
            web_server: "http://localhost:8080"

            # file server on port 8081
            files_server: "http://localhost:8081"
        }

**Note**: If you have set up your **ClearML Server** in a sub-domain configuration, then there is no need to specify a port number,
it will be inferred from the http/s scheme.

After launching the **ClearML Server** and configuring the **ClearML** client to use the **ClearML Server**,
you can [use](https://github.com/allegroai/clearml) **ClearML** in your experiments and view them in your **ClearML Server** web server,
for example http://localhost:8080.  
For more information about the ClearML client, see [**ClearML**](https://github.com/allegroai/clearml).

## ClearML-Agent Services  <a name="services"></a> 

As of version 0.15 of **ClearML Server**, dockerized deployment includes a **ClearML-Agent Services** container running as 
part of the docker container collection.

ClearML-Agent Services is an extension of ClearML-Agent that provides the ability to launch long-lasting jobs 
that previously had to be executed on local / dedicated machines. It allows a single agent to 
launch multiple dockers (Tasks) for different use cases. To name a few use cases, auto-scaler service (spinning instances 
when the need arises and the budget allows), Controllers (Implementing pipelines and more sophisticated DevOps logic),
Optimizer (such as Hyper-parameter Optimization or sweeping), and Application (such as interactive Bokeh apps for 
increased data transparency)

ClearML-Agent Services container will spin **any** task enqueued into the dedicated `services` queue. 
Every task launched by ClearML-Agent Services  will be registered as a new node in the system, 
providing tracking and transparency capabilities.  
You can also run the ClearML-Agent Services manually, see details in [ClearML-agent services mode](https://github.com/allegroai/clearml-agent#clearml-agent-services-mode-)

**Note**: It is the user's responsibility to make sure the proper tasks are pushed into the `services` queue. 
Do not enqueue training / inference tasks into the `services` queue, as it will put unnecessary load on the server.

## Advanced Functionality

The **ClearML Server** provides a few additional useful features, which can be manually enabled:
 
* [Web login authentication](https://clear.ml/docs/latest/docs/deploying_clearml/clearml_server_config#web-login-authentication)
* [Non-responsive experiments watchdog](https://clear.ml/docs/latest/docs/deploying_clearml/clearml_server_config#non-responsive-task-watchdog)  

## Restarting ClearML Server

To restart the **ClearML Server**, you must first stop the containers, and then restart them.

   ```bash
   docker-compose down
   docker-compose -f docker-compose.yml up
   ```

## Upgrading <a name="upgrade"></a>

**ClearML Server** releases are also reflected in the [docker compose configuration file](https://github.com/allegroai/trains-server/blob/master/docker/docker-compose.yml).  
We strongly encourage you to keep your **ClearML Server** up to date, by keeping up with the current release.

**Note**: The following upgrade instructions use the Linux OS as an example.

To upgrade your existing **ClearML Server** deployment:

1. Shut down the docker containers
   ```bash
   docker-compose down
   ```

1. We highly recommend backing up your data directory before upgrading.

   Assuming your data directory is `/opt/clearml`, to archive all data into `~/clearml_backup.tgz` execute:

   ```bash
   sudo tar czvf ~/clearml_backup.tgz /opt/clearml/data
   ```    

   <details>
   <summary>Restore instructions:</summary>

   To restore this example backup, execute:
   ```bash
   sudo rm -R /opt/clearml/data
   sudo tar -xzf ~/clearml_backup.tgz -C /opt/clearml/data
   ```
   </details>

1. Download the latest `docker-compose.yml` file.

   ```bash
   curl https://raw.githubusercontent.com/allegroai/trains-server/master/docker/docker-compose.yml -o docker-compose.yml 
   ```

1. Configure the ClearML-Agent Services (not supported on Windows installation). 
   If `CLEARML_HOST_IP` is not provided, ClearML-Agent Services will use the external 
   public address of the **ClearML Server**. If `CLEARML_AGENT_GIT_USER` / `CLEARML_AGENT_GIT_PASS` are not provided, 
   the ClearML-Agent Services will not be able to access any private repositories for running service tasks.
   
   ```bash
   export CLEARML_HOST_IP=server_host_ip_here
   export CLEARML_AGENT_GIT_USER=git_username_here
   export CLEARML_AGENT_GIT_PASS=git_password_here
   ```

1. Spin up the docker containers, it will automatically pull the latest **ClearML Server** build    
   ```bash
   docker-compose -f docker-compose.yml pull
   docker-compose -f docker-compose.yml up
   ```

**\* If something went wrong along the way, check our FAQ: [Common Docker Upgrade Errors](https://clear.ml/docs/latest/docs/faq/).**


## Community & Support

If you have any questions, look to the ClearML [FAQ](https://clear.ml/docs/latest/docs/faq), or
tag your questions on [stackoverflow](https://stackoverflow.com/questions/tagged/clearml) with '**clearml**' tag.

For feature requests or bug reports, please use [GitHub issues](https://github.com/allegroai/clearml-server/issues).

Additionally, you can always find us at *clearml@allegro.ai*

## License

[Server Side Public License v1.0](https://github.com/mongodb/mongo/blob/master/LICENSE-Community.txt)

The **ClearML Server** relies on both [MongoDB](https://github.com/mongodb/mongo) and [ElasticSearch](https://github.com/elastic/elasticsearch).
With the recent changes in both MongoDB's and ElasticSearch's OSS license, we feel it is our responsibility as a
member of the community to support the projects we love and cherish.
We believe the cause for the license change in both cases is more than just,
and chose [SSPL](https://www.mongodb.com/licensing/server-side-public-license) because it is the more general and flexible of the two licenses.

This is our way to say - we support you guys!
