# Launching the **trains-server** Docker in Windows 10

## **NOTE**: These instructions are deprecated. See the [ClearML documentation](https://clear.ml/docs/latest/docs/deploying_clearml/clearml_server) for up-to-date deployment instructions 

For Windows, we recommend launching our pre-built Docker image on a Linux virtual machine. 
However, you can launch **trains-server** on Windows 10 using Docker Desktop for Windows (see the Docker [System Requirements](https://docs.docker.com/docker-for-windows/install/#system-requirements)).

To launch **trains-server** on Windows 10:

1. Install the Docker Desktop for Windows application by either:

    * Following the [Install Docker Desktop on Windows](https://docs.docker.com/docker-for-windows/install/) instructions.
    * Running the Docker installation [wizard](https://hub.docker.com/?overlay=onboarding).

1. Increase the memory allocation in Docker Desktop to `4GB`.

    1. In your Windows notification area (system tray), right click the Docker icon.
    
    1. Click *Settings*, *Advanced*, and then set the memory to at least `4096`. 
    
    1. Click *Apply*.
    
1. Remove any previous installation of **trains-server**.

    **WARNING**: This clears all existing **Trains** databases.

        rmdir c:\opt\trains /s

1. Create local directories for data and logs. Open PowerShell and execute the following commands:

        cd c:
        mkdir c:\opt\trains\data
        mkdir c:\opt\trains\logs

1. Save the **trains-server** docker-compose YAML file.
 
        cd c:\opt\trains
        curl https://raw.githubusercontent.com/allegroai/trains-server/master/docker-compose-win10.yml -o docker-compose-win10.yml 
 
1. Run `docker-compose`. In PowerShell, execute the following commands:

        docker-compose -f docker-compose-win10.yml up
   
    Your server is now running on [http://localhost:8080](http://localhost:8080) and the following ports are available:

    * Web server on port `8080`
    * API server on port `8008`
    * File server on port `8081`

## Next Step

Configure the [Trains client for trains-server](https://github.com/allegroai/trains/blob/master/README.md#configuration).