# Deploying Trains Server on Google Cloud Platform

# **NOTE**: These instructions are deprecated. See the [ClearML documentation](https://clear.ml/docs/latest/docs/deploying_clearml/clearml_server) for up-to-date deployment instructions 

To easily deploy Trains Server on GCP, use one of our pre-built GCP Custom Images.  
We provide Custom Images for each released version of Trains Server, see [Released versions](#released-versions) below. 

Once your GCP instance is up and running using our Custom Image, [configure the Trains client](https://github.com/allegroai/trains/blob/master/README.md#configuration) to use your **trains-server**.
  
#### Default Trains Server Service ports
The service port numbers on our Trains Server GCP Custom Image are:

- Web application: `8080`
- API Server: `8008`
- File Server: `8081`

#### Default Trains Server Storage paths
The persistent storage configuration:

- MongoDB: `/opt/trains/data/mongo/`
- ElasticSearch: `/opt/trains/data/elastic/`
- File Server: `/mnt/fileserver/`

For examples and use cases, check the [Trains usage examples](https://github.com/allegroai/trains/blob/master/docs/trains_examples.md).

## Importing the Custom Image to your GCP account

In order to launch an instance using the Trains Server GCP Custom Image, you'll need to import the image to your custom images list.

**Note:** there's **no need** to upload the image file to Google Cloud Storage - we already provide links to image files stored in Google Storage

To import the image to your custom images list:
1. In the Cloud Console, go to the [Images](https://console.cloud.google.com/compute/images) page.
1. At the top of the page, click **Create image**.
1. In the **Name** field, specify a unique name for the image.
1. Optionally, specify an image family for your new image, or configure specific encryption settings for the image.
1. Click the **Source** menu and select **Cloud Storage file**.
1. Enter the Trains Server image bucket path (see [Trains Server GCP Custom Image](#released-versions)), for example:
    `allegro-files/trains-server/trains-server.tar.gz`
1. Click the **Create** button to import the image. The process can take several minutes depending on the size of the boot disk image.

For more information see [Import the image to your custom images list](https://cloud.google.com/compute/docs/import/import-existing-image#import_image) in the [Compute Engine Documentation](https://cloud.google.com/compute/docs).

## Launching an instance with a Custom Image

For instructions on launching an instance using a GCP Custom Image, see the [Manually importing virtual disks](https://cloud.google.com/compute/docs/import/import-existing-image#overview) in the [Compute Engine Documentation](https://cloud.google.com/compute/docs).
For more information on Custom Images, see [Custom Images](https://cloud.google.com/compute/docs/images#custom_images) in the Compute Engine Documentation.

The minimum recommended requirements for Trains Server are:
- 2 vCPUs
- 7.5GB RAM

## Upgrading

To upgrade **trains-server** on an existing GCP instance based on one of these Custom Images, SSH into the instance and follow the [upgrade instructions](../README.md#upgrade) for **trains-server**.

## Network and Security

Please make sure your instance is properly secured. 

If not specifically set, a GCP instance will use default firewall rules that allow public access to various ports. 
If your instance is open for public access, we recommend you follow best practices for access management, including:
- Allow access only to the specific ports used by Trains Server (see [Default Trains Server Service ports](#default-trains-server-service-ports)). Remember to allow access to port `443` if `https` access is configured for your instance.
- Configure Trains Server to use fixed user names and passwords (see [Can I add web login authentication to trains-server?](./faq.md#web-auth))    

## Released versions

The following sections contain lists of Custom Image URLs (exported in different formats) for each released **trains-server** version.

### Latest version image

- https://storage.googleapis.com/allegro-files/trains-server/trains-server.tar.gz

### All released images 

- v0.15.1 - https://storage.googleapis.com/allegro-files/trains-server/trains-server-0-15-1.tar.gz
- v0.15.0 - https://storage.googleapis.com/allegro-files/trains-server/trains-server-0-15-0.tar.gz
- v0.14.1 - https://storage.googleapis.com/allegro-files/trains-server/trains-server-0-14-1.tar.gz