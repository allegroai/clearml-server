# Deploying **trains-server** on Google Cloud Platform

To easily deploy **trains-server** on GCP, use one of our pre-built GCP Custom Images.  
We provide Custom Images for each released version of **trains-server**, see [Released versions](#released-versions) below. 

Once your GCP instance is up and running using our Custom Image, [configure the Trains client](https://github.com/allegroai/trains/blob/master/README.md#configuration) to use your **trains-server**.  
The service port numbers on our **trains-server** GCP Custom Image are:

- Web application: `8080`
- API Server: `8008`
- File Server: `8081`

The persistent storage configuration:

- MongoDB: `/opt/trains/data/mongo/`
- ElasticSearch: `/opt/trains/data/elastic/`
- File Server: `/mnt/fileserver/`

For examples and use cases, check the [Trains usage examples](https://github.com/allegroai/trains/blob/master/docs/trains_examples.md).

For instructions on launching an instance using a GCP Custom Image, see the [Manually importing virtual disks](https://cloud.google.com/compute/docs/import/import-existing-image#overview) in the [Compute Engine Documentation](https://cloud.google.com/compute/docs).
For more information on Custom Images, see [Custom Images](https://cloud.google.com/compute/docs/images#custom_images) in the Compute Engine Documentation.

The minimum recommended amount of RAM for a **trains-server** instance is 8GB.

## Upgrading

To upgrade **trains-server** on an existing GCP instance based on one of these Custom Images, SSH into the instance and follow the [upgrade instructions](../README.md#upgrade) for **trains-server**.

## Released versions

The following sections contain lists of Custom Image URLs (exported in different formats) for each released **trains-server** version.

### Latest version Custom Images - v0.14.1

- https://storage.googleapis.com/allegro-files/trains-server/trains-server.qcow2
- https://storage.googleapis.com/allegro-files/trains-server/trains-server.vhd
- https://storage.googleapis.com/allegro-files/trains-server/trains-server.vhdx
- https://storage.googleapis.com/allegro-files/trains-server/trains-server.vmdk
