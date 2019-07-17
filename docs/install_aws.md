# **TRAINS-server**: AWS pre-installed images

In order to easily deploy **trains-server** on AWS, we created the following Amazon Machine Images (AMIs).

Service port numbers on these AMIs are:
 - Web: 8080
 - API: 8008
 - File Server: 8081

Persistent storage configuration:
 - MongoDB: /opt/trains/data/mongo/
 - ElasticSearch: /opt/trains/data/elastic/
 - File Server: /mnt/fileserver/

Instructions on launching a custom AMI from the EC2 console can be found [here](https://aws.amazon.com/premiumsupport/knowledge-center/launch-instance-custom-ami/)
and a detailed version [here](https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/launching-instance.html).

The minimum recommended instance type is **t3a.large**

## Upgrading

In order to upgrade **trains-server** on an existing EC2 instance based on one of these AMIs, SSH into the instance and follow the [upgrade instructions](../README.md#upgrade) for **trains-server**.

## Released versions

The following sections provide a list containing AMI Image ID per region for each released **trains-server** version.

### v0.10.1
* **eu-north-1** : ami-075179c6226cd9095
* **ap-south-1** : ami-060f4ed126053eb04
* **eu-west-3** : ami-064db17c89563ab4d
* **eu-west-2** : ami-03fd7ed12335a28a7
* **eu-west-1** : ami-0ccc70c4a240ad561
* **ap-northeast-2** : ami-0a396c6a9aa957f5a
* **ap-northeast-1** : ami-03e2f80d6f507dd01
* **sa-east-1** : ami-05860b0f84623709f
* **ca-central-1** : ami-09070d5996ab859bd
* **ap-southeast-1** : ami-0f6837004376bcf17
* **ap-southeast-2** : ami-0fedf36691cdbe1ae
* **eu-central-1** : ami-075ffc9f0c89fe67f
* **us-east-2** : ami-0e762227086ba49e4
* **us-west-1** : ami-06845a6c0f1c9bd13
* **us-west-2** : ami-03ddd7af1dcab9bf9
* **us-east-1** : ami-0e599e219060f7def

### v0.10.0
* **eu-north-1** : ami-05ba33c763877e54e
* **ap-south-1** : ami-0529eec569161cae5
* **eu-west-3** : ami-03cb9396f63e26ff6
* **eu-west-2** : ami-0dd28cc97283cc201
* **eu-west-1** : ami-059cf379ae14b0a24
* **ap-northeast-2** : ami-031409d71f1280616
* **ap-northeast-1** : ami-0171437c68b3660aa
* **sa-east-1** : ami-0eb440a3b6e591c7a
* **ca-central-1** : ami-097da9ec155ee654a
* **ap-southeast-1** : ami-0ab7ff3ea09826e39
* **ap-southeast-2** : ami-00969c550ef2d1f60
* **eu-central-1** : ami-02246400c51990acb
* **us-east-2** : ami-0cafc1d730381d6fa
* **eu-central-1** : ami-02246400c51990acb
* **us-west-1** : ami-0e82a98ddbe995a65
* **us-west-2** : ami-04a522ecb2250fb44
* **us-east-1** : ami-0a66ddbd50959f91e

### v0.9.0

* **us-east-1** : ami-0991ad536ecbacdac
* **eu-north-1** : ami-07cbcdff501b14afe
* **ap-south-1** : ami-014cf398b00d4db83
* **eu-west-3** : ami-0396ba51e9b733581
* **eu-west-2** : ami-09134f4c7a20bad09
* **eu-west-1** : ami-00427ed0a1bbfa7b0
* **ap-northeast-2** : ami-041756675ca1be954
* **ap-northeast-1** : ami-0c09ebad05c9128ff
* **sa-east-1** : ami-017a8de4e8d1e8c8e
* **ca-central-1** : ami-049ec444470f852be
* **ap-southeast-1** : ami-0c919b8f821a6c635
* **ap-southeast-2** : ami-04844a0594712d27b
* **eu-central-1** : ami-0b4e756e0f7c0617d
* **us-east-2** : ami-03b01914b07428488
* **us-west-1** : ami-0cf4768e9d47ed076
* **us-west-2** : ami-0b145f37da31eb9fb
