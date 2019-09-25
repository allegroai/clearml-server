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

### Latest Version AMI <a name="autoupdate"></a>
**For easier upgrades: The following AMI automatically update to the latest release every reboot**

* **eu-north-1** : ami-075099a3efe13ff44
* **ap-south-1** : ami-0ffcb769c06663df0
* **eu-west-3** : ami-0e3147b1a5a0d7769
* **eu-west-2** : ami-02560f572e1e49e9c
* **eu-west-1** : ami-094eec51d30bb6cf2
* **ap-northeast-2** : ami-02bf7eb1b2b3879cc
* **ap-northeast-1** : ami-0e631ad6ed1e0952c
* **sa-east-1** : ami-08d9fed1bd83887b8
* **ca-central-1** : ami-017c0d0122603e9ba
* **ap-southeast-1** : ami-098739c8694396f7e
* **ap-southeast-2** : ami-0ce9f8006fb6a01b1
* **eu-central-1** : ami-0e9aa4cde368a3b2f
* **us-east-2** : ami-0c84932a9ec8ba69e
* **us-west-1** : ami-0793446f57fc3e0dd
* **us-west-2** : ami-09024fc28c9768181
* **us-east-1** : ami-0ff392a18e027420e

### v0.11.0
* **eu-north-1** : ami-0cbe338f058018c97
* **ap-south-1** : ami-06d72ff894f7a5e5d
* **eu-west-3** : ami-00f2a45d67df2d2f3
* **eu-west-2** : ami-0627ae688f4533237
* **eu-west-1** : ami-00bf924ccb0354418
* **ap-northeast-2** : ami-0800edf1d1dec1da8
* **ap-northeast-1** : ami-07b2ed9709cdc4b15
* **sa-east-1** : ami-0012c1648618b812c
* **ca-central-1** : ami-02870b965d002fc8a
* **ap-southeast-1** : ami-068ec23abf2473192
* **ap-southeast-2** : ami-06664624728b5e01a
* **eu-central-1** : ami-05f2a9304f237a6f0
* **us-east-2** : ami-0ec242e6dca2b72b9
* **us-west-1** : ami-050b6577acf246ceb
* **us-west-2** : ami-0e384b6f78bf96ebe
* **us-east-1** : ami-0a7b46f907d5d9c4a

### v0.10.1
* **eu-north-1** : ami-09937ec4d18350c32
* **ap-south-1** : ami-089d6ba7541ec4c7f
* **eu-west-3** : ami-0accb1a94bdd5c5c1
* **eu-west-2** : ami-0dd2c97bc678b8570
* **eu-west-1** : ami-07a38865cbe7ca3cb
* **ap-northeast-2** : ami-09aa0b7fe1cf3dd55
* **ap-northeast-1** : ami-0905e7d1543e5ed36
* **sa-east-1** : ami-08c0627daa67d7372
* **ca-central-1** : ami-034add081712ff648
* **ap-southeast-1** : ami-0c6caee3689b6e066
* **ap-southeast-2** : ami-04994afd8dae5b417
* **eu-central-1** : ami-06b10f8c30e1434f1
* **us-east-2** : ami-0d3abe7a1fec535cc
* **us-west-1** : ami-02bb610b70c55018b
* **us-west-2** : ami-0d1cb8ba7de246ff0
* **us-east-1** : ami-049ccba6abdb40cba

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
