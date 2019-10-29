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

### Upgrading AMI's to v0.12 
**Including the automatically updated AMI**

Version 0.12 introduced an additional REDIS docker to the trains-server setup.

AMI upgrading instructions:

1. SSH to the EC2 machine running one of the `Latest Version AMI's`
2. Execute the following bash commands
    ```bash
    sudo bash
    echo "" >>  /usr/bin/start_or_update_server.sh
    echo "sudo mkdir -p \${datadir}/redis" >>  /usr/bin/start_or_update_server.sh
    echo "sudo docker stop trains-redis || true && sudo docker rm -v trains-redis || true" >>  /usr/bin/start_or_update_server.sh
    echo "echo never | sudo tee -a  /sys/kernel/mm/transparent_hugepage/enabled" >>  /usr/bin/start_or_update_server.sh
    echo "sudo sysctl vm.overcommit_memory=1" >>  /usr/bin/start_or_update_server.sh
    echo "sudo docker run -d --restart=always --name=trains-redis -v \${datadir}/redis:/data --network=host redis:5 redis-server" >>  /usr/bin/start_or_update_server.sh
    ``` 
3. Reboot the EC2 machine


## Released versions

The following sections provide a list containing AMI Image ID per region for each released **trains-server** version.

### Latest Version AMI <a name="autoupdate"></a>
**For easier upgrades: The following AMI automatically update to the latest release every reboot**

* **eu-north-1** : ami-072aef14041e70651
* **ap-south-1** : ami-08032d881daca4de1
* **eu-west-3** : ami-0b39c123d4343d408
* **eu-west-2** : ami-0e0fe6fd14b2e9029
* **eu-west-1** : ami-087c81e06d722e938
* **ap-northeast-2** : ami-0caf74f03322b994c
* **ap-northeast-1** : ami-0f723b3d49c0f2749
* **sa-east-1** : ami-0ac5595ad0e106502
* **ca-central-1** : ami-053049b463869469a
* **ap-southeast-1** : ami-0b440ec389d6ff541
* **ap-southeast-2** : ami-02af978ddc2c15b71
* **eu-central-1** : ami-09ef364aa8df29760
* **us-east-2** : ami-02e33f8ab77071509
* **us-west-1** : ami-0ff33f256907fd460
* **us-west-2** : ami-0387728fb09c8cda7
* **us-east-1** : ami-02c47c5233eed7f88

### v0.12.0
* **eu-north-1** : ami-0ebb4bb8637d0da65
* **ap-south-1** : ami-0fb3c89eb8a8fc294
* **eu-west-3** : ami-0b55ea4a6698d5875
* **eu-west-2** : ami-02979b6d77856b842
* **eu-west-1** : ami-07f4c17a636489574
* **ap-northeast-2** : ami-06071092427dd5ab4
* **ap-northeast-1** : ami-0fbacddfc0e8d2651
* **sa-east-1** : ami-073590d3b3e6f4cfd
* **ca-central-1** : ami-0839610fc0101e41c
* **ap-southeast-1** : ami-0ff0adeef7f9fa879
* **ap-southeast-2** : ami-03ed15d31bfc2844c
* **eu-central-1** : ami-0813c06d8b2462c39
* **us-east-2** : ami-07c593425f988b054
* **us-west-1** : ami-0eb0e13b1f06c03c0
* **us-west-2** : ami-000568ca142798412
* **us-east-1** : ami-062d9da44f96c8a87

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
