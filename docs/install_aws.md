# Deploying **trains-server** on AWS

To easily deploy **trains-server** on AWS, use one of our pre-built Amazon Machine Images (AMIs).  
We provide AMIs per region for each released version of **trains-server**, see [Released versions](#released-versions) below. 

Once the AMI is up and running, [configure the Trains client](https://github.com/allegroai/trains/blob/master/README.md#configuration) to use your **trains-server**.  
The service port numbers on our **trains-server** AMIs:

- Web application: `8080`
- API Server: `8008`
- File Server: `8081`

The persistent storage configuration:

- MongoDB: `/opt/trains/data/mongo/`
- ElasticSearch: `/opt/trains/data/elastic/`
- File Server: `/mnt/fileserver/`

For examples and use cases, check the [Trains usage examples](https://github.com/allegroai/trains/blob/master/docs/trains_examples.md).

For instructions on launching a custom AMI from the EC2 console, see the [AWS Knowledge Center](https://aws.amazon.com/premiumsupport/knowledge-center/launch-instance-custom-ami/) or detailed instructions in the [AWS Documentation](https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/launching-instance.html).

The minimum recommended amount of RAM is 8GB. For example, **t3.large** or **t3a.large** would have the minimum recommended amount of resources.

## Upgrading

To upgrade **trains-server** on an existing EC2 instance based on one of these AMIs, SSH into the instance and follow the [upgrade instructions](../README.md#upgrade) for **trains-server**.

### Upgrading AMIs to v0.12 

This upgrade includes the automatically updated AMI in Version 0.12. It also includes an additional REDIS docker to the **trains-server** setup.

To upgrade the AMI:

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

The following sections contain lists of AMI Image IDs, per region, for each released **trains-server** version.

### Latest version AMI - v0.13.0 (auto update)<a name="autoupdate"></a>

For easier upgrades, the following AMIs automatically update to the latest release every reboot:

* **eu-north-1** : ami-003024b7b575d3f2a 
* **ap-south-1** : ami-0d784c7ac2ab4cc72 
* **eu-west-3** : ami-091d745be445b69db 
* **eu-west-2** : ami-0a4ebf5d45c672411 
* **eu-west-1** : ami-021e3421c50d1482c 
* **ap-northeast-2** : ami-0d0a25ec610d6d122 
* **ap-northeast-1** : ami-01d896f9ae5d87890 
* **sa-east-1** : ami-09bcb93835428a412 
* **ca-central-1** : ami-077fa58c9f73690c7 
* **ap-southeast-1** : ami-046fe4832b077b517 
* **ap-southeast-2** : ami-0ab9acb41f8abbba7 
* **eu-central-1** : ami-079be664aae12db00 
* **us-east-2** : ami-0d48555f80cb7993a 
* **us-west-1** : ami-0ed85ab91a7bb5a8a 
* **us-west-2** : ami-0b4fe4ca18e9b1227 
* **us-east-1** : ami-043b95dd034e581e6 

### v0.13.0 (static update)
* **eu-north-1** : ami-0e26c3af1663428dc 
* **ap-south-1** : ami-07451eb44f51380a8 
* **eu-west-3** : ami-0108e506c6e0be8d8 
* **eu-west-2** : ami-0fc1fdbc7699f0dde 
* **eu-west-1** : ami-0efbf8d2f580a9cee 
* **ap-northeast-2** : ami-08f0bbd7e08d0603e 
* **ap-northeast-1** : ami-024522bea34dbe3ce 
* **sa-east-1** : ami-0fe5b6e0ddc1553d9 
* **ca-central-1** : ami-0037c26178a584ade 
* **ap-southeast-1** : ami-049dbcc0f0a6dba20 
* **ap-southeast-2** : ami-02d1ce8d31c27f187 
* **eu-central-1** : ami-0550b14b40371182a 
* **us-east-2** : ami-040a1f16ceda8f255 
* **us-west-1** : ami-003b5673c08d68cdb 
* **us-west-2** : ami-0fec951d8043da62d 
* **us-east-1** : ami-049694de0137fdea4 

### v0.12.1 (static update)
* **eu-north-1** : ami-003118a8103286d84 
* **ap-south-1** : ami-02dfe86baa48e096f 
* **eu-west-3** : ami-0cc1f01267d2a780d 
* **eu-west-2** : ami-0e4c8332e5ce09585 
* **eu-west-1** : ami-03459a2f0b0a3b1ab 
* **ap-northeast-2** : ami-08f6c2aed3a53f24c 
* **ap-northeast-1** : ami-0b798eab95a7c5435 
* **sa-east-1** : ami-0d3ee166c09f0d1b2 
* **ca-central-1** : ami-00a758c56bd63acd5 
* **ap-southeast-1** : ami-0be64d4988cd03fbb 
* **ap-southeast-2** : ami-02087310d43a63f31 
* **eu-central-1** : ami-097bbefeac0c74225 
* **us-east-2** : ami-07eda256712b90f4d 
* **us-west-1** : ami-02ef2b55cbd01c7df 
* **us-west-2** : ami-037c6176ef4735360 
* **us-east-1** : ami-08715c20c0e3f1c15 

### v0.12.0 (static update)

* **eu-north-1** : ami-03ff8ab48cd43e77e
* **ap-south-1** : ami-079c1a41ff836487c
* **eu-west-3** : ami-0121ef0398ae87ab0
* **eu-west-2** : ami-09f0f97654d8c79de
* **eu-west-1** : ami-0b7ba303f757bfcd9
* **ap-northeast-2** : ami-053f416517b5f40a6
* **ap-northeast-1** : ami-056dff06c698c2d9d
* **sa-east-1** : ami-017ab655119258639
* **ca-central-1** : ami-03bf5fa1d86ac97f6
* **ap-southeast-1** : ami-0e667958002b0360c
* **ap-southeast-2** : ami-091f1b69cb43b1933
* **eu-central-1** : ami-068ec2f0e98c26541
* **us-east-2** : ami-0524bbdc1b64ff83f
* **us-west-1** : ami-0b4facd7534e393c9
* **us-west-2** : ami-0018d5a7e58966848
* **us-east-1** : ami-08f24178fc14a84d2

### v0.11.0 (static update)

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

### v0.10.1 (static update)

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

### v0.10.0 (static update)

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

### v0.9.0 (static update)

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

