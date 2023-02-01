#!/usr/bin/env bash
set -o errexit
set -o nounset
set -o pipefail

yum update -y
#yum install -y https://dl.fedoraproject.org/pub/epel/epel-release-latest-7.noarch.rpm
yum install -y epel-release
yum install -y python36 python36-pip nginx gcc gcc-c++ python3-devel gettext
#yum -y upgrade
python3 -m pip install setuptools-rust
python3 -m pip install --upgrade pip
python3 -m pip install -r /opt/clearml/fileserver/requirements.txt
python3 -m pip install -r /opt/clearml/apiserver/requirements.txt
mkdir -p /opt/clearml/log
mkdir -p /opt/clearml/config
ln -s /dev/stdout /var/log/nginx/access.log
ln -s /dev/stderr /var/log/nginx/error.log
mv /etc/nginx/nginx.conf /etc/nginx/nginx.conf.orig
mv /tmp/internal_files/clearml.conf.template /etc/nginx/clearml.conf.template
mv /tmp/internal_files/clearml_subpath.conf.template /etc/nginx/clearml_subpath.conf.template
yum clean all
