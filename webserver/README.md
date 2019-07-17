# Webserver (NGINX)

## Introduction

The webserver is the **trains-server**'s component responsible for serving the TRAINS webapp.
For this purpose, we use an [NGINX](https://www.nginx.com/) server.

## Configuration

In order to serve the TRAINS webapp, the following is required:
* The pre-built TRAINS webapp should be copied to the NGINX html directory (usually `/usr/share/nginx/html`)
* The default NGINX port (usually `80`) should be changed to match the **trains-server** configuration (usually `8080`)

NOTE: This configuration may vary in different systems, depending on the NGINX version and distribution used.

#### Example: Centos 7

The following commands can be used to install and run NGINX in the Centos 7 OS:
```bash
yum install nginx
cp -R /path/to/trains-webapp/build/* /var/www/html
systemctl enable nginx
systemctl start nginx
```
