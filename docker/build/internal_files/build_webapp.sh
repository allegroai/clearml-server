#!/usr/bin/env bash
set -x
set -e

cd /opt/open-webapp/
npm ci --unsafe-perm node-sass

cd /opt/open-webapp/
npm run build
