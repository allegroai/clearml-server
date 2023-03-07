#!/usr/bin/env bash
set -x
set -e

cd /opt/open-webapp/
npm ci --legacy-peer-deps

cd /opt/open-webapp/
npm run build
npm run build-widgets
