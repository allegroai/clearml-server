FROM node:12.21.0-buster-slim AS webapp

RUN apt-get update \
    && apt-get install -y --no-install-recommends \
      build-essential=12.6 \
      python=2.7.16-1 \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /opt

COPY ./webserver ./open-webapp
COPY docker/build/internal_files/build_webapp.sh /tmp/internal_files/
RUN /bin/bash -c '/tmp/internal_files/build_webapp.sh'

FROM centos:centos7.9.2009 AS staging_image
COPY docker/build/internal_files/entrypoint.sh /opt/clearml/
COPY fileserver /opt/clearml/fileserver/
COPY apiserver /opt/clearml/apiserver/

FROM centos:centos7.9.2009
COPY --from=staging_image /opt/clearml/ /opt/clearml/

COPY docker/build/internal_files/final_image_preparation.sh /tmp/internal_files/
COPY docker/build/internal_files/clearml.conf.template /tmp/internal_files/
COPY docker/build/internal_files/clearml_subpath.conf.template /tmp/internal_files/
RUN /bin/bash -c '/tmp/internal_files/final_image_preparation.sh'

COPY --from=webapp /opt/open-webapp/build /usr/share/nginx/html

EXPOSE 8080
EXPOSE 8008
EXPOSE 8081

ARG VERSION
ARG BUILD
ENV CLEARML_SERVER_VERSION=${VERSION}
ENV CLEARML_SERVER_BUILD=${BUILD}

WORKDIR /opt/clearml/
ENTRYPOINT ["/opt/clearml/entrypoint.sh"]
