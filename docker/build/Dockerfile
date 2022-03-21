FROM centos/nodejs-12-centos7 AS webapp

ARG CLEARML_WEB_GIT_URL=https://github.com/allegroai/clearml-web.git

USER root
WORKDIR /opt

RUN git clone ${CLEARML_WEB_GIT_URL} clearml-web
RUN mv clearml-web /opt/open-webapp
COPY --chmod=744 docker/build/internal_files/build_webapp.sh /tmp/internal_files/
RUN /bin/bash -c '/tmp/internal_files/build_webapp.sh'

FROM centos:7 AS staging_image
COPY --chmod=744 docker/build/internal_files/entrypoint.sh /opt/clearml/
COPY fileserver /opt/clearml/fileserver/
COPY apiserver /opt/clearml/apiserver/

FROM centos:7
COPY --from=staging_image /opt/clearml/ /opt/clearml/

COPY --chmod=744 docker/build/internal_files/final_image_preparation.sh /tmp/internal_files/
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
