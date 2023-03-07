FROM centos/nodejs-12-centos7 AS webapp

USER root
WORKDIR /opt

RUN git clone https://github.com/allegroai/clearml-web.git
RUN mv clearml-web /opt/open-webapp
COPY docker/build/internal_files/build_webapp.sh /tmp/internal_files/
RUN /bin/bash -c '/tmp/internal_files/build_webapp.sh'

FROM centos:7 AS staging_image
COPY docker/build/internal_files/entrypoint.sh /opt/clearml/
COPY fileserver /opt/clearml/fileserver/
COPY apiserver /opt/clearml/apiserver/

FROM centos:7
COPY --from=staging_image /opt/clearml/ /opt/clearml/

COPY docker/build/internal_files/final_image_preparation.sh /tmp/internal_files/
COPY docker/build/internal_files/clearml.conf.template /tmp/internal_files/
RUN /bin/bash -c '/tmp/internal_files/final_image_preparation.sh'

COPY --from=webapp /opt/open-webapp/build /usr/share/nginx/html
COPY --from=webapp_builder /opt/open-webapp/dist/report-widgets /usr/share/nginx/widgets

EXPOSE 8080
EXPOSE 8008
EXPOSE 8081

ARG VERSION
ARG BUILD
ENV CLEARML_SERVER_VERSION=${VERSION}
ENV CLEARML_SERVER_BUILD=${BUILD}

WORKDIR /opt/clearml/
ENTRYPOINT ["/opt/clearml/entrypoint.sh"]
