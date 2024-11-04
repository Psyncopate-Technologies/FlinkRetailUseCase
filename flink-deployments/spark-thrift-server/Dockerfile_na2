FROM deltaio/delta-docker:latest

ARG WORKDIR=/opt/spark/work-dir
ARG NBuser=NBuser
ARG GROUP=NBuser

USER root

COPY custom_startup.sh "${WORKDIR}"

USER root
# Establish entrypoint
ENTRYPOINT ["bash", "custom_startup.sh"]