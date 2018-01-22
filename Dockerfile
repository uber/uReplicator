FROM anapsix/alpine-java
# This is the release of https://github.com/hashicorp/docker-base to pull in order
# to provide HashiCorp-built versions of basic utilities like dumb-init and gosu.
ENV DOCKER_BASE_VERSION=0.0.4

# This is the location of the releases.
ENV HASHICORP_RELEASES=https://releases.hashicorp.com

# Create a consul-template user and group first so the IDs get set the same way,
# even as the rest of this may change over time.
RUN addgroup consul-template && \
    adduser -S -G consul-template consul-template

# Set up certificates, our base tools, and Consul Template (CT).
RUN apk add --no-cache curl dumb-init gettext &&  \
  (curl "https://releases.hashicorp.com/consul-template/0.19.4/consul-template_0.19.4_linux_amd64.tgz" | tar -C /bin/ -xvz) && \
  apk del curl

# The agent will be started with /consul-template/config as the configuration directory
# so you can add additional config files in that location.
RUN mkdir -p /consul-template/data && \
    mkdir -p /consul-template/config && \
    chown -R consul-template:consul-template /consul-template

# Expose the consul-template data directory as a volume since that's where
# shared results should be rendered.
VOLUME /consul-template/data

ENV SRC_CLUSTER_NAME platform
ENV DEST_CLUSTER_NAME visits
ENV CONSUL_ADDR "localhost:8500"
ENV UREPLICATOR_PORT 8080

ADD . /app/

# Install gosu
ENV GOSU_VERSION 1.10 RUN set -ex; \
	\
	apk add --no-cache --virtual .gosu-deps \
		dpkg \
		openssl \
	; \
	\
	dpkgArch="$(dpkg --print-architecture | awk -F- '{ print $NF }')"; \
	wget -O /bin/gosu "https://github.com/tianon/gosu/releases/download/$GOSU_VERSION/gosu-$dpkgArch"; \
	chmod +x /bin/gosu; \
# verify that the binary works
	gosu nobody true; \
	\
	apk del .gosu-deps


WORKDIR /app

ENTRYPOINT ["/app/run"]

EXPOSE 8080
