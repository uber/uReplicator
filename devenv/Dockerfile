#
# Copyright (C) 2015-2019 Uber Technologies, Inc. (streaming-data@uber.com)
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#         http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

FROM openjdk:8-jre
#
# Copyright (C) 2015-2017 Uber Technologies, Inc. (streaming-data@uber.com)
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#         http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

ENV DEBIAN_FRONTEND noninteractive
ENV SCALA_VERSION 2.11
ENV KAFKA_VERSION 0.10.2.2
ENV KAFKA_HOME /opt/kafka_"$SCALA_VERSION"-"$KAFKA_VERSION"
ENV DOWNLOAD_ZOOKEEPER http://archive.apache.org/dist/zookeeper/zookeeper-3.4.3/zookeeper-3.4.3.tar.gz

# Install Kafka, Zookeeper and other needed things
RUN apt-get update && \
apt-get install -y zookeeper wget supervisor dnsutils netcat && \
rm -rf /var/lib/apt/lists/* && \
apt-get clean

RUN wget -q https://archive.apache.org/dist/kafka/"$KAFKA_VERSION"/kafka_"$SCALA_VERSION"-"$KAFKA_VERSION".tgz -O /tmp/kafka_"$SCALA_VERSION"-"$KAFKA_VERSION".tgz && \
tar xfz /tmp/kafka_"$SCALA_VERSION"-"$KAFKA_VERSION".tgz -C /opt && \
rm /tmp/kafka_"$SCALA_VERSION"-"$KAFKA_VERSION".tgz

ADD scripts/start-kafka.sh /usr/bin/start-kafka.sh

RUN mkdir /usr/kafka
ADD server1.properties /usr/kafka/server1.properties
ADD server2.properties /usr/kafka/server2.properties

RUN echo "advertised.host.name=devenv" >> /usr/kafka/server1.properties
RUN echo "advertised.host.name=devenv" >> /usr/kafka/server2.properties

ADD produce-data-to-kafka-topic-dummyTopic.sh /usr/kafka/produce-data-to-kafka-topic-dummyTopic.sh
ADD dummyTopicData.log /usr/kafka/dummyTopicData.log

# Supervisor config
ADD supervisor/kafka.conf supervisor/zookeeper.conf /etc/supervisor/conf.d/

# 2181 is zookeeper, 9093,9094 is kafka
EXPOSE 2181 9093 9094

# hack for advertised.host.name
RUN echo "127.0.0.1 devenv" >> /etc/hosts
ENV PATH="/usr/share/zookeeper/bin:${PATH}"


ADD prepare-zk.sh .

CMD ["supervisord", "-n"]
