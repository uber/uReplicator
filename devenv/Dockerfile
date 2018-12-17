FROM java:openjdk-8-jre

ENV DEBIAN_FRONTEND noninteractive
ENV SCALA_VERSION 2.11
ENV KAFKA_VERSION 0.10.2.2
ENV KAFKA_HOME /opt/kafka_"$SCALA_VERSION"-"$KAFKA_VERSION"
ENV DOWNLOAD_ZOOKEEPER http://archive.apache.org/dist/zookeeper/zookeeper-3.4.3/zookeeper-3.4.3.tar.gz

# Install Kafka, Zookeeper and other needed things
RUN apt-get update && \
apt-get install -y zookeeper wget supervisor dnsutils && \
rm -rf /var/lib/apt/lists/* && \
apt-get clean

RUN wget -q http://apache.mirrors.spacedump.net/kafka/"$KAFKA_VERSION"/kafka_"$SCALA_VERSION"-"$KAFKA_VERSION".tgz -O /tmp/kafka_"$SCALA_VERSION"-"$KAFKA_VERSION".tgz && \
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

CMD ["supervisord", "-n"]