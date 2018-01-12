FROM maven:3.5-jdk-8

ARG MAVEN_OPTS="-Xmx1024M -Xss128M -XX:MetaspaceSize=512M -XX:MaxMetaspaceSize=1024M -XX:+CMSClassUnloadingEnabled"
COPY . /usr/src/app
WORKDIR /usr/src/app

RUN mvn clean package -DskipTests
RUN chmod +x /usr/src/app/bin/pkg/*.sh

COPY docker-entrypoint.sh /docker-entrypoint.sh

ENTRYPOINT [ "/docker-entrypoint.sh" ]
CMD [ "controller" ]
