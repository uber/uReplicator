#!/bin/bash -e
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

set -x

NAME_BEFORE="<name>uReplicator</name>"

NAME_AFTER="<name>uReplicator</name> \
  <distributionManagement> \
    <repository> \
      <id>central</id> \
      <url>http://artifactory.uber.internal:4587/artifactory/libs-release-local/</url> \
    </repository> \
    <snapshotRepository> \
      <id>snapshots</id> \
      <url>http://artifactory.uber.internal:4587/artifactory/libs-snapshot-local/</url> \
    </snapshotRepository> \
  </distributionManagement>"

VERSION_BEFORE="<version>1.0.0-SNAPSHOT</version>"

VERSION_AFTER="<version>1.0.3.7-SNAPSHOT</version>"


# Root
cp pom.xml pom.xml-org
sed -i .bak $"s|$NAME_BEFORE|$NAME_AFTER|g" pom.xml
sed -i .bak $"s|$VERSION_BEFORE|$VERSION_AFTER|g" pom.xml

# Distribution
cp uReplicator-Distribution/pom.xml uReplicator-Distribution/pom.xml-org
sed -i .bak $"s|$VERSION_BEFORE|$VERSION_AFTER|g" uReplicator-Distribution/pom.xml

# Common
cp uReplicator-Common/pom.xml uReplicator-Common/pom.xml-org
sed -i .bak $"s|$VERSION_BEFORE|$VERSION_AFTER|g" uReplicator-Common/pom.xml

# Manager
cp uReplicator-Manager/pom.xml uReplicator-Manager/pom.xml-org
sed -i .bak $"s|$VERSION_BEFORE|$VERSION_AFTER|g" uReplicator-Manager/pom.xml

# Controller
cp uReplicator-Controller/pom.xml uReplicator-Controller/pom.xml-org
sed -i .bak $"s|$VERSION_BEFORE|$VERSION_AFTER|g" uReplicator-Controller/pom.xml

# Worker
cp uReplicator-Worker/pom.xml uReplicator-Worker/pom.xml-org
sed -i .bak $"s|$VERSION_BEFORE|$VERSION_AFTER|g" uReplicator-Worker/pom.xml


mvn clean install -DskipTests
mvn deploy -DskipTests

# Root
rm pom.xml
mv pom.xml-org pom.xml
rm pom.xml.bak

# Distribution
rm uReplicator-Distribution/pom.xml
mv uReplicator-Distribution/pom.xml-org uReplicator-Distribution/pom.xml
rm uReplicator-Distribution/pom.xml.bak

# Common
rm uReplicator-Common/pom.xml
mv uReplicator-Common/pom.xml-org uReplicator-Common/pom.xml
rm uReplicator-Common/pom.xml.bak

# Manager
rm uReplicator-Manager/pom.xml
mv uReplicator-Manager/pom.xml-org uReplicator-Manager/pom.xml
rm uReplicator-Manager/pom.xml.bak

# Controller
rm uReplicator-Controller/pom.xml
mv uReplicator-Controller/pom.xml-org uReplicator-Controller/pom.xml
rm uReplicator-Controller/pom.xml.bak

# Worker
rm uReplicator-Worker/pom.xml
mv uReplicator-Worker/pom.xml-org uReplicator-Worker/pom.xml
rm uReplicator-Worker/pom.xml.bak
