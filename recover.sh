#!/bin/bash -e
set -x
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
