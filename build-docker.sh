#!/bin/bash
#
#  Licensed to the Apache Software Foundation (ASF) under one
#  or more contributor license agreements.  See the NOTICE file
#  distributed with this work for additional information
#  regarding copyright ownership.  The ASF licenses this file
#  to you under the Apache License, Version 2.0 (the
#  "License"); you may not use this file except in compliance
#  with the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing,
#  software distributed under the License is distributed on an
#  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
#  KIND, either express or implied.  See the License for the
#  specific language governing permissions and limitations
#  under the License.
#

# create addax docker image

function get_version() {
    version=$(grep -o -E '<version>.*</version>' pom.xml | head -n 1 | sed -e 's/<version>\(.*\)<\/version>/\1/')
    echo $version
}
function compress_plugins() {
    version=$(get_version)
    TMPDIR=$(ls -d -w1 target/addax-${version})
    [ -n "$TMPDIR" ] || exit 2

    (
        cd ${TMPDIR} || exit 3
        # should be in target/addax/addax-<version>
        [ -d shared ] || mkdir shared

        for jar in $(find  plugin/*/*/libs -type f -name *.jar)
        do
            plugin_dir=$(dirname $jar)
            file_name=$(basename $jar)
            # 1. move it to shared folder
            /bin/mv -f ${jar} shared/
            # 2. create symbol link
            ( cd ${plugin_dir} && ln -sf ../../../../shared/${file_name} $file_name )
        done
    )
    echo "The addax-${version} is ready to be compressed"
}

version=$(get_version)
export MAVEN_OPTS="-DskipTests -Dmaven.javadoc.skip=true -Dmaven.source.skip=true -Dgpg.skip=true"
# first compile basic images
mvn -B -V -T 1  clean package
mvn package -Pdistribution
compress_plugins

# write a simple Dockerfile for build
cat > /tmp/Dockerfile <<EOF
FROM eclipse-temurin:17-jre-noble
LABEL maintainer="wgzhao <wgzhao@gmail.com>"
LABEL version="$version"
LABEL description="Addax is a versatile open-source ETL tool that can seamlessly transfer data between various RDBMS and NoSQL databases, making it an ideal solution for data migration."
COPY addax-${version} /opt/addax
WORKDIR /opt/addax
RUN chmod +x bin/*.sh
EOF

# build it
docker build -t quay.io/wgzhao/addax:${version} -f /tmp/Dockerfile target/addax
docker tag quay.io/wgzhao/addax:${version} quay.io/wgzhao/addax:latest
docker push quay.io/wgzhao/addax:${version}
docker push quay.io/wgzhao/addax:latest
