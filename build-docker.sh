#!/bin/bash
# create addax docker image
# the image including two type default and basic
# default: addax with all modules
# basic: addax with basic modules

function get_version() {
    version=$(grep -o -E '<version>.*</version>' pom.xml | head -n 1 | sed -e 's/<version>\(.*\)<\/version>/\1/')
    echo $version
}
function compress_plugins() {
    version=$(get_version)
    TMPDIR=$(ls -d -w1 target/addax/addax-${version})
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
mvn -B -V -T 1  clean package -Pbasic
mvn package assembly:single -Pbasic
compress_plugins

# write a simple Dockerfile for build
cat > /tmp/Dockerfile <<EOF
FROM openjdk:8-jre-alpine
LABEL maintainer="wgzhao <wgzhao@gmail.com>"
LABEL version="$version"
LABEL description="Addax is a versatile open-source ETL tool that can seamlessly transfer data between various RDBMS and NoSQL databases, making it an ideal solution for data migration."
COPY addax-${version} /opt/addax
WORKDIR /opt/addax
RUN chmod +x bin/*.sh
EOF

# build it
docker build -t quay.io/wgzhao/addax:${version}-lite -f /tmp/Dockerfile target/addax
docker tag quay.io/wgzhao/addax:${version}-lite quay.io/wgzhao/addax:latest-lite
docker push quay.io/wgzhao/addax:${version}-lite
docker push quay.io/wgzhao/addax:latest-lite

# then compile default images
mvn -B -V -T 1  clean package -Pdefault
mvn package assembly:single -Pdefault
compress_plugins

# write a simple Dockerfile for build
cat > /tmp/Dockerfile <<EOF
FROM openjdk:8-jre-alpine
LABEL maintainer="wgzhao <wgzhao@gmail.com>"
LABEL version="${version}"
LABEL description="Addax is a versatile open-source ETL tool that can seamlessly transfer data between various RDBMS and NoSQL databases, making it an ideal solution for data migration."
COPY addax-${version} /opt/addax
WORKDIR /opt/addax
RUN chmod +x bin/*.sh
EOF

# build it
docker build -t quay.io/wgzhao/addax:${version} -f /tmp/Dockerfile target/addax
docker tag quay.io/wgzhao/addax:${version} quay.io/wgzhao/addax:latest

# push
docker push quay.io/wgzhao/addax:${version}
docker push quay.io/wgzhao/addax:latest