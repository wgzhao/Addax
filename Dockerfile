# define an alias
FROM docker.m.daocloud.io/library/maven:3.8.3-jdk-8 AS build

COPY . /src
WORKDIR /src

RUN <<EOF
    export MAVEN_OPTS="-DskipTests -Dmaven.javadoc.skip=true -Dmaven.source.skip=true -Dgpg.skip=true"
    mvn clean package
    mvn package -Pdistribution
    ./shrink_package.sh 
EOF

FROM docker.m.daocloud.io/library/openjdk:8u232-jre-stretch
LABEL maintainer="wgzhao <wgzhao@gmail.com>"
LABEL version="latest"
LABEL description="Addax is a versatile open-source ETL tool that can seamlessly transfer data between various RDBMS and NoSQL databases, making it an ideal solution for data migration."

COPY --from=build  /src/target/addax-* /opt/addax/

WORKDIR /opt/addax

RUN chmod 755 /opt/addax/bin/*

