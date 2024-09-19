# define an alias
FROM maven:3.6-jdk-8 as build

COPY . /src
WORKDIR /src

ARG type="basic"
ENV package_type=$type

RUN <<EOF
    mvn clean package -P${package_type} -DskipTests 
    mvn package assembly:single 
    ./shrink_package.sh 
EOF

FROM openjdk:8u232-jre-stretch
LABEL maintainer="wgzhao <wgzhao@gmail.com>"
LABEL version="4.1.8"
LABEL description="Addax is a versatile open-source ETL tool that can seamlessly transfer data between various RDBMS and NoSQL databases, making it an ideal solution for data migration."

COPY --from=build  /src/target/addax/addax-* /opt/addax/

WORKDIR /opt/addax

RUN chmod 755 /opt/addax/bin/*

