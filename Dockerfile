# define an alias
FROM maven:3.6-jdk-8 as build

COPY . /src
WORKDIR /src

ARG type="basic"
ENV package_type=$type

RUN <<EOF
  mkdir /root/.m2 
  echo '<?xml version="1.0" encoding="UTF-8"?><settings xmlns="http://maven.apache.org/SETTINGS/1.0.0" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xsi:schemaLocation="http://maven.apache.org/SETTINGS/1.0.0 http://maven.apache.org/xsd/settings-1.0.0.xsd"><mirrors><mirror><id>tencent</id><mirrorOf>*</mirrorOf><name>tencent cloud</name><url>https://mirrors.cloud.tencent.com/nexus/repository/maven-public/</url></mirror></mirrors></settings>' >/root/.m2/settings.xml 
    mvn clean package -P${package_type} -DskipTests 
    mvn package assembly:single 
    ./shrink_package.sh 
    rm -f target/addax/addax-*.tar.gz
EOF

FROM openjdk:8u232-jre-stretch
COPY --from=build  /src/target/addax/addax-* /opt/addax/

WORKDIR /opt/addax

RUN chmod 755 /opt/addax/bin/*

