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
    rm -f target/addax/addax-*.tar.gz
EOF

FROM openjdk:8u232-jre-stretch
COPY --from=build  /src/target/addax/addax-* /opt/addax/

WORKDIR /opt/addax

RUN chmod 755 /opt/addax/bin/*

