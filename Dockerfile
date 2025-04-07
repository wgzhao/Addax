# define an alias
FROM maven:3.8.3-jdk-8 AS build

ARG use_cn=0

RUN if [ "$use_cn" = "1" ]; then \
        echo "Configuring Maven to use Aliyun (China) mirrors"; \
        mkdir -p /root/.m2; \
        echo '<settings xmlns="http://maven.apache.org/SETTINGS/1.0.0"' > /root/.m2/settings.xml; \
        echo '    xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"' >> /root/.m2/settings.xml; \
        echo '    xsi:schemaLocation="http://maven.apache.org/SETTINGS/1.0.0' >> /root/.m2/settings.xml; \
        echo '                        http://maven.apache.org/xsd/settings-1.0.0.xsd">' >> /root/.m2/settings.xml; \
        echo '  <mirrors>' >> /root/.m2/settings.xml; \
        echo '    <mirror>' >> /root/.m2/settings.xml; \
        echo '      <id>nexus-tencentyun</id>' >> /root/.m2/settings.xml; \
        echo '      <mirrorOf>central</mirrorOf>' >> /root/.m2/settings.xml; \
        echo '      <name>Nexus tencentyun</name>' >> /root/.m2/settings.xml; \
        echo '      <url>http://mirrors.cloud.tencent.com/nexus/repository/maven-public/</url>' >> /root/.m2/settings.xml; \
        echo '    </mirror>' >> /root/.m2/settings.xml; \
        echo '  </mirrors>' >> /root/.m2/settings.xml; \
        echo '</settings>' >> /root/.m2/settings.xml; \
    fi

COPY . /src
WORKDIR /src

RUN <<EOF
    export MAVEN_OPTS="-DskipTests -Dmaven.javadoc.skip=true -Dmaven.source.skip=true -Dgpg.skip=true"
    mvn clean package
    mvn package -Pdistribution
    ./shrink_package.sh 
EOF

FROM openjdk:8u232-jre-stretch
LABEL maintainer="wgzhao <wgzhao@gmail.com>"
LABEL version="latest"
LABEL description="Addax is a versatile open-source ETL tool that can seamlessly transfer data between various RDBMS and NoSQL databases, making it an ideal solution for data migration."

COPY --from=build  /src/target/addax-* /opt/addax/

WORKDIR /opt/addax

RUN chmod 755 /opt/addax/bin/*

