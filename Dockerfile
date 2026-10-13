# Two entry points share one runtime base so the images they produce stay interchangeable:
#   --target release      payload is the assembled distribution taken from the build
#                         context; nothing is compiled. CI builds this from the tree it
#                         just assembled, so the image content matches the release archive.
#   default (from-source) full compilation inside the container, for `docker build .` from
#                         a clone. Must stay the LAST stage: it is the default target.
FROM maven:3.8.3-openjdk-17 AS build

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

FROM eclipse-temurin:17-jre-noble AS base
LABEL maintainer="wgzhao <wgzhao@gmail.com>"
LABEL version="latest"
LABEL description="Actively maintained successor to Alibaba DataX — a fast, versatile, open-source ETL tool for 30+ RDBMS and NoSQL data sources."

WORKDIR /opt/addax

# release path: the context provides the assembled addax-<version>/ directory. The chmod
# doubles as an assertion -- a context that only ships the tarball has no bin/ to chmod
# and fails the build instead of producing a broken image.
FROM base AS release
COPY addax-* /opt/addax/
RUN chmod 755 /opt/addax/bin/*

# default path
FROM base AS from-source
COPY --from=build  /src/target/addax-* /opt/addax/
RUN chmod 755 /opt/addax/bin/*
