#!/bin/bash
mvn gpg:sign-and-deploy-file \
 -DgroupId=com.wgzhao.datax -DartifactId=mysqlreader \
 -Dversion=3.2.1 -DrepositoryId=ossrh -Dpackaging=zip \
 -Durl=https://oss.sonatype.org/service/local/staging/deploy/maven2/ \
 -DpomFile=./plugin/reader/mysqlreader/pom.xml \
 -Dsources=./plugin/reader/mysqlreader/target/mysqlreader-3.2.1-sources.jar \
 -Djavadoc=./plugin/reader/mysqlreader/target/mysqlreader-3.2.1-javadoc.jar \
 -Dfile=./plugin/reader/mysqlreader/target/mysqlreader-3.2.1.zip \
 -Dfile=./plugin/reader/mysqlreader/target/mysqlreader-3.2.1.jar

#mvn deploy:sign-and-deploy-file \
