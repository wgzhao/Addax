#!/bin/bash
# compile specify module and copy to specify directory
version=$(head -n10 pom.xml | awk -F'[<>]' '/<version>/ {print $3; exit}')
mvn_opts="-Dmaven.test.skip=true -Dmaven.javadoc.skip=true -Dmaven.source.skip=true --quiet"
function build_base() {
    cd $SRC_DIR
    mvn package -B $mvn_opts -pl :addax-core,:addax-rdbms,:addax-storage,:addax-transformer -am || exit 1
    rsync -a core/target/addax/* ${ADDAX_HOME}
    rsync -azv lib/addax-{rdbms,storage,transformer}/target/addax/lib/* ${ADDAX_HOME}/lib/
}

if [ -z "$SRC_DIR" ]; then
  SRC_DIR=$HOME/code/personal/Addax
fi

if [ -z "$ADDAX_HOME" ]; then
   ADDAX_HOME=/opt/app/addax
fi

if [ -z $1 ]; then
    echo "Usage: $0 module_name"
    exit 1
fi



if [ -n "${REMOTE_HOST}" ]; then
    echo "The building module will upload to ${REMOTE_HOST}"
    ADDAX_HOME=${REMOTE_HOST}:${ADDAX_HOME}
else
    [ -d $ADDAX_HOME ] || mkdir -p $ADDAX_HOME || exit 1
    [ -d $ADDAX_HOME/bin ] || build_base
fi

MODULE_NAME=$1

cd $SRC_DIR
mvn package -B $mvn_opts -pl :$MODULE_NAME -am  || exit 1

if [ "$MODULE_NAME" == "addax-core" ]; then
    scp  core/target/${MODULE_NAME}-${version}.jar ${ADDAX_HOME}/lib
    exit 0
fi

if [ "$MODULE_NAME" == "addax-common" ];then
    scp  common/target/${MODULE_NAME}-${version}.jar ${ADDAX_HOME}/lib
    exit 0
fi

if [ "$MODULE_NAME" == "addax-rdbms"  -o "$MODULE_NAME" == "addax-storage" -o "$MODULE_NAME" == "addax-transformer" ]; then
    scp  lib/${MODULE_NAME}/target/${MODULE_NAME}-${version}.jar ${ADDAX_HOME}/lib
    exit 0
fi
# if the module nam ends with reader, then the module base directory is plugin/reader,
# else the directory is plugin/writer
if [[ $MODULE_NAME =~ .*"reader" ]]; then
    MODULE_DIR=plugin/reader
elif [[ $MODULE_NAME =~ .*"writer" ]]; then
    MODULE_DIR=plugin/writer
else
    echo "module name must end with reader or writer"
    exit 1
fi
[ -d $ADDAX_HOME/$MODULE_DIR ] || mkdir -p $ADDAX_HOME/$MODULE_DIR || exit 1

scp  $MODULE_DIR/$MODULE_NAME/target/${MODULE_NAME}-${version}/$MODULE_DIR/${MODULE_NAME} \
       	$ADDAX_HOME/$MODULE_DIR
