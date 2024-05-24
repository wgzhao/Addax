#!/bin/bash
# compile specify module and copy to specify directory
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

MODULE_NAME=$1
cd $SRC_DIR
mvn package -B --quiet -pl :$MODULE_NAME -am -Dmaven.test.skip=true || exit 1
# if the module nam ends with reader, then the module base directory is plugin/reader,
# else the directory is plugin/writer
if [[ $MODULE_NAME =~ .*"reader" ]]; then
    MODULE_DIR=plugin/reader
elif [[ $MODULE_NAME =~ .*"writer" ]]; then
    MODULE_DIR=plugin/writer
else
    MODULE_DIR=""
fi
cp -a $MODULE_DIR/$MODULE_NAME/target/$MODULE_NAME-*/$MODULE_DIR/${MODULE_NAME} \
       	$ADDAX_HOME/$MODULE_DIR
