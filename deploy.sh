#!/bin/bash
version=$(head -n10 pom.xml|grep '<version>' |tr -d '<version>/ \t') 
fname="datax-${version}.tar.gz"
DESTDIR="/opt/infalog"
IP=10.60.192.12

function to_deploy
{
  # copy new package to machine
  scp target/${fname} ${IP}:/tmp/
  ssh $IP "cd ${DESTDIR} && rm -f datax; rm -rf datax-*; tar -xzf /tmp/${fname} && ln -sf datax-${version} datax; chown -R hive:hadoop datax-${version}"
  return $?
}

if [ ! -d target/${fname} ];then
  echo "target ${fname} not exists, check your build output"
  exit 65
fi

if [ "$CI_ENVIRONMENT_NAME" = "stage" ];then
  echo "Deploying to stage environment......"
  to_deploy
  exit $?
elif [ "$CI_ENVIRONMENT_NAME" = "prod" ];then
  echo "Deploying to production environment......"
  IP=10.60.242.211
  to_deploy
  exit $?
else
  echo "Unknown environment"
  exit 16
fi

