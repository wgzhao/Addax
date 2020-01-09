#!/bin/bash
if [ "$CI_ENVIRONMENT_NAME" = "stage" ];then
  echo "Deploying to stage environment......"
  exit 0
elif [ "$CI_ENVIRONMENT_NAME" = "prod" ];then
  echo "Deploying to production environment......"
  exit 0
else
  echo "Unknown environment"
  exit 16
fi
