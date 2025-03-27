#!/bin/bash
#
# BASH script to build docker image:
#
read -r image < ./docker/_image-name.txt
#
# remove previous image:
#
if docker manifest inspect "$image" > /dev/null 2>&1; then
  docker rmi "$image"
fi
#
# build new image:
#
docker build -t "$image" ./docker
