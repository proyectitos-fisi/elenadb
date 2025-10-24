#!/bin/bash

docker build --no-cache -f Dockerfile -t elenadb .
docker tag elenadb:latest paoloose/elenadb:latest
docker push paoloose/elenadb:latest

docker build --no-cache -f Dockerfile.ttyd -t ttyd-elenadb .
docker tag ttyd-elenadb:latest paoloose/ttyd-elenadb:latest
docker push paoloose/ttyd-elenadb:latest
