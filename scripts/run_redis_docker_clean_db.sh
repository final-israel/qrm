#!/usr/bin/env bash
sudo service redis-server stop
sudo docker stop qrm
sudo docker rm qrm
sudo docker run --name qrm -p 6379:6379 -d redis
