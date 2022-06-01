#!/usr/bin/env bash


curl --header "Content-Type: application/json" --request POST --data '[{"name": "res1", "type": "server", "tags": ["server", "res1"]}]'  http://localhost:8080/add_resources
curl --header "Content-Type: application/json" --request POST --data '{"resource_name": "res1", "status": "active" }'  http://localhost:8080/set_resource_status


curl --header "Content-Type: application/json" --request POST --data '[{"name": "res2", "type": "server", "tags": ["server", "res2"]}]'  http://localhost:8080/add_resources
curl --header "Content-Type: application/json" --request POST --data '{"resource_name": "res2", "status": "active" }'  http://localhost:8080/set_resource_status

curl --header "Content-Type: application/json" --request POST --data '{"status": "active"}'  http://localhost:8080/set_server_status
