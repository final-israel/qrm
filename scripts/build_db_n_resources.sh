#!/usr/bin/env bash

# $1: number of resources to add

for (( res=1; res<=$1; res++ ))
do     
       	res_name=resource_"$res"
	echo $res_name
        curl --header "Content-Type: application/json" --request POST --data '[{"name": '\"$res_name\"', "type": "server"}]'  http://localhost:8080/add_resources
	curl --header "Content-Type: application/json" --request POST --data '{"resource_name": '\"$res_name\"', "status": "active"}'  http://localhost:8080/set_resource_status
done

curl --header "Content-Type: application/json" --request POST --data '{"status": "active"}'  http://localhost:8080/set_server_status
