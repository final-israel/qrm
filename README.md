# qrm
Queue Resources Manager

# management server
Add resource to qrm:

```bash
curl --header "Content-Type: application/json" --request POST --data '[{"name": "resource_2", "type": "server"}]'  http://localhost:8080/add_resources
```

Remove resource from qrm:

```bash
curl --header "Content-Type: application/json" --request POST --data '[{"name": "resource_2", "type": "server"}]'  http://localhost:8080/remove_resources
```

Set resource status:

```bash
curl --header "Content-Type: application/json" --request POST --data '{"resource_name": "resource_2", "status": "active"}'  http://localhost:8080/set_resource_status
curl --header "Content-Type: application/json" --request POST --data '{"resource_name": "resource_2", "status": "disabled"}'  http://localhost:8080/set_resource_status
```

Add job to resource:

```bash
curl --header "Content-Type: application/json" --request POST --data '{"resource_name": "resource_1", "job": {"token": 1, "job_name": "foo"}}'  http://localhost:8080/add_job_to_resource

````

Remove job from resources:

```bash
curl --header "Content-Type: application/json" --request POST --data '{"token": 1, "resources": ["resource_1"]}'  http://localhost:8080/remove_job
```


Set server status (control the global qrm state):

```bash
curl --header "Content-Type: application/json" --request POST --data '{"status": "disabled"}'  http://localhost:8080/set_server_status
curl --header "Content-Type: application/json" --request POST --data '{"status": "active"}'  http://localhost:8080/set_server_status
```

Show status of the server with it's resources and their jobs url:
```console
http://127.0.0.1:8080/status
```
