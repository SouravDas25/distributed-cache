$env:PORT = "8005"

$env:MASTER_NODE_URL = "http://localhost:8001"

$env:VCAP_APPLICATION='{
    "application_name": "datanode-3",
    "application_uris": [
        "http://localhost:8005"
    ],
    "application_id": "node-3"
}'


python main/server.py



