$env:PORT = "8003"

$env:MASTER_NODE_URL = "http://localhost:8001"

$env:VCAP_APPLICATION='{
    "application_name": "datanode-1",
    "application_uris": [
        "http://localhost:8003"
    ],
    "application_id": "node-1"
}'


python main/server.py



