$env:PORT = "8002"

$env:MASTER_NODE_URL = "http://localhost:8001"

$env:VCAP_APPLICATION='{
    "application_name": "datanode-0",
    "application_uris": [
        "http://localhost:8002"
    ],
    "application_id": "node-0"
}'


python main/server.py



