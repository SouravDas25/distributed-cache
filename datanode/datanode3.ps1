$env:PORT = "8004"

$env:MASTER_NODE_URL = "http://localhost:8001"

$env:VCAP_APPLICATION='{
    "application_name": "datanode-2",
    "application_uris": [
        "http://localhost:8004"
    ],
    "application_id": "node-2"
}'


python main/server.py



