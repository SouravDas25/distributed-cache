param (
    [Parameter(Mandatory=$true)][int]$node_no
 )


$env:CF_INSTANCE_INDEX="$node_no"

$env:PORT = "800${node_no}"

Write-Output "PORT : ${env:PORT}"

$env:MASTER_NODE_URL = "http://localhost:8091"

$MyJsonHashTable = @{
    application_name="datanode-$node_no";
    application_uris= @( "http://localhost:$env:PORT" );
    application_id="node-$node_no";
}

$env:VCAP_APPLICATION= $MyJsonHashTable | ConvertTo-Json

Write-Output "VCAP_APPLICATION : ${env:VCAP_APPLICATION}"

python main/server.py



