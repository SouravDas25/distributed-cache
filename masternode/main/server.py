import json
import os

from flask import Flask, request

from cache import DistributedCache
from datanodes.network_datanode import NetworkDataNode

app = Flask(__name__)
distributedCache = DistributedCache()


@app.route("/")
def hello_world():
    return "<p>Hello, World!</p>"


@app.route("/post-new-instance", methods=["POST"])
def post_new_instance():
    data = request.get_json()
    datanode = NetworkDataNode(data["application_name"], data["instanceId"], data["application_url"], data["appId"])
    distributedCache.serverCluster.addFreeInstance(datanode.appId, datanode)
    print("Discovered instance", data)
    return json.dumps({"success": True})


@app.route("/post-copy-completion", methods=["POST"])
def post_copy_completion():
    data = request.get_json()
    print("copy completion: ", data)
    # cache.serverCluster.copyCompleted(data["name"])
    return {'name': "Copied successfully"}


@app.route('/retrieve/<key>', methods=['GET'])
def retrieve(key):
    if distributedCache.has(key):
        return distributedCache.get(key)
    return app.response_class(
        response="not found",
        status=404,
        mimetype='application/json'
    )


@app.route('/contains/<key>', methods=['GET'])
def contains(key):
    if distributedCache.has(key):
        return app.response_class(
            response='{"available": true}',
            status=200,
            mimetype='application/json'
        )
    return app.response_class(
        response='{"available": false}',
        status=404,
        mimetype='application/json'
    )


@app.route('/remove/<key>', methods=['POST'])
def remove(key):
    distributedCache.remove(key)
    return app.response_class(
        response=json.dumps({"status": "removed successfully"}),
        status=200,
        mimetype='application/json'
    )


@app.route('/save/<key>', methods=['POST'])
def save(key):
    value = request.get_json()
    try:
        distributedCache.put(key, value)
        return_msg = json.dumps({"status": "saved"})
        status = 200
    except:
        return_msg = json.dumps({"status": "failed"})
        status = 400
    return app.response_class(
        response=return_msg,
        status=status,
        mimetype='application/json'
    )


if __name__ == "__main__":
    PORT = os.getenv('PORT', 8001)
    app.run(debug=False, port=PORT, host="0.0.0.0")
