import json
import os
import sys

# package resolution
SCRIPT_DIR = os.path.dirname(os.path.abspath(os.path.join(__file__, "../../")))
print("package_path :", SCRIPT_DIR)
sys.path.append(SCRIPT_DIR)

from flask import Flask, request

from cache import DistributedCache, CacheConfig
from datanodes.network_datanode import NetworkDataNode

config = CacheConfig()
app = Flask(__name__)
distributed_cache = DistributedCache(config)


@app.route("/")
def hello_world():
    return "<p>Hello, World!</p>"


@app.route("/post-new-instance", methods=["POST"])
def post_new_instance():
    data = request.get_json()
    datanode = NetworkDataNode(data["application_name"], data["instanceId"], data["application_url"], data["appId"])
    distributed_cache.ring.add_free_node(datanode)
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
    if distributed_cache.has(key):
        return distributed_cache.get(key)
    return app.response_class(
        response="not found",
        status=404,
        mimetype='application/json'
    )


@app.route('/contains/<key>', methods=['GET'])
def contains(key):
    if distributed_cache.has(key):
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
    distributed_cache.remove(key)
    return app.response_class(
        response=json.dumps({"status": "removed successfully"}),
        status=200,
        mimetype='application/json'
    )


@app.route('/save/<key>', methods=['POST'])
def save(key):
    value = request.get_json()
    try:
        distributed_cache.put(key, value)
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


@app.route('/status/', methods=['GET'])
def status():
    try:
        data = distributed_cache.status()
        return_msg = json.dumps(data)
        status = 200
    except Exception as e:
        print("Error in status: ", e)
        return_msg = json.dumps({"status": "failed"})
        status = 400
    return app.response_class(
        response=return_msg,
        status=status,
        mimetype='application/json'
    )


if __name__ == "__main__":
    PORT = os.getenv('PORT', 8091)
    app.run(debug=False, port=PORT, host="0.0.0.0")
