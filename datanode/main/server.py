import json
import os
import sys
import time
from datetime import datetime

SCRIPT_DIR = os.path.dirname(os.path.abspath(os.path.join(__file__, "../../")))
print("package_path :", SCRIPT_DIR)
sys.path.append(SCRIPT_DIR)

import psutil
import requests
from apscheduler.schedulers.background import BackgroundScheduler
from flask import Flask, request, Response

from LRUCache import LRUCache



app = Flask(__name__)
cache = LRUCache()

CF_INSTANCE_INDEX = os.environ.get('CF_INSTANCE_INDEX', 0)
# Access VCAP_SERVICES environment variable
VCAP_SERVICES = json.loads(os.getenv('VCAP_SERVICES', '{}'))

# Access VCAP_APPLICATION environment variable
VCAP_APPLICATION = json.loads(os.getenv('VCAP_APPLICATION', '{}'))
MASTER_NODE_URL = os.environ.get('MASTER_NODE_URL', "")

LAST_REQ_TIME = datetime.now()

def post_to_masternode():
    url = f"{MASTER_NODE_URL}/post-new-instance"
    body = {
        "instanceId": CF_INSTANCE_INDEX,
        "appId": VCAP_APPLICATION["application_id"],
        "application_name": VCAP_APPLICATION["application_name"],
        "application_url": f"{VCAP_APPLICATION["application_uris"][0]}"
    }
    print("registering with masternode: ", url)
    print("registering with masternode: ", body)
    try:
        response = requests.post(url, json=body, verify=False)
        print(response.json())
    except Exception as e:
        print("ERROR registering instance : ", e)


def background_ping_job():
    time_difference = datetime.now() - LAST_REQ_TIME
    if time_difference.seconds >= 60:
        post_to_masternode()
    return


def post_copy_completion(name):
    url = f"{MASTER_NODE_URL}/post-copy-completion"
    body = {
        "name": name
    }
    try:
        response = requests.post(url, json=body, verify=False)
        print(response.json())
    except Exception as e:
        print("ERROR registering instance : ", e)
    pass


@app.before_request
def before_request():
    global LAST_REQ_TIME
    LAST_REQ_TIME = datetime.now()
    pass

@app.route("/")
def hello_world():
    return "<p>Hello, World!</p>"


@app.route("/health/")
def health_check():
    return {"status": "active"}


@app.route('/metrics/', methods=['GET'])
def metrics():
    memory = psutil.virtual_memory()
    metrics = {
        "used_memory": memory.used,
        "available_memory": memory.available,
        "percent": memory.percent,
        "size": cache.size(),
        "human_readable_used_memory": f"{memory.used / (1024 * 1024)} MB"
    }
    return metrics


@app.route('/remove/<key>', methods=['GET'])
def remove(key):
    if cache.has(key):
        return cache.remove(key)
    return app.response_class(
        response='{"available": false}',
        status=404,
        mimetype='application/json'
    )


@app.route('/retrieve/<key>', methods=['GET'])
def retrieve(key):
    if cache.has(key):
        return cache.get(key)
    return app.response_class(
        response='{"available": false}',
        status=404,
        mimetype='application/json'
    )


@app.route('/contains/<key>', methods=['GET'])
def contains(key):
    if cache.has(key):
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


@app.route('/save/<key>', methods=['POST'])
def save(key):
    value = request.get_json()
    try:
        cache.put(key, value)
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


@app.route('/calculate-mid-key/', methods=['GET'])
def calculateMidKey():
    midKey = cache.calculateMidKey()
    return {
        'midKey': midKey
    }


@app.post('/copy-keys/')
def copy_keys():
    body = request.get_json()
    print("request body for copy", body)
    targetServer = body['targetServer']
    fromKey = body['fromKey']
    toKey = body['toKey']

    cache.copy_keys(targetServer, fromKey, toKey)

    # post_copy_completion(targetServer["name"])
    return {"success": True}


@app.route('/compact-keys/', methods=['GET'])
def compactKeys():
    cache.compact()
    return {"success": True}


if __name__ == "__main__":
    PORT = os.getenv('PORT', 8080)

    scheduler = BackgroundScheduler()
    scheduler.add_job(lambda: background_ping_job(), "interval", seconds=10)
    scheduler.start()
    post_to_masternode()

    app.run(debug=False, port=PORT, host="0.0.0.0")

    scheduler.shutdown()
