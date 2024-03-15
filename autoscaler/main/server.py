import json
import os
import subprocess

from flask import Flask

app = Flask(__name__)

NODE_NUMS = 1


@app.route("/")
def hello_world():
    return {"active": True}

@app.route("/start-process")
def start_process():
    global NODE_NUMS
    NODE_NUMS += 1
    my_env = os.environ.copy()
    port = 8000 + NODE_NUMS
    print("PORT :", port)
    my_env["PORT"] = f"{port}"
    my_env["MASTER_NODE_URL"] = "http://localhost:8001"
    my_env["VCAP_APPLICATION"] = json.dumps({
        "application_name": "datanode-" + str(NODE_NUMS),
        "application_uris": [
            "http://localhost:" + str(port)
        ],
        "application_id": "node-" + str(NODE_NUMS)
    })
    print(os.getcwd())
    os.chdir("../../datanode")
    subprocess.Popen("python main/server.py", env=my_env , shell=True)

    return {"started": True}
    pass


if __name__ == "__main__":
    PORT = os.getenv('PORT', 8080)

    app.run(debug=True, port=PORT, host="0.0.0.0")
