from flask import Flask, request, Response

from manager.cache import DistributedCache

app = Flask(__name__)
cache = DistributedCache()


@app.route("/")
def hello_world():
    return "<p>Hello, World!</p>"


@app.route('/retrieve/<key>', methods=['GET'])
def retrieve(key):
    if cache.has(key):
        return cache.get(key)
    return app.response_class(
        response="not found",
        status=404,
        mimetype='application/json'
    )


@app.route('/contains/<key>', methods=['GET'])
def contains(key):
    if cache.has(key):
        return app.response_class(
            response="available",
            status=200,
            mimetype='application/json'
        )
    return app.response_class(
        response="not found",
        status=404,
        mimetype='application/json'
    )


@app.route('/save/<key>', methods=['POST'])
def save(key):
    value = request.get_json()
    return_msg = ""
    status = 400
    try:
        cache.put(key, value)
        return_msg = "Success"
        status = 200
    except:
        return_msg = "Failed"
        status = 400
    return app.response_class(
        response=return_msg,
        status=status,
        mimetype='application/json'
    )


if __name__ == "__main__":
    app.run(debug=True, port=8000, host="0.0.0.0")
