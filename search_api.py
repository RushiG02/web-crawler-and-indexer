from flask import Flask, request, jsonify
from cassandra.cluster import Cluster

app = Flask(__name__)
cluster = Cluster(['localhost'])
session = cluster.connect('webcrawler')

@app.route("/search", methods=["GET"])
def search():
    query = request.args.get("query")
    rows = session.execute("SELECT url FROM index_table WHERE word = %s", (query,))
    return jsonify({"results": [row.url for row in rows]})

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000, debug=True)
