from flask import Flask
from flask import request
from Preprocessing.Crawler import crawler_util
from pymongo import MongoClient
import json

app = Flask(__name__)

mongo_host = "127.0.0.1"
mongo_post = 27017
account_db = crawler_util.initMongo(MongoClient(mongo_host, mongo_post), "account")
block_db = crawler_util.initMongo(MongoClient(mongo_host, mongo_post), "block")
tx_db = crawler_util.initMongo(MongoClient(mongo_host, mongo_post), "transactions")


@app.route('/')
def index_html():
    return "hello, welcome ethermint"


@app.route('/account', methods=['GET'])
def account():
    args = request.args
    page = int(args.get("page", 0))
    size = int(args.get("size", 20))

    accounts = []
    for item in account_db.find().limit(size).skip(page * size):
        accounts.append(item)
    return json.dumps(accounts)


@app.route('/tx', methods=['GET'])
def tx():
    args = request.args
    page = int(args.get("page", 0))
    size = int(args.get("size", 20))
    addr = args.get("account", None)

    accounts = []
    if addr is not None:
        for item in tx_db.find({"$or": [{"from": addr}, {"to": addr}]}).limit(size).skip(page * size):
            del item["_id"]
            accounts.append(item)
    else:
        for item in tx_db.find().limit(size).skip(page * size):
            del item["_id"]
            accounts.append(item)
    return json.dumps(accounts)


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=80, debug=True)
