from flask import Flask
from flask import request
from Preprocessing.Crawler import crawler_util
from pymongo import MongoClient
import json
from configparser import ConfigParser

app = Flask(__name__)

cp = ConfigParser()
cp.read('../app.config')
mongo_host = cp.get('mongodb', 'host')
mongo_post = int(cp.get('mongodb', 'post'))
account_db = crawler_util.initMongo(MongoClient(mongo_host, mongo_post), "account")
block_db = crawler_util.initMongo(MongoClient(mongo_host, mongo_post), "block")
tx_db = crawler_util.initMongo(MongoClient(mongo_host, mongo_post), "transactions")
receipt_db = crawler_util.initMongo(MongoClient(mongo_host, mongo_post), "receipt")


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
        for item in receipt_db.find({"$or": [{"from": addr}, {"to": addr}]}).limit(size).skip(page * size):
            del item["_id"]
            accounts.append(item)
    else:
        for item in tx_db.find().limit(size).skip(page * size):
            del item["_id"]
            accounts.append(item)
    return json.dumps(accounts)


@app.route('/receipt', methods=['GET'])
def receipt():
    args = request.args
    transaction_hash = args.get("tx", None)
    result = {}
    if transaction_hash is not None:
        for item in receipt_db.find({"transactionHash": transaction_hash}):
            result = item
            break
    return json.dumps(result)


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=3434, debug=True)
