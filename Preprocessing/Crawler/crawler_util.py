"""Util functions for interacting with geth and mongo."""
import pymongo
from collections import deque
import os

DB_NAME = "blockchain"


# mongodb
# -------
def initMongo(client, collection):
    db = client[DB_NAME]
    try:
        db.create_collection(collection)
    except:
        pass
    return db[collection]


def insert(client, d):
    try:
        client.insert(d)
        return None
    except Exception as err:
        pass


def save(client, d):
    try:
        client.save(d)
        return None
    except Exception as err:
        pass


def find(client, _id):
    return client.find_one(_id)


def highestBlock(client):
    n = client.find_one(sort=[("number", pymongo.DESCENDING)])
    if not n:
        # If the database is empty, the highest block # is 0
        return 0
    assert "number" in n, "Highest block is incorrectly formatted"
    return n["number"]


def makeBlockQueue(client):
    """
    Form a queue of blocks that are recorded in mongo.

    Params:
    -------
    client <mongodb Client>

    Returns:
    --------
    <deque>
    """
    queue = deque()
    all_n = client.find({}, {"number": 1, "_id": 0},
                        sort=[("number", pymongo.ASCENDING)])
    for i in all_n:
        queue.append(i["number"])
    return queue

def refresh_logger(filename):
    """Remove old logs and create new ones."""
    if os.path.isfile(filename):
        try:
            os.remove(filename)
        except Exception:
            pass
    open(filename, 'a').close()
