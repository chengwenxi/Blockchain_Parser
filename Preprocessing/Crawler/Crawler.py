"""A client to interact with node and to save data to mongo."""
import random
import threading
from queue import Queue

from pymongo import MongoClient
from Preprocessing.Crawler import crawler_util
import requests
import json
import sys
import os
import logging
import time
import tqdm

sys.path.append(os.path.realpath(os.path.dirname(__file__)))

os.environ['BLOCKCHAIN_MONGO_DATA_DIR'] = "D:/util/mongodb-win32-x86_64-3.0.6/DATA"
DIR = os.environ['BLOCKCHAIN_MONGO_DATA_DIR']
LOGFIL = "crawler.log"
if "BLOCKCHAIN_ANALYSIS_LOGS" in os.environ:
    LOGFIL = "{}/{}".format(os.environ['BLOCKCHAIN_ANALYSIS_LOGS'], LOGFIL)
crawler_util.refresh_logger(LOGFIL)
logging.basicConfig(filename=LOGFIL, level=logging.DEBUG)
logging.getLogger("urllib3").setLevel(logging.WARNING)


class Crawler(object):
    """
    A client to migrate blockchain from geth to mongo.

    Description:
    ------------
    Before starting, make sure geth is running in RPC (port 8545 by default).
    Initializing a Crawler object will automatically scan the blockchain from
    the last block saved in mongo to the most recent block in geth.

    Parameters:
    -----------
    rpc_port: <int> default 8545 	# The port on which geth RPC can be called
    host: <string> default "http://localhost" # The geth host
    start: <bool> default True		# Create the graph upon instantiation

    Usage:
    ------
    Default behavior:
        crawler = Crawler()

    Interactive mode:
        crawler = Crawler(start=False)

    Get the data from a particular block:
        block = crawler.getBlock(block_number)

    Save the block to mongo. This will fail if the block already exists:
        crawler.saveBlock(block)

    """

    def __init__(
            self,
            start=True,
            rpc_port=8545,
            host="http://116.62.62.39",
            delay=0.0001,
            mongo_host="127.0.0.1",
            mongo_post=27017
    ):
        """Initialize the Crawler."""
        logging.debug("Starting Crawler")
        self.url = "{}:{}".format(host, rpc_port)
        self.headers = {"content-type": "application/json"}

        # Initializes to default host/port = localhost/27017
        self.account_db = crawler_util.initMongo(MongoClient(mongo_host, mongo_post), "account")
        self.block_db = crawler_util.initMongo(MongoClient(mongo_host, mongo_post), "block")
        self.tx_db = crawler_util.initMongo(MongoClient(mongo_host, mongo_post), "transactions")
        # The max block number that is in mongo
        self.max_block_mongo = None
        # The max block number in the public blockchain
        self.max_block_geth = None
        # Record errors for inserting block data into mongo
        self.insertion_errors = list()
        # Make a stack of block numbers that are in mongo
        # self.block_queue = crawler_util.makeBlockQueue(self.block_db)
        # The delay between requests to geth
        self.delay = delay

        self.block_data = Queue()
        self.blocks = Queue()
        self.txs = Queue()

        if start:
            self.max_block_mongo = self.highestBlockMongo()
            self.max_block_geth = self.highestBlockEth()
            self.run()

    def _rpcRequest(self, method, params, key):
        """Make an RPC request to geth on port 8545."""

        payload = {
            "method": method,
            "params": params,
            "jsonrpc": "2.0",
            "id": 1
        }
        time.sleep(self.delay)
        res = requests.post(
            self.url,
            data=json.dumps(payload),
            headers=self.headers).json()
        return res[key]

    def get_block(self, n):
        """Get a specific block from the blockchain and filter the data."""
        data = self._rpcRequest("eth_getBlockByNumber", [hex(n), True], "result")
        return data

    def get_balance(self, account):
        """Get a specific block from the blockchain and filter the data."""
        data = self._rpcRequest("eth_getBalance", [account, "latest"], "result")
        return int(data, 16)

    def highestBlockEth(self):
        """Find the highest numbered block in geth."""
        num_hex = self._rpcRequest("eth_blockNumber", [], "result")
        return int(num_hex, 16)

    def save_block(self, block):
        """Insert a given parsed block into mongo."""
        e = crawler_util.insert(self.block_db, block)
        if e:
            self.insertion_errors.append(e)

    def save_transactions(self, txs):
        """Insert a given parsed transactions into mongo."""
        e = crawler_util.insert(self.tx_db, txs)
        if e:
            self.insertion_errors.append(e)

    def save_account(self, account):
        """Insert a given parsed transactions into mongo."""
        e = crawler_util.save(self.account_db, account)
        if e:
            self.insertion_errors.append(e)

    def find_account(self, account):
        return crawler_util.find(self.account_db, account)

    def highestBlockMongo(self):
        """Find the highest numbered block in the mongo database."""
        highest_block = crawler_util.highestBlock(self.block_db)
        logging.info("Highest block found in mongodb:{}".format(highest_block))
        return highest_block

    def add_block(self, n):
        """Add a block to mongo."""
        b = self.get_block(n)
        if b:
            self.block_data.put(b)
            time.sleep(0.002)
        else:
            self.save_block({"number": n, "transactions": []})

    def run(self):
        """
        Run the process.

        Iterate through the blockchain on geth and fill up mongodb
        with block data.
        """
        logging.debug("Processing geth blockchain:")
        logging.info("Highest block found as: {}".format(self.max_block_geth))

        # Make sure the database isn't missing any blocks up to this point

        for i in range(0, 5):
            t = threading.Thread(target=self.add_tx)
            t.setDaemon(True)
            t.start()
        for i in range(0, 2):
            t = threading.Thread(target=self.store_block)
            t.setDaemon(True)
            t.start()
        for i in range(0, 2):
            t = threading.Thread(target=self.decode_block)
            t.setDaemon(True)
            t.start()
        logging.debug("Verifying that mongo isn't missing any blocks...")
        # if len(self.block_queue) > 0:
        #     print("Looking for missing blocks...")
        #     self.max_block_mongo = self.block_queue.pop()
        #     for n in tqdm.tqdm(range(1, self.max_block_mongo)):
        #         if len(self.block_queue) == 0:
        #             # If we have reached the max index of the queue,
        #             # break the loop
        #             break
        #         else:
        #             # -If a block with number = current index is not in
        #             # the queue, add it to mongo.
        #             # -If the lowest block number in the queue (_n) is
        #             # not the current running index (n), then _n > n
        #             # and we must add block n to mongo. After doing so,
        #             # we will add _n back to the queue.
        #             _n = self.block_queue.popleft()
        #             if n != _n:
        #                 self.add_block(n)
        #                 self.block_queue.appendleft(_n)
        #                 logging.info("Added block {}".format(n))

        # Get all new blocks
        print("Processing remainder of the blockchain...")
        for n in tqdm.tqdm(range(self.max_block_mongo, self.max_block_geth)):
            self.add_block(n)

        print("Done!\n")

    def add_tx(self):
        while True:
            if self.txs.qsize() <= 0:
                time.sleep(0.5)
                continue
            if self.txs.qsize() > 20:
                print("txs count: ", self.txs.qsize())
            t = self.txs.get()
            if t and len(t) > 0:
                self.save_transactions(t)
            for tx in t:
                _creates = tx.get("creates", None)
                if _creates is not None:
                    self.save_account({"_id": _creates, "type": "contract"})
                _from = tx.get("from", None)
                if _from is not None:
                    _account = self.find_account(_from)
                    if _account is not None and "type" in _account:
                        self.save_account(
                            {"_id": _from, "type": _account.get('type'), "balance": self.get_balance(_from)})
                    else:
                        self.save_account(
                            {"_id": _from, "type": "normal", "balance": self.get_balance(_from) / (10 ** 17)})

                _to = tx.get("to", None)
                if _to is not None:
                    _account = self.find_account(_to)
                    if _account is not None and "type" in _account:
                        self.save_account({"_id": _to, "type": _account.get('type'), "balance": self.get_balance(_to)})
                    else:
                        self.save_account({"_id": _from, "type": "normal", "balance": self.get_balance(_from)})

    def store_block(self):
        while True:
            if self.blocks.qsize() <= 0:
                time.sleep(0.5)
                continue
            if self.blocks.qsize() > 20:
                print("blocks count: ", self.blocks.qsize())
            b = self.blocks.get()
            self.save_block(b)

    def decode_block(self):
        while True:
            if self.block_data.qsize() <= 0:
                time.sleep(0.5)
                continue
            if self.block_data.qsize() > 20:
                print("block_data count: ", self.block_data.qsize())
            data = self.block_data.get()
            block, transactions = crawler_util.decodeBlock(data)
            self.blocks.put(block)
            self.txs.put(transactions)
