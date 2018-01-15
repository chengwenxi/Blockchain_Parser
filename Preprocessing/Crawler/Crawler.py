"""A client to interact with node and to save data to mongo."""
import threading
from configparser import ConfigParser
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
LOGFILE = "crawler.log"
crawler_util.refresh_logger(LOGFILE)
logging.basicConfig(filename=LOGFILE, level=logging.DEBUG)
logging.getLogger("urllib3").setLevel(logging.WARNING)

cp = ConfigParser()
cp.read('../app.config')


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
            url=cp.get('ethermint', 'url'),
            delay=float(cp.get('ethermint', 'delay')),
            mongo_host=cp.get('mongodb', 'host'),
            mongo_post=int(cp.get('mongodb', 'post'))
    ):
        """Initialize the Crawler."""
        logging.debug("Starting Crawler")
        self.url = url
        self.headers = {"content-type": "application/json"}
        self.delay = delay
        # Initializes to default host/port = localhost/27017
        self.account_db = crawler_util.initMongo(MongoClient(mongo_host, mongo_post), "account")
        self.block_db = crawler_util.initMongo(MongoClient(mongo_host, mongo_post), "block")
        self.tx_db = crawler_util.initMongo(MongoClient(mongo_host, mongo_post), "transactions")
        self.receipt_db = crawler_util.initMongo(MongoClient(mongo_host, mongo_post), "receipt")
        # The max block number that is in mongo
        self.max_block_mongo = None
        # The max block number in the public blockchain
        self.max_block_eth = None
        # Record errors for inserting block data into mongo
        self.insertion_errors = list()
        # Make a stack of block numbers that are in mongo
        # self.blocks = crawler_util.makeBlockQueue(self.block_db)
        # The delay between requests to geth
        self.delay = delay
        self.blocks = Queue()
        self.txs = Queue()
        self.receipts = Queue()

        if start:
            self.max_block_mongo = self.highest_block_mongo()
            self.max_block_eth = self.highest_block_eth()
            self.run()

    def rpc_request(self, method, params, key):
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
        data = self.rpc_request("eth_getBlockByNumber", [hex(n), True], "result")
        return data

    def get_receipt(self, tx_hash):
        """Get a specific block from the blockchain and filter the data."""
        data = self.rpc_request("eth_getTransactionReceipt", [tx_hash], "result")
        return data

    def get_balance(self, account):
        """Get a specific block from the blockchain and filter the data."""
        data = self.rpc_request("eth_getBalance", [account, "latest"], "result")
        return data

    def highest_block_eth(self):
        """Find the highest numbered block in geth."""
        num_hex = self.rpc_request("eth_blockNumber", [], "result")
        return int(num_hex, 16)

    def save_block(self, block):
        """Insert a given parsed block into mongo."""
        e = crawler_util.insert(self.block_db, block)
        if e:
            self.insertion_errors.append(e)

    def save_receipt(self, receipt):
        """Insert a given parsed block into mongo."""
        e = crawler_util.insert(self.receipt_db, receipt)
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

    def highest_block_mongo(self):
        """Find the highest numbered block in the mongo database."""
        highest_block = crawler_util.highestBlock(self.block_db)
        logging.info("Highest block found in mongodb:{}".format(highest_block))
        if highest_block is None or highest_block == 0:
            return 0
        else:
            return int(highest_block, 16)

    def add_block(self, n):
        """Add a block to mongo."""
        b = self.get_block(n)
        if b:
            self.blocks.put(b)
            time.sleep(self.delay)
        else:
            self.save_block({"number": n, "transactions": []})

    def run(self):
        """
        Run the process.

        Iterate through the blockchain on geth and fill up mongodb
        with block data.
        """
        logging.debug("Processing geth blockchain:")
        logging.info("Highest block found as: {}".format(self.max_block_eth))

        # Make sure the database isn't missing any blocks up to this point
        for i in range(0, 3):
            t = threading.Thread(target=self.add_tx)
            t.setDaemon(True)
            t.start()
            time.sleep(1)
        logging.debug("Verifying that mongo isn't missing any blocks...")
        print("Processing remainder of the blockchain...")
        for n in tqdm.tqdm(range(self.max_block_mongo, self.max_block_eth)):
            self.add_block(n)

        print("Done!\n")

    def add_tx(self):
        while True:
            if self.blocks.qsize() <= 0:
                time.sleep(0.5)
                continue
            if self.blocks.qsize() > 20:
                print("blocks count: ", self.blocks.qsize())
            block = self.blocks.get()
            if block:
                block, transactions, receipts = self.get_block_detail(block)
                if block is not None and len(block) > 0:
                    self.save_block(block)
                if transactions is not None and len(transactions) > 0:
                    self.save_transactions(transactions)
                if receipts is not None and len(receipts) > 0:
                    self.deal_receipt(receipts)

    def get_block_detail(self, block):
        if block:
            # Filter the block
            block["_id"] = int(block["number"], 16)
        #     block["number"] = int(block["number"], 16)
        #     block["timestamp"] = int(block["timestamp"], 16)
        #     block["difficulty"] = int(block["difficulty"], 16)
        #     block["gasLimit"] = int(block["gasLimit"], 16)
        #     block["gasUsed"] = int(block["gasUsed"], 16)
        #     block["size"] = int(block["size"], 16)
        #     block["totalDifficulty"] = int(block["totalDifficulty"], 16)
        # Filter and decode each transaction and add it back
        # 	Value, gas, and gasPrice are all converted to ether
        transactions = []
        receipts = []
        i = 0
        for t in block["transactions"]:
            t["_id"] = t["hash"]
            # t["value"] = float(int(t["value"], 16))
            # t["blockNumber"] = int(t["blockNumber"], 16)
            # t["gas"] = int(t["gas"], 16)
            # t["gasPrice"] = int(t["gasPrice"], 16)
            # t["nonce"] = int(t["nonce"], 16)
            # t["transactionIndex"] = int(t["transactionIndex"], 16)
            transactions.append(t)
            block["transactions"][i] = t["hash"]
            receipt = self.get_receipt(t["hash"])
            if receipt:
                # receipt["blockNumber"] = int(receipt["blockNumber"], 16)
                # receipt["cumulativeGasUsed"] = int(receipt["cumulativeGasUsed"], 16)
                # receipt["gasUsed"] = int(receipt["gasUsed"], 16)
                # receipt["transactionIndex"] = int(receipt["transactionIndex"], 16)
                receipts.append(receipt)
            i += 1
        return block, transactions, receipts

    def deal_receipt(self, receipts):
        for receipt in receipts:
            tx = receipt.get("transactionHash") if receipt is not None else None
            if tx is not None:
                if receipt is not None:
                    receipt["_id"] = receipt["transactionHash"]
                    self.save_receipt(receipt)
                    contract = receipt.get("contractAddress")
                    if contract is not None:
                        self.save_account(
                            {"_id": contract, "type": "contract", "receipt": receipt.get("transactionHash")})
                    _from = receipt.get("from")
                    if _from is not None:
                        _account = self.find_account(_from)
                        if _account is not None and "type" in _account and _account["type"] == "contract":
                            self.save_account(
                                {"_id": _from, "type": _account.get('type'), "balance": self.get_balance(_from),
                                 "receipt": receipt.get("transactionHash")})
                        else:
                            self.save_account({"_id": _from, "type": "normal", "balance": self.get_balance(_from),
                                               "receipt": receipt.get("transactionHash")})
                    _to = receipt.get("to")
                    if _to is not None:
                        _account = self.find_account(_to)
                        if _account is not None and "type" in _account and _account["type"] == "contract":
                            self.save_account(
                                {"_id": _to, "type": _account.get('type'), "balance": self.get_balance(_to),
                                 "receipt": receipt.get("transactionHash")})
                        else:
                            self.save_account({"_id": _from, "type": "normal", "balance": self.get_balance(_from),
                                               "receipt": receipt.get("transactionHash")})
        pass
