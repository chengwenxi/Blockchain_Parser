"""Pull data from geth and parse it into mongo."""

import sys
sys.path.append("./../Preprocessing")
sys.path.append("./../Analysis")
from Preprocessing.Crawler import Crawler
from Analysis.ContractMap import ContractMap
import subprocess
LOGDIR = "./../Preprocessing/logs"


subprocess.call([
    "(geth --rpc --rpcport 8545 > {}/geth.log 2>&1) &".format(LOGDIR),
    "(mongod --dbpath mongo/data --port 27017 > {}/mongo.log 2>&1) &".format(LOGDIR)
], shell=True)

print("Booting processes.")
# Catch up with the crawler
c = Crawler.Crawler()

print("Updating contract hash map.")
# Update the contract addresses that have been interacted with
ContractMap(c.mongo_client, last_block=c.max_block_mongo)

print("Update complete.")
subprocess.call([
    "(geth --rpc --rpcport 8545 > {}/geth.log 2>&1) &".format(LOGDIR),
    "(mongod --dbpath mongo/data --port 27017 > {}/mongo.log 2>&1) &".format(LOGDIR)
], shell=True)
