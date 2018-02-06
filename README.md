# Ethereum Blockchain Parser

This is a project to parse the Ethermint blockchain from a local geth node. Blockchains are perfect data sets because they contain every transaction ever made on the network. This is valuable data if you want to analyze the network, but Ethereum stores its blockchain in [RLP](https://github.com/ethereum/wiki/wiki/RLP) encoded binary blobs within a series of LevelDB files and these are surprisingly difficult to access, even given the available tools. This project takes the approach of querying a local node via [JSON-RPC](https://github.com/ethereum/wiki/wiki/JSON-RPC), which returns unencoded transactional data, and then moves that data to a mongo database.


## Usage

### Backfilling your Mongo database

Make sure you have both geth and mongo. To get data from the blockchain as it exists now and then stop parsing, simply run the following scripts, which are located in the `Scripts` directory. Note that at the time of writing, the Ethereum blockchain has about 1.5 million blocks so this will likely take several hours.

1. Funnel the data from geth to MongoDB:


        python3 preprocess.py

        
## Prerequisites:

Before using this tool to analyze your copy of the blockchain, you need the following things:

### Geth
[Geth](https://github.com/ethereum/go-ethereum/wiki/Geth) is the Go implementation of a full Ethereum node. We will need to run it with the `--rpc` flag in order to request data (**WARNING** if you run this on a geth client containing an account that has ether in it, make sure you put a firewall 8545 or whatever port you run geth RPC on).

A geth instance downloads the blockchain and processes it, saving the blocks as LevelDB files in the specified data directory (`~/.ethereum/chaindata` by default). The geth instance can be queried via RPC with the `eth_getBlockByNumber([block, true])` endpoint (see [here](https://github.com/ethereum/wiki/wiki/JSON-RPC#eth_getblockbynumber)) to get the `X-th` block (with `true` indicating we want the transactional data included), which returns data of the form:

    {
      number: 1000000,
      timestamp: 1465003569,
      ...
      transactions: [
        {
          blockHash: "0x2052ce710a08094b81b5047ea9df5119773ce4b263a23d86659fa7293251055e",
          blockNumber: 1284937,
          from: "0x1f57f826caf594f7a837d9fc092456870a289365",
          gas: 22050,
          gasPrice: 20000000000,
          hash: "0x654ac26084ee6e40767e8735f38274ef5f594454a4d34cfdd70c93aa95be0c64",
          input: "0x",
          nonce: 6610,
          to: "0xfbb1b73c4f0bda4f67dca266ce6ef42f520fbb98",
          transactionIndex: 27,
          value: 201544820000000000
        }
      ]
    }

Since I am only interested in `number`, `timestamps`, and `transactions` for this application, I have omitted the rest of the data, but there is lots of additional information in the block (explore [here](https://etherchain.org/blocks)), including a few Merkle trees to maintain hashes of state, transactions, and receipts (read [here](https://blog.ethereum.org/2015/11/15/merkling-in-ethereum/).

Using the `from` and `to` addresses in the `transactions` array, I can map the flow of ether through the network as time processes. Note that the value, gas, and gasPrice are in Wei, where 1 Ether = 10<sup>18</sup> Wei. The numbers are converted into Ether automatically with this tool.

### MongoDB

We will use mongo to essentially copy each block served by Geth, preserving its structure. The data outside the scope of this analysis will be omitted. Note that this project also requires pymongo.

### python3

This was written for python 3.6. Some things will probably break if you try to do this analysis in python 2.