## Contributors

[Zexi Huang](https://github.com/zexihuang) and [Mert Kosan](https://github.com/mertkosan)

## Introduction

This is a Python implementation of the [RAFT distributed consensus protocal](https://raft.github.io/) with a fault-tolerant distributed banking application based on the blockchain. 

### Distributed Banking

To ensure safety and high availability, the bank has a set of servers with replicated lists of transactions made by clients. When a client makes a transaction request, it will be sent to one of the servers to validate. If the transaction is valid, the server will distribute the transaction to other servers until a consensus is reached, i.e., the transaction is stored in all servers. Then, the server will notify the client of the transaction result.  

### Blockchain

Each server uses the blockchain as the underlying data structure for storing the transactions. This allows untrusted servers to join and reach an agreement. The blockchain is based on the [Nakamoto's Proof-of-Work concept](https://nakamotoinstitute.org/bitcoin/) that has been used in [Bitcoin](https://bitcoin.org/). 

### RAFT

RAFT has been used as the underlying consensus protocol to ensure proper blockchain replication. It consists of two different phases: leader election and normal operation. In the leader election phase elects the leader among the servers who communicates directly with the clients. In the normal operation phase, the leader exchanges messages with other servers (followers) to replicate the blockchain of transactions. 

### Fault-tolerance

We guarantee fault-tolerance for two types of failures: node failure (crash) and network partition. 
* Node failure: This is the scenario when one or more servers crash. If a follower fails, the rest of the servers should still perform normal operations as long as they can form a majority quorum. If a leader fails, the rest of the servers starts a new leader election, and once a leader is elected by a quorum, the normal operations resume. Servers also save their blockchains to the disk as persistent states, so that when they recover, they can resume their operations with the save states. We simulate this type of failure by killing and restarting the server processes. 
* Network partition: This is the scenario when the servers are decomposed into multiple partitions, where only servers within the same partition can communicate with each other. In this case, the partition that still covers a majority quorum can (possibly elect a new leader and) resume normal operations. We simulate this type of failure by a centralized channel that can be configured to relay messages between subsets of servers. The channel also adds randomized delay in relaying to simulate network delays. 

## How to Run

All our codes are written and tested with Python 3.7. 

### Starting the processes

First, start the centralized channel with `python channel.py` in a terminal. Then, start three servers with `python server.py` in three different terminals. Each one will prompt you to select its id (from 1, 2, and 3) and you should select different ids for different processes. Do the same to start three clients with `python client.py`. 

### Testing normal scenarios

Once the channel, servers, and clients are started, you can send transaction requests in the client terminals. Two types of transactions are available: check the client's balance and transfer money from this client to another client. Follow the prompts to send each type of transaction. 

After a transaction is sent by a client, it will receive feedback once the transaction is replicated properly in all servers and committed. New transactions can be sent before feedback for previous transactions are received. Note that the order of transactions sent and the order of transactions executed may not be consistent due to randomized network delays. 

Primary debug messages (such as server states and terms in RAFT) are displayed in server terminals during operation. Complete server logs that include secondary debug messages are stored in the disk with `server_{id}.log`. All server persistent states (such as the blockchains) are stored in the `server_{id}_states` folder in readable JSON format and are updated in real-time during operation. The channel also keeps `channel.log` that contains all messages it relays. 

### Testing faulty scenarios

* Node failure: Enter `CTRL+C` to the server processes to kill them at any time to simulate the node failure. They can be restarted by running `python server.py` again with the corresponding server id. During the restart, the server will read its persistent states before failure from the disk. 
* Network partition: In the channel terminal, follow the prompt to set the partition of servers. The channel starts with no partition by default and can be configured to simulate arbitrary partitions. The new partition set in the channel overwrites the existing partition. 
