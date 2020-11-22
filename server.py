import socket
import os
import time
import random
import string
import logging
from _thread import start_new_thread
from threading import Lock
import pickle
import numpy as np
import copy


class Server:
    CHANNEL_PORT = 10000
    SERVER_PORTS = {
        # Client listener port, raft vote listener port, raft operation listener port.
        0: (11001, 12001, 13001),
        1: (11002, 12002, 13002),
        2: (11003, 12003, 13003),
    }
    MAX_CONNECTION = 100
    BUFFER_SIZE = 65536

    LEADER_ELECTION_TIMEOUT = 10
    MESSAGE_SENDING_TIMEOUT = 10

    def __init__(self):
        # Initialize blockchains, balance tables, proof of work working area, etc.
        pass

    # Operation utilities.
    def threaded_on_receive_operation(self, connection):
        # Receive and process append request/response and heartbeat messages.

        pass

    def threaded_response_watch(self):
        # Watch whether we receive response for a specific normal operation message sent. If not, resend the message.

        pass

    def threaded_send_append_request(self):
        # Send append requests to followers.

        pass

    def threaded_heartbeat(self):
        # Send normal operation heartbeats to the followers.

        pass

    def start_operation_listener(self):
        # Start listener for operation messages.
        start_new_thread(self.threaded_on_receive_operation, ())
        pass

    # Vote utilities.
    def threaded_leader_election_watch(self):
        # Watch whether the


    def threaded_on_leader_election_timeout(self):
        # Raise self to leader if timeout. Send request for votes. Step down if another leader elected.

        pass

    def threaded_on_receive_vote(self, connection):
        # Receive and process the vote request/response messages.

        pass

    def start_vote_listener(self):
        # Start listener for vote messages.

        pass

    # Blockchain and client message utilities.
    def threaded_commit_watch(self):
        # Inform the client if the transaction's block has been committed.

        pass

    def proof_of_work(self):
        # Doing proof of work based on the queue of transactions.

        # Once a block is added to the block chain, call commit watch and send append request.

        pass

    def threaded_on_receive_client(self, connection):
        # Receive transaction request from client.

        # Relay the message to the current leader if self is not.

        # Add the transaction to the transaction queue.

        pass

    def start_client_listener(self):
        # Start listener for client messages.

        pass

    def start(self):
        # Start the listeners for messages and timeout watches.

        pass


if __name__ == '__main__':
    server = Server()
    server.start()
