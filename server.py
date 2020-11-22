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
import utils


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
        # Get the server name.
        while True:
            self.server_id = int(input('Which server are you? Enter 0, 1 or 2. \n'))
            if self.server_id in Server.SERVER_PORTS:
                self.other_servers = {0, 1, 2} - {self.server_id}
                self.sockets = [None, None, None]  # Client port, vote port, and operation port.
                for i in range(3):
                    self.sockets[i] = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                    self.sockets[i].bind((socket.gethostname(), Server.SERVER_PORTS[self.server_id][i]))
                break
            else:
                print('Wrong server name. Please enter 0, 1 or 2.')

        # Initialize blockchains, balance tables, proof of work working area, etc.

        # Server state.
        self.server_state = 'Follower'  # Follower, Leader, Candidate
        self.server_state_lock = Lock()

        # Server term.
        self.server_term = 0
        self.server_term_lock = Lock()

        # Voted candidate.
        self.voted_candidate = None
        self.voted_candidate_lock = Lock()

        # State variables for operation.
        self.servers_operation_last_seen = [time.time(), time.time(), time.time()]
        self.servers_operation_last_seen_lock = Lock()

        self.servers_log_next_index = [0, 0, 0]
        self.servers_log_next_index_lock = Lock()

        self.commit_index = 0
        self.commit_index_lock = Lock()

        # State variables for vote.
        self.last_election_time = 0
        self.last_election_time_lock = Lock()

        # State variables for client.

        self.blockchain = []
        self.blockchain_lock = Lock()
        self.balance_table = []

    # Operation utilities.
    def threaded_on_receive_operation(self, msg):
        # Receive and process append request/response and heartbeat messages.
        if msg[0] == 'Operation-Request':
            if len(msg[3]['entries']) == 0:  # heartbeat message
                start_new_thread(self.threaded_leader_election_watch, ())
            else:  # append message
                pass

        elif msg[0] == 'Operation-Response':
            pass
        else:
            raise NotImplementedError(f'Header {msg[0]} is not related!')


    def threaded_response_watch(self, receiver):
        # Watch whether we receive response for a specific normal operation message sent. If not, resend the message.
        timeout = random.uniform(40.0, 80.0)
        time.sleep(timeout)
        self.servers_operation_last_seen_lock.acquire()
        if time.time() - self.servers_operation_last_seen[receiver] > timeout:  # timed out, resend
            self.servers_operation_last_seen_lock.release()
            start_new_thread(self.threaded_response_watch, (receiver,))
            start_new_thread(self.threaded_send_append_request, ([receiver],))

    def generate_operation_request_message(self, receiver, is_heartbeat=False):
        header = 'Operation-Request'
        sender = self.server_id

        self.servers_log_next_index_lock.acquire()
        self.blockchain_lock.acquire()
        previous_log_index = self.servers_log_next_index[receiver] - 1
        next_log_index = self.servers_log_next_index[receiver]
        # TODO: checkout 'term' key when implement blockchain
        previous_log_term = self.blockchain[previous_log_index]['term']
        self.servers_log_next_index_lock.release()
        self.blockchain_lock.release()

        message = {
            'term': self.server_term,
            'leader_id': self.server_id,
            'previous_log_index': previous_log_index,
            'previous_log_term': previous_log_term,
            'entries': [] if is_heartbeat else self.blockchain[next_log_index:],
            'commit_index': self.commit_index
        }
        return [header, sender, receiver, message]

    def threaded_send_append_request(self, receivers):
        # Send append requests to followers.
        for receiver in receivers:
            msg = self.generate_operation_request_message(receiver)
            start_new_thread(self.threaded_on_receive_operation, ())
            start_new_thread(utils.send_message, (tuple(msg), Server.CHANNEL_PORT))

    def threaded_heartbeat(self):
        # Send normal operation heartbeats to the followers.
        for receiver in self.other_servers:
            msg = self.generate_operation_request_message(receiver, is_heartbeat=True)
            start_new_thread(self.threaded_on_receive_operation, ())
            start_new_thread(utils.send_message, (tuple(msg), Server.CHANNEL_PORT))

    def start_operation_listener(self):
        # Start listener for operation messages.
        start_new_thread(self.threaded_on_receive_operation, ())
        start_new_thread(self.threaded_heartbeat, ())
        pass

    # Vote utilities.
    def generate_vote_request_message(self, ):

    def threaded_leader_election_watch(self):
        # Watch whether the election has timed out. Call on leader election timeout if so.

        # Update the last updated time.
        self.last_election_time_lock.acquire()
        self.last_election_time = time.time()
        self.last_election_time_lock.release()

        timeout = random.uniform(5.0, 10.0)
        time.sleep(timeout)
        self.last_election_time_lock.acquire()
        if time.time() - self.last_election_time >= timeout:
            self.last_election_time_lock.release()
            start_new_thread(self.threaded_on_leader_election_timeout, ())

    def threaded_on_leader_election_timeout(self):
        # Send request for votes. Step down if another leader elected.

        self.server_state_lock.acquire()
        self.server_term_lock.acquire()
        self.server_state = 'Candidate'
        self.server_term += 1



        self.server_state_lock.release()
        self.server_term_lock.release()




    def threaded_on_receive_vote(self, connection):
        # Receive and process the vote request/response messages.

        pass

    def start_vote_listener(self):
        # Start listener for vote messages.

        self.sockets[1].listen(Server.MAX_CONNECTION)
        while True:
            connection, (ip, port) = self.sockets[1].accept()
            start_new_thread(self.threaded_on_receive_vote, (connection, ))

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
