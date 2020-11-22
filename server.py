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
                self.sockets = [None, None, None]
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
    def on_receive_operation_request(self, msg):
        self.server_term_lock.acquire()
        if msg[3]['term'] < self.server_term:
            self.server_term_lock.release()
            # TODO: reject with message term=current_term and success is failed, with 'operation-response'
            pass
        elif msg[3]['term'] >= self.server_term:
            if msg[3]['term'] > self.server_term:
                self.server_term = msg[3]['term']
                self.server_state_lock.acquire()
                self.voted_candidate_lock.acquire()
                self.server_state = 'Follower'
                self.voted_candidate = None
                self.voted_candidate_lock.release()
                self.server_state_lock.release()
                self.server_term_lock.release()
                start_new_thread(self.threaded_leader_election_watch, ())

            # server term and message term are equal
            if len(msg[3]['entries']) == 0:  # heartbeat message
                start_new_thread(self.threaded_leader_election_watch, ())
            else:  # append message
                self.blockchain_lock.acquire()
                # TODO: checkout 'term' key when implement blockchain
                if msg[3]['previous_log_term'] != self.blockchain[msg[3]['previous_log_index']]['term']:
                    # TODO: reject with message term=current_term and success is failed, with 'operation-response'
                    pass
                else:  # matches update blockchain
                    self.blockchain = self.blockchain[:msg[3]['previous_log_index'] + 1] + msg[3]['entries']
                self.blockchain_lock.release()

            self.commit_index_lock.acquire()
            self.commit_index = msg[3]['commit_index']
            self.commit_index_lock.release()

    def on_receive_operation_response(self, msg):
        term = msg[3]['term']
        success = msg[3]['success']
        term_change = False

        if success:
            self.servers_log_next_index_lock.acquire()
            self.blockchain_lock.acquire()
            self.servers_log_next_index[msg[1]] = len(self.blockchain) - 1
            self.blockchain_lock.release()
            self.servers_log_next_index_lock.release()

            # TODO: check majority

        self.server_term_lock.acquire()
        if term > self.server_term:
            self.server_term = term
            term_change = True
            self.server_state_lock.acquire()
            self.voted_candidate_lock.acquire()
            self.server_state = 'Follower'
            self.voted_candidate = None
            self.voted_candidate_lock.release()
            self.server_state_lock.release()
            start_new_thread(self.threaded_leader_election_watch, ())
        self.server_term_lock.release()

        if not success and not term_change:  # index problem, retry
            self.servers_log_next_index_lock.acquire()
            self.servers_log_next_index[msg[1]] -= 1
            self.servers_log_next_index_lock.release()
            start_new_thread(self.threaded_response_watch, (msg[1],))
            start_new_thread(self.threaded_send_append_request, ([msg[1]],))


    def threaded_on_receive_operation(self, msg):
        # TODO: get message from channel
        # Receive and process append request/response and heartbeat messages.
        if msg[0] == 'Operation-Request':
            self.on_receive_operation_request(msg)
        elif msg[0] == 'Operation-Response':
            self.on_receive_operation_response(msg)
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
        return header, sender, receiver, message

    def threaded_send_append_request(self, receivers):
        # Send append requests to followers.
        for receiver in receivers:
            msg = self.generate_operation_request_message(receiver)
            start_new_thread(self.threaded_on_receive_operation, ())
            start_new_thread(utils.send_message, (msg, Server.CHANNEL_PORT))

    def threaded_heartbeat(self):
        # Send normal operation heartbeats to the followers.
        for receiver in self.other_servers:
            msg = self.generate_operation_request_message(receiver, is_heartbeat=True)
            start_new_thread(self.threaded_on_receive_operation, ())
            start_new_thread(utils.send_message, (msg, Server.CHANNEL_PORT))

    def start_operation_listener(self):
        # Start listener for operation messages.
        # start_new_thread(self.threaded_on_receive_operation, ())
        # start_new_thread(self.threaded_heartbeat, ())
        pass

    # Vote utilities.
    def threaded_leader_election_watch(self, timeout):
        # Watch whether the election has timed out. Call on leader election timeout if so.

        time.sleep(timeout)
        self.last_election_time_lock.acquire()
        if time.time() - self.last_election_time >= timeout:
            self.last_election_time_lock.release()
            start_new_thread(self.threaded_on_leader_election_timeout, ())

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

        # TODO: commit watch will check commit index variable, but we need to consider commit index variable can be larger
        # TODO: than the size of the blockchain, so it will pass.
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
