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
import math
import hashlib
from collections import deque


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

    LEADER_ELECTION_TIMEOUT = 60
    MESSAGE_SENDING_TIMEOUT = 30

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
        print('Follower!')
        self.server_state = 'Follower'  # Follower, Leader, Candidate
        self.server_state_lock = Lock()

        self.leader_id = None
        self.leader_id_lock = Lock()

        # Server term.
        self.server_term = 0
        self.server_term_lock = Lock()

        # State variables for operation.
        self.servers_operation_last_seen = [time.time(), time.time(), time.time()]
        self.servers_operation_last_seen_lock = Lock()

        self.servers_log_next_index = [0, 0, 0]
        self.servers_log_next_index_lock = Lock()

        self.received_success = 0
        self.received_success_lock = Lock()

        self.commit_index = 0
        self.commit_index_lock = Lock()

        # State variables for vote.
        self.last_election_time = 0
        self.last_election_time_lock = Lock()

        self.voted_candidate = None
        self.voted_candidate_lock = Lock()

        self.received_votes = 0
        self.received_votes_lock = Lock()

        # State variables for client.
        self.blockchain = []  # each block: {'term': ..., 'phash': ..., 'nonce': ...,
        # 'transactions': ((unique_id, (A, B, 5)), (unique_id, (A)), None)}
        self.blockchain_lock = Lock()

        self.balance_table = [10, 10, 10]
        self.balance_table_lock = Lock()

        self.transaction_queue = deque()
        self.transaction_queue_lock = Lock()

        self.transaction_ids = set()
        self.transaction_ids_lock = Lock()

    # Operation utilities.
    def generate_operation_response_message(self, receiver, success):
        # server_term is already locked here...
        header = 'Operation-Response'
        sender = self.server_id

        message = {
            'term': self.server_term,
            'success': success
        }

        return header, sender, receiver, message

    def generate_operation_request_message(self, receiver, is_heartbeat=False):
        header = 'Operation-Request'
        sender = self.server_id

        self.server_term_lock.acquire()
        self.commit_index_lock.acquire()
        self.servers_log_next_index_lock.acquire()
        self.blockchain_lock.acquire()

        next_log_index = self.servers_log_next_index[receiver]
        previous_log_index = next_log_index - 1
        previous_log_term = self.blockchain[previous_log_index]['term'] if len(self.blockchain) > 0 else -1
        message = {
            'term': self.server_term,
            'leader_id': self.server_id,
            'previous_log_index': previous_log_index,
            'previous_log_term': previous_log_term,
            'entries': [] if is_heartbeat else self.blockchain[next_log_index:],
            'commit_index': self.commit_index
        }

        self.servers_log_next_index_lock.release()
        self.blockchain_lock.release()
        self.commit_index_lock.release()
        self.server_term_lock.release()

        return header, sender, receiver, message

    def on_receive_operation_request(self, sender, message):
        self.server_term_lock.acquire()
        print(message)
        if message['term'] < self.server_term:
            # reject message because term is smaller.
            msg = self.generate_operation_response_message(sender, success=False)
            start_new_thread(utils.send_message, (msg, Server.CHANNEL_PORT))
        elif message['term'] >= self.server_term:
            if message['term'] > self.server_term:
                # saw bigger term from another process, step down, and continue
                self.server_term = message['term']

                self.server_state_lock.acquire()
                self.voted_candidate_lock.acquire()

                print('Follower!')
                self.server_state = 'Follower'
                self.voted_candidate = None

                self.voted_candidate_lock.release()
                self.server_state_lock.release()
                start_new_thread(self.threaded_leader_election_watch, ())

            # server term and message term are equal
            self.leader_id_lock.acquire()
            self.leader_id = message['leader_id']
            self.leader_id_lock.release()

            if len(message['entries']) == 0:  # heartbeat message
                start_new_thread(self.threaded_leader_election_watch, ())
                success = True
            else:  # append message
                self.blockchain_lock.acquire()

                prev_log_index = message['previous_log_index']
                if len(self.blockchain) > prev_log_index and \
                        message['previous_log_term'] == self.blockchain[prev_log_index]['term']:  # matches update blockchain
                    self.blockchain = self.blockchain[:message['previous_log_index'] + 1] + message['entries']
                    success = True
                else:
                    success = False
                self.blockchain_lock.release()
            msg = self.generate_operation_response_message(sender, success=success)
            start_new_thread(utils.send_message, (msg, Server.CHANNEL_PORT))

            # update commit index depends on given message
            self.commit_index_lock.acquire()
            self.commit_index = message['commit_index']
            self.commit_index_lock.release()
        self.server_term_lock.release()

    def on_receive_operation_response(self, sender, message):
        term = message['term']
        success = message['success']
        term_change = False
        print(message)
        if success:
            self.servers_log_next_index_lock.acquire()
            self.blockchain_lock.acquire()
            self.received_success_lock.acquire()
            self.commit_index_lock.acquire()

            self.servers_log_next_index[sender] = len(self.blockchain) - 1
            self.received_success += 1

            if self.received_success >= len(Server.SERVER_PORTS) // 2 + 1:  # Received enough success to commit.
                # TODO: make sure that majority from current term, += 1 commit index is wrong
                # TODO: commit blocks in between received local commit index -> commit
                self.commit_index += 1

            self.commit_index_lock.release()
            self.received_success_lock.release()
            self.blockchain_lock.release()
            self.servers_log_next_index_lock.release()

        self.server_term_lock.acquire()
        if term > self.server_term:
            self.server_term = term
            term_change = True
            self.server_state_lock.acquire()
            self.voted_candidate_lock.acquire()
            print('Follower!')
            self.server_state = 'Follower'
            self.voted_candidate = None
            self.voted_candidate_lock.release()
            self.server_state_lock.release()
            start_new_thread(self.threaded_leader_election_watch, ())
        self.server_term_lock.release()

        if not success and not term_change:  # index problem, retry
            self.servers_log_next_index_lock.acquire()
            self.servers_log_next_index[sender] -= 1
            self.servers_log_next_index_lock.release()
            start_new_thread(self.threaded_response_watch, (sender,))
            start_new_thread(self.threaded_send_append_request, ([sender],))

    def threaded_on_receive_operation(self, connection):
        # Receive and process append request/response and heartbeat messages.

        header, sender, receiver, message = utils.receive_message(connection)

        if header == 'Operation-Request':
            self.on_receive_operation_request(sender, message)
        elif header == 'Operation-Response':
            self.on_receive_operation_response(sender, message)
        else:
            raise NotImplementedError(f'Header {header} is not related!')

    def threaded_response_watch(self, receiver):
        # Watch whether we receive response for a specific normal operation message sent. If not, resend the message.
        timeout = random.uniform(Server.MESSAGE_SENDING_TIMEOUT, Server.MESSAGE_SENDING_TIMEOUT*2)
        time.sleep(timeout)
        self.servers_operation_last_seen_lock.acquire()
        if time.time() - self.servers_operation_last_seen[receiver] > timeout:  # timed out, resend
            self.servers_operation_last_seen_lock.release()
            start_new_thread(self.threaded_response_watch, (receiver,))
            start_new_thread(self.threaded_send_append_request, ([receiver],))

    def threaded_send_append_request(self, receivers):
        # Send append requests to followers.
        self.received_success_lock.acquire()
        self.received_success = 1 if self.received_success == 0 else self.received_success
        self.received_success_lock.release()

        for receiver in receivers:
            msg = self.generate_operation_request_message(receiver)
            # start_new_thread(self.threaded_on_receive_operation, ())
            start_new_thread(utils.send_message, (msg, Server.CHANNEL_PORT))

    def threaded_heartbeat(self):
        # Send normal operation heartbeats to the followers.
        for receiver in self.other_servers:
            msg = self.generate_operation_request_message(receiver, is_heartbeat=True)
            # start_new_thread(self.threaded_on_receive_operation, ())
            start_new_thread(utils.send_message, (msg, Server.CHANNEL_PORT))

    def threaded_become_leader(self):
        # Initialize the next index, last log index, send the first heartbeat.
        self.server_state_lock.acquire()
        self.servers_log_next_index_lock.acquire()
        self.blockchain_lock.acquire()
        self.leader_id_lock.acquire()

        print('Leader!')
        self.server_state = 'Leader'
        self.servers_log_next_index = 3 * [len(self.blockchain)]
        self.leader_id = self.server_id

        self.blockchain_lock.release()
        self.servers_log_next_index_lock.release()
        self.server_state_lock.release()
        self.leader_id_lock.release()

        self.server_state_lock.acquire()
        while self.server_state == 'Leader':
            start_new_thread(self.threaded_heartbeat, ())
            self.server_state_lock.release()
            self.server_state_lock.acquire()
        self.server_state_lock.release()

    def start_operation_listener(self):
        # Start listener for operation messages.

        self.sockets[2].listen(Server.MAX_CONNECTION)
        while True:
            connection, (ip, port) = self.sockets[2].accept()
            start_new_thread(self.threaded_on_receive_operation, (connection,))

    # Vote utilities.
    def threaded_leader_election_watch(self):
        # Watch whether the election has timed out. Call on leader election timeout if so.

        # Update the last updated time.
        self.last_election_time_lock.acquire()
        self.last_election_time = time.time()
        self.last_election_time_lock.release()

        timeout = random.uniform(Server.LEADER_ELECTION_TIMEOUT, Server.LEADER_ELECTION_TIMEOUT*2)
        time.sleep(timeout)
        self.last_election_time_lock.acquire()
        if time.time() - self.last_election_time >= timeout:
            self.last_election_time_lock.release()
            start_new_thread(self.threaded_on_leader_election_timeout, ())

    def generate_vote_request_message(self, receiver):

        header = 'Vote-Request'
        sender = self.server_id
        message = {
            'candidate_id': self.server_id,
            'term': self.server_term,
            'last_log_index': len(self.blockchain) - 1,
            'last_log_term': self.blockchain[-1]['term'] if len(self.blockchain) > 0 else -1,
        }
        return header, sender, receiver, message

    def threaded_on_leader_election_timeout(self):
        # Send request for votes.

        self.server_state_lock.acquire()
        self.server_term_lock.acquire()
        self.blockchain_lock.acquire()
        self.voted_candidate_lock.acquire()
        self.received_votes_lock.acquire()
        self.leader_id_lock.acquire()

        print('Candidate!')
        self.server_state = 'Candidate'
        self.server_term += 1
        self.voted_candidate = self.server_id
        self.received_votes = 1
        self.leader_id = None
        msgs = [self.generate_vote_request_message(receiver) for receiver in self.other_servers]

        self.server_state_lock.release()
        self.server_term_lock.release()
        self.blockchain_lock.release()
        self.voted_candidate_lock.release()
        self.received_votes_lock.release()
        self.leader_id_lock.release()

        for msg in msgs:
            start_new_thread(utils.send_message, (msg, Server.CHANNEL_PORT))
        start_new_thread(self.threaded_leader_election_watch, ())

    def generate_vote_response_message(self, receiver, vote):

        header = 'Vote-Response'
        sender = self.server_id
        message = {
            'term': self.server_term,
            'vote': vote
        }
        return header, sender, receiver, message

    def on_receive_vote_request(self, message):
        # Receive and process vote request.

        self.server_term_lock.acquire()
        self.server_state_lock.acquire()
        self.voted_candidate_lock.acquire()
        self.blockchain_lock.acquire()

        # Update term.
        if message['term'] > self.server_term:
            self.server_term = message['term']
            print('Follower!')
            self.server_state = 'Follower'
            self.voted_candidate = None

        # Decide whether to cast vote.
        last_log_term = self.blockchain[-1]['term'] if len(self.blockchain) > 0 else -1
        if message['term'] == self.server_term \
                and self.voted_candidate in {None, message['candidate_id']} \
                and not \
                (last_log_term > message['last_log_term']
                 or (last_log_term == message['last_log_term'] and len(self.blockchain) - 1 > message['last_log_index'])):

            vote = True
            self.voted_candidate = message['candidate_id']

        else:
            vote = False
        msg = self.generate_vote_response_message(message['candidate_id'], vote)

        self.server_term_lock.release()
        self.server_state_lock.release()
        self.voted_candidate_lock.release()
        self.blockchain_lock.release()

        # Send message and reset election timeout if vote.
        start_new_thread(utils.send_message, (msg, Server.CHANNEL_PORT))
        if vote:
            start_new_thread(self.threaded_leader_election_watch, ())

    def on_receive_vote_response(self, message):
        # Receive and process vote response.

        self.server_state_lock.acquire()
        self.server_term_lock.acquire()
        self.received_votes_lock.acquire()
        self.last_election_time_lock.acquire()

        become_leader = False

        if message['term'] > self.server_term:  # Discover higher term.
            self.server_term = message['term']
            print('Follower!')
            self.server_state = 'Follower'

        if self.server_state == 'Candidate':  # Hasn't stepped down yet.
            if message['vote'] and message['term'] == self.server_term:  # Receive vote for current term.
                self.received_votes += 1
            if self.received_votes >= len(Server.SERVER_PORTS) // 2 + 1:  # Received enough votes to become leader.
                become_leader = True
                self.last_election_time = time.time()  # Update the last election time to avoid previous timeout watches. Don't start new timeout watch.

        self.server_state_lock.release()
        self.server_term_lock.release()
        self.received_votes_lock.release()
        self.last_election_time_lock.release()

        if become_leader:
            start_new_thread(self.threaded_become_leader, ())

    def threaded_on_receive_vote(self, connection):
        # Receive and process the vote request/response messages.

        header, sender, receiver, message = utils.receive_message(connection)

        if header == 'Vote-Request':
            self.on_receive_vote_request(message)
        elif header == 'Vote-Response':
            self.on_receive_vote_response(message)
        else:
            raise NotImplementedError(f'Header {header} is not related!')

    def start_vote_listener(self):
        # Start listener for vote messages.

        self.sockets[1].listen(Server.MAX_CONNECTION)
        while True:
            connection, (ip, port) = self.sockets[1].accept()
            start_new_thread(self.threaded_on_receive_vote, (connection,))

    # Blockchain and client message utilities.
    def generate_client_response_message(self, transaction_id, transaction, transaction_result):

        header = 'Client-Response'
        sender = self.server_id
        receiver = transaction[0]
        message = {
            'id': transaction_id,
            'transaction': transaction,
            'result': transaction_result,
        }

        return header, sender, receiver, message

    def threaded_send_client_response(self, transaction_id, transaction, transaction_result):
        # Compose the response for client.
        # transaction = (A, B, amt) or (A, ), transaction_result = (True/False, balance_of_A).

        msg = self.generate_client_response_message(transaction_id, transaction, transaction_result)
        start_new_thread(utils.send_message, (msg, Server.CHANNEL_PORT))

    def threaded_commit_watch(self, transactions_ids, transactions, block_index):
        # TODO: think about the edge case happening when previous majority haven't been committed...
        # Inform the client if the transaction's block has been committed.
        sent = False
        while not sent:
            self.commit_index_lock.acquire()
            if block_index <= self.commit_index:
                self.blockchain_lock.acquire()
                if self.commit_index < len(self.blockchain):
                    self.blockchain_lock.release()
                    for i, transactions_id in enumerate(transactions_ids):
                        self.balance_table_lock.acquire()
                        balance = self.balance_table[transactions[i][0]]
                        self.balance_table_lock.release()
                        start_new_thread(self.threaded_send_client_response,
                                         (transactions_id, transactions[i], (True, balance)))
                    sent = True
            self.commit_index_lock.release()

    def is_transaction_valid(self, transaction):
        self.balance_table_lock.acquire()
        if len(transaction) == 3 and self.balance_table[transaction[0]] < transaction[2]:
            self.balance_table_lock.release()
            return False
        self.balance_table_lock.release()
        return True

    def proof_of_work(self):
        def get_hash(transactions, nonce):
            will_encode = "|".join(transactions) + "|" + nonce
            return hashlib.sha3_256(will_encode.encode('utf-8')).hexdigest()

        # Doing proof of work based on the queue of transactions.
        max_transaction_count = 3
        while True:
            self.transaction_queue_lock.acquire()
            if len(self.transaction_queue) > 0:
                so_far, index = 0, 0
                transactions_ids, transactions = [], [None, None, None]
                while so_far < max_transaction_count and len(self.transaction_queue) > index:
                    transaction_id, transaction = self.transaction_queue[index]
                    if self.is_transaction_valid(transaction):
                        # TODO: handle double spending in one block
                        transactions_ids.append(transaction_id)
                        transactions[so_far] = transaction
                        so_far += 1
                    else:
                        # sending invalid transactions
                        self.balance_table_lock.acquire()
                        balance = self.balance_table[transaction[0]]
                        self.balance_table_lock.release()
                        start_new_thread(self.generate_client_response_message,
                                         (transaction_id, transaction, (False, balance)))
                    index += 1
                self.transaction_queue_lock.release()

                found = False
                found_nonce = None
                while found:
                    nonce = utils.generate_random_string_with_ending(length=6, ending={'0', '1', '2'})
                    cur_pow = get_hash(transactions, nonce)
                    if '2' > cur_pow[-1] > '0':
                        found_nonce = nonce
                        found = True

                # PoW is found...
                self.blockchain_lock.acquire()
                self.server_term_lock.acquire()

                phash = None
                if len(self.blockchain) > 0:
                    previous_nonce = self.blockchain[-1]['nonce']
                    previous_transactions = self.blockchain[-1]['transactions']
                    phash = get_hash(previous_transactions, previous_nonce)

                # added to the block chain
                self.blockchain.append({
                    'term': self.server_term,
                    'phash': phash,
                    'nonce': found_nonce,
                    'transactions': transactions
                })

                block_index = len(self.blockchain) - 1

                self.server_term_lock.release()
                self.blockchain_lock.release()

                # call commit watch
                start_new_thread(self.threaded_commit_watch, (transactions_ids, transactions, block_index,))

                # send append request
                start_new_thread(self.threaded_send_append_request, (self.other_servers,))

            else:
                self.transaction_queue_lock.release()

        # Once a block is , call commit watch and send append request.

    def threaded_on_receive_client(self, connection):
        # Receive transaction request from client.

        header, sender, receiver, message = utils.receive_message(connection)
        if self.server_state == 'Leader':  # Process the request.

            transaction_id = message['id']
            transaction = message['transaction']

            self.transaction_ids_lock.acquire()
            self.transaction_queue_lock.acquire()
            if transaction_id not in self.transaction_ids:  # Transactions hasn't been processed yet.
                self.transaction_ids.add(transaction_id)
                self.transaction_queue.append((transaction_id, transaction))
            self.transaction_ids_lock.release()
            self.transaction_queue_lock.release()

        else:  # Relay the client message to the current leader.
            while True:
                self.leader_id_lock.acquire()
                if self.leader_id:  # Wait until a leader is elected.
                    msg = (header, self.server_id, self.leader_id, message)
                    start_new_thread(utils.send_message, (msg, server.CHANNEL_PORT))
                    self.leader_id_lock.release()
                    break
                self.leader_id_lock.release()

    def start_client_listener(self):
        # Start listener for client messages.

        self.sockets[0].listen(Server.MAX_CONNECTION)
        while True:
            connection, (ip, port) = self.sockets[0].accept()
            start_new_thread(self.threaded_on_receive_operation, (connection,))

    def start(self):
        # Start the listeners for messages and timeout watches.

        start_new_thread(self.start_client_listener, ())
        start_new_thread(self.start_vote_listener, ())
        start_new_thread(self.start_operation_listener, ())
        start_new_thread(self.threaded_leader_election_watch, ())

        while 1:
            pass


if __name__ == '__main__':
    server = Server()
    server.start()
