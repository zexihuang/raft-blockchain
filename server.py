import socket
import os
import time
import random
import logging
from _thread import start_new_thread
from threading import Lock
import pickle
import copy
import utils
from collections import deque
from heapq import heappush, heappop
import json
import sys


class Server:
    CHANNEL_PORT = 10000
    SERVER_PORTS = {
        # Client listener port, raft vote listener port, raft operation listener port.
        0: (11001, 12001, 13001),
        1: (11002, 12002, 13002),
        2: (11003, 12003, 13003),
    }
    CLIENTS = [0, 1, 2]
    MAX_CONNECTION = 100
    BUFFER_SIZE = 65536

    LEADER_ELECTION_TIMEOUT = 10
    MESSAGE_SENDING_TIMEOUT = 10
    HEARTBEAT_TIMEOUT = 1

    MAX_TRANSACTION_COUNT = 3

    SLEEP_INTERVAL = 0.01

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
        self.server_state_lock_by = ''

        self.leader_id = None
        self.leader_id_lock = Lock()
        self.leader_id_lock_by = ''

        # Server term.
        self.server_term = 0
        self.server_term_lock = Lock()
        self.server_term_lock_by = ''

        # State variables for operation.
        self.servers_operation_last_seen = [time.time(), time.time(), time.time()]
        self.servers_operation_last_seen_lock = Lock()
        self.servers_operation_last_seen_lock_by = ''

        self.servers_log_next_index = [0, 0, 0]
        self.servers_log_next_index_lock = Lock()
        self.servers_log_next_index_lock_by = ''

        self.accept_indexes = [-1, -1, -1]
        self.accept_indexes_lock = Lock()
        self.accept_indexes_lock_by = ''

        self.commit_index = -1
        self.commit_index_lock = Lock()
        self.commit_index_lock_by = ''

        self.commit_watches = []
        self.commit_watches_lock = Lock()
        self.commit_watches_lock_by = ''

        # State variables for vote.
        self.last_election_time = 0
        self.last_election_time_lock = Lock()
        self.last_election_time_lock_by = ''

        self.voted_candidate = None
        self.voted_candidate_lock = Lock()
        self.voted_candidate_lock_by = ''

        self.received_votes = 0
        self.received_votes_lock = Lock()
        self.received_votes_lock_by = ''

        # State variables for client.
        self.blockchain = []  # each block: {'term': ..., 'phash': ..., 'nonce': ...,
        # 'transactions': ((unique_id, (A, B, 5)), ((unique_id, (A,)), None)}
        self.first_blockchain_read = False
        self.blockchain_lock = Lock()
        self.blockchain_lock_by = ''

        self.balance_table = [100, 100, 100]
        self.balance_table_lock = Lock()
        self.balance_table_lock_by = ''

        self.transaction_queue = deque()
        self.transaction_queue_lock = Lock()
        self.transaction_queue_lock_by = ''

        self.transaction_ids = set()
        self.transaction_ids_lock = Lock()
        self.transaction_ids_lock_by = ''

        # Set up loggers.
        log_file = f'server_{self.server_id}.log'
        # if os.path.exists(log_file):
        #     os.remove(log_file)
        self.logger = logging.getLogger(f'Server_{self.server_id}')
        file_handler = logging.FileHandler(log_file)
        formatter = logging.Formatter('%(asctime)s %(message)s', "%H:%M:%S")
        file_handler.setFormatter(formatter)
        self.logger.addHandler(file_handler)
        self.logger.setLevel(logging.INFO)
        self.logger.info("==============================================STARTING==============================================")

    def save_state(self, variable_names):
        state = self.__dict__
        for variable_name in variable_names:

            value = state[variable_name]
            if variable_name == 'transaction_queue':
                jso_object = {variable_name: list(value)}
            elif variable_name == 'transaction_ids':
                jso_object = {variable_name: list(value)}
            else:
                jso_object = {variable_name: value}

            with open(f'server_{self.server_id}_states/{variable_name}.json', 'w') as _file:
                json.dump(jso_object, _file)

    def load_state(self, variable_names):
        for variable_name in variable_names:
            path = f'server_{self.server_id}_states/{variable_name}.json'
            if os.path.exists(path):
                with open(path, 'r') as _file:
                    state = dict(json.load(_file))

                if 'transaction_queue' in state.keys():
                    state['transaction_queue'] = deque(state['transaction_queue'])
                elif 'transaction_ids' in state.keys():
                    state['transaction_ids'] = set(state['transaction_ids'])

                self.__dict__.update(state)

    # Operation utilities.
    def generate_operation_response_message(self, receiver, last_log_index_after_append, success):
        # server_term is already locked here...
        header = 'Operation-Response'
        sender = self.server_id

        message = {
            'term': self.server_term,
            'last_log_index_after_append': last_log_index_after_append,
            'success': success
        }

        return header, sender, receiver, message

    def generate_operation_request_message(self, receiver, is_heartbeat=False):

        header = 'Operation-Request'
        sender = self.server_id

        func_name = sys._getframe().f_code.co_name
        acquired_by = 'ACQUIRED by ' + func_name
        released_by = 'RELEASED by ' + func_name

        self.server_term_lock.acquire()
        self.server_term_lock_by = acquired_by
        self.save_state(['server_term_lock_by'])

        self.servers_log_next_index_lock.acquire()
        self.servers_log_next_index_lock_by = acquired_by
        self.save_state(['servers_log_next_index_lock_by'])

        self.commit_index_lock.acquire()
        self.commit_index_lock_by = acquired_by
        self.save_state(['commit_index_lock_by'])

        self.blockchain_lock.acquire()
        self.blockchain_lock_by = acquired_by
        self.save_state(['blockchain_lock_by'])

        next_log_index = self.servers_log_next_index[receiver]
        previous_log_index = next_log_index - 1
        previous_log_term = self.blockchain[previous_log_index]['term'] if len(self.blockchain) > 0 and previous_log_index > -1 else -1

        message = {
            'term': self.server_term,
            'leader_id': self.server_id,
            'previous_log_index': previous_log_index,
            'previous_log_term': previous_log_term,
            'entries': [] if is_heartbeat else self.blockchain[next_log_index:],
            'commit_index': self.commit_index
        }
        self.server_term_lock_by = released_by
        self.save_state(['server_term_lock_by'])
        self.server_term_lock.release()

        self.servers_log_next_index_lock_by = released_by
        self.save_state(['servers_log_next_index_lock_by'])
        self.servers_log_next_index_lock.release()

        self.commit_index_lock_by = released_by
        self.save_state(['commit_index_lock_by'])
        self.commit_index_lock.release()

        self.blockchain_lock_by = released_by
        self.save_state(['blockchain_lock_by'])
        self.blockchain_lock.release()

        return header, sender, receiver, message

    def update_transaction_ids(self, block, add=True):
        # if add is False, remove transactions ids

        func_name = sys._getframe().f_code.co_name
        acquired_by = 'ACQUIRED by ' + func_name
        released_by = 'RELEASED by ' + func_name

        self.transaction_ids_lock.acquire()
        self.transaction_ids_lock_by = acquired_by
        self.save_state(['transaction_ids_lock_by'])

        for transaction in block['transactions']:
            if transaction is not None:
                transaction_id = transaction[0]
                if add:
                    self.transaction_ids.add(transaction_id)
                else:
                    self.transaction_ids.remove(transaction_id)
                self.save_state(['transaction_ids'])

        self.transaction_ids_lock_by = released_by
        self.save_state(['transaction_ids_lock_by'])
        self.transaction_ids_lock.release()

    def on_receive_operation_request(self, sender, message):

        func_name = sys._getframe().f_code.co_name
        acquired_by = 'ACQUIRED by ' + func_name
        released_by = 'RELEASED by ' + func_name

        self.server_state_lock.acquire()
        self.server_state_lock_by = acquired_by
        self.save_state(['server_state_lock_by'])

        self.leader_id_lock.acquire()
        self.leader_id_lock_by = acquired_by
        self.save_state(['leader_id_lock_by'])

        self.server_term_lock.acquire()
        self.server_term_lock_by = acquired_by
        self.save_state(['server_term_lock_by'])

        self.commit_index_lock.acquire()
        self.commit_index_lock_by = acquired_by
        self.save_state(['commit_index_lock_by'])

        self.voted_candidate_lock.acquire()
        self.voted_candidate_lock_by = acquired_by
        self.save_state(['voted_candidate_lock_by'])

        if message['term'] < self.server_term:
            # reject message because term is smaller.
            msg = self.generate_operation_response_message(sender, None, success=False)
            start_new_thread(utils.send_message, (msg, Server.CHANNEL_PORT))
        else:  # message['term'] >= self.server_term:
            # saw bigger term from another process, step down, and continue

            if message['term'] > self.server_term:
                self.voted_candidate = None
                self.save_state(['voted_candidate'])
            self.server_term = message['term']
            self.save_state(['server_term'])

            if self.server_state != 'Follower':
                self.server_state = 'Follower'
                self.save_state(['server_state'])
                print(f'Becomes Follower for Term: {self.server_term}')
                self.logger.info(f'Follower! Term: {self.server_term} because of on_receive_operation_request')
            self.leader_id = message['leader_id']
            self.save_state(['leader_id'])

            if len(message['entries']) > 0:  # append message
                print(f'Append RPC from Server {sender}')
                self.logger.info(message)

                self.blockchain_lock.acquire()
                self.blockchain_lock_by = acquired_by
                self.save_state(['blockchain_lock_by'])

                print(f'Blockchain before Append RPC: {utils.blockchain_print_format(self.blockchain)}')
                self.logger.info(f'Blockchain before Append RPC: {self.blockchain}')
                prev_log_index = message['previous_log_index']
                if prev_log_index == -1 or \
                        (len(self.blockchain) > prev_log_index and
                         message['previous_log_term'] == self.blockchain[prev_log_index]['term']):  # matches update blockchain
                    # Overwrite any new entries
                    for i, entry in enumerate(message['entries']):
                        if len(self.blockchain) < prev_log_index + i + 2:
                            self.blockchain.append(entry)
                            self.update_transaction_ids(entry)
                        elif entry['term'] != self.blockchain[prev_log_index + i + 1]['term']:
                            # remove overwritten transactions_ids
                            for will_be_deleted_block in self.blockchain[prev_log_index + i + 1:]:
                                self.update_transaction_ids(will_be_deleted_block, add=False)
                            self.blockchain = self.blockchain[:prev_log_index + i + 1]
                            self.blockchain.append(entry)
                            self.update_transaction_ids(entry)
                    self.save_state(['blockchain'])
                    success = True
                    print(f'Follower: Before commit balance table: {self.balance_table}')
                    self.logger.info(f'Follower: Before commit balance table: {self.balance_table}')
                    # update commit index depends on given message, and commit the previous entries
                    if self.commit_index < message['commit_index']:  # If not, the block has been already committed.
                        first_commit_index = self.commit_index
                        self.commit_index = min(len(self.blockchain) - 1, message['commit_index'])
                        self.save_state(['commit_index'])
                        for i in range(first_commit_index + 1, self.commit_index + 1):
                            block = self.blockchain[i]
                            print(f'Committing: Block index: {i}, Block: {utils.blockchain_print_format([block])}')
                            self.logger.info(f'Committing: Block index: {i}, Block: {block}')
                            self.commit_block(block)
                    print(f'Follower: After commit balance table: {self.balance_table}')
                    self.logger.info(f'Follower: After commit balance table: {self.balance_table}')
                else:
                    success = False
                print(f'Blockchain after Append RPC: {utils.blockchain_print_format(self.blockchain)}')
                self.logger.info(f'Blockchain after Append RPC: {self.blockchain}')
                last_log_index_after_append = len(self.blockchain) - 1

                self.blockchain_lock_by = released_by
                self.save_state(['blockchain_lock_by'])
                self.blockchain_lock.release()

                msg = self.generate_operation_response_message(sender, last_log_index_after_append, success=success)
                start_new_thread(utils.send_message, (msg, Server.CHANNEL_PORT))
            else:  # heartbeat
                self.logger.info(f'Heartbeat from Server {sender}')

            start_new_thread(self.threaded_leader_election_watch, ())

        self.server_state_lock_by = released_by
        self.save_state(['server_state_lock_by'])
        self.server_state_lock.release()

        self.leader_id_lock_by = released_by
        self.save_state(['leader_id_lock_by'])
        self.leader_id_lock.release()

        self.server_term_lock_by = released_by
        self.save_state(['server_term_lock_by'])
        self.server_term_lock.release()

        self.commit_index_lock_by = released_by
        self.save_state(['commit_index_lock_by'])
        self.commit_index_lock.release()

        self.voted_candidate_lock_by = released_by
        self.save_state(['voted_candidate_lock_by'])
        self.voted_candidate_lock.release()

    def on_receive_operation_response(self, sender, message):

        func_name = sys._getframe().f_code.co_name
        acquired_by = 'ACQUIRED by ' + func_name
        released_by = 'RELEASED by ' + func_name

        term = message['term']
        last_log_index_after_append = message['last_log_index_after_append']
        success = message['success']

        self.servers_operation_last_seen_lock.acquire()
        self.servers_operation_last_seen_lock_by = acquired_by
        self.save_state(['servers_operation_last_seen_lock_by'])

        self.servers_operation_last_seen[sender] = time.time()
        self.save_state(['servers_operation_last_seen'])

        self.servers_operation_last_seen_lock_by = released_by
        self.save_state(['servers_operation_last_seen_lock_by'])
        self.servers_operation_last_seen_lock.release()

        if success:
            self.server_term_lock.acquire()
            self.server_term_lock_by = acquired_by
            self.save_state(['server_term_lock_by'])

            self.servers_log_next_index_lock.acquire()
            self.servers_log_next_index_lock_by = acquired_by
            self.save_state(['servers_log_next_index_lock_by'])

            self.accept_indexes_lock.acquire()
            self.accept_indexes_lock_by = acquired_by
            self.save_state(['accept_indexes_lock_by'])

            self.commit_index_lock.acquire()
            self.commit_index_lock_by = acquired_by
            self.save_state(['commit_index_lock_by'])

            self.blockchain_lock.acquire()
            self.blockchain_lock_by = acquired_by
            self.save_state(['blockchain_lock_by'])

            if self.accept_indexes[sender] < last_log_index_after_append:
                self.accept_indexes[sender] = last_log_index_after_append
                self.save_state(['accept_indexes'])

                sorted_accept_indexes = sorted(self.accept_indexes)

                target_accept_index = sorted_accept_indexes[int((len(sorted_accept_indexes) - 1) / 2)]
                if self.blockchain[target_accept_index]['term'] == self.server_term:
                    self.commit_index = target_accept_index
                    self.save_state(['commit_index'])

            self.servers_log_next_index[sender] = len(self.blockchain)
            self.save_state(['servers_log_next_index'])

            self.server_term_lock_by = released_by
            self.save_state(['server_term_lock_by'])
            self.server_term_lock.release()

            self.servers_log_next_index_lock_by = released_by
            self.save_state(['servers_log_next_index_lock_by'])
            self.servers_log_next_index_lock.release()

            self.accept_indexes_lock_by = released_by
            self.save_state(['accept_indexes_lock_by'])
            self.accept_indexes_lock.release()

            self.commit_index_lock_by = released_by
            self.save_state(['commit_index_lock_by'])
            self.commit_index_lock.release()

            self.blockchain_lock_by = released_by
            self.save_state(['blockchain_lock_by'])
            self.blockchain_lock.release()

        self.server_state_lock.acquire()
        self.server_state_lock_by = acquired_by
        self.save_state(['server_state_lock_by'])

        self.server_term_lock.acquire()
        self.server_term_lock_by = acquired_by
        self.save_state(['server_term_lock_by'])

        self.voted_candidate_lock.acquire()
        self.voted_candidate_lock_by = acquired_by
        self.save_state(['voted_candidate_lock_by'])

        if term > self.server_term:
            # success = False

            self.server_state = 'Follower'
            self.server_term = term
            self.voted_candidate = None
            print(f'Becomes Follower for Term: {self.server_term}')
            self.logger.info(f'Follower! Term: {self.server_term} because of on_receive_operation_response')
            self.save_state(['server_state', 'server_term', 'voted_candidate'])

            start_new_thread(self.threaded_leader_election_watch, ())

        self.server_state_lock_by = released_by
        self.save_state(['server_state_lock_by'])
        self.server_state_lock.release()

        self.server_term_lock_by = released_by
        self.save_state(['server_term_lock_by'])
        self.server_term_lock.release()

        self.voted_candidate_lock_by = released_by
        self.save_state(['voted_candidate_lock_by'])
        self.voted_candidate_lock.release()

        self.server_state_lock.acquire()
        self.server_state_lock_by = acquired_by
        self.save_state(['server_state_lock_by'])

        self.servers_log_next_index_lock.acquire()
        self.servers_log_next_index_lock_by = acquired_by
        self.save_state(['servers_log_next_index_lock_by'])

        if not success and self.server_state == "Leader":  # index problem, retry
            self.servers_log_next_index[sender] -= 1
            self.save_state(['servers_log_next_index'])
            start_new_thread(self.threaded_response_watch, (sender,))
            start_new_thread(self.threaded_send_append_request, ([sender],))

        self.server_state_lock_by = released_by
        self.save_state(['server_state_lock_by'])
        self.server_state_lock.release()

        self.servers_log_next_index_lock_by = released_by
        self.save_state(['servers_log_next_index_lock_by'])
        self.servers_log_next_index_lock.release()

    def threaded_on_receive_operation(self, connection):
        # Receive and process append request/response and heartbeat messages.

        header, sender, receiver, message = utils.receive_message(connection)
        self.logger.info(f"Received {header} from Server {sender}: {message}")
        if header == 'Operation-Request':
            self.on_receive_operation_request(sender, message)
        elif header == 'Operation-Response':
            self.on_receive_operation_response(sender, message)
        else:
            raise NotImplementedError(f'Header {header} is not related!')

    def start_threaded_response_watch(self, receiver, commit_index_lock=True, blockchain_lock=True, servers_log_next_index=True):
        func_name = sys._getframe().f_code.co_name
        acquired_by = 'ACQUIRED by ' + func_name
        released_by = 'RELEASED by ' + func_name

        if servers_log_next_index:
            self.servers_log_next_index_lock.acquire()
            self.servers_log_next_index_lock_by = acquired_by
            self.save_state(['servers_log_next_index_lock_by'])

        if commit_index_lock:
            self.commit_index_lock.acquire()
            self.commit_watches_lock_by = acquired_by
            self.save_state(['commit_watches_lock_by'])

        if blockchain_lock:
            self.blockchain_lock.acquire()
            self.blockchain_lock_by = acquired_by
            self.save_state(['blockchain_lock_by'])

        if self.commit_index < len(self.blockchain) - 1 and self.servers_log_next_index[receiver] < len(self.blockchain):
            # at least one index is not committed
            # print('Leader: Uncommitted blocks are detected... Sending request to servers...')
            self.logger.info('Leader: Uncommitted blocks are detected... Sending request to servers...')
            start_new_thread(self.threaded_response_watch, (receiver,))

        if servers_log_next_index:
            self.servers_log_next_index_lock_by = released_by
            self.save_state(['servers_log_next_index_lock_by'])
            self.servers_log_next_index_lock.release()

        if commit_index_lock:
            self.commit_watches_lock_by = released_by
            self.save_state(['commit_watches_lock_by'])
            self.commit_index_lock.release()

        if blockchain_lock:
            self.blockchain_lock_by = released_by
            self.save_state(['blockchain_lock_by'])
            self.blockchain_lock.release()

    def threaded_response_watch(self, receiver):
        # Watch whether we receive response for a specific normal operation message sent. If not, resend the message.

        func_name = sys._getframe().f_code.co_name
        acquired_by = 'ACQUIRED by ' + func_name
        released_by = 'RELEASED by ' + func_name

        self.logger.info(f'Response watch starts, {time.time()}')
        timeout = random.uniform(Server.MESSAGE_SENDING_TIMEOUT, Server.MESSAGE_SENDING_TIMEOUT * 2)
        time.sleep(timeout)

        self.servers_operation_last_seen_lock.acquire()
        self.servers_operation_last_seen_lock_by = acquired_by
        self.save_state(['servers_operation_last_seen_lock_by'])

        if time.time() - self.servers_operation_last_seen[receiver] >= timeout:  # timed out, resend
            # start_new_thread(self.threaded_response_watch, (receiver,))
            start_new_thread(self.threaded_send_append_request, ([receiver],))

        self.servers_operation_last_seen_lock_by = released_by
        self.save_state(['servers_operation_last_seen_lock_by'])
        self.servers_operation_last_seen_lock.release()

    def threaded_send_append_request(self, receivers):
        # Send append requests to followers.

        func_name = sys._getframe().f_code.co_name
        acquired_by = 'ACQUIRED by ' + func_name
        released_by = 'RELEASED by ' + func_name

        self.server_state_lock.acquire()
        self.server_state_lock_by = acquired_by
        self.save_state(['server_state_lock_by'])

        if self.server_state == 'Leader':
            for receiver in receivers:
                msg = self.generate_operation_request_message(receiver)
                # start_new_thread(self.threaded_on_receive_operation, ())
                self.logger.info(f'Sending append request {msg} to {receiver}')
                start_new_thread(utils.send_message, (msg, Server.CHANNEL_PORT))
                start_new_thread(self.threaded_response_watch, (receiver,))

        self.server_state_lock_by = released_by
        self.save_state(['server_state_lock_by'])
        self.server_state_lock.release()

    def threaded_send_heartbeat(self):

        func_name = sys._getframe().f_code.co_name
        acquired_by = 'ACQUIRED by ' + func_name
        released_by = 'RELEASED by ' + func_name

        self.server_state_lock.acquire()
        self.server_state_lock_by = acquired_by
        self.save_state(['server_state_lock_by'])

        while self.server_state == 'Leader':
            # heartbeat broadcast
            for receiver in self.other_servers:
                # print(f'Sending heartbeat to {receiver}')
                msg = self.generate_operation_request_message(receiver, is_heartbeat=True)
                # start_new_thread(self.threaded_on_receive_operation, ())
                start_new_thread(utils.send_message, (msg, Server.CHANNEL_PORT))

            self.server_state_lock_by = released_by
            self.save_state(['server_state_lock_by'])
            self.server_state_lock.release()

            time.sleep(Server.HEARTBEAT_TIMEOUT)

            self.server_state_lock.acquire()
            self.server_state_lock_by = acquired_by
            self.save_state(['server_state_lock_by'])

        print('Step down from leader. Heartbeat stops.')
        self.logger.info('Step down from leader. Heartbeat stops.')

        self.server_state_lock_by = released_by
        self.save_state(['server_state_lock_by'])
        self.server_state_lock.release()

    def threaded_become_leader(self, term):
        # Initialize the next index, last log index, send the first heartbeat.

        func_name = sys._getframe().f_code.co_name
        acquired_by = 'ACQUIRED by ' + func_name
        released_by = 'RELEASED by ' + func_name

        self.server_state_lock.acquire()
        self.server_state_lock_by = acquired_by
        self.save_state(['server_state_lock_by'])

        self.leader_id_lock.acquire()
        self.leader_id_lock_by = acquired_by
        self.save_state(['leader_id_lock_by'])

        self.server_term_lock.acquire()
        self.server_term_lock_by = acquired_by
        self.save_state(['server_term_lock_by'])

        self.servers_log_next_index_lock.acquire()
        self.servers_log_next_index_lock_by = acquired_by
        self.save_state(['servers_log_next_index_lock_by'])

        self.commit_index_lock.acquire()
        self.commit_index_lock_by = acquired_by
        self.save_state(['commit_index_lock_by'])

        self.commit_watches_lock.acquire()
        self.commit_watches_lock_by = acquired_by
        self.save_state(['commit_watches_lock_by'])

        self.blockchain_lock.acquire()
        self.blockchain_lock_by = acquired_by
        self.save_state(['blockchain_lock_by'])

        self.transaction_queue_lock.acquire()
        self.transaction_queue_lock_by = acquired_by
        self.save_state(['transaction_queue_lock_by'])

        if self.server_term == term:
            print(f'Becomes Leader for Term: {self.server_term}')
            self.logger.info(f'Leader! Term: {self.server_term}')
            self.server_state = 'Leader'
            self.servers_log_next_index = 3 * [len(self.blockchain)]
            self.leader_id = self.server_id
            self.transaction_queue = deque()

            self.commit_watches = []
            for block_index in range(self.commit_index + 1, len(self.blockchain)):
                heappush(self.commit_watches, block_index)
                start_new_thread(self.threaded_commit_watch, (block_index,))

            self.save_state(['server_state', 'servers_log_next_index', 'leader_id', 'transaction_queue', 'commit_watches'])

            start_new_thread(self.threaded_send_heartbeat, ())
            start_new_thread(self.threaded_proof_of_work, ())
            start_new_thread(self.threaded_announce_leadership_to_clients, ())

        self.server_state_lock_by = released_by
        self.save_state(['server_state_lock_by'])
        self.server_state_lock.release()

        self.leader_id_lock_by = released_by
        self.save_state(['leader_id_lock_by'])
        self.leader_id_lock.release()

        self.server_term_lock_by = released_by
        self.save_state(['server_term_lock_by'])
        self.server_term_lock.release()

        self.servers_log_next_index_lock_by = released_by
        self.save_state(['servers_log_next_index_lock_by'])
        self.servers_log_next_index_lock.release()

        self.commit_index_lock_by = released_by
        self.save_state(['commit_index_lock_by'])
        self.commit_index_lock.release()

        self.commit_watches_lock_by = released_by
        self.save_state(['commit_watches_lock_by'])
        self.commit_watches_lock.release()

        self.blockchain_lock_by = released_by
        self.save_state(['blockchain_lock_by'])
        self.blockchain_lock.release()

        self.transaction_queue_lock_by = released_by
        self.save_state(['transaction_queue_lock_by'])
        self.transaction_queue_lock.release()

    def start_operation_listener(self):
        # Start listener for operation messages.

        self.sockets[2].listen(Server.MAX_CONNECTION)
        while True:
            connection, (ip, port) = self.sockets[2].accept()
            start_new_thread(self.threaded_on_receive_operation, (connection,))

    # Vote utilities.
    def threaded_leader_election_watch(self):
        # Watch whether the election has timed out. Call on leader election timeout if so.

        func_name = sys._getframe().f_code.co_name
        acquired_by = 'ACQUIRED by ' + func_name
        released_by = 'RELEASED by ' + func_name

        # Update the last updated time.
        self.last_election_time_lock.acquire()
        self.last_election_time_lock_by = acquired_by
        self.save_state(['last_election_time_lock_by'])

        self.last_election_time = time.time()
        self.save_state(['last_election_time'])

        self.logger.info(f'Election watch time is started: {self.last_election_time}')

        self.last_election_time_lock_by = released_by
        self.save_state(['last_election_time_lock_by'])
        self.last_election_time_lock.release()

        timeout = random.uniform(Server.LEADER_ELECTION_TIMEOUT, Server.LEADER_ELECTION_TIMEOUT * 2)
        time.sleep(timeout)
        self.last_election_time_lock.acquire()
        self.last_election_time_lock_by = acquired_by
        self.save_state(['last_election_time_lock_by'])

        diff = time.time() - self.last_election_time
        if diff >= timeout:
            start_new_thread(self.threaded_on_leader_election_timeout, ())

        self.last_election_time_lock_by = released_by
        self.save_state(['last_election_time_lock_by'])
        self.last_election_time_lock.release()

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

        func_name = sys._getframe().f_code.co_name
        acquired_by = 'ACQUIRED by ' + func_name
        released_by = 'RELEASED by ' + func_name

        self.server_state_lock.acquire()
        self.server_state_lock_by = acquired_by
        self.save_state(['server_state_lock_by'])

        self.leader_id_lock.acquire()
        self.leader_id_lock_by = acquired_by
        self.save_state(['leader_id_lock_by'])

        self.server_term_lock.acquire()
        self.server_term_lock_by = acquired_by
        self.save_state(['server_term_lock_by'])

        self.voted_candidate_lock.acquire()
        self.voted_candidate_lock_by = acquired_by
        self.save_state(['voted_candidate_lock_by'])

        self.received_votes_lock.acquire()
        self.received_votes_lock_by = acquired_by
        self.save_state(['received_votes_lock_by'])

        self.blockchain_lock.acquire()
        self.blockchain_lock_by = acquired_by
        self.save_state(['blockchain_lock_by'])

        self.server_term += 1
        self.server_state = 'Candidate'
        print(f'Becomes Candidate for Term: {self.server_term}')
        self.logger.info(f'Candidate! Term: {self.server_term}')
        self.voted_candidate = self.server_id
        self.received_votes = 1
        self.leader_id = None
        msgs = [self.generate_vote_request_message(receiver) for receiver in self.other_servers]

        self.save_state(['server_term', 'server_state', 'voted_candidate', 'received_votes', 'leader_id'])

        self.server_state_lock_by = released_by
        self.save_state(['server_state_lock_by'])
        self.server_state_lock.release()

        self.leader_id_lock_by = released_by
        self.save_state(['leader_id_lock_by'])
        self.leader_id_lock.release()

        self.server_term_lock_by = released_by
        self.save_state(['server_term_lock_by'])
        self.server_term_lock.release()

        self.voted_candidate_lock_by = released_by
        self.save_state(['voted_candidate_lock_by'])
        self.voted_candidate_lock.release()

        self.received_votes_lock_by = released_by
        self.save_state(['received_votes_lock_by'])
        self.received_votes_lock.release()

        self.blockchain_lock_by = released_by
        self.save_state(['blockchain_lock_by'])
        self.blockchain_lock.release()

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

        func_name = sys._getframe().f_code.co_name
        acquired_by = 'ACQUIRED by ' + func_name
        released_by = 'RELEASED by ' + func_name

        self.server_state_lock.acquire()
        self.server_state_lock_by = acquired_by
        self.save_state(['server_state_lock_by'])

        self.server_term_lock.acquire()
        self.server_term_lock_by = acquired_by
        self.save_state(['server_term_lock_by'])

        self.voted_candidate_lock.acquire()
        self.voted_candidate_lock_by = acquired_by
        self.save_state(['voted_candidate_lock_by'])

        self.blockchain_lock.acquire()
        self.blockchain_lock_by = acquired_by
        self.save_state(['blockchain_lock_by'])

        reset_leader_election = False

        # Update term.
        if message['term'] > self.server_term:
            self.server_term = message['term']
            print(f'Becomes Follower for Term: {self.server_term}')
            self.logger.info(f'Follower! Term: {self.server_term} because of on_receive_vote_request')
            self.server_state = 'Follower'
            self.voted_candidate = None
            reset_leader_election = True

        # Decide whether to cast vote.
        last_log_term = self.blockchain[-1]['term'] if len(self.blockchain) > 0 else -1
        if message['term'] == self.server_term \
                and self.voted_candidate in {None, message['candidate_id']} \
                and not \
                (last_log_term > message['last_log_term']
                 or (last_log_term == message['last_log_term'] and len(self.blockchain) - 1 > message['last_log_index'])):

            vote = True
            reset_leader_election = True
            self.voted_candidate = message['candidate_id']

        else:
            vote = False
        msg = self.generate_vote_response_message(message['candidate_id'], vote)

        self.save_state(['server_term', 'server_state', 'voted_candidate'])

        self.server_state_lock_by = released_by
        self.save_state(['server_state_lock_by'])
        self.server_state_lock.release()

        self.server_term_lock_by = released_by
        self.save_state(['server_term_lock_by'])
        self.server_term_lock.release()

        self.voted_candidate_lock_by = released_by
        self.save_state(['voted_candidate_lock_by'])
        self.voted_candidate_lock.release()

        self.blockchain_lock_by = released_by
        self.save_state(['blockchain_lock_by'])
        self.blockchain_lock.release()

        # Send message and reset election timeout if vote.
        start_new_thread(utils.send_message, (msg, Server.CHANNEL_PORT))
        if reset_leader_election:
            start_new_thread(self.threaded_leader_election_watch, ())

    def on_receive_vote_response(self, message):
        # Receive and process vote response.

        func_name = sys._getframe().f_code.co_name
        acquired_by = 'ACQUIRED by ' + func_name
        released_by = 'RELEASED by ' + func_name

        self.server_state_lock.acquire()
        self.server_state_lock_by = acquired_by
        self.save_state(['server_state_lock_by'])

        self.server_term_lock.acquire()
        self.server_term_lock_by = acquired_by
        self.save_state(['server_term_lock_by'])

        self.last_election_time_lock.acquire()
        self.last_election_time_lock_by = acquired_by
        self.save_state(['last_election_time_lock_by'])

        self.received_votes_lock.acquire()
        self.received_votes_lock_by = acquired_by
        self.save_state(['received_votes_lock_by'])

        become_leader = False

        if message['term'] > self.server_term:  # Discover higher term.
            self.server_term = message['term']
            print(f'Becomes Follower for Term: {self.server_term}')
            self.logger.info(f'Follower! Term: {self.server_term} because of on_receive_vote_response')
            self.server_state = 'Follower'

        if self.server_state == 'Candidate':  # Hasn't stepped down yet.
            if message['vote'] and message['term'] == self.server_term:  # Receive vote for current term.
                self.received_votes += 1
            if self.received_votes >= len(Server.SERVER_PORTS) // 2 + 1:  # Received enough votes to become leader.
                become_leader = True
                self.last_election_time = time.time()  # Update the last election time to avoid previous timeout watches. Don't start new timeout watch.

        if become_leader and self.server_state != 'Leader':
            start_new_thread(self.threaded_become_leader, (self.server_term,))

        self.save_state(['server_term', 'server_state', 'received_votes', 'last_election_time'])

        self.server_state_lock_by = released_by
        self.save_state(['server_state_lock_by'])
        self.server_state_lock.release()

        self.server_term_lock_by = released_by
        self.save_state(['server_term_lock_by'])
        self.server_term_lock.release()

        self.received_votes_lock_by = released_by
        self.save_state(['received_votes_lock_by'])
        self.received_votes_lock.release()

        self.last_election_time_lock_by = released_by
        self.save_state(['last_election_time_lock_by'])
        self.last_election_time_lock.release()

    def threaded_on_receive_vote(self, connection):
        # Receive and process the vote request/response messages.

        header, sender, receiver, message = utils.receive_message(connection)
        self.logger.info(f"Received {header} from Server {sender}: {message}")
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
    def generate_client_response_message(self, transaction, transaction_result):

        header = 'Client-Response'
        sender = self.server_id
        receiver = transaction[1][0]
        message = {
            'transaction': transaction,
            'result': transaction_result,
        }

        return header, sender, receiver, message

    def threaded_send_client_response(self, transaction, transaction_result):
        # Compose the response for client.
        # transaction = (id, (A, B, amt)) or (id, (A, )), transaction_result = (True/False, balance_of_A).

        msg = self.generate_client_response_message(transaction, transaction_result)
        start_new_thread(utils.send_message, (msg, Server.CHANNEL_PORT))

    def threaded_announce_leadership_to_clients(self):
        # Announce self is the new leader by sending a special client-response message without actual transaction or result.

        func_name = sys._getframe().f_code.co_name
        acquired_by = 'ACQUIRED by ' + func_name
        released_by = 'RELEASED by ' + func_name

        self.server_state_lock.acquire()
        self.server_state_lock_by = acquired_by
        self.save_state(['server_state_lock_by'])

        if self.server_state == 'Leader':
            for client in Server.CLIENTS:
                header = 'Client-Response'
                sender = self.server_id
                message = {
                    'transaction': None,
                    'result': None
                }
                msg = (header, sender, client, message)
                start_new_thread(utils.send_message, (msg, Server.CHANNEL_PORT))

        self.server_state_lock_by = released_by
        self.save_state(['server_state_lock_by'])
        self.server_state_lock.release()

    def update_balance_table(self, transaction_content):
        if len(transaction_content) == 3:  # transfer transaction
            sender, receiver, amount = transaction_content
            self.balance_table[sender] -= amount
            self.balance_table[receiver] += amount
            self.save_state(['balance_table'])

    def commit_block(self, block, lock_balance_table=True):

        func_name = sys._getframe().f_code.co_name
        acquired_by = 'ACQUIRED by ' + func_name
        released_by = 'RELEASED by ' + func_name

        if lock_balance_table:
            self.balance_table_lock.acquire()
            self.balance_table_lock_by = acquired_by
            self.save_state(['balance_table_lock_by'])

        for transaction in block['transactions']:
            if transaction is not None:
                transaction_id, transaction_content = transaction
                self.update_balance_table(transaction_content)

        if lock_balance_table:
            self.balance_table_lock_by = released_by
            self.save_state(['balance_table_lock_by'])
            self.balance_table_lock.release()

    def send_clients_responses(self, block, lock_balance_table=True):

        func_name = sys._getframe().f_code.co_name
        acquired_by = 'ACQUIRED by ' + func_name
        released_by = 'RELEASED by ' + func_name

        if lock_balance_table:
            self.balance_table_lock.acquire()
            self.balance_table_lock_by = acquired_by
            self.save_state(['balance_table_lock_by'])

        for i, transaction in enumerate(block['transactions']):
            if transaction is not None:
                transaction_id, transaction_content = transaction
                balance = self.balance_table[transaction_content[0]]
                estimated_balance = self.get_estimate_balance_table(
                    lock_commit_table=False, lock_balance_table=False, lock_blockchain=False)[transaction_content[0]]
                start_new_thread(self.threaded_send_client_response,
                                 (transaction, (True, balance, estimated_balance)))
        if lock_balance_table:
            self.balance_table_lock_by = released_by
            self.save_state(['balance_table_lock_by'])
            self.balance_table_lock.release()

    def threaded_commit_watch(self, block_index):
        # Inform the client if the transaction's block has been committed.

        func_name = sys._getframe().f_code.co_name
        acquired_by = 'ACQUIRED by ' + func_name
        released_by = 'RELEASED by ' + func_name

        sent = False
        print(f'Commit watch started for block index {block_index}')
        self.logger.info(f'Commit watch started for block index {block_index}')
        self.server_state_lock.acquire()
        self.server_state_lock_by = acquired_by
        self.save_state(['server_state_lock_by'])

        while not sent and self.server_state == 'Leader':

            self.commit_index_lock.acquire()
            self.commit_index_lock_by = acquired_by
            self.save_state(['commit_index_lock_by'])

            self.commit_watches_lock.acquire()
            self.commit_watches_lock_by = acquired_by
            self.save_state(['commit_watches_lock_by'])

            if self.commit_index >= block_index == self.commit_watches[0]:
                self.blockchain_lock.acquire()
                self.blockchain_lock_by = acquired_by
                self.save_state(['blockchain_lock_by'])

                print(f'Leader: Before commit balance table: {self.balance_table}')
                self.logger.info(f'Leader: Before commit balance table: {self.balance_table}')
                block = self.blockchain[block_index]
                print(f'Committing: Block index: {block_index}, Block: {utils.blockchain_print_format([block])}')
                self.logger.info(f'Committing: Block index: {block_index}, Block: {block}')
                self.commit_block(block)
                print(f'Leader: After commit balance table: {self.balance_table}')
                self.logger.info(f'Leader: After commit balance table: {self.balance_table}')
                self.send_clients_responses(block)
                sent = True
                # Remove the commit watch from the commit watch list.
                heappop(self.commit_watches)

                self.blockchain_lock_by = released_by
                self.save_state(['blockchain_lock_by'])
                self.blockchain_lock.release()

            self.save_state(['commit_watches'])

            self.commit_index_lock_by = released_by
            self.save_state(['commit_index_lock_by'])
            self.commit_index_lock.release()

            self.commit_watches_lock_by = released_by
            self.save_state(['commit_watches_lock_by'])
            self.commit_watches_lock.release()

            self.server_state_lock_by = released_by
            self.save_state(['server_state_lock_by'])
            self.server_state_lock.release()

            time.sleep(Server.SLEEP_INTERVAL)

            self.server_state_lock.acquire()
            self.server_state_lock_by = acquired_by
            self.save_state(['server_state_lock_by'])

        self.server_state_lock_by = released_by
        self.save_state(['server_state_lock_by'])
        self.server_state_lock.release()

    @classmethod
    def is_transaction_valid(cls, estimated_balance_table, transactions, new_transaction):
        estimated_balance_table_copy = copy.deepcopy(estimated_balance_table)
        for transaction in transactions:
            if transaction is not None and len(new_transaction[1]) == 3:  # transfer transaction
                sender, receiver, amount = new_transaction[1]
                estimated_balance_table_copy[sender] -= amount
                estimated_balance_table_copy[receiver] += amount
        if new_transaction is not None and len(new_transaction[1]) == 3:
            sender, receiver, amount = new_transaction[1]
            if estimated_balance_table_copy[sender] < amount:
                return False
        return True

    def get_estimate_balance_table(self, lock_balance_table=True, lock_commit_table=True, lock_blockchain=True, from_scratch=False):
        # lock_balance_table: True if lock should be used.

        func_name = sys._getframe().f_code.co_name
        acquired_by = 'ACQUIRED by ' + func_name
        released_by = 'RELEASED by ' + func_name

        def get_balance_table_change(blockchain, start_index):
            table_diff = [0, 0, 0]
            for block in blockchain[start_index:]:
                for transaction in block['transactions']:
                    if transaction is not None:
                        transaction_content = transaction[1]
                        if len(transaction_content) == 3:  # transfer transaction
                            sender, receiver, amount = transaction_content
                            table_diff[sender] -= amount
                            table_diff[receiver] += amount
            return table_diff

        if lock_commit_table:
            self.commit_index_lock.acquire()
            self.commit_index_lock_by = acquired_by
            self.save_state(['commit_index_lock_by'])

        if lock_blockchain:
            self.blockchain_lock.acquire()
            self.blockchain_lock_by = acquired_by
            self.save_state(['blockchain_lock_by'])

        if lock_balance_table:
            self.balance_table_lock.acquire()
            self.balance_table_lock_by = acquired_by
            self.save_state(['balance_table_lock_by'])

        balance_table_copy = copy.deepcopy(self.balance_table)
        estimated_balance_table = [100, 100, 100]
        if from_scratch:
            # assuming everyone has 10-10-10 in the beginning
            balance_table_diff = get_balance_table_change(self.blockchain, start_index=0)
        elif len(self.blockchain) - 1 == self.commit_index:
            # last index is committed no need to update
            estimated_balance_table = balance_table_copy
            balance_table_diff = [0, 0, 0]
        else:
            # from commit index
            estimated_balance_table = balance_table_copy
            balance_table_diff = get_balance_table_change(self.blockchain, start_index=self.commit_index + 1)

        for i, diff in enumerate(balance_table_diff):
            estimated_balance_table[i] += diff

        if lock_commit_table:
            self.commit_index_lock_by = released_by
            self.save_state(['commit_index_lock_by'])
            self.commit_index_lock.release()
        if lock_blockchain:
            self.blockchain_lock_by = released_by
            self.save_state(['blockchain_lock_by'])
            self.blockchain_lock.release()
        if lock_balance_table:
            self.balance_table_lock_by = released_by
            self.save_state(['balance_table_lock_by'])
            self.balance_table_lock.release()

        return estimated_balance_table

    def threaded_proof_of_work(self):

        func_name = sys._getframe().f_code.co_name
        acquired_by = 'ACQUIRED by ' + func_name
        released_by = 'RELEASED by ' + func_name
        # Doing proof of work based on the queue of transactions.
        transactions = []
        nonce = None
        found = False
        estimated_balance_table = self.get_estimate_balance_table()
        # print(f'estimated_balance_table: {estimated_balance_table}')

        self.server_state_lock.acquire()
        self.server_state_lock_by = acquired_by
        self.save_state(['server_state_lock_by'])

        while self.server_state == 'Leader':
            # Add new transactions to current proof of work.
            self.server_state_lock_by = released_by
            self.save_state(['server_state_lock_by'])
            self.server_state_lock.release()

            self.transaction_queue_lock.acquire()
            self.transaction_queue_lock_by = acquired_by
            self.save_state(['transaction_queue_lock_by'])

            while len(transactions) < Server.MAX_TRANSACTION_COUNT and len(self.transaction_queue) > 0:
                # print(f'before transaction: {self.transaction_queue}')
                transaction = self.transaction_queue.popleft()
                # print(f'after transaction: {self.transaction_queue}')

                self.transaction_queue_lock_by = released_by
                self.save_state(['transaction_queue_lock_by'])
                self.transaction_queue_lock.release()

                if Server.is_transaction_valid(estimated_balance_table, transactions, transaction):  # Transaction valid
                    transactions.append(transaction)
                else:  # Transaction invalid.

                    self.balance_table_lock.acquire()
                    self.balance_table_lock_by = acquired_by
                    self.save_state(['balance_table_lock_by'])

                    transaction_content = transaction[1]
                    balance = self.balance_table[transaction_content[0]]
                    estimated_balance = self.get_estimate_balance_table(lock_balance_table=False)[transaction_content[0]]

                    self.balance_table_lock_by = released_by
                    self.save_state(['balance_table_lock_by'])
                    self.balance_table_lock.release()

                    start_new_thread(self.threaded_send_client_response,
                                     (transaction, (False, balance, estimated_balance)))
                self.save_state(['transaction_queue'])

                self.transaction_queue_lock.acquire()
                self.transaction_queue_lock_by = acquired_by
                self.save_state(['transaction_queue_lock_by'])

            self.transaction_queue_lock_by = released_by
            self.save_state(['transaction_queue_lock_by'])
            self.transaction_queue_lock.release()

            # Do proof of work if transactions are not empty.
            if len(transactions) > 0:
                nonce = utils.generate_random_string_with_ending(length=6, ending={'0', '1', '2'})
                cur_pow = utils.get_hash(transactions, nonce)
                if '2' >= cur_pow[-1] >= '0':
                    found = True

            # If PoW is found:
            if found:
                self.server_state_lock.acquire()
                self.server_state_lock_by = acquired_by
                self.save_state(['server_state_lock_by'])

                if self.server_state == 'Leader':

                    # Update the blockchain.
                    self.server_term_lock.acquire()
                    self.server_term_lock_by = acquired_by
                    self.save_state(['server_term_lock_by'])

                    self.accept_indexes_lock.acquire()
                    self.accept_indexes_lock_by = acquired_by
                    self.save_state(['accept_indexes_lock_by'])

                    self.commit_watches_lock.acquire()
                    self.commit_watches_lock_by = acquired_by
                    self.save_state(['commit_watches_lock_by'])

                    self.blockchain_lock.acquire()
                    self.blockchain_lock_by = acquired_by
                    self.save_state(['blockchain_lock_by'])

                    phash = None
                    if len(self.blockchain) > 0:
                        previous_nonce = self.blockchain[-1]['nonce']
                        previous_transactions = self.blockchain[-1]['transactions']
                        phash = utils.get_hash(previous_transactions, previous_nonce)

                    while len(transactions) < Server.MAX_TRANSACTION_COUNT:
                        transactions.append(None)

                    block = {
                        'term': self.server_term,
                        'phash': phash,
                        'nonce': nonce,
                        'transactions': transactions,
                    }
                    self.blockchain.append(block)

                    self.accept_indexes[self.server_id] = len(self.blockchain) - 1

                    block_index = len(self.blockchain) - 1

                    # Send append request.
                    start_new_thread(self.threaded_send_append_request, (self.other_servers,))

                    # Call commit watch.
                    heappush(self.commit_watches, block_index)
                    self.save_state(['commit_watches'])
                    start_new_thread(self.threaded_commit_watch, (block_index,))

                    self.save_state(['blockchain', 'accept_indexes'])

                    self.server_term_lock_by = released_by
                    self.save_state(['server_term_lock_by'])
                    self.server_term_lock.release()

                    self.accept_indexes_lock_by = released_by
                    self.save_state(['accept_indexes_lock_by'])
                    self.accept_indexes_lock.release()

                    self.commit_watches_lock_by = released_by
                    self.save_state(['commit_watches_lock_by'])
                    self.commit_watches_lock.release()

                    self.blockchain_lock_by = released_by
                    self.save_state(['blockchain_lock_by'])
                    self.blockchain_lock.release()

                    print("Proof of work is done.")
                    self.logger.info(f'Proof of work is done for block: {block}')

                    # Reset proof of work variables.
                    transactions = []
                    nonce = None
                    found = False
                    estimated_balance_table = self.get_estimate_balance_table()

                self.server_state_lock_by = released_by
                self.save_state(['server_state_lock_by'])
                self.server_state_lock.release()

            time.sleep(Server.SLEEP_INTERVAL)

            self.server_state_lock.acquire()
            self.server_state_lock_by = acquired_by
            self.save_state(['server_state_lock_by'])

        self.server_state_lock_by = released_by
        self.save_state(['server_state_lock_by'])
        self.server_state_lock.release()

    def threaded_on_receive_client(self, connection):
        # Receive transaction request from client.

        func_name = sys._getframe().f_code.co_name
        acquired_by = 'ACQUIRED by ' + func_name
        released_by = 'RELEASED by ' + func_name

        header, sender, receiver, message = utils.receive_message(connection)
        self.logger.info(f"Received {header} from Client {sender}: {message}")

        self.server_state_lock.acquire()
        self.server_state_lock_by = acquired_by
        self.save_state(['server_state_lock_by'])

        if self.server_state == 'Leader':  # Process the request.

            transaction = message['transaction']
            transaction_id = transaction[0]

            self.transaction_queue_lock.acquire()
            self.transaction_queue_lock_by = acquired_by
            self.save_state(['transaction_queue_lock_by'])

            self.transaction_ids_lock.acquire()
            self.transaction_ids_lock_by = acquired_by
            self.save_state(['transaction_ids_lock_by'])

            if transaction_id not in self.transaction_ids:  # Transactions hasn't been processed yet.
                self.transaction_ids.add(transaction_id)
                self.transaction_queue.append(transaction)
                self.save_state(['transaction_ids', 'transaction_queue'])

            self.server_state_lock_by = released_by
            self.save_state(['server_state_lock_by'])
            self.server_state_lock.release()

            self.transaction_queue_lock_by = released_by
            self.save_state(['transaction_queue_lock_by'])
            self.transaction_queue_lock.release()

            self.transaction_ids_lock_by = released_by
            self.save_state(['transaction_ids_lock_by'])
            self.transaction_ids_lock.release()

        else:  # Relay the client message to the current leader.
            self.server_state_lock_by = released_by
            self.save_state(['server_state_lock_by'])
            self.server_state_lock.release()

            while True:
                time.sleep(Server.SLEEP_INTERVAL)

                self.leader_id_lock.acquire()
                self.leader_id_lock_by = acquired_by
                self.save_state(['leader_id_lock_by'])

                if self.leader_id is not None:  # Wait until a leader is elected.
                    msg = (header, self.server_id, self.leader_id, message)
                    start_new_thread(utils.send_message, (msg, server.CHANNEL_PORT))
                    self.leader_id_lock_by = released_by
                    self.save_state(['leader_id_lock_by'])
                    self.leader_id_lock.release()
                    break
                self.leader_id_lock_by = released_by
                self.save_state(['leader_id_lock_by'])
                self.leader_id_lock.release()

    def start_client_listener(self):
        # Start listener for client messages.

        self.sockets[0].listen(Server.MAX_CONNECTION)
        while True:
            connection, (ip, port) = self.sockets[0].accept()
            start_new_thread(self.threaded_on_receive_client, (connection,))

    def start(self):
        # Start the listeners for messages and timeout watches.

        # creating persistence folder
        if not os.path.exists(f'server_{self.server_id}_states'):
            os.makedirs(f'server_{self.server_id}_states')

        # Load the state, if any.
        persistent_states = ['server_state', 'leader_id', 'server_term', 'servers_operation_last_seen', 'servers_log_next_index',
                             'accept_indexes', 'commit_index', 'commit_watches', 'last_election_time', 'voted_candidate', 'received_votes', 'blockchain',
                             'first_blockchain_read', 'balance_table', 'transaction_queue', 'transaction_ids']
        self.load_state(persistent_states)

        if not self.first_blockchain_read:
            with open('first_blockchain_processed.pkl', 'rb') as _fb:
                self.blockchain = pickle.load(_fb)
            self.commit_index = len(self.blockchain) - 1
            self.first_blockchain_read = True
            self.balance_table = self.get_estimate_balance_table(from_scratch=True)
            self.servers_log_next_index = [len(self.blockchain), len(self.blockchain), len(self.blockchain)]
            self.accept_indexes = [self.commit_index, self.commit_index, self.commit_index]

        self.save_state(persistent_states)

        # Start/Resume operations based on the server state.
        threads = [(self.start_client_listener, ()), (self.start_vote_listener, ()), (self.start_operation_listener, ())]
        if self.server_state in ('Follower', 'Candidate'):
            threads.append((self.threaded_leader_election_watch, ()))
        else:  # Leader.
            threads.append((self.threaded_send_heartbeat, ()))
            threads.append((self.threaded_proof_of_work, ()))
            for commit_watch in self.commit_watches:
                threads.append((self.threaded_commit_watch, (commit_watch,)))
            for receiver in self.other_servers:
                threads.append((self.start_threaded_response_watch, (receiver,)))
        for (thread, args) in threads:
            start_new_thread(thread, args)
        print(f'Starts as {self.server_state} for Term: {self.server_term}')
        self.logger.info(f'Starts as {self.server_state}! Term: {self.server_term}')

        while 1:
            pass


if __name__ == '__main__':
    server = Server()
    server.start()
