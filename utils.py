import socket
import pickle
import random
import string
import time
import hashlib
import os

BUFFER_SIZE = 65536


def send_message(msg, port):
    # Setup socket for the user to be send
    s_temp = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s_temp.connect((socket.gethostname(), port))

    # encode and send message
    msg = pickle.dumps(msg)
    s_temp.send(msg)

    # Receive ack.

    ack = pickle.loads(s_temp.recv(BUFFER_SIZE))
    # message_logger.info(f'Port {port} sends {ack}\n')
    s_temp.close()


def receive_message(connection):
    # Receive message and send acknowledgement.

    header, sender, receiver, message = pickle.loads(connection.recv(BUFFER_SIZE))
    connection.send(pickle.dumps('ACK'))

    return header, sender, receiver, message


def generate_random_string_with_ending(length, ending):
    found = False
    s = ""
    while not found:
        s = ''.join(random.choices(string.ascii_lowercase + string.digits, k=length))
        if s[-1] in ending:
            found = True
    return s


def get_hash(transactions, nonce):
    will_encode = str((tuple(transactions), nonce))
    return hashlib.sha3_256(will_encode.encode('utf-8')).hexdigest()


def read_first_blockchain():
    def prepare_block(blockchain, transactions, term):
        found = False
        nonce = None
        while not found:
            nonce = generate_random_string_with_ending(length=6, ending={'0', '1', '2'})
            cur_pow = get_hash(transactions, nonce)
            if '2' >= cur_pow[-1] >= '0':
                found = True

        phash = None
        if len(blockchain) > 0:
            previous_nonce = blockchain[-1]['nonce']
            previous_transactions = blockchain[-1]['transactions']
            phash = get_hash(previous_transactions, previous_nonce)

        return {'term': term, 'phash': phash, 'nonce': nonce, 'transactions': transactions}

    if not os.path.exists('first_blockchain_processed.pkl'):
        blockchain = []
        file_path = 'first_blockchain.txt'
        with open(file_path, 'r') as _file:
            term = -1
            transactions = []
            for line in _file.readlines():
                sender, receiver, amount = map(int, tuple(line.split()))
                transaction_id = time.time()
                transaction = (transaction_id, (sender, receiver, amount))
                transactions.append(transaction)
                if len(transactions) == 3:
                    # block is finished, find nonce...
                    block = prepare_block(blockchain, transactions, term)
                    blockchain.append(block)
                    transactions = []
            if len(transactions) > 0:
                transactions += [None for _ in range(3 - len(transactions))]
                block = prepare_block(blockchain, transactions, term)
                blockchain.append(block)

        with open('first_blockchain_processed.pkl', 'wb') as _fb:
            pickle.dump(blockchain, _fb)


def blockchain_print_format(blockchain):
    blockchain_str = ""
    for i, block in enumerate(blockchain):
        term = block['term']
        transactions = block['transactions']
        new_block_str = f'[({term}) {[transaction[1] for transaction in transactions if transaction is not None]}]'
        if i < len(blockchain) - 1:
            new_block_str += ' -> '
        blockchain_str += new_block_str
    return blockchain_str
