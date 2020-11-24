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


class Client:
    CHANNEL_PORT = 10000
    CLIENT_PORTS = {
        0: 10001,
        1: 10002,
        2: 10003
    }
    MAX_CONNECTION = 100
    BUFFER_SIZE = 65536

    def __init__(self):

        # Get the client name.
        while True:
            self.client_id = int(input('Which client are you? Enter 0, 1 or 2. \n'))
            if self.client_id in Client.CLIENT_PORTS:
                self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                self.socket.bind((socket.gethostname(), Client.CLIENT_PORTS[self.client_id]))
                break
            else:
                print('Wrong client name. Please enter 0, 1 or 2.')

        self.leader_id_guess = self.client_id
        self.leader_id_guess_lock = Lock()

        self.transaction_receipts = set()
        self.transaction_receipts_lock = Lock()

    def generate_client_request_message(self, transaction_id, transaction):
        header = 'Client-Request'
        sender = self.client_id

        self.leader_id_guess_lock.acquire()
        receiver = self.leader_id_guess
        self.leader_id_guess_lock.release()
        message = {
            'id': transaction_id,
            'transaction': transaction,
        }

        return header, sender, receiver, message

    def threaded_send_client_request(self, transaction):
        # Send the transaction request to the blockchain.
        transaction_id = time.time()
        msg = self.generate_client_request_message(transaction_id, transaction)
        start_new_thread(utils.send_message, (msg, Client.CHANNEL_PORT))
        start_new_thread(self.threaded_response_watch, (transaction_id, msg))

    def threaded_response_watch(self, transaction_id, msg):
        # Resend request if the response for a certain transaction msg timeout.

        timeout = random.uniform(5.0, 10.0)
        time.sleep(timeout)
        self.transaction_receipts_lock.acquire()
        if transaction_id not in self.transaction_receipts:  # Resend request and restart timeout.
            start_new_thread(utils.send_message, (msg, Client.CHANNEL_PORT))
            start_new_thread(self.threaded_response_watch, (transaction_id, msg))
        else:  # Garbage collection since the transaction_id will never be checked again.
            self.transaction_receipts.remove(transaction_id)

        self.transaction_receipts_lock.release()

    def threaded_on_receive_client_response(self, connection):
        # Inform the user of the transaction feedback.

        header, sender, receiver, message = utils.receive_message(connection)

        self.leader_id_guess_lock.acquire()
        self.transaction_receipts_lock.acquire()

        self.leader_id_guess = sender
        self.transaction_receipts.add(message['id'])

        self.leader_id_guess_lock.release()
        self.transaction_receipts_lock.release()

        transaction = message['transaction']
        result = message['result']
        if len(transaction) == 1:  # Balance transaction
            print('Balance transaction successful')
            print(f'Your ({transaction[0]}) balance is: {result[1]}\n')
        else:  # Transfer transaction.
            print(f'Transfer transaction from you ({transaction[0]}) to {transaction[1]} '
                  f'{"successful" if result[0] else "unsuccessful"}.')
            print(f'Your ({transaction[0]}) balance is: {result[1]}\n')

    def start_client_response_listener(self):
        # Start the listener for transaction feedback.

        self.socket.listen(Client.MAX_CONNECTION)
        while True:
            connection, (ip, port) = self.socket.accept()
            start_new_thread(self.threaded_on_receive_client_response, (connection,))

    @staticmethod
    def get_action():
        try:
            action = input('\nWhich action? Enter 1 (Transfer Money), 2 (Check Balance), 3 (Exit). \n')
            if action in ['1', '2', '3']:
                return action
            else:
                print('Wrong input! It should be in {1, 2, 3}')
                return -1
        except Exception as e:
            print('Wrong input! It should be in {1, 2, 3}')
            return -1

    @staticmethod
    def get_transaction(sender):
        try:
            action = input('Receiver;Amount of Transaction (use a;b format ) or Balance?. \n')
            receiver, amount = tuple(action.split(';'))
            receiver, amount = int(receiver), int(amount)
            if receiver == sender:
                print('You cannot send transactions to yourself!')
                return -1
            elif receiver in set(Client.CLIENT_PORTS.keys()) - {sender}:
                return sender, receiver, amount
            else:
                print('No receiver found!')
                return -1
        except Exception as e:
            print('Something is wrong with the input!')
            return -1

    def transaction_handler(self):
        # Get input from the user about transactions.
        done = False
        while not done:
            action = self.get_action()
            if action != -1:
                if action == '1':  # Transaction
                    transaction = self.get_transaction(self.client_id)
                    if transaction == -1:
                        transaction = None
                elif action == '2':  # Balance
                    transaction = (self.client_id, )
                else:  # Exit
                    transaction = None
                    done = True

                if transaction:
                    start_new_thread(self.threaded_send_client_request, (transaction,))
        os._exit(1)

    def start(self):
        # Start listener for transaction feedback and user input transaction handler.

        start_new_thread(self.start_client_response_listener, ())
        start_new_thread(self.transaction_handler, ())
        while 1:
            pass


if __name__ == '__main__':
    client = Client()
    client.start()
