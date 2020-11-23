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

    def generate_client_request_message(self):

        # TODO: impolement
        pass


    def threaded_send_client_request(self):
        # Send the transaction request to the blockchain.

        # TODO: generate message and call send message
        pass

    def threaded_on_receive_client_response(self, connection):
        # Inform the user of the transaction feedback.

        header, sender, receiver, message = utils.receive_message(connection)

        # TODO: print out the message to the user (transaction X fail/succeed, your money is now Y).

    def start_client_response_listener(self):
        # Start the listener for transaction feedback.

        self.socket.listen(Client.MAX_CONNECTION)
        while True:
            connection, (ip, port) = self.socket.accept()
            start_new_thread(self.threaded_on_receive_client_response, (connection, ))

    @staticmethod
    def get_transaction(self):

        # TODO: get transaction from the user.
        pass

    def transaction_handler(self):
        # Get input from the user about transactions.

        while True:
            transaction = self.get_transaction()
            start_new_thread(self.threaded_send_client_request, (transaction, ))

        pass

    def start(self):
        # Start listener for transaction feedback and user input transaction handler.

        start_new_thread(self.start_client_response_listener, ())
        start_new_thread(self.transaction_handler, ())
        while 1:
            pass


if __name__ == '__main__':
    client = Client()
    client.start()
