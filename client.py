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


class Client:

    CHANNEL_PORT = 10000
    CLIENT_PORTS = {
        0: 10001,
        1: 10002,
        2: 10003
    }
    MAX_CONNECTION = 100
    BUFFER_SIZE = 1024

    def __init__(self):
        # Get user id, etc.

        pass

    def threaded_send_transaction_request(self):
        # Send the transaction request to the blockchain.

        pass

    def threaded_on_receive_feedback(self):
        # Inform the user of the transaction feedback.

        pass

    def start_feedback_listener(self):
        # Start the listener for transaction feedback.

        pass

    def transaction_handler(self):
        # Get input from the user about transactions.

        # Call send transaction request.

        pass

    def start(self):
        # Start listener for transaction feedback and user input transaction handler.

        pass


if __name__ == '__main__':
    client = Client()
    client.start()
