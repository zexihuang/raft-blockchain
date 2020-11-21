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

class Channel:

    MAX_CONNECTION = 100
    BUFFER_SIZE = 1024

    CHANNEL_PORT = 10000
    CLIENT_PORTS = {
        0: 10001,
        1: 10002,
        2: 10003
    }
    SERVER_PORTS = {
        # Client listener port, raft vote listener port, raft operation listener port.
        0: (11001, 12001, 13001),
        1: (11002, 12002, 13002),
        2: (11003, 12003, 13003),
    }

    @classmethod
    def network_delay(cls):
        # Network delay are applied when transmitting a message in the channel.
        delay = random.uniform(1.0, 5.0)
        time.sleep(delay)

    def __init__(self):
        # Set up the network configurations.
        # Set up the ports.
        pass

    def threaded_on_receive(self, connection, ip, port):
        # Relay the message from the sender to the receiver.

        Channel.network_delay()
        pass

    def start_message_listener(self):
        # Start the message listener for all incoming messages.

        pass

    def configuration_change_hanlder(self):
        # Get input from the user to change the network configuration for network partition.

        pass

    def start(self):
        # Start the listener for messages and user input handler.
        pass


if __name__ == '__main__':
    channel = Channel()
    channel.start()