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


class Channel:
    MAX_CONNECTION = 100
    BUFFER_SIZE = 65536

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
        # Set up the network configuration and its lock.
        self.is_gate_open = [True, True, True]
        self.lock = Lock()

        # Set up the ports.
        self.port = Channel.CHANNEL_PORT
        self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.socket.bind((socket.gethostname(), self.port))

        # Set up loggers.
        log_file = f'channel.log'
        if os.path.exists(log_file):
            os.remove(log_file)
        self.logger = logging.getLogger('Channel')
        file_handler = logging.FileHandler(log_file)
        formatter = logging.Formatter('%(asctime)s %(message)s', "%H:%M:%S")
        file_handler.setFormatter(formatter)
        self.logger.addHandler(file_handler)
        self.logger.setLevel(logging.INFO)

    def threaded_on_receive(self, connection):
        # Relay the message from the sender to the receiver.

        header, sender, receiver, message = utils.receive_message(connection)

        # Based on the header and network configuration, decides whether to relay the message.
        if header in ('Client-Request', 'Client-Response'):  # Always relay messages between a client and a server.
            relay = True
        else:  # Don't relay messages that involve an isolated server.
            self.lock.acquire()
            if self.is_gate_open[sender] and self.is_gate_open[receiver]:
                relay = True
            else:
                relay = False
            self.lock.release()

        if relay:
            Channel.network_delay()
            if header == 'Client-Response':  # Receiver is a client.
                receiver_port = Channel.CLIENT_PORTS[receiver]
            elif header in ('Client-Request', 'Client-Relay'):  # Receiver is the server's client listener port.
                receiver_port = Channel.SERVER_PORTS[receiver][0]
            elif header in ('Vote-Request', 'Vote-Response'):  # Receiver is the server's vote listener port.
                receiver_port = Channel.SERVER_PORTS[receiver][1]
            else:  # Receiver is the server's operation listener port.
                receiver_port = Channel.SERVER_PORTS[receiver][2]

            try:
                utils.send_message((header, sender, receiver, message), receiver_port)
            except Exception as e:
                self.logger.info(e)

    def start_message_listener(self):
        # Start the message listener for all incoming messages.

        self.socket.listen(Channel.MAX_CONNECTION)
        while True:
            connection, (ip, port) = self.socket.accept()
            start_new_thread(self.threaded_on_receive, (connection,))

    @staticmethod
    def get_partition_config():

        cur = [True, True, True]
        config = input('\nHow do you partition? (use format: a;b-c, a and b in the same partition): ')
        partitions = config.split("-")

        # check if the input is valid:
        seen = set()
        for partition in partitions:
            for node in partition.split(";"):
                if node in ['0', '1', '2']:
                    seen.add(int(node))
                else:
                    print('Config format is wrong')
                    return cur
        if len(seen) < 3:
            print("Config format in wrong")
            return cur

        # format is valid, check partition
        if len(partitions) == 3:
            # all are isolated
            cur = [False, False, False]
        elif len(partitions) == 2:
            # one isolated
            if len(partitions[0]) == 1:
                cur[int(partitions[0])] = False
            else:
                cur[int(partitions[1])] = False
        return cur

    def configuration_change_handler(self):
        # Get input from the user to change the network configuration for network partition.
        while True:
            self.is_gate_open = self.get_partition_config()
            print(f"Configuration has changed to: {self.is_gate_open}")

    def start(self):
        # Start the listener for messages and user input handler.

        start_new_thread(self.start_message_listener, ())
        start_new_thread(self.configuration_change_handler, ())

        # generate first blockchain
        if not os.path.exists('blockchain_processed.pkl'):
            blockchain = utils.read_first_blockchain('blockchain.txt')
            with open('blockchain_processed.pkl', 'wb') as _fb:
                pickle.dump(blockchain, _fb)

        while 1:
            pass


if __name__ == '__main__':
    channel = Channel()
    channel.start()
