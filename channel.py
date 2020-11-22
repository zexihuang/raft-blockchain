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
        self.is_gate_open = [True, True, True]

        # Set up the ports.
        pass

    def threaded_on_receive(self, connection, ip, port):
        # Relay the message from the sender to the receiver.

        Channel.network_delay()
        pass

    def start_message_listener(self):
        # Start the message listener for all incoming messages.

        pass

    def configuration_change_handler(self):
        # Get input from the user to change the network configuration for network partition.
        while True:
            self.is_gate_open = get_partition_config()
            print(f"Configuration has changed to: {self.is_gate_open}")

    def start(self):
        # Start the listener for messages and user input handler.
        pass


if __name__ == '__main__':
    channel = Channel()
    channel.start()
