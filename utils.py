import socket
import pickle
from _thread import start_new_thread


def send_message(msg, channel_port):
    # Setup socket for the user to be send
    s_temp = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s_temp.connect((socket.gethostname(), channel_port))

    # encode and send message
    msg = pickle.dumps(msg)
    s_temp.send(msg)

    # Receive ack.
    BUFFER_SIZE = 65536
    ack = pickle.loads(s_temp.recv(BUFFER_SIZE))
    # message_logger.info(f'Port {port} sends {ack}\n')
    s_temp.close()


def broadcast_message(msg, receivers, channel_port):
    for receiver in receivers:
        msg[2] = receiver
        start_new_thread(send_message, (msg, channel_port))
