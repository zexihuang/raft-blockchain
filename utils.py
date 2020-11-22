import socket
import pickle
from _thread import start_new_thread


def send_message(self, msg, port):
    # Setup socket for the user to be send
    s_temp = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s_temp.connect((socket.gethostname(), port))

    # encode and send message
    msg = pickle.dumps(msg)
    s_temp.send(msg)

    # Receive ack.
    BUFFER_SIZE = 65536
    ack = pickle.loads(s_temp.recv(BUFFER_SIZE))
    self.message_logger.info(f'Port {port} sends {ack}\n')
    s_temp.close()


def broadcast_message(self, msg):
    users_to_be_sent = {0, 1, 2} - {self.user}
    for user in users_to_be_sent:
        start_new_thread(self.send_message, (msg, user))
