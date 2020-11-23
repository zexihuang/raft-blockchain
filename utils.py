import socket
import pickle

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
