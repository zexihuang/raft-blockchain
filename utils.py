import socket
import pickle


def send_message(msg, port):
    # Setup socket for the user to be send
    s_temp = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s_temp.connect((socket.gethostname(), port))

    # encode and send message
    msg = pickle.dumps(msg)
    s_temp.send(msg)

    # Receive ack.
    BUFFER_SIZE = 65536
    ack = pickle.loads(s_temp.recv(BUFFER_SIZE))
    # message_logger.info(f'Port {port} sends {ack}\n')
    s_temp.close()
