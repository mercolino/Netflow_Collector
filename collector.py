import socket
from lib.lib import IP, UDP
import yaml
import sys
import argparse
import threading
import logging
import pika

LEVEL = {'debug': logging.DEBUG,
         'info': logging.INFO,
         'warning': logging.WARNING,
         'error': logging.ERROR,
         'critical': logging.CRITICAL}

def process_data(data, udp_ip, queue_ip, queue_port, username, password, queue_virtual_host, logger):

    # Process IP Header
    ip_header = IP(data[0:20])
    if ip_header.protocol == 'UDP':
        # Process UDP Header
        udp_header = UDP(data[20:28])
        if (ip_header.dst_address == udp_ip) and (udp_header.dst_port == 650):
            # Set Up credentials to connect to queue server
            credentials = pika.PlainCredentials(username, password)
            # Create connection to Queue Server
            conn = pika.BlockingConnection(
                pika.ConnectionParameters(queue_ip, queue_port, queue_virtual_host, credentials))
            channel = conn.channel()

            # Define the queue
            channel.queue_declare(queue='raw_msg_queue')

            channel.basic_publish(exchange='', routing_key='raw_msg_queue', body=data)
            logger.info("**** Message sent to queue! | %s:%s -> %s:%s ****" % (ip_header.src_address,
                                                                               udp_header.src_port,
                                                                               ip_header.dst_address,
                                                                               udp_header.dst_port))
            conn.close()


if __name__ == "__main__":

    # Create the parser for the arguments
    parser = argparse.ArgumentParser()
    parser.add_argument("-v", "--verbose", help="Turn on verbosity on the output", action="store_true", default=False)

    args = parser.parse_args()

    # Load the config.yaml file
    with open('config.yaml', 'r') as f:
        config = yaml.load(f)

    # Set the logging level
    try:
        log_level = LEVEL[config["general"]["collector"]["log"]["level"]]
    except:
        log_level = logging.INFO

    # Create and format the logger and the handler for logging
    logger = logging.getLogger('netflow_collector')
    logger.setLevel(level=log_level)
    handler = logging.StreamHandler()
    handler_formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s',
                                          datefmt='%m/%d/%Y %I:%M:%S %p')
    handler.setFormatter(handler_formatter)
    logger.addHandler(handler)

    # Turn logger on or off depending on the arguments
    if args.verbose:
        logger.disabled = False
    else:
        logger.disabled = True


    # Getting the Queue server Info
    try:
        queue_ip = config["queue_server"]["ip"]
        queue_port = config["queue_server"]["port"]
        queue_username = config["queue_server"]["username"]
        queue_password = config["queue_server"]["password"]
        queue_virtual_host = config["queue_server"]["virtual_host"]
    except:
        logger.critical("**** You should specify, the queue server ip, port, virtual host, username and password ****")
        sys.exit(1)

    # Determining the ip, port and receiving buffer for the collector
    try:
        udp_ip = config["collector_server"]["ip"]
    except:
        logger.critical("**** You should specify the UDP ip address to be used for the collector ****")
        sys.exit(1)

    try:
        udp_port = config["collector_server"]["port"]
    except:
        logger.critical("**** You should specify the UDP port to be used for the collector ****")
        sys.exit(1)

    try:
        udp_buffer = config["collector_server"]["buffer"]
    except:
        udp_buffer = 4096

    # Create the raw socket
    server_socket = socket.socket(socket.AF_INET, socket.SOCK_RAW)

    # Bind the socket to the ip and port specified
    try:
        server_socket.bind((udp_ip, udp_port))
        logger.info("**** Netflow Collector listening on ip %s and port %i ****" % (udp_ip, udp_port))
        logger.info("**** Receive buffer used: %i bytes ****" % (udp_buffer))
    except:
        logger.critical("**** There was a problem binding the socket to the ip %s and port %s ****" % (udp_ip, udp_port))
        sys.exit(1)

    # Receive Data and send to a thread for processing
    while True:
        data, addr = server_socket.recvfrom(udp_buffer)
        logger.info("Connection received from %s, the socket was dispatched to the thread" % addr[0])
        t = threading.Thread(target=process_data, args=(data, udp_ip, queue_ip, queue_port,
                                                        queue_username, queue_password, queue_virtual_host, logger))
        t.start()

