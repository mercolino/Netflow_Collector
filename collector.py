import socket
from lib.lib import IP, UDP
import yaml
import sys
import argparse
import threading
import logging
import pika


def process_data(data, udp_ip, queue_ip, logger):

    # Create connection to Queue Server
    conn = pika.BlockingConnection(pika.ConnectionParameters(queue_ip))
    channel = conn.channel()

    # Define the queue
    channel.queue_declare(queue='raw_msg_queue')

    # Process IP Header
    ip_header = IP(data[0:20])
    if ip_header.protocol == 'UDP':
        # Process UDP Header
        udp_header = UDP(data[20:28])
        if (ip_header.dst_address == udp_ip) and (udp_header.dst_port == 650):
            # print " ".join("%02x" % ord(i) for i in data)
            # print " ".join("%02x" % ord(i) for i in data[0:20])
            # print " ".join("%02x" % ord(i) for i in data[20:28])
            channel.basic_publish(exchange='', routing_key='raw_msg_queue', body=data)
            logger.info("**** Message sent to queue! | %s:%s -> %s:%s ****" % (ip_header.src_address,
                                                                               udp_header.src_port,
                                                                               ip_header.dst_address,
                                                                               udp_header.dst_port))


if __name__ == "__main__":

    # Create the parser for the arguments
    parser = argparse.ArgumentParser()
    parser.add_argument("-v", "--verbose", help="Turn on verbosity on the output", action="store_true", default=False)

    args = parser.parse_args()

    # Create adn format the logger
    logging.basicConfig(format='%(asctime)s - %(name)s - %(levelname)s - %(message)s', datefmt='%m/%d/%Y %I:%M:%S %p',
                        level=logging.DEBUG)
    logger = logging.getLogger('netflow_collector')

    if args.verbose:
        logger.disabled = False
    else:
        logger.disabled = True

    # Load the config.yaml file
    with open('config.yaml', 'r') as f:
        config = yaml.load(f)

    # Getting the Queue server Ip address
    queue_ip = config["queue_server"]["ip"]

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
        logger.info("Connection received form %s, the socket was dispatched to the thread" % addr[0])
        t = threading.Thread(target=process_data, args=(data, udp_ip, queue_ip, logger))
        t.start()

