import pika
from lib.lib import IP, UDP
import yaml
import argparse
import sys
import logging
import functools
import threading

LEVEL = {'debug': logging.DEBUG,
         'info': logging.INFO,
         'warning': logging.WARNING,
         'error': logging.ERROR,
         'critical': logging.CRITICAL}


def parse_packet(ch, method, properties, body, logger):
    # Process IP Header
    ip_header = IP(body[0:20])
    # Process UDP Header
    udp_header = UDP(body[20:28])
    print " ".join("%02x" % ord(i) for i in body)
    logger.info("**** Message retrieved from the queue on thread %s! | %s:%s -> %s:%s ****" %
                (threading.currentThread().getName(),
                 ip_header.src_address,
                 udp_header.src_port,
                 ip_header.dst_address,
                 udp_header.dst_port))


def threaded_parser(queue_ip, queue_port, queue_virtual_host, queue_username, queue_password, logger):
    # Set Up credentials to connect to queue server
    credentials = pika.PlainCredentials(queue_username, queue_password)
    # Create connection to Queue Server
    conn = pika.BlockingConnection(pika.ConnectionParameters(queue_ip, queue_port, queue_virtual_host, credentials))
    channel = conn.channel()

    # Define the queue
    channel.queue_declare(queue='raw_msg_queue')

    # Use functools to be able to pass user data to the callback function
    custom_parse_packet = functools.partial(parse_packet, logger=logger)

    # Create the consumer with the modified callback function
    channel.basic_consume(custom_parse_packet, queue='raw_msg_queue', no_ack=True)

    logger.info("**** Starting to consume messages from the queue on thread %s! ****" % threading.currentThread().getName())

    channel.start_consuming()


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
        log_level = LEVEL[config["general"]["parser"]["log"]["level"]]
    except:
        log_level = logging.INFO

    # Create and format the logger and the handler for logging
    logger = logging.getLogger('netflow_parser')
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

    try:
        queue_ip = config["queue_server"]["ip"]
        queue_port = config["queue_server"]["port"]
        queue_username = config["queue_server"]["username"]
        queue_password = config["queue_server"]["password"]
        queue_virtual_host = config["queue_server"]["virtual_host"]
    except:
        logger.critical("**** You should specify, the queue server ip, port, virtual host, username and password ****")
        sys.exit(1)

    # Get the number of threads configured
    try:
        thread_number = config["general"]["parser"]["threads"]
    except:
        thread_number = 1

    threads = []
    for i in range(thread_number):
        t = threading.Thread(name="parser_" + str(i+1),target=threaded_parser,
                             args=(queue_ip, queue_port, queue_virtual_host, queue_username, queue_password, logger))
        threads.append(t)
        t.start()