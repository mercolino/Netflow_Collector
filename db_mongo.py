import argparse
from lib.constants import LEVEL
import json
import pika
import pymongo
import yaml
import logging
import sys
import threading
import functools


def template_process(ch, method, properties, body, logger, queue_ip, queue_port, queue_virtual_host, queue_username,
                 queue_password):

    data = json.loads(body)
    print "Template ID: %i" % data['id']

    for i in range(1, data['count'] + 1):
        print 'Field ' + str(i) + ': ' + data['fields']['field_' + str(i)]['type'] + ' with a length of ' + \
              str(data['fields']['field_' + str(i)]['length']) + ' bytes'


def threaded_mongo_db(queue_ip, queue_port, queue_virtual_host, queue_username, queue_password, logger):
    # Set Up credentials to connect to queue server
    credentials = pika.PlainCredentials(queue_username, queue_password)
    # Create connection to Queue Server
    conn = pika.BlockingConnection(pika.ConnectionParameters(queue_ip, queue_port, queue_virtual_host, credentials))
    channel = conn.channel()

    # Define the queue
    channel.queue_declare(queue='template_queue')

    # Use functools to be able to pass user data to the callback function
    custom_template_process = functools.partial(template_process, logger=logger,
                                            queue_ip=queue_ip,
                                            queue_port=queue_port,
                                            queue_virtual_host=queue_virtual_host,
                                            queue_username=queue_username,
                                            queue_password=queue_password)

    # Create the consumer with the modified callback function
    channel.basic_consume(custom_template_process, queue='template_queue', no_ack=True)

    logger.info(
        "**** Starting to consume messages from the queue Template on thread %s! ****" % threading.currentThread().getName())

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
        log_level = LEVEL[config["general"]["db_mongo"]["log"]["level"]]
    except:
        log_level = logging.INFO

    # Create and format the logger and the handler for logging
    logger = logging.getLogger('netflow_db_mongo')
    logger.setLevel(level=log_level)
    handler = logging.StreamHandler()
    handler_formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s',
                                          datefmt='%m/%d/%Y %I:%M:%S %p')
    handler.setFormatter(handler_formatter)
    logger.addHandler(handler)

    # Turn logger on or off depending on the arguments
    logger.disabled = not args.verbose


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
        thread_number = config["general"]["db_mongo"]["threads"]
    except:
        thread_number = 1

    threads = []
    for i in range(thread_number):
        t = threading.Thread(name="parser_" + str(i+1), target=threaded_mongo_db,
                             args=(queue_ip, queue_port, queue_virtual_host, queue_username, queue_password, logger))
        threads.append(t)
        t.start()