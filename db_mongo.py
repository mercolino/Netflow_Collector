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


def template_process(ch, method, properties, body, logger, mongo_ip, mongo_port, mongo_username, mongo_password,
                     mongo_db):

    # Converting json string into dict
    data = json.loads(body)

    logger.info("Received a Template with ID %i from netflow device %s" % (data['id'], data['netflow_device']))

    #Connect to Mongo Database
    try:
        conn_string = "mongodb://" + mongo_username + ":" + mongo_password + "@" + mongo_ip + ":" + str(mongo_port)
        conn = pymongo.MongoClient(conn_string)
        logger.info("**** Connected to the Mongo Server %s on port %i ****" % (mongo_ip, mongo_port))
    except pymongo.errors.ConnectionFailure, e:
        logger.critical("**** Could not connect to the Mongo Server %s on port %i ****" % (mongo_ip, mongo_port))
        logger.error("**** Error %s ****" % (e))

    # Connect to the database
    db = conn[mongo_db]

    #Connect to the Collection
    collection = db.templates

    # Querying Db to see if the netflow device already have a template
    query_netflow_device = collection.find({"netflow_device":data['netflow_device']}).count()

    if query_netflow_device > 0:
        # The Netflow device already have a template on the DB
        # Checking if the template id alreadyexists, if it doses pass if not insert it
        query_template_id = collection.find({"netflow_device":data['netflow_device'], "id":data['id']}).count()
        logger.info("**** The netflow device %s already exist on the database ****" % (data['netflow_device']))
        if query_template_id > 0:
            logger.info("**** Template id %i already exist for netflow device %s, template not inserted on the DB****" % (data['id'], data['netflow_device']))
            pass
        else:
            # Insert Template
            collection.insert(data)
            logger.info(
                "**** Template id %i inserted on the DB for netflow device %s ****" % (data['id'], data['netflow_device']))
    else:
        # the netflow device does not have a template on the database, inserting the one received
        collection.insert(data)
        logger.info(
            "**** Template id %i inserted on the DB for netflow device %s ****" % (data['id'], data['netflow_device']))


def threaded_mongo_db(queue_ip, queue_port, queue_virtual_host, queue_username, queue_password, logger,
                      mongo_ip, mongo_port, mongo_username, mongo_password, mongo_db):
    # Set Up credentials to connect to queue server
    credentials = pika.PlainCredentials(queue_username, queue_password)
    # Create connection to Queue Server
    conn = pika.BlockingConnection(pika.ConnectionParameters(queue_ip, queue_port, queue_virtual_host, credentials))
    channel = conn.channel()

    # Define the queue
    channel.queue_declare(queue='template_queue')

    # Use functools to be able to pass user data to the callback function
    custom_template_process = functools.partial(template_process, logger=logger,
                                            mongo_ip=mongo_ip,
                                            mongo_port=mongo_port,
                                            mongo_username=mongo_username,
                                            mongo_password=mongo_password,
                                            mongo_db=mongo_db)

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

    # Getting the Database server Info
    try:
        mongo_ip = config["mongo_server"]["ip"]
        mongo_port = config["mongo_server"]["port"]
        mongo_username = config["mongo_server"]["username"]
        mongo_password = config["mongo_server"]["password"]
        mongo_db = config["mongo_server"]["db"]
    except:
        logger.critical("**** You should specify, the mongo database server ip, port, virtual host, username and password ****")
        sys.exit(1)

    # Get the number of threads configured
    try:
        thread_number = config["general"]["db_mongo"]["threads"]
    except:
        thread_number = 1

    threads = []
    for i in range(thread_number):
        t = threading.Thread(name="parser_" + str(i+1), target=threaded_mongo_db,
                             args=(queue_ip, queue_port, queue_virtual_host, queue_username, queue_password, logger,
                                   mongo_ip, mongo_port, mongo_username, mongo_password, mongo_db))
        threads.append(t)
        t.start()