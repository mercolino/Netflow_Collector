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
import datetime


class ReturnTemplate():

    def __init__(self, netflow_device, template_id, source_id, logger):
        # Load the config.yaml file
        with open('config.yaml', 'r') as f:
            config = yaml.load(f)

        # Getting the Database server Info
        try:
            mongo_ip = config["mongo_server"]["ip"]
            mongo_port = config["mongo_server"]["port"]
            mongo_username = config["mongo_server"]["username"]
            mongo_password = config["mongo_server"]["password"]
            mongo_db = config["mongo_server"]["db"]
        except:
            logger.critical(
                "**** You should specify, the mongo database server ip, port, virtual host, username and password ****")
            sys.exit(1)

        # Connect to Mongo Database
        try:
            conn_string = "mongodb://" + mongo_username + ":" + mongo_password + "@" + mongo_ip + ":" + str(mongo_port)
            conn = pymongo.MongoClient(conn_string)
            logger.info("**** Connected to the Mongo Server %s on port %i ****" % (mongo_ip, mongo_port))
        except pymongo.errors.ConnectionFailure, e:
            logger.critical("**** Could not connect to the Mongo Server %s on port %i ****" % (mongo_ip, mongo_port))
            logger.error("**** Error %s ****" % (e))

        # Connect to the database
        db = conn[mongo_db]

        # Connect to the Collection
        collection = db.templates

        # Querying Db to see if the netflow device already have a template
        self.query_netflow_device = collection.find_one({"netflow_device": netflow_device, "source_id":source_id,
                                                         "id": template_id})

        # Querying Db to see if the netflow device already have a template
        self.min_template_id = collection.find_one({"netflow_device": netflow_device, "source_id": source_id},
                                               sort=[("id", pymongo.ASCENDING)])

        conn.close()


def template_process(ch, method, properties, body, logger, mongo_ip, mongo_port, mongo_username, mongo_password,
                     mongo_db, message_durability):

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
    query_netflow_device = collection.find({"netflow_device": data['netflow_device'], "source_id": data['source_id']}).count()

    if query_netflow_device > 0:
        # The Netflow device already have a template on the DB
        # Checking if the template id already exists, if it does, pass, if not, insert it
        query_template_id = collection.find({"netflow_device":data['netflow_device'], "source_id":data['source_id'], "id":data['id']}).count()
        logger.info("**** The netflow device %s already exist on the database ****" % (data['netflow_device']))
        if query_template_id > 0:
            logger.info("**** Template id %i already exist for netflow device %s, template not inserted on the DB****" % (data['id'], data['netflow_device']))

            # Send Ack for the processed template to the template queue
            ch.basic_ack(delivery_tag=method.delivery_tag)

            pass
        else:
            # Insert Template
            collection.insert(data)

            # Send Ack for the processed template to the template queue
            ch.basic_ack(delivery_tag=method.delivery_tag)

            logger.info(
                "**** Template id %i inserted on the DB for netflow device %s ****" % (data['id'], data['netflow_device']))
    else:
        # the netflow device does not have a template on the database, inserting the one received
        collection.insert(data)

        # Send Ack for the processed template to the template queue
        ch.basic_ack(delivery_tag=method.delivery_tag)

        logger.info(
            "**** Template id %i inserted on the DB for netflow device %s ****" % (data['id'], data['netflow_device']))

    conn.close()


def threaded_templates_mongo_db(queue_ip, queue_port, queue_virtual_host, queue_username, queue_password, logger,
                      mongo_ip, mongo_port, mongo_username, mongo_password, mongo_db, message_durability):
    # Set Up credentials to connect to queue server
    credentials = pika.PlainCredentials(queue_username, queue_password)
    # Create connection to Queue Server
    conn = pika.BlockingConnection(pika.ConnectionParameters(queue_ip, queue_port, queue_virtual_host, credentials))
    channel = conn.channel()

    # Define the queue
    channel.queue_declare(queue='template_queue', durable=message_durability)

    # Use functools to be able to pass user data to the callback function
    custom_template_process = functools.partial(template_process, logger=logger,
                                            mongo_ip=mongo_ip,
                                            mongo_port=mongo_port,
                                            mongo_username=mongo_username,
                                            mongo_password=mongo_password,
                                            mongo_db=mongo_db,
                                            message_durability=message_durability)

    # Create the consumer with the modified callback function
    channel.basic_consume(custom_template_process, queue='template_queue')

    logger.info(
        "**** Starting to consume messages from the queue Template on thread %s! ****" % threading.currentThread().getName())

    channel.start_consuming()


def flows_process(ch, method, properties, body, logger, mongo_ip, mongo_port, mongo_username, mongo_password,
                     mongo_db, message_durability):

    # Converting json string into dict
    data = json.loads(body)

    logger.info("Received a flow with from netflow device %s" % (data['netflow_device']))

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

    # Connect to the Collection
    collection = db.flows

    # modify string timestamp to datetime object
    date_object = datetime.datetime.strptime(data['timestamp'], '%Y-%m-%d %H:%M:%S')
    data['timestamp'] = date_object

    # Insert the data on the DB
    collection.insert(data)
    logger.info("**** Flow inserted on the DB for netflow device %s ****" % (data['netflow_device']))

    # Send Ack for the processed flow to the flows queue
    ch.basic_ack(delivery_tag=method.delivery_tag)

    conn.close()


def threaded_flows_mongo_db(queue_ip, queue_port, queue_virtual_host, queue_username, queue_password, logger,
                      mongo_ip, mongo_port, mongo_username, mongo_password, mongo_db, message_durability):
    # Set Up credentials to connect to queue server
    credentials = pika.PlainCredentials(queue_username, queue_password)
    # Create connection to Queue Server
    conn = pika.BlockingConnection(pika.ConnectionParameters(queue_ip, queue_port, queue_virtual_host, credentials))
    channel = conn.channel()

    # Define the queue
    channel.queue_declare(queue='flows_queue', durable=message_durability)

    # Use functools to be able to pass user data to the callback function
    custom_flows_process = functools.partial(flows_process, logger=logger,
                                            mongo_ip=mongo_ip,
                                            mongo_port=mongo_port,
                                            mongo_username=mongo_username,
                                            mongo_password=mongo_password,
                                            mongo_db=mongo_db,
                                            message_durability=message_durability)

    # Create the consumer with the modified callback function
    channel.basic_consume(custom_flows_process, queue='flows_queue')

    logger.info(
        "**** Starting to consume messages from the queue Flows on thread %s! ****" % threading.currentThread().getName())

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

    # Message Durability is enabled?
    try:
        message_durability = config["queue_server"]["message_durability"]
    except:
        message_durability = False

    # Get the number of threads configured for templates
    try:
        thread_number_templates = config["general"]["db_mongo"]["threads_template"]
    except:
        thread_number_templates = 1

    threads_templates = []
    for i in range(thread_number_templates):
        t = threading.Thread(name="parser_template_" + str(i+1), target=threaded_templates_mongo_db,
                             args=(queue_ip, queue_port, queue_virtual_host, queue_username, queue_password, logger,
                                   mongo_ip, mongo_port, mongo_username, mongo_password, mongo_db, message_durability))
        threads_templates.append(t)
        t.start()

    # Get the number of threads configured for flows
    try:
        thread_number_flows = config["general"]["db_mongo"]["threads_flows"]
    except:
        thread_number_flows = 1

    threads_flows = []
    for i in range(thread_number_flows):
        t = threading.Thread(name="parser_flows_" + str(i + 1), target=threaded_flows_mongo_db,
                             args=(queue_ip, queue_port, queue_virtual_host, queue_username, queue_password, logger,
                                   mongo_ip, mongo_port, mongo_username, mongo_password, mongo_db, message_durability))
        threads_flows.append(t)
        t.start()