import pika
from lib.lib import IP, UDP, Netflow, Flowset, DataTemplate, TemplateFields
from lib.constants import LEVEL
import yaml
import argparse
import sys
import logging
import functools
import threading
import json


def parse_packet(ch, method, properties, body, logger, queue_ip, queue_port, queue_virtual_host, queue_username,
                 queue_password):

    logger.info("**** Message retrieved from the queue on thread %s!****" % (threading.currentThread().getName()))

    # Process IP Header 20 bytes
    ip_header = IP(body[0:20])
    # Process UDP Header 8 bytes
    udp_header = UDP(body[20:28])
    # Process Netflow Header 20 bytes
    netflow_header = Netflow(body[28:48])

    # Check the version of the netflow data
    if netflow_header.version == 9:
        # Process Flowset header and determine type of netflow packet data-template, option-template or data
        flowset_header = Flowset(body[48:52])
        # Check that the netflow packet is a data-template
        if flowset_header.id == 0:
            # Process the data_template header to find id and field count
            data_template_header = DataTemplate(body[52:56])
            # Process the fields
            template_fields = TemplateFields(body[56:])

            # Adding template id and field count to the returning dictionary of the fields
            data_template_doc = template_fields.decoded_fields
            data_template_doc['netflow_device'] = ip_header.src_address
            data_template_doc['id'] = data_template_header.id
            data_template_doc['count'] = data_template_header.field_count

            # Add data to template queue
            # Set Up credentials to connect to queue server
            credentials = pika.PlainCredentials(queue_username, queue_password)
            # Create connection to Queue Server
            conn = pika.BlockingConnection(
                pika.ConnectionParameters(queue_ip, queue_port, queue_virtual_host, credentials))
            channel = conn.channel()

            # Define the queue
            channel.queue_declare(queue='template_queue')

            channel.basic_publish(exchange='', routing_key='template_queue', body=json.dumps(data_template_doc))

            conn.close()

            # Log info
            logger.info("**** Data-Template Netflow packet version %i from %s:%s to %s:%s with template id %i with %i fields****" %
                        (netflow_header.version,
                         ip_header.src_address,
                         udp_header.src_port,
                         ip_header.dst_address,
                         udp_header.dst_port,
                         data_template_header.id,
                         data_template_header.field_count))
            # TODO: Send template document to a queue to be processed by the database module
        elif flowset_header.id == 1:
            logger.info("**** Options-Template Netflow packet version %i from %s:%s to %s:%s with %i flows ****" %
                        (netflow_header.version,
                         ip_header.src_address,
                         udp_header.src_port,
                         ip_header.dst_address,
                         udp_header.dst_port,
                         netflow_header.count))
        elif flowset_header.id > 255:
            logger.info("**** Data Netflow packet version %i from %s:%s to %s:%s with %i flows "
                        "to be decoded by template %i ****" %
                        (netflow_header.version,
                         ip_header.src_address,
                         udp_header.src_port,
                         ip_header.dst_address,
                         udp_header.dst_port,
                         netflow_header.count,
                         flowset_header.id))
        else:
            logger.info("**** Unknown Netflow packet version %i from %s:%s to %s:%s with %i flows ****" %
                        (netflow_header.version,
                         ip_header.src_address,
                         udp_header.src_port,
                         ip_header.dst_address,
                         udp_header.dst_port,
                         netflow_header.count))

    print " ".join("%02x" % ord(i) for i in body)



def threaded_parser(queue_ip, queue_port, queue_virtual_host, queue_username, queue_password, logger):
    # Set Up credentials to connect to queue server
    credentials = pika.PlainCredentials(queue_username, queue_password)
    # Create connection to Queue Server
    conn = pika.BlockingConnection(pika.ConnectionParameters(queue_ip, queue_port, queue_virtual_host, credentials))
    channel = conn.channel()

    # Define the queue
    channel.queue_declare(queue='raw_msg_queue')

    # Use functools to be able to pass user data to the callback function
    custom_parse_packet = functools.partial(parse_packet, logger=logger,
                                            queue_ip=queue_ip,
                                            queue_port=queue_port,
                                            queue_virtual_host=queue_virtual_host,
                                            queue_username=queue_username,
                                            queue_password=queue_password)

    # Create the consumer with the modified callback function
    channel.basic_consume(custom_parse_packet, queue='raw_msg_queue', no_ack=True)

    logger.info("**** Starting to consume messages from the queue Raw Msg on thread %s! ****" % threading.currentThread().getName())

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
        thread_number = config["general"]["parser"]["threads"]
    except:
        thread_number = 1

    threads = []
    for i in range(thread_number):
        t = threading.Thread(name="parser_" + str(i+1),target=threaded_parser,
                             args=(queue_ip, queue_port, queue_virtual_host, queue_username, queue_password, logger))
        threads.append(t)
        t.start()