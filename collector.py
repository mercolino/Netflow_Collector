import socket
import struct
from ctypes import *
import yaml
import sys
import argparse
import threading
import logging


class IP(Structure):
    _fields_ = [
        ("ihl", c_ubyte, 4),
        ("version", c_ubyte, 4),
        ("tos", c_ubyte),
        ("len", c_ushort),
        ("id", c_ushort),
        ("flags", c_ushort),
        ("ttl", c_ubyte),
        ("protocol_num", c_ubyte),
        ("sum", c_ushort),
        ("src", c_ulong),
        ("dst", c_ulong)
    ]

    def __new__(self, socket_buffer=None):
        return self.from_buffer_copy(socket_buffer)

    def __init__(self, socket_buffer=None):

        # map protocol constants to their names
        self.protocol_map = {6: "TCP", 17: "UDP"}

        # human readable IP addresses
        self.src_address = socket.inet_ntoa(struct.pack("<L", self.src))
        self.dst_address = socket.inet_ntoa(struct.pack("<L", self.dst))

        # human readable protocol
        try:
            self.protocol = self.protocol_map[self.protocol_num]
        except:
            self.protocol = str(self.protocol_num)


class UDP(Structure):
    _fields_ = [
        ("src", c_ushort),
        ("dst", c_ushort),
        ("len", c_ushort),
        ("sum", c_ushort)
    ]

    def __new__(self, socket_buffer):
        return self.from_buffer_copy(socket_buffer)

    def __init__(self, socket_buffer):

        # Conver from little endian to big endian the ports
        self.src_port = struct.unpack(">H", struct.pack("<H", self.src))[0]
        self.dst_port = struct.unpack(">H", struct.pack("<H", self.dst))[0]


def process_data(data):

    # Process Data
    ip_header = IP(data[0:20])
    if ip_header.protocol == 'UDP':
       udp_header = UDP(data[20:28])
       if udp_header.dst_port == 650:
           print " ".join("%02x" % ord(i) for i in data)
           print " ".join("%02x" % ord(i) for i in data[0:20])
           print " ".join("%02x" % ord(i) for i in data[20:28])
           print "Protocol: %s | %s:%s -> %s:%s" % (ip_header.protocol, ip_header.src_address, udp_header.src_port,
                                                ip_header.dst_address, udp_header.dst_port)


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

    # Determining the ip, port and receiving buffer for the collector
    try:
        udp_ip = config["collector_server"]["udp_ip"]
    except:
        logger.critical("**** You should specify the UDP ip address to be used for the collector ****")
        sys.exit(1)

    try:
        udp_port = config["collector_server"]["udp_port"]
    except:
        logger.critical("**** You should specify the UDP port to be used for the collector ****")
        sys.exit(1)

    try:
        udp_buffer = config["collector_server"]["udp_buffer"]
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
        t = threading.Thread(target=process_data, args=(data,))
        t.start()

