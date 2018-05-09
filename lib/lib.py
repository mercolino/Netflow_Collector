import struct
from ctypes import *
import socket


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

        # Convert from little endian to big endian the ports
        self.src_port = struct.unpack(">H", struct.pack("<H", self.src))[0]
        self.dst_port = struct.unpack(">H", struct.pack("<H", self.dst))[0]


class NETFLOW(Structure):
    _fields_ = [
        ("version", c_ushort),
        ("count", c_ushort),
        ("sysuptime", c_ulong),
        ("timestamp", c_ulong),
        ("flow_sequence", c_ulong),
        ("source_id", c_ulong)
    ]

    def __new__(self, socket_buffer=None):
        return self.from_buffer_copy(socket_buffer)

    def __init__(self, socket_buffer=None):

        # Convert from little endian to big endian the ports
        self.version = struct.unpack(">H", struct.pack("<H", self.version))[0]
        self.count = struct.unpack(">H", struct.pack("<H", self.count))[0]
        self.sysuptime = struct.unpack(">L", struct.pack("<L", self.sysuptime))[0]
        self.timestamp = struct.unpack(">L", struct.pack("<L", self.timestamp))[0]
        self.flow_sequence = struct.unpack(">L", struct.pack("<L", self.flow_sequence))[0]
        self.source_id = struct.unpack(">L", struct.pack("<L", self.source_id))[0]


class FLOWSET(Structure):
    _fields_ = [
        ("id", c_ushort),
        ("length", c_ushort),
    ]

    def __new__(self, socket_buffer=None):
        return self.from_buffer_copy(socket_buffer)

    def __init__(self, socket_buffer=None):

        # Convert from little endian to big endian the ports
        self.id = struct.unpack(">H", struct.pack("<H", self.id))[0]
        self.length = struct.unpack(">H", struct.pack("<H", self.length))[0]


class DATA_TEMPLATE(Structure):
    _fields_ = [
        ("id", c_ushort),
        ("field_count", c_ushort),
    ]

    def __new__(self, socket_buffer=None):
        return self.from_buffer_copy(socket_buffer)

    def __init__(self, socket_buffer=None):

        # Convert from little endian to big endian the ports
        self.id = struct.unpack(">H", struct.pack("<H", self.id))[0]
        self.field_count = struct.unpack(">H", struct.pack("<H", self.field_count))[0]