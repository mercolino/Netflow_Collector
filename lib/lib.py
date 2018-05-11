import struct
from ctypes import *
import socket
import constants



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


class Netflow(Structure):
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


class Flowset(Structure):
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


class DataTemplate(Structure):
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


class TemplateFields():

    def __init__(self, buffer):
        n = 4
        fields_list = []
        self.decoded_fields = {}
        decoded_fields = {}
        # Divide buffer in 4 byte chunks
        for i in range(0, len(buffer), n):
            fields_list.append(buffer[i:i+n])
        j = 0
        # Process the chunks and divide the type and length
        for i in fields_list:
            j += 1
            decoded_fields['field_' + str(j)] = {'type': constants.FIELD_TYPE_MAP[struct.unpack(">HH", i)[0]],
                                                      'length': struct.unpack(">HH", i)[1]}

        self.decoded_fields['fields'] = decoded_fields


class DataFlow():

    def __init__(self, buffer, template):
        def decode(bytes, field_type):
            byte_list = []
            word = ''

            # Convert string to list of hex strings
            for b in bytes:
                byte_list.append(hex(ord(b)))

            # Convert list to string without 0x
            for b in byte_list:
                if len(b[2:]) == 1:
                    word = word + "0" + b[2:]
                else:
                    word = word + b[2:]

            # Return the result
            if field_type in constants.IPV4_FIELD:
                return socket.inet_ntoa(struct.pack(">L", int(word, 16)))
            elif field_type in ["TOS", "TCP_FLAGS"]:
                return hex(int(word,16))
            elif field_type == "PROTOCOL":
                return constants.PROTOCOLS[int(word, 16)]
            else:
                return int(word, 16)

        self.decoded_fields = {}
        decoded_fields = {}
        template_fields = {}
        # Create a new dictionary only with the template fields, where the key is an integer so the sorting is easier
        for key in template['fields']:
            template_fields[int(key[6:])] = template['fields'][key]
        # Process the Buffer wit the template
        for key in sorted(template_fields):
            new_key = template_fields[key]['type']
            raw_value = buffer[:template_fields[key]['length']]
            decoded_fields[key] = {new_key: decode(raw_value, new_key)}
            buffer = buffer[template_fields[key]['length']:]

        self.decoded_fields['fields'] = decoded_fields