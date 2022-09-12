import struct
from struct import *


def set_structure(structPat):
    struct = Struct(structPat)
    return struct

def estimate_buffer_size(tagCount):
    buffer = bytearray(10 + (tagCount * 12))
    return buffer


def buffer_data_get_padding(struct, buffer, offset, timeStamp, tagCount):
    struct.pack_into(buffer, offset, timeStamp, tagCount)
    return buffer

def buffer_data_get(struct, buffer, data):
    i = 0
    for y in data:
        offset = 10 + i
        struct.pack_into(buffer, offset, y, y["values"], y["percents"], 0, 0)
        i += 12
    return buffer
