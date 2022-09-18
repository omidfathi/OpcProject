import struct
from struct import *


def set_structure(structPat):
    struct = Struct(structPat)
    return struct

def estimate_buffer_size(tagCount):
    buffer = bytearray(10 + (tagCount * 12))
    return buffer


async def buffer_data_get_padding(struct, buffer, offset, timeStamp, tagCount):
    struct.pack_into(buffer, offset, timeStamp, tagCount)
    return buffer

async def buffer_data_get(struct, buffer,id, data):
    i = 0

    for y in range(len(data["values"])):
        offset = 10 + i
        struct.pack_into(buffer, offset, id, float(data["values"][y]), data["percent"][y], 0, 0)
        i += 12
    return buffer
