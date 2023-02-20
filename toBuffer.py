import struct
from struct import *


def set_structure(structPat):
    struct = Struct(structPat)
    return struct

def estimate_buffer_size(tagCount):
    buffer = bytearray(10 + (tagCount * 12))
    return buffer

def estimate_event_buffer_size():

    event_buffer = bytearray(20)
    return event_buffer

def buffer_data_get_padding(struct, buffer, offset, timeStamp, tagCount):
    struct.pack_into(buffer, offset, timeStamp, tagCount)
    return buffer

def buffer_data_get(struct, buffer, values, quality, status):
    i = 0

    for y in range(len(values["values"])):
        offset = 10 + i
        struct.pack_into(buffer, offset, values["id"][y], float(values["values"][y]), values["percent"][y], quality, values["status"][y])
        i += 12
    return buffer

def buffer_event_data_get(event_values, time_stamp, quality, status):
        buffer = bytearray(20)
        offset = 0
        struct_0 = set_structure('<h')
        # print(unpack_from(">6sh", time_stamp, offset=0))
        struct_0.pack_into(buffer, offset, event_values["id"])
        offset += 2
        struct_0 = set_structure('>6s')
        struct_0.pack_into(buffer, offset, time_stamp)
        offset += 6
        milisecond = time_stamp[6:8]
        milisecond = int.from_bytes(time_stamp[6:8], "big")
        struct_0 = set_structure('<h')
        struct_0.pack_into(buffer, offset, milisecond)
        offset += 2
        # event_buffer = pack_into("<2s", buffer, offset, time_stamp[6:8])
        # offset =+ 2


        struct_0 = set_structure('<ffbb')
        struct_0.pack_into(buffer, offset, float(event_values["value"]),
                                 event_values["percent"], quality, status)

        return buffer


def buffer_quality_data_get(id, time_stamp, quality, status):
    buffer = bytearray(20)
    offset = 0
    struct_0 = set_structure('<h')
    # print(unpack_from(">6sh", time_stamp, offset=0))
    struct_0.pack_into(buffer, offset, id)
    offset += 2
    struct_0 = set_structure('>6s')
    struct_0.pack_into(buffer, offset, time_stamp)
    offset += 6
    milisecond = time_stamp[6:8]
    milisecond = int.from_bytes(time_stamp[6:8], "big")
    struct_0 = set_structure('<h')
    struct_0.pack_into(buffer, offset, milisecond)
    offset += 2
    # event_buffer = pack_into("<2s", buffer, offset, time_stamp[6:8])
    # offset =+ 2
    struct_0 = set_structure('<ffbb')
    struct_0.pack_into(buffer, offset, 0, 0, quality, status)
    # print(len(buffer))

    return buffer