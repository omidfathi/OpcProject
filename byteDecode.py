from toBuffer import *
import time
from struct import *
from paho.mqtt import client as mqtt_client
def make_data_buf(timeStamp, tagCount, id, data):
    buffer = bytearray(10 + (tagCount*12))
    print(buffer)
    t = timeStamp
    c = tagCount
    dataDict = {}

    x = set_structure('>8sh')
    x.pack_into(buffer, 0, b'\x16\t\x05\x0f\x18%\x01\t', c)
    print(buffer)
    print(type(dataDict))
    print(len(dataDict.keys()))
    struct1 = Struct(f'>8sh')
    struct2 = Struct('>hf')
    struct1.pack_into(buffer, 0, b'\x16\t\x05\x0f\x18%\x01\t',c)
    print(buffer)
    i = 0
    for key in dataDict:
        print(key)
        print(dataDict[key])
        struct2.pack_into(buffer, 10+i, key, dataDict[key])
        i += 12
        print(buffer)

x = set_structure('>8sh')
x2 = set_structure('>hf')
dataDict={
        101: 75.75,
        102: 80.80,
        103: 85.85,
    }
buffer = estimate_buffer_size(3)
data = buffer_data_get_padding(struct=x, buffer=buffer , offset=0, timeStamp=b'\x16\t\x05\x0f\x18%\x01\t', tagCount=3)
data = buffer_data_get(x2, buffer, dataDict)
print(data)

b = data

broker = '192.168.1.51'
port = 1883
topic = "omid_test_topic"
# generate client ID with pub prefix randomly
client_id = "OPC_client"

# s = BitArray(floatbe=85.85, length=32)
#
# b = s.tobytes()
# print(b)
# print(b.hex())

# s.append('int:16=85')
# print(s)
def connect_mqtt():
    def on_connect(client, userdata, flags, rc):
        if rc == 0:
            print("Connected to MQTT Broker!")
        else:
            print("Failed to connect, return code %d\n", rc)

    client = mqtt_client.Client(client_id)
    client.on_connect = on_connect
    client.connect(broker, port)
    return client


def publish(client, bb):

    while True:
        time.sleep(1)
        msg = bb
        result = client.publish(topic, msg)
        status = result[0]
        if status == 0:
            print(f"Send `{msg}` to topic `{topic}`")
        else:
            print(f"Failed to send message to topic {topic}")


def run():
    client = connect_mqtt()
    client.loop_start()
    publish(client, b)


if __name__ == '__main__':
    run()





