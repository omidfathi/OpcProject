import time

from Pyro4.util import buffer
from bitstring import BitArray, BitStream
import random
import time
import struct
from paho.mqtt import client as mqtt_client


broker = '192.168.1.51'
port = 1883
topic = "omid_test_topic"
# generate client ID with pub prefix randomly
client_id = "OPCclient"

s = BitArray(floatbe=85.85, length=32)

b = s.tobytes()
print(b)
print(b.hex())

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





