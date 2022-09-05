import paho.mqtt.client as mqtt
import datetime
import json
from time import sleep

from queue import Queue



q = Queue()

from pprint import pprint

def on_connect(client, userdata, flags, rc):
    client.subscribe("omid_test_topic")

def on_message(client, userdata, msg):
    #here I had the job to queqe for example
    q.put(msg)
    print(msg.payload)

#where should I call the queue

client = mqtt.Client()
client.on_connect = on_connect
client.on_message = on_message

client.connect("192.168.1.51", 1883, 60)

client.loop_start()