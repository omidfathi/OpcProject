import paho.mqtt.client as mqtt
import time
import logging


def on_connect(mqttc, obj, flags, rc):
    print("rc: " + str(rc))


def on_message(mqttc, userdata, message):
    print("message received  ", str(message.payload.decode("utf-8")), \
          "topic", message.topic, "retained ", message.retain)
    if message.retain == 1:
        print("This is a retained message")


def on_publish(mqttc, obj, mid):
    print("mid: " + str(mid))


def on_subscribe(mqttc, obj, mid, granted_qos):
    print("Subscribed: " + str(mid) + " " + str(granted_qos))


def on_log(mqttc, obj, level, string):
    print(string)


# If you want to use a specific client id, use
# mqttc = mqtt.Client("client-id")
# but note that the client id must be unique on the broker. Leaving the client
# id parameter empty will generate a random id for you.
mqttc = mqtt.Client("OPC_client")
mqttc.on_message = on_message
mqttc.on_connect = on_connect
mqttc.on_publish = on_publish
mqttc.on_subscribe = on_subscribe
# Uncomment to enable debug messages
# mqttc.on_log = on_log
mqttc.connect("192.168.1.51", 1883, 60)
mqttc.subscribe("send_opc_tag", 0)
mqttc.loop_forever()
