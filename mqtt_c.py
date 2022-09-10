import asyncio
import json

from asyncua.ua import ObjectIds  # type: ignore[import]
import paho.mqtt.client as mqtt
from queue import Queue

set_connection = True
catchingNodes = True
server_state = True
bMessage = {
    "send_opc_tag": 0,
    "TimeSync": 0,
}
send_value = {
    "nodeTag": 0,
    "value": 0,
}
send_data_list = []
send_value_list = []
jsonDatabase = {}
signal = []
timeStamp = []
dataList = []
dataCatchList = []


def on_connect(clientMqtt, obj, flags, rc):
    print("rc: " + str(rc))


def on_publish(clientMqtt, obj, mid):
    print("mid: " + str(mid))
    pass


def on_subscribe(clientMqtt, obj, mid, granted_qos):
    print("Subscribed: " + str(mid) + " " + str(granted_qos))


def on_log(clientMqtt, obj, level, string):
    print(string)


def on_message(clientMqtt, userdata, message):
    # time.sleep(0.5)
    q.put(message)


def queue(qu):
    message = qu.get()
    return message


async def get_timesync(q):
    try:
        message = q.get()

        if message.topic == "TimeSync":
            bMessage["TimeSync"] = message.payload
        return bMessage["TimeSync"]
    except:
        get_timesync(q)


async def checkMessageFrom(q):
    try:
        message = q.get()

        if message.topic == "send_opc_tag":
            if bMessage["send_opc_tag"] == 0:
                bMessage["send_opc_tag"] = message.payload.decode('UTF-8')
                dataBase = json.loads(bMessage["send_opc_tag"])
    except asyncio.TimeoutError:
        print("Subscription Problem !!!")
        checkMessageFrom(q)
    return dataBase


async def brokerConnection(set_connection):
    while set_connection:
        try:
            clientMqtt = mqtt.Client("OPC_client")
            if clientMqtt.connect(host="192.168.1.51", port=1883, keepalive=0) == 0:
                clientMqtt.loop_start()
                print(clientMqtt)
                MQTT_TOPIC = [("send_opc_tag", 0), ("TimeSync", 0)]
                clientMqtt.subscribe(MQTT_TOPIC)
                clientMqtt.publish(payload="", topic="ready_to_Receive_opc_topic")
                set_connection = False
        except:
            print("Can't Connect to broker")
            set_connection = True

    return clientMqtt

