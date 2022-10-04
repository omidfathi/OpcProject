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

def get_timesync(q):
    try:
        message = q.get()
        if message.topic == "TimeSync":
            return message.payload
    except:
        get_timesync(q)


def checkMessageFrom(q, clientMqtt, firstTime):

    dataBase = {}
    if firstTime is True:
        clientMqtt.publish(payload="", topic="ready_to_Receive_opc_topic")
        message = q.get()
        if message.topic == "send_opc_tag":
            # if bMessage["send_opc_tag"] == 0:
            bMessage["send_opc_tag"] = message.payload.decode('UTF-8')
            dataBase = json.loads(bMessage["send_opc_tag"])
        firstTime = False
        return dataBase, firstTime
    else:
        try:
            message = q.get()
            if message.topic == "send_opc_tag":

                # if bMessage["send_opc_tag"] == 0:
                bMessage["send_opc_tag"] = message.payload.decode('UTF-8')
                dataBase = json.loads(bMessage["send_opc_tag"])
            # if message.topic == "OPCTagAdded":
            #     if message.payload.decode('UTF-8') ==
            #         bMessage["send_opc_tag"] = message.payload.decode('UTF-8')
            #         dataBase = json.loads(bMessage["send_opc_tag"])
            #         return dataBase
            #
            # if message.topic == "OPCtagDelete":
            #     if message.payload.decode('UTF-8') !=
            #
            # bMessage["send_opc_tag"] = 0
            else:
                dataBase = 0
            return dataBase, firstTime
        except asyncio.TimeoutError:
            print("Subscription Problem !!!")
            checkMessageFrom(q, clientMqtt, firstTime)
            return dataBase, firstTime

def brokerConnection(set_connection, mqttTopic):
    while set_connection:
        try:
            clientMqtt = mqtt.Client("OPC_client")
            if clientMqtt.connect(host="192.168.1.51", port=1883, keepalive=10) == 0:
                clientMqtt.loop_start()
                print(clientMqtt)
                MQTT_TOPIC = mqttTopic
                # MQTT_TOPIC = [("send_opc_tag", 0), ("TimeSync", 0), ("OPCTagAdded", 0)]
                clientMqtt.subscribe(MQTT_TOPIC)
                set_connection = False
        except:
            print("Can't Connect to broker")
            set_connection = True
            brokerConnection(set_connection, mqttTopic)

    return clientMqtt

def mqtt_disconnect(clientMqtt):
    clientMqtt.disconnect()
    print("mqtt Dis")