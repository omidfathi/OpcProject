import asyncio
import json

from asyncua.ua import ObjectIds  # type: ignore[import]
import paho.mqtt.client as mqtt
from queue import Queue
from moat.mqtt.client import open_mqttclient
from moat.mqtt.mqtt.constants import QOS_1, QOS_2
from asyncio_mqtt import Client, MqttError, ProtocolVersion

import moat.mqtt.client
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

async def get_timesync(q):

    if q.topic == "TimeSync":
        return q.payload



async def checkMessageFrom(message):
    # message = await C.deliver_message()
    # packet = message.publish_packet
    opcServers = []


    bMessage["send_opc_tag"] = message.payload.decode('UTF-8')
    dataBase = json.loads(bMessage["send_opc_tag"])
    return dataBase
    # else:
    #     try:
    #         message = q.get()
    #         if message.topic == "Receive_OPC_Server":
    #             bMessage["Receive_OPC_Server"] = message.payload.decode('UTF-8')
    #             dataBase = json.loads(bMessage["send_opc_tag"])
    #         elif message.topic == "send_opc_tag":
    #             bMessage["send_opc_tag"] = message.payload.decode('UTF-8')
    #             dataBase = json.loads(bMessage["send_opc_tag"])
    #         else:
    #             dataBase = 0
    #         return dataBase, firstTime
    #     except asyncio.TimeoutError:
    #         print("Subscription Problem !!!")
    #         checkMessageFrom(q, clientMqtt, firstTime)
    #         return dataBase, firstTime


async def brokerConnection(set_connection, mqttTopic):
        try:
            clientMqtt = await Client(hostname="192.168.1.51", port=1883, client_id="OPC_client")
            await clientMqtt.subscribe(mqttTopic)

                # await C.publish("OPC_Server_Tree", b"TEST MESSAGE WITH QOS_0", qos=0x00)
                # await C.publish("OPC_Server_Tree", b"TEST MESSAGE WITH QOS_1", qos=0x01)
                # await C.publish("OPC_Server_Tree", b"TEST MESSAGE WITH QOS_2", qos=0x02)
                # MQTT_TOPIC = [("send_opc_tag", 0), ("TimeSync", 0), ("OPCTagAdded", 0)]
            return clientMqtt
        except:
            print("Can't Connect to broker")
            set_connection = True
            await brokerConnection(set_connection, mqttTopic)

    # return C

def mqtt_disconnect(clientMqtt):
    clientMqtt.disconnect()
    print("mqtt Dis")