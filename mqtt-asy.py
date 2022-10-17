import asyncio
import json

from asyncua.ua import ObjectIds  # type: ignore[import]
from asyncio_mqtt import Client, MqttError, ProtocolVersion

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


async def brokerConnection(set_connection, mqttTopic):
        try:
            async with Client(hostname="192.168.1.51", port=1883, client_id="OPC_client") as clientMqtt:
                await clientMqtt.subscribe(mqttTopic)
                message = await clientMqtt.deliver_message()
                # packet = message.publish_packet
                # await C.publish("OPC_Server_Tree", b"TEST MESSAGE WITH QOS_0", qos=0x00)
                # await C.publish("OPC_Server_Tree", b"TEST MESSAGE WITH QOS_1", qos=0x01)
                # await C.publish("OPC_Server_Tree", b"TEST MESSAGE WITH QOS_2", qos=0x02)
                # MQTT_TOPIC = [("send_opc_tag", 0), ("TimeSync", 0), ("OPCTagAdded", 0)]
                set_connection = False
                return message
        except:
            print("Can't Connect to broker")
            set_connection = True
            brokerConnection(set_connection, mqttTopic)

