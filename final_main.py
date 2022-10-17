import asyncio
import json
import logging
import time

from dataStructure import *
from opcConnection import *
from asyncua.ua import ObjectIds
from asyncua.ua.uatypes import NodeId
from asyncua import Client, Node, ua
from asyncio_mqtt import Client as ClientM
from asyncio_mqtt import MqttError
from asyncer import asyncify

server_state = True
opcServers = []
newNodeslist = []
values = []
dataBase = None


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


q = Queue()


def stayOnDemand(clientMqtt):
    clientMqtt.on_connect = on_connect
    clientMqtt.on_publish = on_publish
    clientMqtt.on_subscribe = on_subscribe
    clientMqtt.on_message = on_message
    clientMqtt.on_log


async def get_values(dataDict, client):
    try:
        for i in dataDict:
            if dataDict[i]["nodeList"] != []:

                dataDict[i]["values"] = await client.read_values(dataDict[i]["nodeList"])
        return dataDict
    except:
        return dataDict


async def create_database(message):

    dataBase = await checkMessageFrom(message)

    if dataBase != 0:
        dataDict = dataAccusation(dataBase)
        for i in dataDict:
            opcServers.append(dataDict[i]["opcServer"])
        return dataDict, opcServers
    else:
        return 0, 0, False




async def create_dataDict(dataDict):
    clients = []
    for i in dataDict:
        client = await opcConnection(dataDict[i]["opcServer"])
        dataDict[i]["client"] = client
    # taskOPC = asyncio.create_task(opcConnection(server_state, opcServers[0]))
    # client, server_state = await taskOPC
    if clients != 0:
        # NodesofServer = await catchNodes(client, opcServers[0], True)
        for i in dataDict:
            for y in dataDict[i]["signals"]:
                if y != "":
                    try:
                        dataDict[i]["nodeList"].append(client.get_node(y))
                    except:
                        dataDict[i]["nodeList"].append(client.get_node('i=2253'))
    return dataDict, client



async def data_catchSend(clientMqtt, server_state):
    structPad = set_structure('>8sh')
    structData = set_structure('<hffbb')
    firstTime = True

    try:
        while True:
            # message = q.get()
            # if message.topic == "Receive_OPC_Server":
            #     bMessage["Receive_OPC_Server"] = message.payload.decode('UTF-8')
            #     dataBase = json.loads(bMessage["send_opc_tag"]

            if firstTime:
                dataBase, opcServers, firstTime = create_database(firstTime)
                dataDict, client, server_state = await create_dataDict(dataBase, opcServers, server_state)

                # while server_state:


            else:
                message = q.get()
                if message.topic == "Receive_OPC_Server":
                    server_state = True
                    catchingNodes = True
                    bMessage = message.payload.decode('UTF-8')
                    # dataBase = json.loads(bMessage["send_opc_tag"])
                    client = await opcConnection(server_state, bMessage)
                    if client != 0:
                        nodesTree = await catchNodes(client, catchingNodes)
                        clientMqtt.publish('OPC_Server_Tree', nodesTree)
                        print("Tree Cached")
                    else:
                        clientMqtt.publish('OPC_Server_Tree', "")
                        print("Not Tree")

                # z = asyncio.create_task(create_database(firstTime))
                dataBase, opcServers, firstTime = create_database(firstTime)
                # while server_state:

                if dataBase != 0:
                    server_state = True
                    dataDict, client, server_state = await create_dataDict(dataBase, opcServers, server_state)

                else:
                    # dataDict = firstData

                    pass
            # firstData = dataDict
            # x = loop2.create_task(get_values(dataDict, client))
            # y = loop2.create_task(get_timesync(q))
            timeSync = get_timesync(q)
            dataDict = await get_values(dataDict, client)
            print(timeSync)
            value = []
            percentt = []
            id = []
            for i in dataDict:
                id.append(i)
                if dataDict[i]["nodeList"] != []:
                    if dataDict[i]["values"][0] is None:
                        value.append(00.00)
                        dataDict[i]["percent"] = 00.00
                        percentt.append(0.0)
                    else:
                        value.append(dataDict[i]["values"][0])
                        dataDict[i]["percent"] = percentage(dataDict[i]["VMX"], dataDict[i]["VMN"], dataDict[i]["values"])
                        percentt.append(dataDict[i]["percent"][0])
                    dataDict[i]["timeStamp"] = timeSync
                    dataDict[i]['bufferSize'] = buffer_data_get_padding(structPad, dataDict[i]["bufferSize"], 0,
                                                                              timeSync, 1)
                    # dataDict[i]['bufferSize'] = await buffer_data_get(structData, dataDict[i]["bufferSize"], i, dataDict[i])
            values = {
                "id":id,
                "values":value,
                "percent":percentt,
                "buffer":estimate_buffer_size(len(value))
            }
            buffer = values["buffer"]
            buffer_data_get_padding(structPad, buffer, 0, timeSync, len(value))
            buffer_data_get(structData, buffer, values)
            i = 0
            print(values["id"])
            # await asyncio.sleep(0.8)
            clientMqtt.publish('omid_test_topic', buffer)
    except:
        server_state = True
        await data_catchSend(clientMqtt, server_state)




async def set_time_stamp(dataDict, q):
    try:
        for i in dataDict:
            dataDict[i]["timeStamp"] = await get_timesync(q)
        return dataDict
    except:
        await set_time_stamp(dataDict, q)

logger = logging.getLogger(__name__)
mqttTopic = [("TimeSync", QOS_1), ("send_opc_tag", QOS_1), ("Receive_OPC_Server", QOS_1)]


async def test() -> None:
    structPad = set_structure('>8sh')
    structData = set_structure('<hffbb')
    firstTime = True
    timeSync = None
    dataDict = {}
    try:

        logger.info("Connecting to MQTT")
        async with ClientM(hostname="192.168.1.51", port=1883, client_id="OPC_client") as clientMqtt:
            logger.info("Connection to MQTT open")
            async with clientMqtt.unfiltered_messages() as messages:
                await clientMqtt.subscribe(mqttTopic)
                async for message in messages:
                    if message.topic == "TimeSync":
                        timeSync = message.payload
                        print(timeSync)
                    if firstTime:
                        await clientMqtt.publish("ready_to_Receive_opc_topic", payload="")
                        firstTime = False
                    if message.topic == "send_opc_tag":
                        dataBase, opcServers = await create_database(message)
                        dataDict, client = await create_dataDict(dataBase)
                        logger.info(
                            "Message %s %s", message.topic, message.payload
                        )
                    if dataDict != {}:
                        dataDict = await get_values(dataDict, client)

                        value = []
                        percentt = []
                        id = []
                        for i in dataDict:
                            id.append(i)
                            if dataDict[i]["nodeList"] != []:
                                if dataDict[i]["values"][0] is None:
                                    value.append(00.00)
                                    dataDict[i]["percent"] = 00.00
                                    percentt.append(0.0)
                                else:
                                    value.append(dataDict[i]["values"][0])
                                    dataDict[i]["percent"] = percentage(dataDict[i]["VMX"], dataDict[i]["VMN"],
                                                                        dataDict[i]["values"])
                                    percentt.append(dataDict[i]["percent"][0])
                                dataDict[i]["timeStamp"] = timeSync
                                dataDict[i]['bufferSize'] = buffer_data_get_padding(structPad, dataDict[i]["bufferSize"], 0,
                                                                                    timeSync, 1)
                                # dataDict[i]['bufferSize'] = await buffer_data_get(structData, dataDict[i]["bufferSize"], i, dataDict[i])
                        values = {
                            "id": id,
                            "values": value,
                            "percent": percentt,
                            "buffer": estimate_buffer_size(len(value))
                        }
                        buffer = values["buffer"]
                        buffer_data_get_padding(structPad, buffer, 0, timeSync, len(value))
                        buffer_data_get(structData, buffer, values)
                        await clientMqtt.publish("omid_test_topic", payload=buffer)

            await asyncio.sleep(2)
    except MqttError as e:
        logger.error("Connection to MQTT closed: " + str(e))
    except Exception:
        logger.exception("Connection to MQTT closed")
    await asyncio.sleep(3)


clientMqtt = None
# set_connection = True
server_state = True


def main(server_state):
    mqttTopic = [("send_opc_tag", QOS_1), ("TimeSync", QOS_1), ("Receive_OPC_Server", QOS_1)]
    clientMqtt = asyncio.SelectorEventLoop().run_until_complete(asyncio.wait([test()]))

    # await data_catchSend(clientMqtt, server_state)



if __name__ == "__main__":
    try:
        set_connection = True

        main(server_state)
        # asyncio.run(main(server_state))
        # asyncio.run(checkMessageFrom(q))


        # loop.run_forever()
    except KeyboardInterrupt:
        pass
    finally:
        print("Closing Loop")
        for i in range(3):
            time.sleep(1)
            print(f"Reconnect to Server in {3 - i} ...")
        # mqtt_disconnect(clientMqtt)
        asyncio.run(main(server_state))
