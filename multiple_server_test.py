import asyncio
import json
import time
from mqtt_c import *
from dataStructure import *
from opcConnection import *
from asyncua.ua import ObjectIds
from asyncua.ua.uatypes import NodeId
from asyncua import Client, Node, ua

server_state = True
opcServers = []
newNodeslist = []
values = []
dataBase = None
dataDict = {}


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
    y = 0
    for i in dataDict:
        client = client[y]
        if dataDict[i]["nodeList"] != []:
            dataDict[i]["values"].append(await client.get_values(dataDict[i]["nodeList"]))
            print(dataDict[i]["values"])
        y = y+1
    y = 0
    return dataDict


def create_database():
    dataBase = checkMessageFrom(q)
    dataDict = {}
    if dataBase is not None:
        dataDict = dataAccusation(dataBase)
        for i in dataDict:
            opcServers.append(dataDict[i]["opcServer"])
    return dataDict, opcServers


async def create_dataDict(dataDict, opcServers, server_state):
    try:
        client = []
        for i in opcServers:
            client.append(await opcConnection(server_state, i))
        # taskOPC = asyncio.create_task(opcConnection(server_state, opcServers[0]))
        # client, server_state = await taskOPC
        NodesofServer = []
        if client != 0:
            x = 0
            for i in client:
                NodesofServer.append(await catchNodes(i, opcServers[x], True))
                x =+ 1
                for z in dataDict:
                    for y in dataDict[z]["signals"]:
                        if y != "":
                            dataDict[z]["nodeList"].append(i.get_node(y))
                server_state = False
        return NodesofServer, client, server_state
    except:
        create_dataDict(NodesofServer, opcServers, server_state)



async def data_catchSend(client, dataDict, structPad, structData, clientMqtt):
    while client != 0:

        x = asyncio.create_task(get_values(dataDict, client))
        y = asyncio.create_task(get_timesync(q))
        dataDict = await x
        timeSync = await y

        for i in dataDict:
            if dataDict[i]["values"] != 0:
                dataDict[i]["percent"] = percentage(dataDict[i]["VMX"], dataDict[i]["VMN"], dataDict[i]["values"])
                dataDict[i]["timeStamp"] = timeSync
                dataDict[i]['bufferSize'] = await buffer_data_get_padding(structPad, dataDict[i]["bufferSize"], 0,
                                                                          timeSync, 1)
                dataDict[i]['bufferSize'] = await buffer_data_get(structData, dataDict[i]["bufferSize"], i, dataDict[i])
        i = 0
        print(f'time:{timeSync}')
        # clientMqtt.publish('omid_test_topic', dataDict[1]["bufferSize"])


async def set_time_stamp(dataDict, q):
    try:
        for i in dataDict:
            dataDict[i]["timeStamp"] = await get_timesync(q)
        return dataDict
    except:
        await set_time_stamp(dataDict, q)


clientMqtt = None
# set_connection = True
server_state = True


async def main(clientMqtt, server_state):

    try:
        while clientMqtt !=0:
            dataStart = True
            while dataStart:
                dataDict, opcServers = create_database()
                opcServers1 = ["opc.tcp://fateme:62640/IntegrationObjects/ServerSimulator", "opc.tcp://fateme:49580" ]
                if opcServers != []:
                    dataStart = False
                print(dataDict)
                print(opcServers1)
            clientMqtt.unsubscribe('send_opc_tag')
            while server_state:
                dataDict, clients, server_state = await create_dataDict(dataDict, opcServers1, server_state)
            print("DONE")
            structPad = set_structure('>8sh')
            structData = set_structure('>hffbb')
            dataDicts = {1:
                          {'opcServer': clients[0],
                           'VMN': 1,
                           'VMX': 100,
                           'signals': ['ns=2;s=Tag11'],
                           'tagCount': 1,
                           'bufferSize': bytearray(b'\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00'),
                           'nodeList': [clients[0].get_node('ns=2;s=Tag11')],
                           'values': 0,
                           'percent': [],
                           'timeStamp': 0},
                      9:
                          {'opcServer': clients[1],
                           'VMN': 1,
                           'VMX': 100,
                           'signals': ['ns=2;s=Process Data.Temperature'],
                           'tagCount': 1,
                           'bufferSize': bytearray(b'\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00'),
                           'nodeList': [clients[1].get_node('ns=2;s=Process Data.Temperature')],
                           'values': 0,
                           'percent': [],
                           'timeStamp': 0}
                      }

            await data_catchSend(clients, dataDicts, structPad, structData, clientMqtt)
    except:
        return 0


if __name__ == "__main__":
    try:
        set_connection = True
        clientMqtt = brokerConnection(set_connection)
        stayOnDemand(clientMqtt)
        asyncio.run(main(clientMqtt, server_state))
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
        asyncio.run(main(clientMqtt, server_state))
