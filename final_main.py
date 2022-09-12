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


async def stayOnDemand(clientMqtt):
    clientMqtt.on_connect = on_connect
    clientMqtt.on_publish = on_publish
    clientMqtt.on_subscribe = on_subscribe
    clientMqtt.on_message = on_message
    clientMqtt.on_log


async def get_values(dataDict, client):
    for i in dataDict:
        if dataDict[i]["nodeList"] != []:
            dataDict[i]["values"] = await client.get_values(dataDict[i]["nodeList"])
    return dataDict


async def set_time_stamp(dataDict, q):
    try:
        for i in dataDict:
            dataDict[i]["timeStamp"] = await get_timesync(q)
        return dataDict
    except:
        await set_time_stamp(dataDict, q)
clientMqtt = None
set_connection = True

async def main(clientMqtt):
    if clientMqtt == None:
        clientMqtt = await brokerConnection(set_connection)
    task1 = asyncio.create_task(stayOnDemand(clientMqtt))
    await task1
    dataBase = await checkMessageFrom(q)
    clientMqtt.unsubscribe('send_opc_tag')
    if dataBase is not None:
        dataDict = await dataAccusation(dataBase)
        for i in dataDict:
            opcServers.append(dataDict[i]["opcServer"])
    try:
        taskOPC = asyncio.create_task(opcConnection(server_state, opcServers[0]))
        client = await taskOPC
        if client is not None:
            NodesofServer = await catchNodes(client, opcServers[0], True)
            for i in dataDict:
                for y in dataDict[i]["signals"]:
                    if y != "":
                        dataDict[i]["nodeList"].append(client.get_node(y))
            structPad = set_structure('>8sh')
            structData = set_structure('>hffbb')
            while True:

                x = asyncio.create_task(get_values(dataDict, client))
                y = asyncio.create_task(get_timesync(q))
                dataDict_f = await x
                timeSync = await y
                for i in dataDict_f:
                    dataDict_f[i]["timeStamp"] = timeSync
                    buffer_data_get_padding(structPad, dataDict_f[i]["bufferSize"], 0, timeSync, 1)
                    buffer_data_get(structData, dataDict_f[i]["bufferSize"], dataDict_f[i])
                print(dataDict_f[1]["values"])
                print(dataDict_f[1]["timeStamp"])
        elif client == None:
            await main(clientMqtt)

        # a = [client.get_node("ns=2;s=Tag11")]
        # b = await client.get_values(a)

        # opcSerer = dataDict.
        # x = asyncio.create_task(opcConnection(server_state, opcServer))
        # client = await x
        # print(dataDict[1]["timeStamp"])
        # print(dataDict[1]["values"])
    except:
        await main(clientMqtt)
if __name__ == "__main__":
    try:
        asyncio.run(main(clientMqtt))
        # asyncio.run(checkMessageFrom(q))

        # loop.run_forever()
    except KeyboardInterrupt:
        pass
    finally:
        print("Closing Loop")
