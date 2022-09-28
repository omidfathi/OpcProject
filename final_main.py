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
    try:
        for i in dataDict:
            if dataDict[i]["nodeList"] != []:

                dataDict[i]["values"] = await client.read_values(dataDict[i]["nodeList"])
        return dataDict
    except:
        return dataDict


async def create_database(firstTime):

    dataBase, firstTime = await checkMessageFrom(q, clientMqtt, firstTime)

    if dataBase != 0:
        dataDict = dataAccusation(dataBase)
        for i in dataDict:
            opcServers.append(dataDict[i]["opcServer"])
        return dataDict, opcServers, firstTime
    else:
        return 0, 0, False




async def create_dataDict(dataDict, opcServers, server_state):

    client = await opcConnection(server_state, opcServers[0])
    # taskOPC = asyncio.create_task(opcConnection(server_state, opcServers[0]))
    # client, server_state = await taskOPC
    if client != 0:
        # NodesofServer = await catchNodes(client, opcServers[0], True)
        for i in dataDict:
            for y in dataDict[i]["signals"]:
                if y != "":
                    try:
                        dataDict[i]["nodeList"].append(client.get_node(y))
                    except:
                        dataDict[i]["nodeList"].append(client.get_node('i=2253'))
        server_state = False
    return dataDict, client, server_state


async def data_catchSend(structPad, structData, clientMqtt, server_state, firstTime):
    loop2 = asyncio.get_event_loop()
    while True:
        z = loop2.create_task(create_database(firstTime))
        if firstTime:
            dataBase, opcServers, firstTime = await z
            dataDict, client, server_state = await create_dataDict(dataBase, opcServers, server_state)

            # while server_state:


        else:
            # z = asyncio.create_task(create_database(firstTime))
            dataBase, opcServers, firstTime = await z
            # while server_state:

            if dataBase != 0:
                server_state = True
                dataDict, client, server_state = await asyncio.create_task(create_dataDict(dataBase, opcServers, server_state))

            else:
                # dataDict = firstData

                pass
        # firstData = dataDict
        x = loop2.create_task(get_values(dataDict, client))
        y = loop2.create_task(get_timesync(q))
        timeSync = await y
        dataDict = await x
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
                dataDict[i]['bufferSize'] = await buffer_data_get_padding(structPad, dataDict[i]["bufferSize"], 0,
                                                                          timeSync, 1)
                # dataDict[i]['bufferSize'] = await buffer_data_get(structData, dataDict[i]["bufferSize"], i, dataDict[i])
        values = {
            "id":id,
            "values":value,
            "percent":percentt,
            "buffer":estimate_buffer_size(len(value))
        }
        buffer = values["buffer"]
        await buffer_data_get_padding(structPad, buffer, 0, timeSync, len(value))
        await buffer_data_get(structData, buffer, values)
        i = 0
        print(values["id"])
        # await asyncio.sleep(0.8)
        clientMqtt.publish('omid_test_topic', buffer)





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
            # dataStart = True
            # while dataStart:
            #     dataDict1, opcServers = await create_database()
            #     if opcServers != []:
            #         dataStart = False
                    # opcServers = dataDict[1]["opcServer"]
                    # signals = []
                    # for i in dataDict:
                    #     print(dataDict[i]["opcServer"])
                    #     if opcServers == dataDict[i]["opcServer"]:
                #     #         signals.append(dataDict[i]["signals"][0])
                # print(dataDict1)
                # print(opcServers)
            # clientMqtt.unsubscribe('send_opc_tag')
            # while server_state:
            #     dataDict, client, server_state = await create_dataDict(dataDict1, opcServers, server_state)
            # print("DONE")
            structPad = set_structure('>8sh')
            structData = set_structure('<hffbb')
            firstTime = True
            await data_catchSend(structPad, structData, clientMqtt, server_state, firstTime)

    except:

        return 0


if __name__ == "__main__":
    try:
        loop = asyncio.get_event_loop()

        set_connection = True
        mqttTopic = [("send_opc_tag", 0), ("TimeSync", 0)]
        clientMqtt = brokerConnection(set_connection, mqttTopic)
        stayOnDemand(clientMqtt)
        task = loop.create_task(main(clientMqtt, server_state))
        # asyncio.run(main(clientMqtt, server_state))
        # asyncio.run(checkMessageFrom(q))
        loop.run_forever()
        tasks = asyncio.all_tasks(loop=loop)
        group = asyncio.gather(*tasks, return_exceptions=True)
        loop.run_until_complete(group)
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
