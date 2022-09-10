import asyncio
import json

from mqtt_c import *
from dataStructure import *
from opcConnection import *
opcServers = []
newNodeslist = []


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

async def main():
    clientMqtt = await brokerConnection(True)
    task1 = asyncio.create_task(stayOnDemand(clientMqtt))
    x = await task1
    dataBase = await checkMessageFrom(q)
    dataDict = await dataAccusation(dataBase)
    for i in dataDict:
        opcServers.append(dataDict[i]["opcServer"])
    client = await opcConnection(True, opcServers[0])
    NodesofServer = await catchNodes(client, opcServers[0], True)
    for i in dataDict:
        for y in dataDict[i]["signals"]:
            if y != "":
                dataDict[i]["nodeList"].append(client.get_node(y))
    for i in dataDict:
        if dataDict[i]["nodeList"] != []:
            dataDict[i]["values"] = await client.get_values(dataDict[i]["nodeList"])
    # opcSerer = dataDict.
    # x = asyncio.create_task(opcConnection(server_state, opcServer))
    # client = await x
    print(dataDict)
if __name__ == "__main__":
    try:
        asyncio.run(main())
        # asyncio.run(checkMessageFrom(q))

        # loop.run_forever()
    except KeyboardInterrupt:
        pass
    finally:
        print("Closing Loop")