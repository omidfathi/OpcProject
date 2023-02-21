import asyncio
import json
import logging
import time
from opc_events import Comparison, Quality
from dataStructure import *
from opcConnection import *
from asyncua.ua import ObjectIds
from asyncua.ua.uatypes import NodeId
from asyncua import Client, Node, ua
from asyncio_mqtt import Client as ClientM
from asyncio_mqtt import MqttError

server_state = True
opcServers = []
newNodeslist = []
values = []
dataBase = None
events = []
bMessage = {
    "send_opc_tag": 0,
    "TimeSync": 0,
}


async def checkMessageFrom(message):
    # message = await C.deliver_message()
    # packet = message.publish_packet
    opcServers = []


    bMessage["send_opc_tag"] = message.payload.decode('UTF-8')
    dataBase = json.loads(bMessage["send_opc_tag"])
    return dataBase
# def on_connect(clientMqtt, obj, flags, rc):
#     print("rc: " + str(rc))
#
#
# def on_publish(clientMqtt, obj, mid):
#     print("mid: " + str(mid))
#     pass
#
#
# def on_subscribe(clientMqtt, obj, mid, granted_qos):
#     print("Subscribed: " + str(mid) + " " + str(granted_qos))
#
#
# def on_log(clientMqtt, obj, level, string):
#     print(string)
#
#
# def on_message(clientMqtt, userdata, message):
#     # time.sleep(0.5)
#     q.put(message)
#
#
# def queue(qu):
#     message = qu.get()
#     return message
#
#
#
#
# def stayOnDemand(clientMqtt):
#     clientMqtt.on_connect = on_connect
#     clientMqtt.on_publish = on_publish
#     clientMqtt.on_subscribe = on_subscribe
#     clientMqtt.on_message = on_message
#     clientMqtt.on_log


async def get_values(dataDict):
    try:
        values = []
        for i in dataDict:
            if i["nodeList"] != [] :
                i["values"] = await i['client'].read_values(i["nodeList"])
                for j in range(len(i["values"])):
                    if i["values"][j] == True:
                        i["values"][j] = 1
                    elif i["values"][j] == False:
                        i["values"][j] = 0
        return dataDict
    except:
        return dataDict


async def create_database(message):

    dataBase = await checkMessageFrom(message)
    dataBase = combine_dicts(dataBase)
    if dataBase != 0:
        dataDict = dataAccusation(dataBase)
        for i in dataDict:
            opcServers.append(i["SOA"])
        return dataDict, opcServers
    else:
        return 0, 0, False




async def create_dataDict(dataDict):
    clients = []
    signal_count = 0
    for i in dataDict:
        client = await opcConnection(i["SOA"][0])
        if client != 0:
            i["client"] = client
            i["opc_connection"] = 1
        else:
            i["opc_connection"] = 0
            i["nodeList"] = []
    for i in dataDict:
        if i["client"] != 0:
            try:
                nodelist = []
                if i["opc_connection"] == 1:
                    for y in i["signaladdress"]:
                        signal_count += 1
                        nodelist.append(i["client"].get_node(y))
                    i["nodeList"] = nodelist

            except:
                i["opc_connection"] = 0
                # i["nodeList"].append(client.get_node('i=2253'))
    return dataDict, signal_count

def combine_dicts(lst_of_dicts):
    soa_dict = {}
    for d in lst_of_dicts:
        soa = d['SOA']
        if soa in soa_dict:
            for key, value in d.items():
                if key != 'SOA':
                    soa_dict[soa][key].append(value)
        else:
            soa_dict[soa] = {key: [value] for key, value in d.items()}

    return list(soa_dict.values())

# async def set_time_stamp(dataDict, q):
#     try:
#         for i in dataDict:
#             dataDict[i]["timeStamp"] = await get_timesync(q)
#         return dataDict
#     except:
#         await set_time_stamp(dataDict, q)

logger = logging.getLogger(__name__)
mqttTopic = [("TimeSync", 1), ("send_opc_tag", 1), ("Receive_OPC_Server", 1)]


async def test() -> None:
    structPad = set_structure('>8sh')
    structData = set_structure('<hffbb')
    firstTime = True
    timeSync = None
    dataDict = {}
    old_status = []
    old_values = []
    status = []
    quality = []
    deleted_dic = []
    old_quality = []
    try:

        logger.info("Connecting to MQTT")
        async with ClientM(hostname="192.168.1.173", port=1883, client_id="OPC_client") as clientMqtt:
            logger.info("Connection to MQTT open")
            async with clientMqtt.unfiltered_messages() as messages:
                await clientMqtt.subscribe(mqttTopic)
                async for message in messages:
                    if message.topic == "TimeSync":
                        timeSync = message.payload
                        # print(timeSync)
                    if firstTime:
                        await clientMqtt.publish("ready_to_Receive_opc_topic", payload="")
                        firstTime = False
                    if message.topic == "send_opc_tag":
                        print(message.payload.decode("UTF-8"))
                        dataBase, opcServers = await create_database(message)
                        dataDict, client = await create_dataDict(dataBase)
                        print(dataDict)
                        logger.info(
                            "Message %s %s", message.topic, message.payload
                        )
                    if message.topic == "Receive_OPC_Server":
                        bMessage = message.payload.decode('UTF-8')
                        # dataBase = json.loads(bMessage["send_opc_tag"])
                        client_rec = await opcConnection(bMessage)
                        if client_rec != 0:
                            nodesTree = await catchNodes(client_rec)
                            await clientMqtt.publish('OPC_Server_Tree', nodesTree)
                            print("Tree Cached")
                            print(nodesTree)
                        else:
                            await clientMqtt.publish('OPC_Server_Tree', "")
                            print("Not Tree")
                    if dataDict != {}:
                        dataDict = await get_values(dataDict, client)

                        value = []
                        percentt = []
                        id = []
                        hysteresis = []
                        over_high_value = []
                        high_value = []
                        low_value = []
                        under_low_value = []
                        for i in dataDict:
                            if dataDict[i]['opc_connection'] == 1:
                                quality.append(1)
                            elif dataDict[i]['opc_connection'] == 0:
                                deleted_dic.append(i)
                                quality.append(0)
                                quality_buffer = buffer_quality_data_get(i, timeSync,
                                                                     quality=0, status=1)
                                await clientMqtt.publish("OPC_Events", payload=quality_buffer)
                        if deleted_dic != []:
                            for i in deleted_dic:
                                del dataDict[i]
                        deleted_dic.clear()
                        if old_values == []:
                            for j in range(len(dataDict)):
                                old_values.append(0)
                                old_status.append(3)
                                status.append(3)
                                quality.append(1)
                                old_quality.append(1)

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
                                hysteresis.append(dataDict[i]["HYS"])
                                over_high_value.append(dataDict[i]["OHV"])
                                high_value.append(dataDict[i]["HIV"])
                                low_value.append(dataDict[i]["LOV"])
                                under_low_value.append(dataDict[i]["ULV"])

                        values = {
                            "id": id,
                            "values": value,
                            "percent": percentt,
                            "buffer": estimate_buffer_size(len(value)),
                            "old_values": old_values,
                            "hysteresis": hysteresis,
                            "over_high_value": over_high_value,
                            "high_value": high_value,
                            "low_value": low_value,
                            "under_low_value": under_low_value,
                            "status": status,
                            "old_status": old_status,
                            "old_quality": old_quality,
                            "quality": quality

                        }
                        status = []
                        for p in range(len(values["id"])):
                            compare_values = Comparison(values["old_values"][p], values["values"][p], values["hysteresis"][p],
                                                        values["over_high_value"][p], values["high_value"][p], values["low_value"][p],
                                                        values["under_low_value"][p], values["status"][p], values["old_status"][p])
                            # print("old_value:", values["old_values"])
                            # print("values:", values["values"])

                            values["status"][p] = compare_values.compare()
                            status.append(values["status"][p])
                            event_status = values["status"][p]
                            # print("status:", compare_values.compare())
                            events = compare_values.event()
                            if events == 1:
                                events_value = {
                                    "id": values["id"][p],
                                    "value": values["values"][p],
                                    "percent": values["percent"][p]
                                }
                                event_buffer = buffer_event_data_get(events_value, timeSync, quality=1, status=event_status)
                                await clientMqtt.publish("OPC_Events", payload=event_buffer)
                        buffer = values["buffer"]
                        buffer_data_get_padding(structPad, buffer, 0, timeSync, len(value))
                        buffer_data_get(structData, buffer, values, 1, status=values["status"])
                        old_values = values["values"]
                        old_status = values["status"]

                        # print(values["values"])
                        await clientMqtt.publish("live_OPC_tags_value", payload=buffer)


    except MqttError as e:
        logger.error("Connection to MQTT closed: " + str(e))
    except Exception:
        logger.exception("Connection to MQTT closed")
    await asyncio.sleep(3)


clientMqtt = None
# set_connection = True
server_state = True


async def main(server_state):
    mqttTopic = [("send_opc_tag", 1), ("TimeSync", 1), ("Receive_OPC_Server", 1)]
    # clientMqtt = asyncio.SelectorEventLoop().run_until_complete(asyncio.wait([test()]))
    structPad = set_structure('>8sh')
    structData = set_structure('<hffbb')
    firstTime = True
    timeSync = None
    dataDict = {}
    old_status = []
    old_values = []
    status = []
    quality = []
    deleted_dic = []
    old_quality = []
    while True:
        try:

            logger.info("Connecting to MQTT")
            async with ClientM(hostname="192.168.1.173", port=1883, client_id="OPC_client") as clientMqtt:
                logger.info("Connection to MQTT open")
                print("Connection to Broker Establish")
                async with clientMqtt.unfiltered_messages() as messages:
                    await clientMqtt.subscribe(mqttTopic)
                    async for message in messages:
                        if message.topic == "TimeSync":
                            timeSync = message.payload
                            # print(timeSync)
                        if firstTime:
                            await clientMqtt.publish("ready_to_Receive_opc_topic", payload="")
                            firstTime = False
                        if message.topic == "send_opc_tag":
                            # print(message.payload.decode("UTF-8"))
                            dataBase, opcServers = await create_database(message)
                            dataDict, signal_count = await create_dataDict(dataBase)
                            for i in range(len(dataDict)):
                                old_quality.append(1)
                            if len(old_values) < signal_count:
                                for j in range(signal_count):
                                    old_values.append(0)
                                    old_status.append(3)
                                    status.append(3)

                                for i in dataDict:
                                    i["old_quality"] = 1
                            # print(dataDict)
                            logger.info(
                                "Message %s %s", message.topic, message.payload
                            )
                        if message.topic == "Receive_OPC_Server":
                            bMessage = message.payload.decode('UTF-8')
                            # dataBase = json.loads(bMessage["send_opc_tag"])
                            client_rec = await opcConnection(bMessage)
                            if client_rec != 0:
                                nodesTree = await catchNodes(client_rec)
                                await clientMqtt.publish('OPC_Server_Tree', nodesTree)
                                print("Tree Cached")
                                print(nodesTree)
                            else:
                                await clientMqtt.publish('OPC_Server_Tree', "")
                                print("Getting nodes unsuccessful")
                        if dataDict != {}:

                            for i in dataDict:
                                if i['opc_connection'] == 1:
                                    i["quality"] = 1
                                elif i['opc_connection'] == 0:
                                    i["quality"] = 0
                            # if deleted_dic != []:
                            #     for i in deleted_dic:
                            #         dataDict.remove(i)
                            #         deleted_dic.clear()
                            for i in dataDict:
                                if i["opc_connection"] != 0:
                                    value = []
                                    percentt = []
                                    id = []
                                    hysteresis = []
                                    over_high_value = []
                                    high_value = []
                                    low_value = []
                                    under_low_value = []
                                    for i in dataDict:
                                        if i["opc_connection"] != 0:
                                            dataDict = await get_values(dataDict)



                                            # if deleted_dic != []:
                                            #     for i in deleted_dic:
                                            #         del dataDict[i]
                                            # deleted_dic.clear()


                                            # for i in dataDict:
                                            id = id + i["id"]
                                            if i["nodeList"] != []:
                                                if i["values"][0] is None:
                                                    value.append(00.00)
                                                    dataDict[i]["percent"] = 00.00
                                                    percentt.append(0.0)
                                                else:
                                                    value = value + i["values"]
                                                    i["percent"] = percentage(i["VMX"], i["VMN"],
                                                                                        i["values"])
                                                    percentt = percentt + (i["percent"])
                                                i["timeStamp"] = timeSync
                                                i['bufferSize'] = buffer_data_get_padding(structPad,
                                                                                                    i["bufferSize"],
                                                                                                    0,
                                                                                                    timeSync, 1)
                                                hysteresis = hysteresis + i["HYS"]
                                                over_high_value = over_high_value + i["OHV"]
                                                high_value = high_value + i["HIV"]
                                                low_value = low_value + i["LOV"]
                                                under_low_value = under_low_value + i["ULV"]
                            values = {
                                "id": id,
                                "values": value,
                                "percent": percentt,
                                "buffer": estimate_buffer_size(len(value)),
                                "old_values": old_values,
                                "hysteresis": hysteresis,
                                "over_high_value": over_high_value,
                                "high_value": high_value,
                                "low_value": low_value,
                                "under_low_value": under_low_value,
                                "status": status,
                                "old_status": old_status,
                                "old_quality": old_quality,
                                "quality": quality

                            }
                            status = []
                            for p in range(len(values["id"])):
                                compare_values = Comparison(values["old_values"][p], values["values"][p],
                                                            values["hysteresis"][p],
                                                            values["over_high_value"][p], values["high_value"][p],
                                                            values["low_value"][p],
                                                            values["under_low_value"][p], values["status"][p],
                                                            values["old_status"][p])
                                values["status"][p] = compare_values.compare()
                                status.append(values["status"][p])
                                event_status = values["status"][p]
                                events = compare_values.event()
                                if events == 1:
                                    events_value = {
                                        "id": values["id"][p],
                                        "value": values["values"][p],
                                        "percent": values["percent"][p]
                                    }
                                    event_buffer = buffer_event_data_get(events_value, timeSync, quality=1,
                                                                         status=event_status)
                                    await clientMqtt.publish("OPC_Events", payload=event_buffer)
                                for i in dataDict:
                                    events_value_0 = {
                                        "id": i["id"][0],
                                        "value": 0,
                                        "percent": 0
                                    }
                                    if i["old_quality"] != i["quality"]:

                                        event_buffer_0 = buffer_event_data_get(events_value_0, timeSync, quality=i["quality"],
                                                                             status=event_status)
                                        await clientMqtt.publish("OPC_Events", payload=event_buffer_0)
                                        i["old_quality"] = i["quality"]

                            buffer = values["buffer"]
                            buffer_data_get_padding(structPad, buffer, 0, timeSync, len(value))
                            buffer_data_get(structData, buffer, values, 1, status=values["status"])
                            old_values = values["values"]
                            old_status = values["status"]
                            for i in dataDict:
                                if i["opc_connection"] != 0:
                                    if await Client.check_connection(i["client"]) == None:
                                        i["quality"] = 1
                                    else:
                                        events_value_0 = {
                                            "id": i["id"][0],
                                            "value": 0,
                                            "percent": 0
                                        }
                                        i["opc_connection"] = 0
                                        i["quality"] = 0
                                        event_buffer_0 = buffer_event_data_get(events_value_0, timeSync,
                                                                               quality=0,
                                                                               status=event_status)
                                        await clientMqtt.publish("OPC_Events", payload=event_buffer_0)
                                        i["old_quality"] = i["quality"]

                            await clientMqtt.publish("live_OPC_tags_value", payload=buffer)
                            print(values["values"])


        except MqttError as e:
            logger.error("Connection to MQTT closed: " + str(e))
            await asyncio.sleep(3)
            asyncio.run(await main(server_state))

        except Exception:
            logger.exception("Connection to MQTT closed")
            await asyncio.sleep(3)
            asyncio.run(await main(server_state))

if __name__ == "__main__":
    asyncio.run(main(server_state))

