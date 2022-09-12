import asyncio
import time
from toBuffer import *
from asyncua.ua import ObjectIds # type: ignore[import]
from asyncua.ua.uatypes import NodeId
from Pyro4.socketutil import setKeepalive
from asyncua import Client, Node, ua
import json
from nodeSubscribe import SubscriptionHandler
import codecs
import paho.mqtt.client as mqtt
from queue import Queue

from ast import literal_eval

# sys.path.insert(0, "..")
all_variable = []
all_dict = {}
dataNow = {}
nodeList = []
newNodeList = []
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
# logging.basicConfig(level=logging.INFO)
# _logger = logging.getLogger('asyncua')


# class SubscriptionHandler:
#     """
#     The SubscriptionHandler is used to handle the data that is received for the subscription.
#     """
#
#     async def datachange_notification(self, node: Node, val, data):
#         """
#         Callback for asyncua Subscription.
#         This method will be called when the Client received a data change message from the Server.
#         """
#         # _logger.info('datachange_notification %r %s', node, val)
#         # dataType_v = await node.read_data_type_as_variant_type()
#         print(float(val))
#         # sendValues[str(node)] = {
#         #     "Value": float(val),
#         #     "DataType": str(dataType_v.name)
#         # }
#
#         # print(data.subscription_data.node)
#         # print(len(newNodeList))
#         # print(str(data.subscription_data.node))
#         # for i in range(len(newNodeList)):
#         #     print(i)
#         send_value["value"] = float(val)
#         #     send_value["nodeTag"] = data.subscription_data.node
#         #     send_value_list.append(send_value)
#         #     print(send_value_list)
#
#         return send_value
#
#         # jsonNodesValue = json.dumps(sendValues, indent=2)
#         # print(jsonNodesValue)
#         # print(send_data)
#

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


async def walk(node, level=0):
    children = await node.get_children()
    nodeClass = []
    child = []
    browseName = []
    dataType = []
    dataValue = []

    for i in children:
        # print(i)
        if i.nodeid.Identifier == 2253:
            children.remove(i)

        else:
            try:
                child.append(str(i))
                parentId = await Node.get_parent(i)
                parentId = str(parentId)


                # parentId = "ns=" + str(parentId.nodeid.NamespaceIndex) + ";i=" + str(parentId.nodeid.Identifier)
                br = await i.read_browse_name()

                browseName.append(str(br.Name))
                nodeClass_v = str(await i.read_node_class())
                nodeClass.append(nodeClass_v)
                if nodeClass_v == "NodeClass.Variable":
                    dataType_v = await i.read_data_type_as_variant_type()
                    dataValue_v = await Node.read_value(i)
                    dataType.append(dataType_v.name)
                    dataValue.append(str(dataValue_v))
                else:
                    dataType.append("None")
                    dataValue.append("")

                dataNow[str(node)] = {
                    "Children": child,
                    "NodeClass": nodeClass,
                    "DataType": dataType,
                    "DataValue": dataValue,
                    "ParentId": parentId,
                    "BrowseName": browseName,
                }
            except:
                pass
    # node_Parent = await Node.get_parent(node)
    # all_dict[node] = [children, node_Parent]

    # all_variable.append(children)
    if children:
        for child in children:
            await walk(child, level + 1)

    else:
        pass
    return dataNow


async def catchNodes(client, opc_url, catchingNodes):
    root_id = client.get_root_node()
    # node_List = await ua_utils.get_node_children(client.nodes.objects)
    if catchingNodes == True:
        obj = client.nodes.objects
        child_1 = await walk(obj)
        json_object = json.dumps(child_1, indent=4)
        rec_opc_mqtt = str(json_object)
        rec_opc_mqtt = opc_url + "**" + rec_opc_mqtt
        # f = open("New Text Document.txt", "a")
        # f.write(rec_opc_mqtt)
        # f.close()
        catchingNodes = False
        return rec_opc_mqtt
    else:
        pass


# num = []
async def get_timesync(q):
    try:
        message = q.get()

        if message.topic == "TimeSync":
            bMessage["TimeSync"] = message.payload
        return bMessage["TimeSync"]
    except:get_timesync(q)

def checkMessageFrom(q):
    try:
        message = q.get()

        if message.topic == "send_opc_tag":
            if bMessage["send_opc_tag"] == 0:
                bMessage["send_opc_tag"] = message.payload.decode('UTF-8')

        # struct1 = Struct('>6bh')
        # name_str = struct1.unpack(bMessage["TimeSync"])
        # print(name_str)
        # name_str = bMessage["TimeSync"]
        # if name_str != 0:
        #     year = name_str[0]
        #     month = name_str[1]
        #     day = name_str[2]
        #     hour = name_str[3]
        #     minute = name_str[4]
        #     second = name_str[5]
        #     milisecond_dec = int(name_str[6:8].hex(), 16)
        #     timeStamp = f"{year}/{month}/{day} ; {hour}:{minute}:{second}:{milisecond_dec}"
        #     print()
        # bMessage["timeStamp"] = timeStamp
        # timeSync = timeSync_decoder(bMessage['TimeSync'])
        # print(timeSync)
    except asyncio.TimeoutError:
        print("Subscription Problem !!!")
        checkMessageFrom(q)
    return bMessage

async def opcConnection(server_state, opcServer):
    try:
        if server_state == True:
            _SERVER_STATE = NodeId(ObjectIds.Server_ServerStatus_State)
            opc_url = opcServer
            client = Client(opc_url)
            print(client)
            await client.connect()
            client.session_timeout = 20000
            server_state = False
            # async with client:
            # await checkMessageFrom(q)

            await catchNodes(client, opc_url, catchingNodes)

            # handler = SubscriptionHandler()
            # We create a Client Subscription.
            # subscription = await client.create_subscription(1500, handler)

            # nodeList = ["ns=2;s=Tag11", "ns=2;s=Tag20"]
            # if nodeList != []:
            #     for i in nodeList:
            #         newNodeList.append(client.get_node(i))
            #     subscription.subscribe_data_change(newNodeList)


            return client
        else:
            print("opc_server connection Faild !!!")
    except:
        await main()
def brokerConnection(set_connection, clientMqtt):
    while set_connection:
        try:
            if clientMqtt.on_connect_fail != None:
                # clientMqtt = mqtt.Client("OPC_client")
                clientMqtt.connect(host="192.168.1.51", port=1883, keepalive=0)
                clientMqtt.loop_start()
                clientMqtt.on_connect = on_connect
                clientMqtt.on_publish = on_publish
                clientMqtt.on_subscribe = on_subscribe
                clientMqtt.on_message = on_message
                clientMqtt.on_log
                print(clientMqtt)
                MQTT_TOPIC = [("send_opc_tag", 0), ("TimeSync", 0)]
                clientMqtt.publish(payload="", topic="ready_to_Receive_opc_topic")
                clientMqtt.subscribe(MQTT_TOPIC)
                set_connection = False
            else:
                pass
                set_connection = False
        except:
            print("Can't Connect to broker")

q = Queue()

# clientMqtt.on_message = on_message
clientMqtt = mqtt.Client("OPC_client")
clientMqtt.connect(host="192.168.1.51", port=1883, keepalive=0)
clientMqtt.loop_start()
clientMqtt.on_connect = on_connect
clientMqtt.on_publish = on_publish
clientMqtt.on_subscribe = on_subscribe
clientMqtt.on_message = on_message
print(clientMqtt)
MQTT_TOPIC = [("send_opc_tag", 0), ("TimeSync", 1)]
clientMqtt.subscribe(MQTT_TOPIC)
clientMqtt.publish(payload="", topic="ready_to_Receive_opc_topic")

def percentage(max, min, values):
    for i in values:
        percent = (i-min)*100/(max-min)
    return percent

async def dataAccusation():
    bMessage = checkMessageFrom(q)
    jsonDatabase = json.loads(bMessage["send_opc_tag"])
    # percent = percentage(max=jsonDatabase[0]["VMX"], min=jsonDatabase[0]["VMX"], value=send_data["value"])
    # send_data["id"] = jsonDatabase[]

    if send_data_list == []:
        for i in jsonDatabase:
            send_data = {}
            send_data["id"] = i["id"]
            send_data["status"] = 0
            send_data["quality"] = 0
            send_data["opcServer"] = i["SOA"]
            send_data["signals"] = i["signaladdress"]
            send_data["VMN"] = i["VMN"]
            send_data["VMX"] = i["VMX"]
            send_data_list.append(send_data)
    print(send_data_list)

    return send_data_list


async def main():

    brokerConnection(set_connection, clientMqtt)
    dataBaseJson = await dataAccusation()
    for i in dataBaseJson:
        dataCatch = {
            i["id"]:{
                "opcServer":i["opcServer"],
                "VMN":i["VMN"],
                "VMX": i["VMX"],
                "signals": [i["signals"]],
                "tagCount": len(["signals"]),
                "bufferSize": estimate_buffer_size(len(["signals"])),
            }
        }
        dataCatchList.append(dataCatch)
        # print(type(dataCatch[1]["tagCount"]))
        # tagCount = len(signal)
    print(dataCatchList[0][1]["opcServer"])

    opcServer = dataCatchList[0][1]["opcServer"]
    print(dataCatchList)
    x = asyncio.create_task(opcConnection(server_state, opcServer))
    client = await x
    for i in signal:
        newNodeList.append(client.get_node(i))
    print(newNodeList)
    structPad = set_structure('>8sh')
    structData = set_structure('>hffbb')
    buffer = estimate_buffer_size(1)


    while True:
        time.sleep(1)
        timeTask = asyncio.create_task(get_timesync(q))
        timeSync = await timeTask
        print(type(timeSync))
        buffer = 0
        buffer = estimate_buffer_size(1)
        buffer_data_get_padding(structPad, buffer, 0, timeSync, 1)
        # print(jsonDatabase[2]["id"])
        print(timeSync)
        print(dataBaseJson)
        a = [client.get_node("ns=2;s=Tag11")]
        y = asyncio.create_task(client.get_values(a))
        values = (await y)
        print(values)

        # percent = percentage(VMX, VMN, values)
        # print(type(percent))
        # data = {
        #     "id": dataBaseJson[0]["id"],
        #     "values": float(values[0]),
        #     "percents": float(percent),
        # }
        # dataList = []
        # dataList.append(data)
        # print(data)
        buffer = buffer_data_get(structData, buffer, dataList)
        print(buffer)
        # clientMqtt.publish(topic='omid_test_topic', payload=buffer)
        # print(await client.get_values(newNodeList))
        # await asyncio.gather(opcConnection(), checkMessageFrom(q))
        # opc_url = mqtt.mqtt_sub(topic="", qos=0)
        if ConnectionError == "Connection is closed":
            await main()

# loop = asyncio.get_event_loop()
if __name__ == "__main__":
    try:
        asyncio.run(main())
        # asyncio.run(checkMessageFrom(q))

        # loop.run_forever()
    except KeyboardInterrupt:
        pass
    finally:
        print("Closing Loop")
        # loop.close()
    # if __name__ == "__main__":
    #     loop = asyncio.get_event_loop()
    #     loop.run_forever()
    #     # logging.basicConfig(level=logging.WARN)
    #     asyncio.run(main())
