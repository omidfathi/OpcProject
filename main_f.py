import sys
from byteDecode import *
import asyncio
import time
from asyncua import Client, Node, ua
import json
from nodeSubscribe import SubscriptionHandler

import paho.mqtt.client as mqtt
from queue import Queue

# sys.path.insert(0, "..")
all_variable = []
all_dict = {}
dataNow = {}
sendValues = {}
nodeList = []
newNodeList = []
set_connection = True


# logging.basicConfig(level=logging.INFO)
# _logger = logging.getLogger('asyncua')


class SubscriptionHandler:
    """
    The SubscriptionHandler is used to handle the data that is received for the subscription.
    """

    async def datachange_notification(self, node: Node, val, data):
        """
        Callback for asyncua Subscription.
        This method will be called when the Client received a data change message from the Server.
        """
        # _logger.info('datachange_notification %r %s', node, val)
        dataType_v = await node.read_data_type_as_variant_type()
        sendValues[str(node)] = {
            "Value": str(val),
            "DataType": str(dataType_v.name)
        }
        jsonNodesValue = json.dumps(sendValues, indent=2)
        # print(jsonNodesValue)
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
    time.sleep(1)
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
                parentId = "ns=" + str(parentId.nodeid.NamespaceIndex) + ";i=" + str(parentId.nodeid.Identifier)
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


async def catchNodes(client, opc_url):
    root_id = client.get_root_node()
    # node_List = await ua_utils.get_node_children(client.nodes.objects)

    obj = client.nodes.objects
    child_1 = await walk(obj)
    json_object = json.dumps(child_1, indent=4)
    rec_opc_mqtt = str(json_object)
    rec_opc_mqtt = opc_url + "**" + rec_opc_mqtt
    # print(json_object)


async def checkMessageFrom(q):
    try:
        message = q.get()

        bMessage = {
            "send_opc_tag": 0,
            "TimeSync": 0,
        }

        if message.topic == "send_opc_tag":
            bMessage["send_opc_tag"] = message.payload
        elif message.topic == "TimeSync":
            bMessage["TimeSync"] = message.payload
        name_str = str(bMessage["TimeSync"])

        print(name_str)
        # timeSync = timeSync_decoder(bMessage['TimeSync'])
        # print(timeSync)
    except asyncio.TimeoutError:
        print("Subscription Problem !!!")
        await checkMessageFrom(q)

async def opcConnection():
    try:
        opc_url = "opc.tcp://fateme:62640/IntegrationObjects/ServerSimulator"
        client = Client(opc_url)
        print(client)
        client.session_timeout = 2000

        async with client:

            # await checkMessageFrom(q)

            await catchNodes(client, opc_url)

            handler = SubscriptionHandler()
            # We create a Client Subscription.
            subscription = await client.create_subscription(500, handler)
            nodeList = ["ns=2;s=Tag11", "ns=2;s=Tag20", "ns=2;s=Tag18"]
            if nodeList != []:
                for i in nodeList:
                    newNodeList.append(client.get_node(i))
                newNodeList.append(client.get_node(ua.ObjectIds.Server_ServerStatus_CurrentTime))
                while True:
                    await subscription.subscribe_data_change(newNodeList)
            else:
                await main()
    except:
        await main()

q = Queue()
clientMqtt = mqtt.Client("OPC_client")
# clientMqtt.on_message = on_message
clientMqtt.connect(host="192.168.1.51", port=1883, keepalive=0)
clientMqtt.loop_start()
clientMqtt.on_connect = on_connect
clientMqtt.on_publish = on_publish
clientMqtt.on_subscribe = on_subscribe
clientMqtt.on_message = on_message

MQTT_TOPIC = [("send_opc_tag", 0), ("TimeSync", 1)]

clientMqtt.subscribe(MQTT_TOPIC)

async def main():
    # clientMqtt.publish(topic="", payload="")
    # clientMqtt.subscribe(topic="Get_OPC_TREE_Topic", qos=0)
    # sub_message = queue(q)
    # print(sub_message)

    # mqtt.mqtt_connection()
    await asyncio.gather(opcConnection(), checkMessageFrom(q))
    # opc_url = mqtt.mqtt_sub(topic="", qos=0)


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
