import sys
import logging
import asyncio
from asyncua import Client, Node, ua, tools
from asyncua.common import ua_utils

import json
from mqtt_c import Mqtt
from nodeSubscribe import SubscriptionHandler

sys.path.insert(0, "..")
all_variable = []
all_dict = {}
dataNow = {}


async def walk(node, level=0):

    children = await node.get_children()
    nodeClass = []
    child = []
    browseName = []
    dataType = []

    for i in children:
        child.append(str(i))
        parentId = await Node.get_parent(i)
        parentId = "i="+str(parentId.nodeid.Identifier)+";ns="+str(parentId.nodeid.NamespaceIndex)
        br = await i.read_browse_name()

        browseName.append(str(br.Name))
        nodeClass_v = str(await i.read_node_class())
        nodeClass.append(nodeClass_v)
        if nodeClass_v == "NodeClass.Variable":
            dataType_v = await i.read_data_type_as_variant_type()
            dataType.append(dataType_v.name)
        else:
            dataType.append("None")

        dataNow[str(node)] = {
            "children": child,
            "NodeClass": nodeClass,
            "DataType": dataType,
            "parentId": parentId,
            "browseName": browseName,
        }

    # node_Parent = await Node.get_parent(node)
    # all_dict[node] = [children, node_Parent]

    # all_variable.append(children)
    if children:
        for child in children:
            await walk(child, level + 1)

    else:
        pass
    return dataNow

mqtt = Mqtt()

async def main():
    mqtt.mqtt_connection_initial("OPC_Client", "192.168.1.51", 1883)

    mqtt.mqtt_connection()


    # opc_url = mqtt.mqtt_sub(topic="", qos=0)
    client = Client("opc.tcp://fateme:49580")
    print(client)
    client.session_timeout = 2000

    while True:

        async with client:

            root_id = client.get_root_node()
            # node_List = await ua_utils.get_node_children(client.nodes.objects)
            obj = client.nodes.objects
            child_1 = await walk(obj)
            json_object = json.dumps(child_1, indent=4)
            print(json_object)
            print(await client.read_values(["ns=2;s=Process Data.Temperature"]))

if __name__ == "__main__":
    logging.basicConfig(level=logging.WARN)
    asyncio.run(main())
