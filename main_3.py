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

logging.basicConfig(level=logging.INFO)
_logger = logging.getLogger('asyncua')

class SubscriptionHandler:
    """
    The SubscriptionHandler is used to handle the data that is received for the subscription.
    """
    def datachange_notification(self, node: Node, val, data):
        """
        Callback for asyncua Subscription.
        This method will be called when the Client received a data change message from the Server.
        """
        # _logger.info('datachange_notification %r %s', node, val)
        print(f"Data change happen in node {node},..., value:{val}")


async def walk(node, level=0):

    children = await node.get_children()
    nodeClass = []
    child = []
    browseName = []
    dataType = []
    dataValue = []

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
            handler = SubscriptionHandler()
            # We create a Client Subscription.
            subscription = await client.create_subscription(500, handler)
            nodes = [
                client.get_node("ns=2;s=Process Data.Temperature"),
                client.get_node(ua.ObjectIds.Server_ServerStatus_CurrentTime),
            ]
            await subscription.subscribe_data_change(nodes)
            await asyncio.sleep(100)

if __name__ == "__main__":
    # logging.basicConfig(level=logging.WARN)
    asyncio.run(main())
