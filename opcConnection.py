from asyncua.ua import ObjectIds
from asyncua.ua.uatypes import NodeId
from asyncua import Client, Node, ua
import json
import asyncio


dataNow = {}
catchingNodes = True


async def walk(node, level=0):
    children = await node.get_children()
    nodeClass = []
    child = []
    browseName = []
    dataType = []
    dataValue = []

    for i in children:
        if i.nodeid.Identifier == 2253:
            children.remove(i)
        else:
            try:
                child.append(str(i))
                parentId = await Node.get_parent(i)
                parentId = str(parentId)
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
        # json_object = json.dumps(child_1, indent=4)
        # rec_opc_mqtt = str(json_object)
        # rec_opc_mqtt = opc_url + "**" + rec_opc_mqtt
        catchingNodes = False
        return child_1
    else:
        pass


async def opcConnection(server_state, opcServer):
    try:
        if server_state:
            print("try to connect")
            _SERVER_STATE = NodeId(ObjectIds.Server_ServerStatus_State)
            opc_url = opcServer
            client = Client(opc_url)
            client.session_timeout = 30000
            await client.connect()
            print("Connected to server")
            server_state = False
            # await catchNodes(client, opc_url, catchingNodes)
            return client

    except:
        await opcConnection(server_state, opcServer)

