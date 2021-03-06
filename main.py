import sys
import logging
import asyncio

from asyncua import Client, ua, Node
from asyncua.common import ua_utils

sys.path.insert(0, "..")

import json


async def get_by_root_object(index, list):
    root_objects = []
    for i in list:
        print(list[i], list[i] == index)
        if list[i] == index:
            root_objects.append(i)
    return root_objects


async def main():
    client = Client("opc.tcp://ofathi:53530/OPCUA/SimulationServer")
    print(client)
    client.session_timeout = 2000
    # print(await client.load_data_type_definitions())
    # root = client.nodes.root
    while True:

        async with client:
            all_variables = []
            all_parents = []
            node_list = await ua_utils.get_node_children(client.nodes.objects)
            parents_list = await ua_utils.get_node_supertypes(client.nodes.objects)
            rootid = client.get_root_node()
            # print(rootid)
            node__list = []
            print(await Node.get_children(rootid))
            for i in node_list:

                node_class = await i.read_node_class()

                if node_class == ua.NodeClass.Variable:
                    all_variables.append(i)

            # print(all_variables)
            # print("//////*/*/*/*/*/*/*/////////")
            # print(await client.load_data_type_definitions())

            node_id_to_value_dict = {}
            for node in all_variables:

                # identifier = await node.nodeid.Identifier
                browse_Name = await node.read_browse_name()
                nodeIdentifier = node.nodeid.Identifier
                nodeNamespace = node.nodeid.NamespaceIndex
                nodeNamespaceUri = node.nodeid.NamespaceUri
                nodeNodeIdType = node.nodeid.NodeIdType
                nodeServerIndex = node.nodeid.ServerIndex
                nodeParent = await Node.get_parent(node)


                # all_parents.append(parents_list)
                # print(node.nodeid)
                base_identifier = 'Identifier = '+str(nodeIdentifier)
                base_namespace = 'Namespace = ' + str(nodeNamespace)
                base_ns_i = base_identifier + ' , ' + base_namespace
            #     # value = await node.read_value()
            #     # print(parents)
            #     await client.load_data_type_definitions()
            #     # print(node.nodeid)
            #     node_id_to_value_dict[base_ns_i] = {
            #         'Browse_Name': browse_Name,
            #         'IdenSpace' : nodeNamespace,
            #         'Node_Id_Type' : nodeNodeIdType,
            #         'Server_Index' : nodeServerIndex}

                node_id_to_value_dict[node.nodeid] = nodeParent
                # node_id_to_value_dict.sort(nodeParent)
            # print(all_parents)
            node_id_to_value_dict_str = ua_utils.val_to_string(node_id_to_value_dict)
            # print(node_id_to_value_dict)
            parents = await get_by_root_object(rootid, node_id_to_value_dict)
            print(parents)
            # for i in parents:
            #     i.children = []
            #     i.children =
            with open('app.json', 'w') as fp:
                json.dump(node_id_to_value_dict_str, fp)
            # print(node_id_to_value_dict)
            await asyncio.sleep(1)


if __name__ == "__main__":
    logging.basicConfig(level=logging.WARN)
    asyncio.run(main())
