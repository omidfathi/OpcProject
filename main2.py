import sys
import logging
import asyncio

from asyncua import Client, ua, Node
from asyncua.common import ua_utils

sys.path.insert(0, "..")

import json




async def main():
    client = Client("opc.tcp://ofathi:53530/OPCUA/SimulationServer")
    print(client)
    client.session_timeout = 2000

    while True:

        async with client:
            all_variables = []
            all_parents = []

            rootid = client.get_root_node()


            print(await Node.get_children(rootid))
            for i in rootid:
                child = Node.get_child(i)
                node_class = await i.read_node_class()

                if node_class == ua.NodeClass.Variable:
                    all_variables.append(child)

            # print(all_variables)
            # print("//////*/*/*/*/*/*/*/////////")
            # print(await client.load_data_type_definitions())
            print(all_variables)
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


                base_identifier = 'Identifier = '+str(nodeIdentifier)
                base_namespace = 'Namespace = ' + str(nodeNamespace)


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
