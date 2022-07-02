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
    # print(await client.load_data_type_definitions())
    # root = client.nodes.root
    while True:

        async with client:
            all_variables = []
            all_parents = []
            node_list = await ua_utils.get_node_children(client.nodes.objects)
            parents_list = await ua_utils.get_node_supertypes(client.nodes.objects)

            for i in node_list:

                node_class = await i.read_node_class()

                if node_class == ua.NodeClass.Variable:
                    all_variables.append(i)

            # print(all_variables)
            # print("//////*/*/*/*/*/*/*/////////")
            # print(await client.load_data_type_definitions())
            node_id_to_value_dict = {}
            for node in all_variables:
                parents_list = await Node.get_parent(node)
                # identifier = await node.nodeid.Identifier
                print(node.nodeid.Identifier)
                print(node.nodeid.NamespaceIndex)
                print(node.nodeid.NamespaceUri)
                print(node.nodeid.NodeIdType)
                print(node.nodeid.ServerIndex)
                # all_parents.append(parents_list)
            #     # print(node.nodeid)
            #
            #     # value = await node.read_value()
            #     # print(parents)
            #     await client.load_data_type_definitions()
            #     # print(node.nodeid)
                node_id_to_value_dict[node.nodeid] = parents_list
            # print(all_parents)
            node_id_to_value_dict_str = ua_utils.val_to_string(node_id_to_value_dict)
            print(node_id_to_value_dict_str)
            with open('app.json', 'w') as fp:
                json.dump(node_id_to_value_dict_str, fp)
            # print(node_id_to_value_dict)
            await asyncio.sleep(1)


if __name__ == "__main__":
    logging.basicConfig(level=logging.WARN)
    asyncio.run(main())
