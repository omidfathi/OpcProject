import sys
import logging
import asyncio
import asyncua
from asyncua import Client, ua, Node
from asyncua.common import ua_utils
import json
import pandas as pd

sys.path.insert(0, "..")
all_variable = []
all_dict = {}
dataNow = {}


i = 0


async def walk(node, level=0):

    children = await node.get_children()
    child=[]
    browseName = []
    for i in children:
        child.append(str(i))
        parentId = await Node.get_parent(i)
        parentId = "i="+str(parentId.nodeid.Identifier)+";ns="+str(parentId.nodeid.NamespaceIndex)
        br = await i.read_browse_name()

        browseName.append(str(br.Name))
        # print(f"{type(nodeId)},{parentId},{browseName}.")
        dataNow[str(node)] = {
            "children":child,

            "parentId":parentId,
            "browseName":browseName,
        }

    # if children == []:
    #
    #     i = 0
    #     pass
    # else:
    node_Parent = await Node.get_parent(node)
    # print(node_Parent)
    all_dict[node] = [children, node_Parent]

    all_variable.append(children)
    if children:
        for child in children:
            # all_dict[child] = []
            await walk(child, level + 1)

    else:
        # value = await node.read_value()
        # try:
        #     value = await Node.read_value(node)
        # except asyncua.ua.uatypes.UaStatusCodeError:
        #     value = ":bad"
        # print('{}:{}'.format(node, value))
        pass
    return dataNow
# async def children_find_end(children_of_root):
#     child_end = []
#     try:
#         for i in children_of_root:
#             new_object = await Node.get_children(i)
#             if new_object == []:
#                 pass
#             else:
#                 child_end.append(new_object[0])
#         return child_end
#     except:
#         child_end = "end"
#         return child_end
#
# async def children_find(children_of_root):
#     child = []
#
#     for i in children_of_root:
#         new_object = await Node.get_children(i)
#         if new_object == []:
#             pass
#         else:
#             child.append(new_object)
#     return child
#
#
# async def ungroup_node(lst):
#     result = []
#     [result.extend(el) for el in lst]
#
#     return result


async def main():
    client = Client("opc.tcp://fateme:49580")
    print(client)
    client.session_timeout = 2000

    counting = 1
    while True:

        async with client:
            all_variables = []
            all_parents = []

            root_id = client.get_root_node()
            children_of_root = await Node.get_children(root_id)
            # print(children_of_root)

            child_1 = await walk(root_id)
            json_object = json.dumps(child_1, indent=4)
            print(json_object)
            df =pd.DataFrame(child_1)


            print(df_csv)
            # for y in child:
            #     node_class = await y.read_node_class()
            #
            #     if node_class == ua.NodeClass.Variable:
            #         all_variables.append(y)

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

                base_identifier = 'Identifier = '+str(nodeIdentifier)
                base_namespace = 'Namespace = ' + str(nodeNamespace)

                node_id_to_value_dict[node.nodeid] = nodeParent
                # node_id_to_value_dict.sort(nodeParent)
            # print(all_parents)
            node_id_to_value_dict_str = ua_utils.val_to_string(node_id_to_value_dict)
            # print(node_id_to_value_dict)
            parents = await get_by_root_object(rootid, node_id_to_value_dict)
            # print(parents)
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
