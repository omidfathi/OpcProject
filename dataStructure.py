from toBuffer import *
from mqtt_c import *

dataDict = {}

def percentage(max, min, values):
    for i in values:
        percent = (i-min)*100/(max-min)
    return percent

async def dataAccusation(database):
    # database = json.loads(bMessage["send_opc_tag"])
    # percent = percentage(max=jsonDatabase[0]["VMX"], min=jsonDatabase[0]["VMX"], value=send_data["value"])
    # send_data["id"] = jsonDatabase[]

    if dataList == []:
        for i in database:
            dataCatch = {
                i["id"]: {
                    "opcServer": i["SOA"],
                    "VMN": i["VMN"],
                    "VMX": i["VMX"],
                    "signals": [i["signaladdress"]],
                    "tagCount": len(["signals"]),
                    "bufferSize": estimate_buffer_size(len(["signals"])),
                    "nodeList": [],
                    "values": 0,
                    "percent": [],
                    "timeStamp": 0,
                }
            }
            dataDict.update(dataCatch)
    print(dataDict)

    return dataDict
