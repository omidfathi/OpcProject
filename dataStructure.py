from toBuffer import *
from mqtt_c import *

dataDict = {}
percent = []
def percentage(max, min, values):
    percent = []
    for i in values:
        percent.append((i-min)*100/(max-min))
    return percent

def dataAccusation(database):
    # database = json.loads(bMessage["send_opc_tag"])
    # percent = percentage(max=jsonDatabase[0]["VMX"], min=jsonDatabase[0]["VMX"], value=send_data["value"])
    # send_data["id"] = jsonDatabase[]
    if database != 0:
        dataDict = {}
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
                        "client": 0,
                    }
                }
                dataDict.update(dataCatch)

    else:
        pass
    return dataDict
