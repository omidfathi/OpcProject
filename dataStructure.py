from toBuffer import *

dataDict = {}
percent = []
def percentage(max, min, values):
    percent = []
    for i in range(len(values)):
        percent.append((values[i]-min[i])*100/(max[i]-min[i]))
    return percent

def dataAccusation(database):
    if database != 0:
        for i in database:
            print(i["id"])
            i["tagCount"] = len(i["signaladdress"])
            i["bufferSize"] = estimate_buffer_size(len(i["signaladdress"]))
            i["values"] = 0
            i["percent"] = []
            i["timeStamp"] = 0
            i["client"] = 0

    else:
        pass
    return database

