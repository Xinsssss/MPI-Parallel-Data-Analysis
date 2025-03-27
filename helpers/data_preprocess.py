#!/usr/bin/env python

def data_reader(filePath, dataRange):

    with open(filePath,"rb") as f:
        f.seek(dataRange["startByte"])
        data = f.read(dataRange["endByte"] - dataRange["startByte"]).decode("utf-8", errors="ignore")
        lines = data.split("\n")
        for line in lines:
            print(line)

    return