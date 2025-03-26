#!/usr/bin/env python

class Preprocessor():

    def __init__(self,filePath,dataRange):
        self.filePath = filePath
        self.dataRange = dataRange
        self.dataRows = None

    def initialiseData(self):
        import json
        with open(self.filePath,"rb") as f:
            f.seek(self.dataRange["startByte"])
            data = f.read(self.dataRange["endByte"] - self.dataRange["startByte"]).decode("utf-8",errors="ignore")
            self.dataRows = data.split("\n")

    def preprocessData(self):
        return

    def extractColumns(self):
        return
    
    def extractHour(self):
        return
    


class Summariser():

    # mode: user or hour
    def __init__(self, mode):
        if mode == "user":
            self.column = "account.id"
        elif mode == "time":
            self.column = "createdAt"

    
    def sum_sentiment(self, data):
        return
        # return sentiment_rank