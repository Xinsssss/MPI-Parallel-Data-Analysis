#!/usr/bin/env python

import re
import json

def data_reader(filePath, dataRange, chunkSize = 1024*1024):

    with open(filePath,"rb") as f:
        f.seek(dataRange["startByte"])
        # checkPoint = dataRange["startByte"] + chunkSize
        buffer = b""
        json_lines = []
        reachEnd = False
        
        while not reachEnd:
        # while checkPoint < dataRange["endByte"]:
            checkPoint = f.tell() + chunkSize
            if checkPoint > dataRange["endByte"]:
                chunkSize = dataRange["endByte"] - f.tell()
                reachEnd = True
            
            chunk = f.read(chunkSize)
            buffer += chunk

            lines = buffer.split(b"\n")
            buffer = lines.pop() # the last segment may be incomplete line so we keep
            # this in the buffer and connect it with the next read

            for line in lines:
                json_line = json.loads(line.decode("utf-8", errors="ignore"))
                json_lines.append(json_line)

        return json_lines

def read_data(filePath,dataRange):
    with open(filePath,"rb") as f:
        f.seek(dataRange["startByte"])
        while f.tell() < dataRange["endByte"]:
            line = f.readline()
            if not line:
                break
            data = json.loads(line.strip())
            yield data

def process_data(filePath,dataRange):

    sentiment_byHour = {}
    sentiment_byUser = {}
    for data in read_data(filePath,dataRange):
        doc = data.get("doc",{})
        if doc:
            sentiment = doc.get("sentiment", None)
            if not sentiment:
                continue
            timeStamp = extract_time(doc.get("createdAt",None))
            accountId = doc.get("account", {}).get("id", None)
            userName = doc.get("account", {}).get("username", None)

            sentiment_byHour[timeStamp] = sentiment_byHour.get(timeStamp,0) + sentiment
            # this is stored as {accountId: (sentiment, userName)}
            # we assume one id mathces to one username, but since user may change their username
            # but the id is the primary key, we update the username everytime we sees a new post
            # from this id
            sentiment_byUser[accountId] = (sentiment_byUser.get(accountId,(0,None))[0] + sentiment,userName)
            


    return sentiment_byHour, sentiment_byUser


def summarise_sentiment_score(data):

    sentiment_by_hour = {}
    sentiment_by_user = {}

    for row in data:
        doc = row.get("doc",{})

        if doc:
            sentiment = doc.get("sentiment", None)
            if not sentiment:
                continue
            timeStamp = extract_time(doc.get("createdAt",None))
            accountId = doc.get("account", {}).get("id", None)
            userName = doc.get("account", {}).get("username", None)

            sentiment_by_hour[timeStamp] = sentiment_by_hour.get(timeStamp,0) + sentiment
            # this is stored as {accountId: (sentiment, userName)}
            # we assume one id mathces to one username, but since user may change their username
            # but the id is the primary key, we update the username everytime we sees a new post
            # from this id
            sentiment_by_user[accountId] = (sentiment_by_user.get(accountId,(0,None))[0] + sentiment,userName)
            


    return sentiment_by_hour, sentiment_by_user

def extract_time(timeStamp):

    if not timeStamp:
        return None
    time = re.search(r"(\d{4}-\d{2}-\d{2}T\d{2})", timeStamp).group(1)
    return time


def print_dictionary(printList,mode):

    if mode == "time":
        for (time,sentiment) in printList:
            timeSplit = time.split("T")
            print("Day " + timeSplit[0] + " " + timeSplit[1] + " to " + str(int(timeSplit[1])+1),end="")
            print(" has a sentiment score of: " + str(sentiment))
    
    if mode == "user":
        for(userId,(sentiment,userName)) in printList:
            print("User " + userName + " with ID " + userId + " has a sentiment score of: " + str(sentiment))