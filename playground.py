# from mpi4py import MPI
from datetime import datetime
import json
import re

START_TIME = datetime.now()

# Extract useful columns
useful = ["createdAt", "sentiment", "account.id", "account.username"]

useful_cols = []
with open("mastodon-106k.ndjson", "r", encoding="utf-8") as f:

    for l in f:
        record = json.loads(l)
        doc = record.get("doc", {})
        
        extracted_record = {
            "createdAt": doc.get("createdAt", None),
            "sentiment": doc.get("sentiment", None),
            "account.id": doc.get("account", {}).get("id", None),
            "account.username": doc.get("account", {}).get("username", None),
        }
        
        useful_cols.append(extracted_record)

# Preprocess timestamps
for item in useful_cols:
    timeStamp = item["createdAt"]
    extract = re.search(r"(\d{4}-\d{2}-\d{2}T\d{2})", timeStamp)
    if extract:
        timeStamp = extract.group(1)
    else:
        timeStamp = None
    item["createdAt"] = timeStamp


# Get sentiment by day and account id
daily_sentiment = {}
user_sentiment = {}
for item in useful_cols:

    currSentiment = item["sentiment"]
    daySentiment = daily_sentiment.get(item["createdAt"],0)
    daily_sentiment[item["createdAt"]] = currSentiment + daySentiment
    userSentiment,_ = user_sentiment.get(item["account.id"],(0,0))
    user_sentiment[item["account.id"]] = (currSentiment + userSentiment,item["account.username"])

    currSentiment += item["sentiment"]
    daily_sentiment[item["createdAt"]] = currSentiment

# print(daily_sentiment)


sorted_sentiment = [(day,sentiment) for day,sentiment in sorted(daily_sentiment.items(), key=lambda item:item[1], reverse = True)]
top5Happy = sorted_sentiment[:5]
top5Sad = sorted_sentiment[-5:]

print("The top 5 happiest day in this dataset are: ")
for day,sentiment in top5Happy:
    print("Day: " + day + " with sentiment score: " + str(sentiment))

print("The top 5 saddest day in this dataset are: ")
for day,sentiment in top5Sad:
    print("Day: " + day + " with sentiment score: " + str(sentiment))






END_TIME = datetime.now()

print("Running time: " + str(END_TIME - START_TIME))

