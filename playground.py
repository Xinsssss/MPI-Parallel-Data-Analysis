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


# Get sentiment by hour and account id
hourly_sentiment = {}
user_sentiment = {}
for item in useful_cols:

    currSentiment = item["sentiment"]
    hourSentiment = hourly_sentiment.get(item["createdAt"],0)
    hourly_sentiment[item["createdAt"]] = currSentiment + hourSentiment
    userSentiment,_ = user_sentiment.get(item["account.id"],(0,0))
    user_sentiment[item["account.id"]] = (currSentiment + userSentiment,item["account.username"])

    currSentiment += item["sentiment"]
    hourly_sentiment[item["createdAt"]] = currSentiment

# print(hourly_sentiment)


sorted_sentiment_hour = [(hour,sentiment) for hour,sentiment in sorted(hourly_sentiment.items(), key=lambda item:item[1], reverse = True)]
top5Happy = sorted_sentiment_hour[:5]
top5Sad = sorted_sentiment_hour[-5:]

print("The top 5 happiest hour in this dataset are: ")
for hour,sentiment in top5Happy:
    print("Time: " + hour + " with sentiment score: " + str(sentiment))

print("The top 5 saddest hour in this dataset are: ")
for hour,sentiment in top5Sad:
    print("Time: " + hour + " with sentiment score: " + str(sentiment))

sorted_sentiment_user = [(userId,userName,sentiment) for userId, (sentiment,userName) in sorted(user_sentiment.items(),key=lambda item:item[1][0], reverse = True)]
top5Happy = sorted_sentiment_user[:5]
top5Sad = sorted_sentiment_user[-5:]
print("The top 5 happiest user in this dataset are: ")
for userId,userName,sentiment in top5Happy:
    print(userName + ", account id " + userId + " with sentiment score: " + str(sentiment))
print("The top 5 saddest user in this dataset are: ")
for userId,userName,sentiment in top5Sad:
    print(userName + ", account id " + userId + " with sentiment score: " + str(sentiment))


END_TIME = datetime.now()

print("Running time: " + str(END_TIME - START_TIME))

