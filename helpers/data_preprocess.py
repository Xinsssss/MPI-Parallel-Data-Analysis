#!/usr/bin/env python

class Preprocessor():

    def __init__(self):
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