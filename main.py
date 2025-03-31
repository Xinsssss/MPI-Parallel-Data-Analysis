#!/usr/bin/env python

from mpi4py import MPI
import argparse
from datetime import datetime
from helpers.data_preprocess import *
import os

COMM = MPI.COMM_WORLD
RANK = COMM.Get_rank()
SIZE = COMM.Get_size()
START_TIME = datetime.now()



def main():

    parser = argparse.ArgumentParser(description="Define Dataset")
    parser.add_argument("datasize", metavar="datasize",type=str,help="Enter the size of data file")

    args=parser.parse_args()

    datasize = args.datasize

    filePath = f"/data/gpfs/projects/COMP90024/mastodon-{datasize}.ndjson"

    if RANK==0:
        
        # returns coordiante of range of data each core will be reading
        #print("Now running on the " + datasize + " file")
        totalSize = os.path.getsize(filePath)
        chunkSize = totalSize // SIZE

        dataPerCore = []
        startByte = 0
        with open(filePath,'rb') as f:
            #print("successfully opend file")
            for i in range(SIZE):
                if i != SIZE - 1:
                    endByte = startByte + chunkSize
                    f.seek(endByte)
                    while f.read(1) != b"\n":
                        endByte += 1
                    dataPerCore.append({"startByte": startByte, "endByte": endByte})
                    startByte = endByte + 1
                    #print("allocated to processor " + str(i))
                if i == SIZE - 1: # last core
                    dataPerCore.append({"startByte":startByte,"endByte":totalSize})
                    #print("finish allocating")


       
    else:
        dataPerCore = None

    COMM.Barrier()
    
    if RANK == 0:
        SCATTER_TIME = datetime.now()
        print("It takes " + str(SCATTER_TIME - START_TIME) + " to decompose data")
    
    dataRange = COMM.scatter(dataPerCore,root=0)

    # print("Rank " + str(RANK) + "out of " + str(SIZE) + " received data from " + str(dataRange["startByte"]) + " to " + str(dataRange["endByte"]))
    # data = data_reader(filePath,dataRange)
    # byHour,byUser = summarise_sentiment_score(data)

    byHour,byUser = process_data(filePath,dataRange)
    

    # print("result from processor " + str(RANK))
    # print_dictionary(byHour,"time")
    # print_dictionary(byUser,"user")

    COMM.Barrier()

    allByHour = COMM.gather(byHour,root=0)
    allByUser = COMM.gather(byUser,root=0)

    if RANK == 0:
        GATHER_TIME = datetime.now()
        print("It takes " + str(GATHER_TIME - SCATTER_TIME) + " to process and return data")
    
    # Summarise result in the root processor
    if RANK == 0:
        
        sumByHour = {}
        for byHour in allByHour:
            for key,value in byHour.items():
                sumByHour[key] = sumByHour.get(key,0) + value

        sortedByHour = sorted(sumByHour.items(), key=lambda x: x[1],reverse=True)
        happiestHour = sortedByHour[:5]
        saddestHour = sortedByHour[-5:]
        
        sumByUser = {}
        for byUser in allByUser:
            for key,value in byUser.items():
                sumByUser[key] = (sumByUser.get(key,(0,None))[0] + value[0],value[1])
        sortedByUser = sorted(sumByUser.items(), key=lambda x: x[1][0],reverse=True)
        happiestUser = sortedByUser[:5]
        saddestUser = sortedByUser[-5:]

        SUM_TIME = datetime.now()
        print("It takes " + str(SUM_TIME - GATHER_TIME) + " to summarise data")

        print("Happiest hours: ") 
        print_dictionary(happiestHour,"time")
        print("Saddest hours: ")
        print_dictionary(saddestHour,"time")
        print("Happiest users: ")
        print_dictionary(happiestUser,"user")
        print("Saddest users: ")
        print_dictionary(saddestUser,"user")
        
        




if __name__=="__main__":

    main()
    if RANK == 0:
        END_TIME = datetime.now()
        print("Total run time is: " + str(END_TIME - START_TIME))
    