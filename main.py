#!/usr/bin/env python

from mpi4py import MPI
import argparse
from datetime import datetime
from helpers.data_preprocess import Preprocessor,Summariser
import os

COMM = MPI.COMM_WORLD
RANK = COMM.Get_rank()
SIZE = COMM.Get_size()

def readData(dataRange,filename):
    
    with open(self.filePath,"rb") as f:
        f.seek(self.dataRange["startByte"])
        data = f.read(self.dataRange["endByte"] - self.dataRange["startByte"]).decode("utf-8",errors="ignore")
        self.dataRows = data.split("\n")



def main():

    parser = argparse.ArgumentParser(description="Define Dataset")
    parser.add_argument("datasize", metavar="datasize",type=str,help="Enter the size of data file")

    args=parser.parse_args()

    datasize = args.datasize

    filePath = f"/data/gpfs/projects/COMP90024/mastodon-{datasize}.ndjson"

    if RANK==0:

        # returns coordiante of range of data each core will be reading
        print("Now running on the " + datasize + " file")
        totalSize = os.path.getsize(filePath)
        chunkSize = totalSize // SIZE

        dataPerCore = []
        startByte = 0
        with open(filePath,'rb') as f:
            print("successfully opend file")
            for i in range(SIZE):
                if i != SIZE - 1:
                    endByte = startByte + chunkSize
                    f.seek(endByte)
                    while f.read(1) != b'\n':
                        endByte += 1
                    dataPerCore.append({"startByte": startByte, "endByte": endByte})
                    startByte = endByte + 1
                    print("allocated to processor " + str(i))
                if i == SIZE - 1: # last core
                    dataPerCore.append({"startByte":startByte,"endByte":totalSize})
                    print("finish allocating")


       
    else:
        dataPerCore = None

    COMM.Barrier()
    
    dataRange = COMM.scatter(dataPerCore,root=0)

    # print("Rank " + str(RANK) + "out of " + str(SIZE) + " received data from " + str(dataRange["startByte"]) + " to " + str(dataRange["endByte"]))
    readData(dataRange,filePath)

if __name__=="__main__":

    START_TIME = datetime.now()
    main()
    END_TIME = datetime.now()
    print("Total process time: " + str(END_TIME - START_TIME))