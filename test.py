#!/usr/bin/env python

from mpi4py import MPI
import os

COMM = MPI.COMM_WORLD
RANK = COMM.Get_rank()
SIZE = COMM.Get_size()

print("At least some output")

if RANK == 0:
   data = []
   filePath = "/data/gpfs/projects/COMP90024/mastodon-106k.ndjson"
   print(filePath)
   try:
      startByte = 0
      totalSize = os.path.getsize(filePath)
      chunkSize = totalSize // SIZE
      with open(filePath,'rb') as f:
         print("Open successfully")
         for i in range(SIZE):
               if i != SIZE - 1:
                  endByte = startByte + chunkSize
                  f.seek(endByte)
                  while f.read(1) != b'\n':
                        endByte += 1
                  data.append({"startByte": startByte, "endByte": endByte})
                  startByte += 1
                  print("allocated for size " + str(i))
               elif i == SIZE - 1: # last core
                  data.append({"startByte":startByte,"endByte":totalSize})
   except FileNotFoundError:
      print("Unable to locate file")
else:
   data = None

print("Rank " + str(RANK) + " of " + str(SIZE))
   
data = COMM.scatter(data, root=0)

COMM.Barrier()

print("Rank " + str(RANK) + " received data " + str(data))
