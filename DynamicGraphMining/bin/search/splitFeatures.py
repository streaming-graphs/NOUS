#!/usr/bin/python
import os, sys

dataTopDir="/pic/projects/nous/PathSearch/yagoFeatures/vertexFeatures/vertexFeaturesAll/"
inFile="vertexAll.txt"
outFileNames="vertexNames.txt"
outFileDegree="vertexDegree.txt"
outFileType="vertexType.txt"

fout1 = open(dataTopDir + outFileNames, "w+")
fout2 = open(dataTopDir + outFileDegree, "w+")
fout3 = open(dataTopDir + outFileType, "w+")

fin = open(dataTopDir + "/" + inFile)
for line in fin:
    arr = line.strip().split(" ;; ")
    if(len(arr) >= 2):
        if(len(arr) >= 3):
            fout1.write(arr[0].strip() + "\n")
            fout2.write(arr[1].strip() + "\n")
            nodeTypes = ";;".join(x for x in arr[2:])
            fout3.write(nodeTypes + "\n")
        else:
            print("FOund line with length not 3", line)
            fout1.flush()
            fout2.flush()
            fout3.flush()
    else:
        print("Found line with length less than 2")

fout1.close
fout2.close
fout3.close
