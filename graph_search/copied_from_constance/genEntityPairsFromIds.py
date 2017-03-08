#!/usr/bin/python
import os, sys, io
import random as r

mainDir = "/people/d3x771/projects/knowledgeGraph/PathSearchBenchmark/data/"
clusterFile = mainDir + "/blogData0/group-edges.csv"
outEntityPairsFile = mainDir + "/blogData0/entityPairs.out"

def gen_entity_pair_cluster(entity_list, numPairs):
    l = len(entity_list)
    output = set()
    if(l == 1):
        return output
    for i in range(0, numPairs):
        node1 = r.randrange(0,l)
        node2 = r.randrange(0,l)
        if(node1 != node2):
            if(node1 < node2):
                x = (entity_list[node1], entity_list[node2])
                output.add(x)
            else:
                x = (entity_list[node2], entity_list[node1])
                output.add(x)
    return output

def readClusters(clusterFile):
    fin = open(clusterFile, "r")
    clusterToIdMap = {}
    for line in fin:
        if(len(line) != 0):
            if(line[0] != '#' and line[0] != '@'):
                arr = line.strip().split(",")
                if(len(arr) == 2):
                    cmember = int(arr[0].strip())
                    cid = arr[1].strip()
                    #cmember = arr[1].strip()[1:-1]
                    #cmemList = [int(x) for x in cmember.strip()]
                    cmemList = []
                    if(cid in clusterToIdMap):
                        cmemList = clusterToIdMap.get(cid)
                    cmemList.append(cmember)
                    clusterToIdMap[cid] = cmemList
    print("COmpletd getting memebrs of each cluster\n")
    print("Number of clusters =", len(clusterToIdMap))
    fin.close()
    return clusterToIdMap

def genEntityPairs(clusterToIdMap, outFile):
    fout = open(outFile, "w+")
    allPairs = set()
    for (id, members) in clusterToIdMap.items():
        pairs = gen_entity_pair_cluster(members, len(members))
        #allPairs = allPairs | pairs
        for pair in pairs:
            fout.write(str(pair[0]) +"\t" +str(pair[1]) + "\n")
    fout.close()

if __name__ == "__main__":
    clusterToIdMap = readClusters(clusterFile)
    genEntityPairs(clusterToIdMap, outEntityPairsFile)
