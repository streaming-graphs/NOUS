#!/usr/bin/python
import os, sys, io
import random as r

mainDir = "/people/d3x771/projects/knowledgeGraph/PathSearchBenchmark/step7/"
wordnetNbrFile = mainDir + "/wordnetNbrIds.out"
outClustersFile = mainDir + "/wordnetClusters.out"
outEntityPairsFile = mainDir + "/entityPairs.out"

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

def getWordnetClusters(wordnetNbrsFile, outFile):
    #wordnet_list = getListWordnetVertices(wordnetFile)
    fin = open(wordnetNbrsFile, "r")
    fout = open(outFile, "w+")
    wordnetClusterMap = {}
    for line in fin:
        if(len(line) != 0):
            if(line[0] != '#' and line[0] != '@'):
                arr = line.strip().split("\t")
                if(len(arr) == 3):
                    src = int(arr[0].strip())
                    nbrs = arr[2].strip()
                    #nbrs = arr[2].strip()[0:-1]
                    wordnetNbrs = [int(x) for x in nbrs.split(",")]
                    for wnbr in wordnetNbrs:
                        if(wnbr in wordnetClusterMap):
                            cluster = wordnetClusterMap[wnbr]
                            cluster.append(src)
                            wordnetClusterMap[wnbr] = cluster
                        else:
                            wordnetClusterMap[wnbr] = [src]
    print("COmpletd getting memebrs of each wordnet class\n")
    print("Number of wordnet classes =", len(wordnetClusterMap))
    for (id, members) in wordnetClusterMap.items():
        strnbrs = ",".join(str(x) for x in members)
        fout.write(str(id) + " : " +  strnbrs + "\n")

    fin.close()
    fout.close()
    return wordnetClusterMap

def genEntityPairs(wordnetClusterMap, outFile):
    fout = open(outFile, "w+")
    allPairs = set()
    for (id, members) in wordnetClusterMap.items():
        pairs = gen_entity_pair_cluster(members, len(members))
        #allPairs = allPairs | pairs
        for pair in pairs:
            fout.write(str(pair[0]) +"\t" +str(pair[1]) + "\n")
    fout.close()

if __name__ == "__main__":
    wordnetClusterMap = getWordnetClusters(wordnetNbrFile, outClustersFile)
    genEntityPairs(wordnetClusterMap, outEntityPairsFile)
