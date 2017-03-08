#!/usr/bin/python
import os, sys, io

mainDir = "/people/d3x771/projects/knowledgeGraph/PathSearchBenchmark/data/yago2/intGraph/withEdgeInt/"
#nbrFile = mainDir + "neighbourListSample.out"
nbrFile = mainDir + "neighbourList.out"
wordnetFile = mainDir + "wordnet_vertices.out"
outputFile = mainDir + "/wordnetNbrIds.out"

def getListWordnetVertices(filename):
    print("trying to wordnet vertices : ", wordnetFile)
    fwordnet = open(filename, "r")
    id_list = []
    for line in fwordnet:
        if(line):
            arr = line.strip().split("\t")
            if(len(arr) == 2):
                id_list.append(int(arr[1]))
    print("Number og wordnet id", len(id_list))
    #print(str(id_list))
    return id_list

def filter_nbrs_withwordnet(nbrFile, wordnetFile):
    wordnet_list = getListWordnetVertices(wordnetFile)
    fin = open(nbrFile, "r")
    fout = open(outputFile, "w+")
    count_zero_wordnet = 0
    for line in fin:
        if(len(line) != 0):
            if(line[0] != '#' and line[0] != '@'):
                arr = line.strip().split("\t")
                if(len(arr) == 4):
                    src = int(arr[0].strip())
                    nbrs = arr[3].strip()[0:-1]
                    nbrlist = [int(x) for x in nbrs.split(",")]
                    if(len(nbrlist) != int(arr[2].strip())):
                        print("incorrect neighbour length found for src", src, 
                              len(nbrlist), int(arr[2].strip()), nbrs, nbrlist)
                        exit
                    wordnet_nbr = []
                    for nbr in nbrlist:
                        if(nbr in wordnet_list):
                            wordnet_nbr.append(str(nbr))
                    tmp = ",".join(x for x in wordnet_nbr)
                    #print(tmp)
                    size = str(len(wordnet_nbr))
                    fout.write(str(src) + "\t" + size + "\t" +
                               tmp + "\n")
                    if(size == 0):
                        count_zero_wordnet = count_zero_wordnet+1
                else:
                    print("Found a line not of length 4")
    print("Number of nodes without wordnet ids = ", count_zero_wordnet)
    fin.close()
    fout.close()

if __name__ == "__main__":
    filter_nbrs_withwordnet(nbrFile, wordnetFile)
