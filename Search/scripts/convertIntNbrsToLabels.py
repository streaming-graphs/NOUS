#!/usr/bin/python
import os, sys, io

mainDir = "/people/d3x771/projects/knowledgeGraph/PathSearchBenchmark/data/yago2//intGraph/withoutEdgeInt/"
vertexDictFile = mainDir + "/wordnet_vertices.out"
nbrFile = mainDir + "/wordnetNbrIds.out"
outFile = mainDir + "/wordnetNbrLabels.out"
#graphDir = mainDir + "/sample/"

def getVertexDict(filename):
    my_dict = {}
    print("trying to get vertex dict: ", filename)
    fdict = io.open(filename, encoding ='utf8')
    for line in fdict:
        if(line):
            arr = line.strip().split("\t")
            if(len(arr) == 2):
                my_dict[int(arr[1].strip())] = arr[0].strip()
    print("Number of nodes in dict =", len(my_dict)) 
    fdict.close()
    return my_dict
    
def convertIdsToLabels(vertexDictFile, inFile, outFile):
    vertex_dict = getVertexDict(vertexDictFile)
    print("trying to read nbrList : ", nbrFile)
    fin = open(nbrFile, "r")
    fout=io.open(outFile, mode="w+", encoding = 'utf8')
    for line in fin:
        if(len(line) != 0):
            if(line[0] != '#' and line[0] != '@'):
                arr = line.strip().split("\t")
                if(len(arr) == 3):
                    src = int(arr[0].strip())
                    count = int(arr[1].strip())
                    nbrs = arr[2].strip().split(",")
                    nbr_labels = ""
                    for nbr in nbrs:
                        nbr_id = int(nbr)
                        if(nbr_id in vertex_dict):
                            nbr_label = vertex_dict.get(nbr_id)
                            nbr_labels = nbr_labels + ";;" + nbr_label
                        else:
                            print("Found a wordnet id not in dict (src, nbrid)=", src, nbr_id)
                    fout.write(str(src) + ";;" + nbr_labels + "\n")
                elif(len(arr) == 2 and int(arr[1].strip()) != 0) :
                    print("find line not of length 3 and count is not zero:"  + line)
    fin.close()
    fout.close()

if __name__ == "__main__":
    mainDir = "./examples/"
    vertexDictFile = mainDir + "/vertexDictionary.out"
    inFile = mainDir + "/wordnetNbrIds.out"
    outFile = mainDir + "/wordnetNbrLabels.out"
    convertIdsToLabels(vertexDictFile, inFile, outFile)
