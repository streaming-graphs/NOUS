#!/usr/bin/python
import os, sys, io

mainDir = "/people/d3x771/projects/knowledgeGraph/PathSearchBenchmark/data/yago2//intGraph/withoutEdgeInt/"
graphDir = mainDir + "datagraph/"
#graphDir = mainDir + "/sample/"

edgeFiles = os.listdir(graphDir)
mydict = {}
for edgeFile in edgeFiles:
    print("trying to read data file : ", edgeFile)
    fin = open(graphDir + "/" + edgeFile, "r")
    for line in fin:
        if(len(line) != 0):
            if(line[0] != '#' and line[0] != '@'):
                arr = line.strip().split("\t")
                if(len(arr) == 3):
                    src = int(arr[0].strip())
                    dest = int(arr[2].strip())
                    if(src in mydict):
                        nbrs = mydict.get(src)
                        nbrs.append(dest)
                        mydict[src] = nbrs
                    else:
                        mydict[src] = [dest]

                    if(dest in mydict):
                        nbrs = mydict.get(dest)
                        nbrs.append(src)
                        mydict[dest] = nbrs
                    else:
                        mydict[dest] = [src]
                else:
                    print("find line not of length 3 :"  + line)
    fin.close()
print("size of dictioary = ", len(mydict))

outFile = mainDir + "/neighbourList.out"
fout=open(outFile, mode="w+")
fout.write("#nodeid\tnum_nbrs\tnum_unique_nbrs\tunique_nbrlist\n")
for node,nbrs in mydict.iteritems():
    unique_nbrs = set(nbrs)
    fout.write(str(node) + "\t" + str(len(nbrs)) + "\t" + str(len(unique_nbrs))
              + "\t")
    for nbr in unique_nbrs:
        fout.write(str(nbr) + ",")
    fout.write("\n")
fout.close
