#!/usr/bin/python
import os, sys

# Reads the paths in string format and converts to integer format using 
#vertexNames.txt -> each line contains a vertex label, assumes vertexid starting from 0
def get_vertex_id_map(filename):
    mydict = {}
    i = 0 
    with open(filename, 'r') as infile:
        for line in infile:
            arr = line.strip()
            mydict[arr] = i
            i = i + 1

    print("size of current dictionary", len(mydict))

    return mydict

inDir = "/pic/projects/nous/PathSearch/path_search_training_data/pathsize_4_deg_500/vertexStringFormat/"
outDir = "/pic/projects/nous/PathSearch/path_search_training_data/pathsize_4_deg_500/vertexIntFormat/"
mapFile = "/pic/projects/nous/PathSearch/path_search_training_data/pathsize_4_deg_500/vertexNames.txt"
vertexIdMap = get_vertex_id_map(mapFile)

pathFiles = os.listdir(inDir)
for pathFile in pathFiles:
    print("trying to read paths from : ", pathFile)
    fin = open(inDir + "/" + pathFile)
    fout = open(outDir + "/" +  pathFile + ".int.txt", "w+")
    for line in fin:
        arr = line.strip().split(";;")
        outLine = "" 
        for i in range(len(arr)):
            vertex = arr[i].strip()
            if(vertex in vertexIdMap):
                id = vertexIdMap.get(arr[i])
                outLine = outLine + str(id) + ","
            else:
                print("Found a vertex in path, not present in global vertex list", vertex, line)
                outLine = outLine + str(-1) + ","


        fout.write(outLine + "\n")
        fout.flush()
                            
    fin.close()
    fout.close
