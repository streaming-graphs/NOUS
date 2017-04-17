#!/usr/bin/python
import os, sys, io

# reads a file of form
# "label	integer_id"
# as {integerid -> label}
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
    
# reads an inFile of format
# "srcid  num_nbrs nbr_id1,nbrid2,nbrid3.."
# and converts it to labeled format
def convertIdsToLabels(vertexDictFile, inFile, outFile):
    vertex_dict = getVertexDict(vertexDictFile)
    print("trying to read nbrList : ", nbrFile)
    fin = open(nbrFile, "r")
    fout=io.open(outFile, mode="w+", encoding = 'utf8')
    for line in fin:
        if(len(line) != 0):
            if(line[0] != '#' and line[0] != '@'):
                arr = line.strip().split(",")
                if(len(arr) >= 2):
                    path_labels = ""
                    for node in arr:
                        node_id = int(node)
                        if(node_id in vertex_dict):
                            node_label = vertex_dict.get(node_id)
                            path_labels = path_labels + ";;" + node_label
                        else:
                            print("Found a node id not in dict (src, nbrid)=", node_id, line)
                    fout.write(nbr_labels + "\n")
                else:
		    print("Found a path with less than length 1", line)
    fin.close()
    fout.close()

if __name__ == "__main__":
    mainDir = "./examples/"
    vertexDictFile = mainDir + "/vertexDictionary.out"
    inFile = mainDir + "/paths.int.out"
    outFile = mainDir + "/paths.labeled.out"
    convertIdsToLabels(vertexDictFile, inFile, outFile)
