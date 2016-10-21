#!/usr/bin/python
import os, sys

# Generates neirghbour list in integer format using vertex to id hashmap from :
#vertexNames.txt -> assumes vertex label per line
# ids are started from id 0
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

inDir = "/pic/projects/nous/PathSearch/yagoFeatures/vertexFeatures/vertex.nbrs/vertices.string.nbrs"
outDir = "/pic/projects/nous/PathSearch/yagoFeatures/vertexFeatures/vetex.nbrs/vertices.int.nbrs/"
mapFile = "/pic/projects/nous/PathSearch/yagoFeatures/vertexFeatures/vertexFeaturesAll/vertexNames.txt"
vertexIdMap = get_vertex_id_map(mapFile)

nbrFiles = os.listdir(inDir)
for nbrFile in nbrFiles:
    print("trying to read nbrs from : ",nbrFile)
    fin = open(inDir + "/" + nbrFile)
    fout = open(outDir + "/" +  nbrFile + ".int.txt", "w+")
    for line in fin:
        cleanLine = line.strip()
        cleanLine2 = cleanLine[1:-1]
        stBracketIndex = cleanLine2.find(",(")
        stSetIndex = cleanLine2.find(",Set")
        closingBracket = cleanLine2.find("))")
        label = cleanLine2[stBracketIndex+2:stSetIndex]
        nbrs = cleanLine2[stSetIndex+5:closingBracket]
        nbrList = nbrs.split(", ")

        if(label in vertexIdMap):
            labelId = vertexIdMap.get(label)
        else:
            print("Found a node in nbrMap that doesnt have an id in vertexList",
                  label)
            labelId = -1

        outLine = str(labelId) + ";;"

        for i in range(len(nbrList)):
            nbr = nbrList[i].strip()
            if(nbr in vertexIdMap):
                id = vertexIdMap.get(nbr)
                outLine = outLine + str(id) + ";;"
            else:
                print("Found a vertex in nbrList, not present in gloval vertex list", label, nbr)

        fout.write(outLine + "\n")
        fout.flush()

    fin.close()
    fout.close
