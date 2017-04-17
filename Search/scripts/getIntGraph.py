#!/usr/bin/python
import os, sys, io

# removes trailing characters from triples file
# like "s p d -1"  is changed to  "s p d"
def cleanTriples(inputFile, outFile):
    f = open(inputFile, "r")
    fout = open(outFile, "w+")
    for line in f:
        arr = line.strip().split()
        if(len(arr) == 4):
            triples = arr[0:-1]
            outLine = "\t".join(x for x in triples)
            fout.write(outLine + "\n")
        else:
            print("FOund a line not matching teh format", line)
    f.close()
    fout.close()

#Reads all triples files in a given directory and creates an integre mapping of
#the vertices
def createVertexMap(graphDir):
    dataFiles = os.listdir(graphDir)
    mydict = {}
    i=0
    for edgeFile in dataFiles:
        print("trying to read data file : ", edgeFile)
        fin = io.open(graphDir + "/" + edgeFile, encoding ='utf8')
        for line in fin:
            if(len(line) != 0):
                if(line[0] != '#' and line[0] != '@'):
                    cleanLine = line.strip().replace("<", "").replace(">","").replace(" .", "")
                    #cleanLine = line.strip()#.replace("<", "").replace(">","").replace(" .", "")
                    arr = cleanLine.split("\t")
                    if(len(arr) == 3):
                        node1 = arr[0].strip()
                        #eLabel = arr[1].strip()
                        node2 = arr[2].strip()
                        if(node1 not in mydict):
                            mydict[node1] = i
                            i=i+1
                        if(node2 not in mydict):
                            mydict[node2] = i
                            i=i+1
                    else:
                        print("find line not of length 3 :"  + line)
        fin.close()
    print("size of dictioary = ", len(mydict))
    return mydict

# Save map in "key \t value \n" format
def saveVertexMap(outFile, mydict):
    fout=io.open(outFile, mode="w+", encoding = 'utf8')
    for k,v in mydict.iteritems():
        fout.write(k + "\t" + str(v) + "\n")
    fout.close

# Loads a map in format "key \t value \n" from given file
def loadVertexMap(filepath):
    mydict = {}
    f = io.open(filepath, encoding = 'utf8')
    for line in f:
        arr = line.strip().split("\t")
        if(len(arr) == 2):
            key = arr[0]
            value = int(arr[1])
            mydict[key] = value
        else:
            print("FOund unrecognized format", line)
    return mydict

# Given a mapping of labels to integers 
# converts labeled graph triples or edgelist into integer format
def convertGraphToInt(graphDir, outputDir):
    vertexMap = createVertexMap(graphDir)
    saveVertexMap(vetexDictFile , vertexMap)
    edgeFiles = os.listdir(graphDir)
    for edgeFile in edgeFiles:
        convertFileToInt(graphDir + "/" + edgeFile, outputDir + "/" + edgeFile,
                        vertexMap)
    return

# Converts given labeled file to integer format using vertex map
def convertGraphFileToInt(inFile, outFile, vertexMap):
    print("trying to read data file : ", inFile)
    fin = io.open(inFile, encoding ='utf8')
    fout = open(outFile, mode = "w+")
    for line in fin:
        if(len(line) != 0 and line[0] != '#' and line[0] != '@'):
            cleanLine = line.strip().replace("<", "").replace(">","").replace(" .", "")
            arr = cleanLine.split("\t")
            if(len(arr) == 3):
                node1 = arr[0].strip()
                eLabel = arr[1].strip()
                node2 = arr[2].strip()
            elif(len(arr) == 2):
                node1 = arr[0].strip()
                node2 = arr[1].strip()
            else:
                print("Unrecogniezed graph format")
                exit


            if(node1 in vertexMap):
                id1 = vertexMap.get(node1)
            else:
                id1 = -1
                print("Found a vertex without an id in map", node1)
                exit()

            if(node2 in vertexMap):
                id2 = vertexMap.get(node2)
            else:
                id2 = -1
                print("Found a vertex without an id in map", node2)
                exit()

		    #if(eLabel in vertexMap):
		    #	edgeId = vertexMap.get(eLabel)
		    #else:
		    #	edgeId = -1
		    #	print("Found a edge , without an id", eLabel)
            #outLine = str(id1) + "\t" + str(edgeId) + "\t" + str(id2) + "\n"

            #outLine = str(id1) + "\t" + eLabel + "\t" + str(id2) + "\n"
            outLine = str(id1) + "\t" + str(id2) + "\n"
            fout.write(outLine)
    fin.close
    fout.close

if __name__ == "__main__":
    mainDir=./examples/
    graphInDir = mainDir + "/graph/"
    graphOutDir = mainDir + "/intGraph/"
    vertexDictFile = mainDir + "/vertexDictionary.out"
    vertexMap = createVertexMap(graphInDir)
    saveVertexMap(vertexDictFile, vertexMap)
    convertGraphFileToInt(graphInDir + "/yagoSample.ttl", graphOutDir +
                      "/yagoSampl.int.ttl", vertexMap)


