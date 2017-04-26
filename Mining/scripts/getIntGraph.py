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
def createVertexEdgeMap(graphDir):
    dataFiles = os.listdir(graphDir)
    mydict = {}
    edgeDict = {}
    i=0
    edgeId = 0
    for edgeFile in dataFiles:
        print("trying to read data file : ", edgeFile)
        fin = io.open(graphDir + "/" + edgeFile, encoding ='utf8')
        for line in fin:
            if(len(line) != 0):
                if(line[0] != '#' and line[0] != '@'):
                    cleanLine = line.strip().replace("<", "").replace(">","").replace(" .", "")
                    #cleanLine = line.strip()#.replace("<", "").replace(">","").replace(" .", "")
                    arr = cleanLine.split("\t")
                    if(len(arr) == 4):
                        node1 = arr[0].strip()
                        eLabel = arr[1].strip()
                        node2 = arr[2].strip()
                        time  = arr[3].strip()
                        if(node1 not in mydict):
                            mydict[node1] = i
                            i=i+1
                        if(node2 not in mydict):
                            mydict[node2] = i
                            i=i+1
			if(eLabel not in edgeDict):
			    edgeDict[eLabel] = edgeId
			    edgeId +=1
                    else:
                        print("find line not of length 3 :"  + line)
        fin.close()
    print("size of dictioary = ", len(mydict))
    return (mydict, edgeDict)

# Save map in "key \t value \n" format
def saveVertexEdgeMap(outFile, mydict):
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
    vertexMap, edgeMap = createVertexEdgeMap(graphDir)
    saveVertexEdgeMap(vertexDictFile , vertexMap)
    saveVertexEdgeMap(edgeDictFile , edgeMap)
    edgeFiles = os.listdir(graphDir)
    for edgeFile in edgeFiles:
        convertGraphFileToInt(graphDir + "/" + edgeFile, outputDir + "/" + edgeFile,
                        vertexMap,edgeMap)
    return

# Converts given labeled file to integer format using vertex map
def convertGraphFileToInt(inFile, outFile, vertexMap, edgeMap):
    print("trying to read data file : ", inFile)
    fin = io.open(inFile, encoding ='utf8')
    fout = open(outFile, mode = "w+")
    for line in fin:
        if(len(line) != 0 and line[0] != '#' and line[0] != '@'):
            cleanLine = line.strip().replace("<", "").replace(">","").replace(" .", "")
            arr = cleanLine.split("\t")
            if(len(arr) == 4):
                node1 = arr[0].strip()
                eLabel = arr[1].strip()
                node2 = arr[2].strip()
                time = arr[3].strip()
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

	    if(eLabel in edgeMap):
		edgeId = edgeMap.get(eLabel)
	    else:
	    	edgeId = -1
	    	print("Found a edge , without an id", eLabel)
            outLine = str(id1) + "\t" + str(edgeId) + "\t" + str(id2) + "\t" + time + "\n"

            #outLine = str(id1) + "\t" + eLabel + "\t" + str(id2) + "\n"
            #outLine = str(id1) + "\t" + str(id2) + "\n"
            fout.write(outLine)
    fin.close
    fout.close

mainDir="../examples/perfData/"
graphInDir = mainDir + "/graph/"
graphOutDir = mainDir + "/intGraph/"
vertexDictFile = mainDir + "/vertexDictionary.out"
edgeDictFile = mainDir + "/edgeDictionary.out"
vertexMap, edgeMap = createVertexEdgeMap(graphInDir)
saveVertexEdgeMap(vertexDictFile, vertexMap)
saveVertexEdgeMap(edgeDictFile, edgeMap)
#convertGraphFileToInt(graphInDir + "/yagoSampleAllDetroit.ttl", graphOutDir +
               #       "/yagoSampleAllDetroit.int.ttl", vertexMap, edgeMap)

convertGraphToInt(graphInDir, graphOutDir)
