#!/usr/bin/python
import os, sys, io

# reads a file of form
# "label	integer_id"
# as {integerid -> label}
def getIdtoLabelDict(filename):
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
    
# reads an inFile containing a path per line of format :
# "srcid : edge1, edge2.. "
# and converts it to labeled format
def convertIdsToLabels(vertex_dict, edge_dict, inFile, outFile, srcToPathSep, edgeSep):
    print("trying to read paths : ", inFile)
    fin = open(inFile, "r")
    fout=io.open(outFile, mode="w+", encoding = 'utf8')
    for line in fin:
        if(len(line) != 0):
            if(line[0] != '#' and line[0] != '@'):
                #allIds = [int(x) for x in re.findall(r'\d+', line.strip())]
                srcPaths = line.strip().split(srcToPathSep)
                if(len(srcPaths) == 2):
		    srcid = int(srcPaths[0].strip())
                    srcLabel = vertex_dict.get(srcid)
                    #print("\nsrcid, srclabel", srcid, srcLabel)
                    edges = srcPaths[1].strip().split(edgeSep)
                    path = srcLabel + srcToPathSep 
		    for edge in edges:
			if(len(edge) > 5):
			    cleanEdge = edge.strip()
                            spos = cleanEdge.find("(")
                            epos = cleanEdge.find("-")
			    edge_id = int(cleanEdge[spos+1:epos])
			    spos2 = cleanEdge.find(")")
                            direction = cleanEdge[epos+1:spos2]
			    node_id = int(cleanEdge[spos2+2:])
                            if(node_id in vertex_dict and edge_id in edge_dict):
				node_label = vertex_dict.get(node_id)
				edge_label = edge_dict.get(edge_id)
                                path = path + " (" + edge_label + "-" +  direction + ") " + node_label + edgeSep + " "
                            else:
                                print("Found a node id not in dict (src, nbrid)=", node_id, edge_id, line)
			        exit
                    fout.write(path + "\n")
    fin.close()
    fout.close()
   

if __name__ == "__main__":
    if(len(sys.argv) == 4):
        mainDir = sys.argv[1]
        pathInDir = sys.argv[2]
        pathOutDir = sys.argv[3]
    	vertexDictFile = mainDir + "/vertexDictionary.out"
    	edgeDictFile = mainDir + "/edgeDictionary.out"

        vertex_dict = getIdtoLabelDict(vertexDictFile)
        edge_dict = getIdtoLabelDict(edgeDictFile)
        for inFile in os.listdir(pathInDir):
	    inPath = pathInDir + "/" + inFile
            outPath = pathOutDir + "/" + inFile + ".labeled"
	    convertIdsToLabels(vertex_dict, edge_dict, inPath, outPath, ":", ",")
	    print("Finished writing", outPath)
    else:
	print("Check command line arguments : ")
	print("Usage <pathToDirContainingVertexEdgeDict> <inputPathsDir> <outputPathsDir>")
