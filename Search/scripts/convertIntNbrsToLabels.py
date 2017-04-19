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
    
# reads an inFile of format
# "srcid  num_nbrs nbr_id1,nbrid2,nbrid3.."
# and converts it to labeled format
def convertIdsToLabels(vertexDictFile, edgeDictFile, inFile, outFile, srcToPathSep, edgeSep):
    vertex_dict = getIdtoLabelDict(vertexDictFile)
    edge_dict = getIdtoLabelDict(edgeDictFile)
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
    mainDir = "../examples/yago/"
    vertexDictFile = mainDir + "/vertexDictionary.out"
    edgeDictFile = mainDir + "edgeDictionary.out"
    inFile = mainDir + "output/1__16848"
    outFile = mainDir + "output/1__16848.labeled.out"
    convertIdsToLabels(vertexDictFile, edgeDictFile, inFile, outFile, ":", ",")
