#!/usr/bin/python
import os, sys
import gensim, logging
import numpy as np
import json

def get_wordnet_vec_map(filename):
  mydict = {}
  with open(filename, 'r') as infile:
    for line in infile:
      arr = line.strip().split(";")
      if(len(arr) == 2):
        label = arr[0].strip()
        v = arr[1].strip()[1:-1].split(", ")
        cleanv = [float(i[1:-1]) for i in v]
        mydict[label] = np.array(cleanv, np.float)
  print("size of current dictionary", len(mydict))
  return mydict

def normalize(v):
  norm = np.linalg.norm(v)
  if(norm == 0):
    return v
  return v/norm

dataTopDir = "/Users/d3x771/projects/nous/datasets/PathSearchBenchmark/wordnetToVector/yagoNoMissingData/"
mapFile = "wordnetVectorFloat.withoutZeros.txt"
inFile = "wordnetNbrLabels.clean.out"
outFile = "vertexWord2VecNormalized.txt"
#mapFile = "allWordnetVector.txt.save"
#inFile = "vertexTypeCleaned.txt"

wordnetVecMap = get_wordnet_vec_map(dataTopDir + "/" + mapFile)      
print("Testing wordnet to vector map,  vector[dilemma]")
example = wordnetVecMap['dilemma']
print(example)

fout = open(dataTopDir + "/" + outFile, "w+")
fin = open(dataTopDir + "/" + inFile)

print("performing word2vec for vertices\n")
num_vertices_without_any_match = 0
for line in fin:
  idWithWordnet = line.strip().split(";;;;")
  if(len(idWithWordnet) != 2):
    println("Formatting error, expecting nodeid;;;;wordnetlabels")
    exit
  srcid = int(idWithWordnet[0].strip())
  wordnetTopics = idWithWordnet[1].strip().split(";;")
  totalVec = np.zeros(300)
  found = 0
  for wordnetTopic in wordnetTopics:
    if(wordnetTopic in wordnetVecMap):
      wordVec = wordnetVecMap.get(wordnetTopic)
      totalVec = np.add(totalVec, wordVec)
      found = found + 1
  
  if(found == 0):
    num_vertices_without_any_match = num_vertices_without_any_match + 1
    print("No wordnet category for this vertex was matched, assigning np.zero(300): ", line) 
    
    #else:
    #  print("Error: A wordnet class , not found in wordnet class map", wordnetTopic)
    #  print("\n")
  
  resultVec = normalize(totalVec)
  #resultVec = totalVec
  formatVec = ["{:0.4f}".format(x) for x in resultVec]
  fout.write(str(srcid) + ";;;;" + str(formatVec) + "\n")
fin.close()                        
fout.close
