#!/usr/bin/python
# Generate average word2vec for entities with multiple wordnet classes
# Uses a hashmap containing word2vec[wordnet]
import os, sys
import gensim, logging
import numpy as np
import json

# Read entity[word2vec] map
# assumes each line is formatted as:
# entity;numpy_array
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

# vector normalize
def normalize(v):
  norm = np.linalg.norm(v)
  if(norm == 0):
    return v
  return v/norm


#@Input :
#@dataTopDir : Main directory path containing input files and path to output files
#@mapFile: Hashmap of each wordnet to its word2vec with format (wordnet_class;word2vec[wordnet_class])
#@inFile: each line containing multiple wordnet classes separated by ";;"
#@outFile : output file name
#@Output:
#@outFile containing average_wordnet_class. Each line in input file corresponds to line in output file 

dataTopDir = "/Users/d3x771/projects/nous/datasets/PathSearchBenchmark/wordnetToVector/"
mapFile = "allWordnetVector.txt"
inFile = "vertexTypeCleaned.txt"
outFile = "vertexWord2VecNormalized.txt"

#@Main
wordnetVecMap = get_wordnet_vec_map(dataTopDir + "/" + mapFile)      
print("Testing wordnet to vector map,  vector[dilemma]")
example = wordnetVecMap['dilemma']
print(example)

fout = open(dataTopDir + "/" + outFile, "w+")
fin = open(dataTopDir + "/" + inFile)

print("performing word2vec for vertices\n")
for line in fin:
  wordnetTopics = line.strip().split(";;")
  totalVec = np.zeros(300)
  for wordnetTopic in wordnetTopics:
    if(wordnetTopic in wordnetVecMap):
      wordVec = wordnetVecMap.get(wordnetTopic)
      totalVec = np.add(totalVec, wordVec)
    else:
      print("Could not find follwing wordnet class in map", wordnetTopic)
      print("\n")
  
  resultVec = normalize(totalVec)
  #resultVec = totalVec
  formatVec = ["{:0.4f}".format(x) for x in resultVec]
  fout.write(str(formatVec) + "\n")
fin.close()                        
fout.close
