#!/usr/bin/python
import os, sys
import gensim, logging
import numpy as np
import json

def normalize(v):
  norm = np.linalg.norm(v)
  if(norm == 0):
    return v
  return v/norm

modelFile="/Users/d3x771/projects/nous/Word2Vec/GoogleNews-vectors-negative300.bin" 
dataTopDir="/Users/d3x771/projects/nous/datasets/PathSearchBenchmark/wordnetToVector/yagoNoMissingData/"
clasNotFound="wordnetWithoutVectors"
inFile="wordnet_vertices.clean.out"
#inFile="allWordnetType.txt"
outFile="wordnetVectorFloat.withoutZeros.txt"
        
fout = open(dataTopDir + "/" + outFile, "w+")
fin = open(dataTopDir + "/" + inFile)
ferr = open(dataTopDir + "/" + clasNotFound, "w+")

print("loading google word2vec model\n")
model = gensim.models.Word2Vec.load_word2vec_format(modelFile, binary=True)

print("loaded model, testing its working [woman+king-man]\n")
answer = model.most_similar(positive=['woman', 'king'], negative=['man'], topn=3)
print(answer)

print("performing word2vec for wordnet categories\n")
notFoundPhrase = 0
foundPartPhrase = 0
for line in fin:
  if(len(line) > 0 and line[0] != '#'):
    wordnetTopic = line.strip()
    
    try:
      wordnetVector = model[wordnetTopic]
      wordnetValue = ["{:0.4f}".format(x) for x in wordnetVector]
      fout.write(wordnetTopic + "; " + str(wordnetValue) + "\n")
    
    except KeyError:
      notFoundPhrase = notFoundPhrase + 1
      wordnetTopics = wordnetTopic.split("_")
      totalVec = np.zeros(300, dtype=np.float)
      found = 0
      for topic in wordnetTopics:
        if(topic in model):
          totalVec = np.add(totalVec, model[topic])
          found = found + 1
      
      if(found > 0):
	foundPartPhrase = foundPartPhrase + 1
      	totalVec = normalize(totalVec)
        wordnetValue = ["{:0.4f}".format(x) for x in wordnetVector]
        fout.write(wordnetTopic + "; " + str(wordnetValue) + "\n")
      
      ferr.write(wordnetTopic + "	found_parts-" + str(found) + "\n")

print("Number of wordnet classes  not found in google model = ", notFoundPhrase)
print("Number of wordnet classes found by splitting the phrase and averaging", foundPartPhrase)
fin.close()                        
fout.close
