#!/usr/bin/python
# Use Google word 2 vec model, to get word2vec[wordnet_classes] 
# The classes not present in google model are set to np.zero(300, float)
# For details of word2vec usage refer https://radimrehurek.com/gensim/models/word2vec.html

import os, sys
import gensim, logging
import numpy as np
import json


#@Input
#@modelFile: Path to Google word2vec model (find link here https://code.google.com/archive/p/word2vec/)
##dataTopDir: Directory path where input file is present and all output files will be written to
#@inFile: File with each line containing a wordnet category (only label)
#
#@Output
#@outFile : Output file name (wordnet_label;wordnet_word2vec_vector)
#@clasNotFound : Output file , to list which wordnet categories were not found in google model

modelFile="/Users/d3x771/projects/nous/Word2Vec/GoogleNews-vectors-negative300.bin" 
dataTopDir="/Users/d3x771/projects/nous/datasets/PathSearchBenchmark/wordnetToVector/"
clasNotFound="wordnetWithoutVectors"
inFile="allWordnetType.txt"
outFile="allWordnetVectorFloat.txt"
        
fout = open(dataTopDir + "/" + outFile, "w+")
fin = open(dataTopDir + "/" + inFile)
ferr = open(dataTopDir + "/" + clasNotFound, "w+")

print("loading google word2vec model\n")
model = gensim.models.Word2Vec.load_word2vec_format(modelFile, binary=True)

print("loaded model, testing its working [woman+king-man]\n")
answer = model.most_similar(positive=['woman', 'king'], negative=['man'], topn=3)
print(answer)

print("performing word2vec for wordnet categories\n")
i = 0
for line in fin:
  if(len(line) > 0 and line[0] != '#'):
    wordnetTopic = line.strip()
    try:
      wordnetVector = model[wordnetTopic]
    except KeyError:
      i = i + 1
      ferr.write(wordnetTopic + "\n")
      wordnetVector = np.zeros(300, dtype=np.float)
    
    wordnetValue = ["{:0.4f}".format(x) for x in wordnetVector]
    fout.write(wordnetTopic + "; " + str(wordnetValue) + "\n")
print("Number of wordnet classes  not found in google model = ", i)
fin.close()                        
fout.close
