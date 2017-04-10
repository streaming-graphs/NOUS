#!/usr/bin/python
import os, sys
import logging
import numpy as np
import json
from itertools import izip

#Reads file, with each line in follwing format
#seq"\t"feature1"\t"feature2"\t" ...feature4
# and creates map seq->(featur1, feature2, ...)
#Current implementation assumes feature1 and feature2 are numpy arrays 
def get_seq_feature_map(filename):
  mydict = {}
  with open(filename, 'r') as fNames:
    for line in fNames:
      arr = line.strip().split("\t")
      if(len(arr) == 6 and (not line.startswith("#"))):
	name = arr[0]
        
	avgTopic = arr[1]
      	v1 = avgTopic[1:-1].split(", ")
      	cleanAvgTopic = [float(i[1:-1]) for i in v1]
        
	topicVar = arr[2]
        v2 = topicVar[1:-1].split(", ")
        cleanTopicVar = [float(i[1:-1]) for i in v2]
        
        topicCosVar = float(arr[3])
        avgDeg = float(arr[4])
        degVar = float(arr[5])
        mydict[name] = (np.array(cleanAvgTopic, np.float), np.array(cleanTopicVar, np.float), topicCosVar, avgDeg, degVar)
      elif(len(arr) != 6):
        print("Found a line with length not 6, ", line)
  print("size of current dictionary", len(mydict))
  return mydict

  
dataTopDir = "/Users/d3x771/projects/nous/datasets/PathSearchBenchmark/path_results/pathsize_4_deg_500/pathsWithEntitySeqOnly/vertexStringFormat/" 
cosVarThresh = 0.45
highVarThresh = 0.35
highDegThresh = 150

print("\nThe #paths in map may be smaller than the file, since there are duplicate node sequences prsent as paths")
print("Duplicate sequence emanate from ignoring edge types in the path sequence") 

# For all files in  output directory
for seqFile in os.listdir(dataTopDir):
  if(seqFile.endswith("out.sequence.score")):
    seqFeatureMap =  get_seq_feature_map(dataTopDir + seqFile)
    fout = open(dataTopDir + seqFile +".ToGradeExamples", "w+")
    HighVarHighDeg = []
    HighVarLowDeg = []
    LowVarHighDeg = []
    LowVarLowDeg = []
    HighCosVarHighDeg = []
    HighCosVarLowDeg = []
    LowCosVarHighDeg = []
    LowCosVarLowDeg = []
    for seq,feature in seqFeatureMap.iteritems():
      avgTopic = feature[0]
      topicVar = feature[1]
      topicCosVar = feature[2]
      avgDeg = feature[3]
      degVar = feature[4]  
      if(sum(topicVar) >= highVarThresh and avgDeg >= highDegThresh):
        HighVarHighDeg.append(seq)
      elif(sum(topicVar) >= highVarThresh and avgDeg < highDegThresh):
        HighVarLowDeg.append(seq)
      elif(sum(topicVar) < highVarThresh and avgDeg < highDegThresh):
        LowVarLowDeg.append(seq)
      elif(sum(topicVar) < highVarThresh and avgDeg >= highDegThresh):
        LowVarHighDeg.append(seq)
      else :
        print("Found a seq that doenst fit any category")
     
      if(topicCosVar >= cosVarThresh and avgDeg >= highDegThresh):
        HighCosVarHighDeg.append(seq)
      elif(topicCosVar >= cosVarThresh and avgDeg < highDegThresh):
        HighCosVarLowDeg.append(seq)
      elif(topicCosVar < cosVarThresh and avgDeg < highDegThresh):
        LowCosVarLowDeg.append(seq)
      elif(topicCosVar < cosVarThresh and avgDeg >= highDegThresh):
        LowCosVarHighDeg.append(seq)
      else :
        print("Found a seq that doenst fit any category")

    ## Data based on avergae variance 
    fout.write("\nHigh topic variance, high average degree" + str(len(HighVarHighDeg)) + "\n")
    for s in HighVarHighDeg:
      fout.write(s+"\n")
   
    fout.write("\nHigh topic varianace, low average degree" + str(len(HighVarLowDeg)) + "\n")
    for s in HighVarLowDeg:
      fout.write(s+"\n")
   
    fout.write("\nLow topic varinace, low average degree" + str(len(LowVarLowDeg)) + "\n")
    for s in LowVarLowDeg:
      fout.write(s+"\n")
	
    fout.write("\nLow topic variance, high average degree" + str(len(LowVarHighDeg)) + "\n")
    for s in LowVarHighDeg:
      fout.write(s+"\n")
   
    ## Data based on cosine varinace
    fout.write("\nHigh Cosine topic variance, high average degree" + str(len(HighCosVarHighDeg)) + "\n")
    for s in HighCosVarHighDeg:
      fout.write(s+"\n")
   
    fout.write("\nHigh Cosine topic varianace, low average degree" + str(len(HighCosVarLowDeg)) + "\n")
    for s in HighCosVarLowDeg:
      fout.write(s+"\n")
   
    fout.write("\nLow Cosine topic varinace, low average degree" + str(len(LowCosVarLowDeg)) + "\n")
    for s in LowCosVarLowDeg:
      fout.write(s+"\n")
	
    fout.write("\nLow Cosine topic variance, high average degree" + str(len(LowCosVarHighDeg)) + "\n")
    for s in LowCosVarHighDeg:
      fout.write(s+"\n")
    fout.close
