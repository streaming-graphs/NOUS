#!/usr/bin/python
import os, sys
import logging
import numpy as np
import json
from itertools import izip
from scipy import spatial

def get_vertex_feature_map(vertexNames, vertexTopics, vertexDegrees):
  mydict = {}
  with open(vertexNames, 'r') as fNames, open(vertexTopics, "r") as fTopics, open(vertexDegrees, "r") as fDegree:
    for line1, line2, line3 in izip(fNames, fTopics, fDegree):
      name = line1.strip()
      topic = line2.strip()
      degree = line3.strip()
      v = topic[1:-1].split(", ")
      cleanv = [float(i[1:-1]) for i in v]
      mydict[name] = (np.array(cleanv, np.float), int(degree))
  print("size of current dictionary", len(mydict))
  return mydict

def avg(topicArray):
  return np.average(topicArray)

def var(topicArray):
  seqVarianceAllFeatures = np.var(topicArray, axis=0, dtype=np.float64)
  totalVariance = np.sum(seqVarianceAllFeatures)
  return totalVariance

def pairWiseVar(topicArray):
  totalVar = 0  
  for i in range(len(topicArray)-1):
    topicVar = np.var(topicArray[i:i+2], axis=0, dtype=np.float64)
    totalVar = totalVar + topicVar
  return totalVar/(len(topicArray-1))
  #return np.sum(totalVar)

def cosineVar(topicArray):
  avgTopic = np.average(topicArray, axis=0)
  totalVar = 0.0
  for topic in topicArray:
    #print(len(topic), len(avgTopic))
    if(np.count_nonzero(topic) > 0 and np.count_nonzero(avgTopic) > 0):
    	tDist = spatial.distance.cosine(topic, avgTopic)
    	totalVar = totalVar + tDist
    else:
	print("Found a sequence with zero in topic", str(topic))
  return totalVar

  
dataTopDir = "/Users/d3x771/projects/nous/datasets/PathSearchBenchmark/path_results/pathsize_4_deg_500/pathsWithEntitySeqOnly"
vertexFile = dataTopDir + "/vertexNames.txt"
degreeFile = dataTopDir + "/vertexDegree.txt"
featureFile = dataTopDir + "/vertexWord2VecNormalized.txt"
seqInputDir = dataTopDir + "/vertexStringFormat/" 
outFile = "seqWithScores.txt"
header = "#Path\tAvergaeTopic\tTotalTopicVariance\tTopicCosineVar\tAvergaeDegree\tDegreeVariance\n"
# get vertex feature map
vertexFeatureMap =  get_vertex_feature_map(vertexFile, featureFile, degreeFile)

# For all files in  output directory
for seqFile in os.listdir(seqInputDir):
  if(seqFile.endswith("out.sequence")):
    fout = open(seqInputDir + seqFile +".score", "w+")
    fout.write(header)
    inFile = open(seqInputDir + seqFile, "r")
    print("Reading file", seqFile)
    for line in inFile:
      vertices = line.strip().split(";;")
      #print("number of vertices in sequence = ", len(vertices))
      featureList = []
      for vert in vertices:
        # Handle missing vertices in the map
        if(vert in vertexFeatureMap):
          vertFeature = vertexFeatureMap.get(vert)
	  featureList.append(vertFeature)
          ##print("Found entity in sequence",  vertFeature)
        else:
          print("Found entity in sequence not in vertexFeatureMap", vert)
     
      if(len(featureList) > 0): 
      	topics = [x[0] for x in featureList]
      	degrees = [x[1] for x in featureList]
        
	pathAvgTopic = np.average(topics, axis=0)
        avgTopicFormatted = ["{:0.4f}".format(x) for x in pathAvgTopic]
        
	pathTopicTotalVar = np.var(topics, axis=0, dtype=np.float64)
        topicVarFormatted = ["{:0.4f}".format(x) for x in pathTopicTotalVar]
        
	pathCosineVar = cosineVar(topics)
      	
	#pathTopicPairVar = pairWiseVar(topics)
	#pairVarFormatted = ["{:0.4f}".format(x) for x in pathTopicPairVar]
        
	pathDegVar = np.var(degrees)
      	pathDegAvg = sum(degrees)/len(degrees)
      	
	#fout.write(line.strip() + "\t" + str(scoreTotalVar) + "\t" + str(scorePairVar) + "\t" + str(scoreAvgDegree) + "\n")

      	fout.write(line.strip() + "\t" + str(avgTopicFormatted) + "\t" + str(topicVarFormatted) + "\t" +  str(pathCosineVar) + "\t" + str(pathDegAvg) + "\t" + str(pathDegVar)+ "\n")
      else:
        print("No Feature found for any verex in this sequence")
    fout.close
