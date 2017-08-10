#!/usr/bin/python
import os, sys
import logging
import numpy as np
import json
from itertools import izip
from scipy import spatial
import matplotlib.pyplot as plt
import random
np.random.seed(7)


# create a map containing features for each vertex
# vertex -> (vector space embedding, vertex degree)
# def get_vertex_feature_map(vertexNames, vertexTopics, vertexDegrees):
#     mydict = {}
#     with open(vertexNames, 'r') as fNames, open(vertexTopics, "r") as fTopics, open(vertexDegrees, "r") as fDegree:
#         for line1, line2, line3 in izip(fNames, fTopics, fDegree):
#             name = line1.strip()
#             topic = line2.strip()
#             degree = line3.strip()
#             v = topic[1:-1].split(", ")
#             cleanv = [float(i[1:-1]) for i in v]
#             mydict[name] = (np.array(cleanv, np.float), int(degree))
#     print("size of current dictionary", len(mydict))
#     return mydict

# given a array of vertex embeddings
# giev average embedding
def avg(topicArray):
    return np.average(topicArray)

# given a sequence of vertex embeddings
# calculate variance as
# sum(variance in each topic space)
def var(topicArray):
    seqVarianceAllFeatures = np.var(topicArray, axis=0, dtype=np.float64)
    totalVariance = np.sum(seqVarianceAllFeatures)
    return totalVariance


# Given a sequence of vertex embedding
# calculate total path varinace as sum of pairwise variance
def pairWiseVar(topicArray):
    totalVar = 0
    for i in range(len(topicArray) - 1):
        topicVar = np.var(topicArray[i:i + 1], axis=0, dtype=np.float64)
        totalVar = totalVar + topicVar
    return totalVar / (len(topicArray)-1)
    # return np.sum(totalVar)


# GIven a sequence of vertex embeddings(topics)
# calculate path variance as cosine distance between node and average path
# embedding
def cosineVar(topicArray):
    avgTopic = np.average(topicArray, axis=0)
    totalVar = 0.0
    for topic in topicArray:
        # print(len(topic), len(avgTopic))
        if np.count_nonzero(topic) > 0 and np.count_nonzero(avgTopic) > 0:
            tDist = spatial.distance.cosine(topic, avgTopic)
            totalVar = totalVar + tDist
        else:
            print("Found a sequence with zero in topic", str(topic))
    return totalVar


# ename = sys.argv[1]
# embeds = np.load(ename)
#
# xtest = sys.argv[2]
# X_test = np.load(xtest)
#
# ytest = sys.argv[3]
# y_test = np.load(ytest)
#
# ypreds = sys.argv[4]
# y_preds = np.load(ypreds)

# tpreds = [1 if pred >= 0.5 else 0 for pred in y_preds]

flips = 1
pl = 7
directory = 'data/wordnet/versions/'
pos_samples = np.genfromtxt(directory + 'pos_samples_nums_' + str(flips) + 'f_pl' + str(pl) + '.txt').astype('int32')
pos_samples = pos_samples[:, :-1]

neg_samples = np.genfromtxt(directory + 'neg_samples_nums_' + str(flips) + 'f_pl' + str(pl) + '.txt').astype('int32')
neg_samples = neg_samples[:, :-1]

# wc = []
# for i in range(len(tpreds)):
#     if tpreds[i] != y_test[i]:
#         wc.append(i)

variance_pos = []
for path in pos_samples:
    # seq = []
    # for j in [0, 2, 4, 6, 8, 10]:
    #     seq.append(path[j])
    # variance_pos.append(var(seq))
    variance_pos.append(var(path))

# mv = max(variance_pos)
# variance_pos = [v/mv for v in variance_pos]

variance_neg = []
for path in neg_samples:
    # seq = []
    # for j in [0, 2, 4, 6, 8, 10]:
    #     seq.append(path[j])
    # variance_neg.append(var(seq))
    variance_neg.append(var(path))

# mv = max(variance_neg)
# variance_neg = [v/mv for v in variance_neg]

diff = [abs(variance_pos[i] - variance_neg[i]) for i in range(len(variance_neg))]
diff = np.array(diff)
mean_diff = np.average(diff)
print mean_diff

# print len(coherence)
# print len(y_preds)
# for i in range(len(y_preds)):
#     if i in wc:
#         plt.plot(variance[i], y_preds[i], 'ro')
#     else:
#         plt.plot(variance[i], y_preds[i], 'bo')

# xax = np.arange(0, len(neg_samples), 1)
# idc = random.sample(xax[:-1], 100)
# plt.plot(xax[:100], diff[idc], 'k-')
# plt.plot(xax[:100], [mean_diff]*100, 'r-')
# # plt.plot(xax[:100], variance_pos[:100], 'g.')
# plt.ylabel('Variance')
# plt.xlabel('Samples')
# plt.savefig('var_' + str(flips) + 'f_pl' + str(pl) + '.pdf')
# # plt.show()
# plt.close()

# #input directory
# dataTopDir = "/Users/d3x771/projects/nous/datasets/PathSearchBenchmark/path_results/pathsize_4_deg_500/
# pathsWithEntitySeqOnly"
# # vertexfile, degreefile, featurefile are all supposed to be synced per line
# # for example line 1 in each file contains
# # vertexFile -> vertexLabel1
# # degreeFile -> vertexLabel1.degree
# # featureFile.txt -> vertexLabel1.embedding
# vertexFile = dataTopDir + "/vertexNames.txt"
# degreeFile = dataTopDir + "/vertexDegree.txt"
# featureFile = dataTopDir + "/vertexWord2VecNormalized.txt"
# seqInputDir = dataTopDir + "/vertexStringFormat/"
# outFile = "seqWithScores.txt"
# header = "#Path\tAvergaeTopic\tTotalTopicVariance\tTopicCosineVar\tAvergaeDegree\tDegreeVariance\n"
# # get (vertex-> embedding, degree) feature map
# vertexFeatureMap =  get_vertex_feature_map(vertexFile, featureFile, degreeFile)
#
# # read all path files in output dircetory
# for seqFile in os.listdir(seqInputDir):
#   if(seqFile.endswith("out.sequence")):
#     fout = open(seqInputDir + seqFile +".score", "w+")
#     fout.write(header)
#     inFile = open(seqInputDir + seqFile, "r")
#     print("Reading file", seqFile)
#     # For each path (vertex label) sequence in output file
#     # map it to corresponsding feature sequence
#     for line in inFile:
#       if(line.startswith('[') and line.endswith(']'):
#          line = line[1:-1]
#       vertices = line.strip().split(",")
#       #print("number of vertices in sequence = ", len(vertices))
#       featureList = []
#       for vert in vertices:
#         # Handle missing vertices in the map
#         if(vert in vertexFeatureMap):
#           vertFeature = vertexFeatureMap.get(vert)
# 	      featureList.append(vertFeature)
#           ##print("Found entity in sequence",  vertFeature)
#         else:
#           print("Found entity in sequence not in vertexFeatureMap", vert)
#
#       if(len(featureList) > 0):
#       	topics = [x[0] for x in featureList]
#       	degrees = [x[1] for x in featureList]
#
# 	    pathAvgTopic = np.average(topics, axis=0)
#         avgTopicFormatted = ["{:0.4f}".format(x) for x in pathAvgTopic]
#
# 	    pathTopicTotalVar = np.var(topics, axis=0, dtype=np.float64)
#         topicVarFormatted = ["{:0.4f}".format(x) for x in pathTopicTotalVar]
#
# 	    pathCosineVar = cosineVar(topics)
#
# 	    #pathTopicPairVar = pairWiseVar(topics)
# 	    #pairVarFormatted = ["{:0.4f}".format(x) for x in pathTopicPairVar]
#
# 	    pathDegVar = np.var(degrees)
#       	pathDegAvg = sum(degrees)/len(degrees)
#
# 	    #fout.write(line.strip() + "\t" + str(scoreTotalVar) + "\t" + str(scorePairVar) + "\t" + str(scoreAvgDegree) + "\n")
#
#       	fout.write(line.strip() + "\t" + str(avgTopicFormatted) + "\t" + str(topicVarFormatted) + "\t" +  str(pathCosineVar) + "\t" + str(pathDegAvg) + "\t" + str(pathDegVar)+ "\n")
#       else:
#         print("No Feature found for any verex in this sequence")
#     fout.close
