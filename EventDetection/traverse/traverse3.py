# Code to compute differences in variances between positive and negative samples

import os, sys
import logging
import numpy as np
import json
from itertools import izip
from scipy import spatial
import matplotlib.pyplot as plt
import random


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
