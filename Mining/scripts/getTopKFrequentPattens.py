#!/usr/bin/python
import os, sys, io, re
from operator import itemgetter

freqPatterFile = "../output/frequentPatterns.tsv"
freqPatterFileTopK = "../output/frequentPatternsTopK.tsv"
k = 40
f = open(freqPatterFile, "r")
fout = open(freqPatterFileTopK, "w+")
patDict = {}
for line in f:
        allstrs = line.strip().split('\t')
        #allstrs = re.split(r'\t+', arr)
        if allstrs[0] in patDict:
            print "duplicate entry"
        else:
            patDict[allstrs[0]] = int(allstrs[1])   
        
print "Done"        
print len(patDict)
sortedTuples = sorted(patDict.items(), key=itemgetter(1))
sortedtuplesTopK = sortedTuples[-k:]
sortedtuplesTopKDict = {}
for i in sortedtuplesTopK:
    fout.write(i[0] + "\t" + str(i[1]) + '\n')
    sortedtuplesTopKDict[i[0]] = str(i[1])
    

f.close()
fout.close()

# Now we analyze pattern per batch using the top K frequent patterns
freqPattersPerBatchFile = "../output/frequentPatternsPerBatch.tsv"
freqPattersPerBatchFileTopK = "../output/frequentPatternsPerBatchTopK.tsv"
freqPattersPerBatchFileTopKFormatted = "../output/frequentPatternsPerBatchTopKFormatted.tsv"
freqPattersPerBatchFileTopKDict = {}
f = open(freqPattersPerBatchFile, "r")
fout = open(freqPattersPerBatchFileTopK, "w+")


for line in f:
        allstrs = line.strip().split('\t')
        if allstrs[1] in sortedtuplesTopKDict:
            fout.write(line)
            freqPattersPerBatchFileTopKDict.setdefault(allstrs[1],list()).append( (allstrs[0],allstrs[2]) )
 
f.close()
fout.close()

fout = open(freqPattersPerBatchFileTopKFormatted,"w+")
for i in freqPattersPerBatchFileTopKDict: 
    fout.write(i+"\t"+str(freqPattersPerBatchFileTopKDict[i])+"\n")

fout.close()        