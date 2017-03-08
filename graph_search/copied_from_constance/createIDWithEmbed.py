import sys, io
import numpy as np

embedFile = "vecEmbedYago.txt"
idFile = "yago2data/vertexDictionary.out"
f1 = open(embedFile , "r")
f2 = open(idFile, "r")
fout = open("vecEmbedWithIdYago.txt", "w+")
for line1,line2 in zip(f1, f2):
    arr2 = line2.strip().split("\t")
    label = arr2[0]
    fout.write(label + ";;" + line1)
f1.close()
f2.close()
fout.close()
    
