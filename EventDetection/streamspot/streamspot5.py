# Visualize the dataset by looking at what nodes appear the most number of times
# and other stats


import matplotlib.pyplot as plt
import numpy as np
import networkx as nx
import pickle as pkl


directory = 'data/streamspot/data/all_divs/'
fname = '0dstar.txt'

gstar = pkl.load(open(directory + fname))

cc = nx.closeness_centrality(gstar)
keyscc = sorted(cc, key=cc.get, reverse=True)
valscc = sorted(cc.values(), reverse=True)
print keyscc[:20], valscc[:20]

bc = nx.betweenness_centrality(gstar)
keysbc = sorted(bc, key=bc.get, reverse=True)
valsbc = sorted(bc.values(), reverse=True)
print keysbc[:20], valsbc[:20]

dc = nx.degree_centrality(gstar)
keysdc = sorted(dc, key=dc.get, reverse=True)
valsdc = sorted(dc.values(), reverse=True)
print keysdc[:20], valsdc[:20]

xax = np.arange(0, 500)
plt.plot(xax, valscc[:500], 'r.')
plt.plot(xax, valsbc[:500], 'b.')
plt.plot(xax, valsdc[:500], 'g.')
plt.savefig('centralityscat500.pdf')

with open(directory + fname, 'r') as f:
    content = f.readlines()

hist = {}

for cont in content:
    words = cont.split('\t')
    for i in [1, 3]:
        temp = hist.get(words[i], 0)
        hist[words[i]] = temp + 1

# print hist
keys = sorted(hist, key=hist.get, reverse=True)
lh = len(hist)
xax = np.arange(0, lh)
plt.plot(xax, sorted(hist.values(), reverse=True), '.')
plt.locator_params(axis='x', nbins=lh-1)
plt.xticks(xax, keys)

plt.savefig('hist' + fname + 'n.pdf')
