# Convert the edge string to numbers
# Feed this set of graphs to the Hierarchical Attention Network

import numpy as np


MAX_SENTS = 124
directory = 'data/streamspot/data/all_divs/'
count = 0
edge2num = {}
for gid in range(0, 600):
    fname = directory + str(gid)
    edges = []
    wfname = fname + 'w.txt'
    wf = open(wfname, 'w')
    with open(fname, 'rb') as f:
        for line in f:
            edge = line.split()[:-1]
            edge_str = ''.join([edge[1], edge[4], edge[3]])
            # Ignore the most popular edges to reduce similarity among graphs
            if edge_str in ['awe', 'auc', 'bGc', 'aGe', 'avc', 'aHe']:
                continue
            if edge_str in edge2num:
                edges.append(edge2num[edge_str])
            else:
                edge2num[edge_str] = count
                count += 1
                edges.append(edge2num[edge_str])
            if len(edges) % 100 == 0:
                wf.write(' '.join(map(str, edges)))
                edges = []
                wf.write('\n')
        wf.write('\n')
    wf.close()

data = np.zeros((600, MAX_SENTS, 100), dtype='int32')
for gid in range(0, 600):
    fname = directory + str(gid)
    doc = np.genfromtxt(fname + 'w.txt')[:MAX_SENTS]
    data[gid] = doc
print data.shape
np.save('kdd_data_' + str(MAX_SENTS) + '.npy', data)
