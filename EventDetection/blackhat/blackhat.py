import numpy as np
import sys


# Directory where the graph edges and nodes files are
directory = sys.argv[1]
midfix = 'graph_'
ext = '.edges.txt'

# Get a list of connections and the number of times they have appeared
hist = {}
name2num = {'Dev': 1001, 'Admin': 1002, 'Mgmt': 1003, 'IT': 1004, 'HR': 1005,
            'SkypeEmail': 1006, 'CloudServices': 1007, 'Computing': 1008, 'Infra': 1009, 'Ext': 1010}
num2name = {}
for k, v in name2num.items():
    num2name[v] = k

training = []
nf = int(sys.argv[2])

for edge_file in range(0, nf):
    fname = directory + midfix + str(edge_file) + ext
    with open(fname, 'rb') as f:
        for line in f:
            line_list = line.split()[1:3]
            src, src_id = line_list[0].split('_')
            dest, dest_id = line_list[1].split('_')
            connection1 = [name2num[src], int(src_id), name2num[dest], int(dest_id)]
            connection2 = [name2num[dest], int(dest_id), name2num[src], int(src_id)]
            training.append(connection1)
            training.append(connection2)
            key1 = (src, dest)
            key2 = (dest, src)
            hist[key1] = hist.get(key1, 0) + 1
            hist[key2] = hist.get(key2, 0) + 1

# Compute score for each type of connection
tot = sum(hist.values()) / 2
for key in hist.keys():
    hist[key] = hist[key] * 100.0 / tot

# Create training samples
labels = []
for sample in training:
    key = (num2name[sample[0]], num2name[sample[2]])
    labels.append(hist[key])

# Save files
training = np.array(training)
labels = np.array(labels)
np.save(directory + 'training.npy', training)
np.save(directory + 'training_labels.npy', labels)
