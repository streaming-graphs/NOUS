import numpy as np


d = {}
with open('vertexDictionary_rels.out') as f:
    for line in f:
        val, key = line.split()
        d[key] = val

paths_preds = np.load('wrongly_classified.npy')
paths = paths_preds[:, :-1].astype('int32')
preds = paths_preds[:, -1]

path_str = ''
for j, path in enumerate(paths):
    if preds[j] >= 0.75 or preds[j] <= 0.25:
        for i, num in enumerate(path):
            path_str += d[str(num)] + ' '
        path_str += '%.2f' % (preds[j]) + str('\n')

with open('wrongly_classified_hilo.txt', 'wb') as f:
    f.write(path_str)
