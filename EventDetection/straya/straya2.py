# This file groups records with same attack, attack-sub-cat pairs together
# The sorted file can be split into multiple files using the bash script lines
# provided in splits.sh


import csv


directory = 'data/straya/'
fn = 'attack'
ext = '.csv'

hist = {}
new_hist = {}
count = 0
with open(directory + fn + ext, 'r') as f:
    for line in f:
        linelist = line.split(',')
        key = (linelist[-3], linelist[-2])
        count = hist.get(key, 0) + 1
        hist[key] = count

f = directory + fn + ext
reader = csv.reader(open(f))
sorted_attack = sorted(reader, key=lambda row: row[-3], reverse=True)

wfn = directory + fn + 'new' + ext
wf = open(wfn, 'w')
with open(directory + fn + ext, 'r') as nf:
    for line in nf:
        linelist = line.split(',')
        newline = [linelist[0]] + [linelist[4]] + [linelist[2]]
        newline += [linelist[5]] + [linelist[6]] + [linelist[13]] + [linelist[24]] + [linelist[25]] + [linelist[32]]
        newline += [linelist[-1]]
        newline = ','.join(newline)
        wf.write(newline)

wf.close()
