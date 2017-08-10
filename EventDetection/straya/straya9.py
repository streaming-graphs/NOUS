# For attack each record:
# create a key: (srcIP, destIP, attack-cat, attack-sub-cat)
# set value: [the record] (or part of the record that you want to keep)
# if key exists, append the record to the list, else: create new key
# For each key in the dict, list of values form a document = no. of keys = no. of docs

# For each normal record:
# Do the same process but the key would be just the IPs
import cPickle as cP
import numpy as np


directory = 'data/straya/'
fn = 'attack'
ext = '.csv'

# attack = {}
# with open(directory + fn + ext, 'r') as f:
#     for line in f:
#         line_list = [x.strip() for x in line.split(',')]
#         if line_list[-3] == 'Backdoors':
#             line_list[-3] = 'Backdoor'
#         src, dest, att_cat, att_sub_cat = line_list[0], line_list[2], line_list[-3], line_list[-2]
#         key = (src, dest, att_cat, att_sub_cat)
#         temp = attack.get(key, [])
#         temp.append(','.join(line_list))  # Change what you want to store here
#         attack[key] = temp
#
# fn = 'normal'
# normal = {}
# with open(directory + fn + ext, 'r') as f:
#     for line in f:
#         line_list = [x.strip() for x in line.split(',')]
#         src, dest = line_list[0], line_list[2]
#         key = (src, dest)
#         temp = normal.get(key, [])
#         temp.append(','.join(line_list))  # Change what you want to store here
#         normal[key] = temp
#
# cf = open(directory + 'attack.pkl', 'w')
# cP.dump(attack, cf)
# cf.close()
# cf2 = open(directory + 'normal.pkl', 'w')
# cP.dump(normal, cf2)
# cf2.close()

# att_vals = [len(x) for x in attack.values()]
# print [x for x in att_vals if x >= 50]
# print max(att_vals), min(att_vals)
# norm_vals = [len(x) for x in normal.values()]
# print [x for x in norm_vals if x >= 50]
# print max(norm_vals), min(norm_vals)

with open(directory + 'attack.pkl', 'r') as af:
    attack = cP.load(af)
with open(directory + 'normal.pkl', 'r') as nf:
    normal = cP.load(nf)

lookup = {}
olf = open(directory + 'ordered_list.txt', 'r')
for i, line in enumerate(olf):
    lookup[line.strip()] = i
olf.close()
count = len(lookup) + 1

attack_docs = []
for val in attack.values():
    if len(val) > 50:
        for i, rowstr in enumerate(val):
            row = rowstr.split(',')
            newrow = [row[0]] + [row[4]] + [row[2]]
            newrow += [row[5]] + [row[6]] + [row[13]] + [row[24]] + [row[25]] + [row[32]]
            newrow = [lookup[x] for x in newrow]
            val[i] = newrow
        docs = len(val)/50
        for d in range(0, docs):
            attack_docs.append(val[50*d:50*(d+1)])

attack_docs = np.array(attack_docs)
np.save(directory + 'attack_ip_pairs_big.npy', attack_docs)
print attack_docs.shape

normal_docs = []
for val in normal.values():
    if len(val) > 50:
        for i, rowstr in enumerate(val):
            row = rowstr.split(',')
            newrow = [row[0]] + [row[4]] + [row[2]]
            newrow += [row[5]] + [row[6]] + [row[13]] + [row[24]] + [row[25]] + [row[32]]
            for j, nr in enumerate(newrow):
                if nr in lookup:
                    newrow[j] = lookup[nr]
                else:
                    lookup[nr] = count
                    newrow[j] = count
                    count += 1
            val[i] = newrow
        docs = len(val) / 50
        for d in range(0, docs):
            normal_docs.append(val[50*d:50*(d+1)])

normal_docs = np.array(normal_docs)
np.save(directory + 'normal_ip_pairs_big.npy', normal_docs)
print normal_docs.shape
print count
