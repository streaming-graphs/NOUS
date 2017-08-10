import numpy as np


directory = 'data/straya/attacks/'
attack = []
for i in range(1, 10):
    for j in range(1, 52):
        doc = []
        try:
            f = open(directory + str(i) + '/' + 'Output_' + str(j) + '.csv', 'r')
            content = f.readlines()
            if len(content) < 50:
                continue
        except IOError:
            break
        for line in content[:50]:
            linelist = [int(x) for x in line.split(',')]
            newline = [linelist[0]] + [linelist[4]] + [linelist[2]]
            newline += [linelist[5]] + [linelist[6]] + [linelist[13]] + [linelist[24]] + [linelist[25]] + [linelist[32]]
            # newline += [1]  # Label
            doc.append(newline)
        attack.append(doc)

attack = np.array(attack)
np.save(directory + 'attack.npy', attack)
# print attack.shape

# For 1-9:
# For 1-51:
# if no file, ignore. Else:
# if len(file) < 50, ignore
# otherwise, read 50 lines, take those 9 features and add 1 as a label and append it to the attack numpy matrix
# 91? x 50 x 10 size

# Normal:
# lookup using the ol=ordered_list file and if the word isn't there, use count+1 where count begins at len(ol)
# Choose the same 9 features and add 0 as the label
# 150? x 50 x 10 size

directory = 'data/straya/'
lookup = {}
olf = open(directory + 'ordered_list.txt', 'r')
for i, line in enumerate(olf):
    lookup[line.strip()] = i
lookup['Backdoors'] = 183
olf.close()
count = len(lookup) + 1
normalf = open(directory + 'normal.csv', 'r')
normalc = normalf.readlines()[:21000]
normal = []
doc = []

for line in normalc:
    linelist = []
    for x in line.split(','):
        if x.strip() in lookup:
            linelist.append(lookup[x.strip()])
        else:
            lookup[x.strip()] = count + 1
            count += 1
            linelist.append(lookup[x.strip()])
    newline = [linelist[0]] + [linelist[4]] + [linelist[2]]
    newline += [linelist[5]] + [linelist[6]] + [linelist[13]] + [linelist[24]] + [linelist[25]] + [linelist[32]]
    # newline += [0]  # Label
    doc.append(newline)
    if len(doc) == 50:
        normal.append(doc)
        doc = []
normalf.close()

normal = np.array(normal)
np.save(directory + 'normal.npy', normal)
# print normal.shape
