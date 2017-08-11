# This file reads the provided raw dataset and formats it (removing white spaces, combining same words that are
# differently spelled etc.) It adds the attack sub-cat to the dataset from the ground truth file

directory = 'data/straya/'
filegt = 'NUSW-NB15_GT'
ext = '.csv'
filen = 'UNSW-NB15_'

gtdict = {}
with open(directory + filegt + ext, 'r') as gtf:
    gtf.seek(151)  # Skip the header line
    count = 0
    for line in gtf:
        count += 1
        linelist = [x.strip() for x in line.split(',')]
        if len(linelist) > 5:
            # key = (stime, endtime, attack cat, srcIP)
            # val = attack sub_cat
            gtdict[(linelist[0], linelist[1], linelist[2], linelist[5])] = linelist[3]

wf1name = directory + 'attack.csv'
wf1 = open(wf1name, 'w')

wf2name = directory + 'normal.csv'
wf2 = open(wf2name, 'w')

for u in range(1, 5):
    with open(directory + filen + str(u) + ext, 'r') as f:
        for line in f:
            linelist = [x.strip() for x in line.split(',')]
            # Attack records:
            if linelist[-2]:
                if linelist[-2] == 'Backdoors':
                    linelist[-2] = 'Backdoor'
                # Use timestamps, attack name and scrIP to identify the relevant
                # record from the ground truth file
                key = (linelist[28], linelist[29], linelist[-2], linelist[0])
                sub_cat = gtdict.get(key, 'misc')
                newline = linelist[:-1] + [sub_cat] + linelist[-1:]
                wf1.write(','.join(newline))
            # Normal Records:
            else:
                linelist[-2] = 'Normal'
                newline = linelist[:-1] + ['Normal'] + linelist[-1:]
                wf2.write(','.join(newline))
wf1.close()
wf2.close()
