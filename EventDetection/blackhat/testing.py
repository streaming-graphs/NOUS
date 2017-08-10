import numpy
import sys


# Directoy that contains the data and pattern files
directory = sys.argv[1]

# Reading pattern file
name2num = {'Dev': 1001, 'Admin': 1002, 'Mgmt': 1003, 'IT': 1004, 'HR': 1005,
            'SkypeEmail': 1006, 'CloudServices': 1007, 'Computing': 1008, 'Infra': 1009, 'Ext': 1010}

num2name = {}
for k, v in name2num.items():
    num2name[v] = k

# Get test data from pattern file
pfname = directory + 'random_graph.pattern.txt'
X_test = numpy.load(directory + 'xtest.npy')
tpredicts = numpy.load(directory + 'tpreds.npy')


# Output English
def get_msg(sc, ec):
    return ' '.join([num2name[nts[0]] + '_' + str(nts[1]) + ' ' + num2name[nts[2]] + '_' + str(nts[3])
                     for xt, nts in enumerate(X_test[sc:ec, :]) if tpredicts[sc+xt] == 1])


new_lines = ''
with open(pfname, 'r') as wf:
    lines = wf.readlines()
    start_ct = 0
    end_ct = 0
    for i, line in enumerate(lines):
        line_list = line.split(',')
        lll = len(line_list)
        if lll < 2:
            continue
        if lll <= 3:
            end_ct += 1
            new_lines += line
        if lll > 3:
            end_ct += 1
            if all(tpredicts[start_ct:end_ct]):
                msg = '. All edges are abnormal'
            elif all([x == 0 for x in tpredicts[start_ct:end_ct]]):
                msg = ''
            else:
                msg = '. The edge(s) ' + get_msg(start_ct, end_ct) + ' is/are abnormal'
            start_ct = end_ct
            line_list[-1] = line_list[-1].strip()
            new_lines += ', '.join(line_list) + msg + '\n'

    with open(directory + 'random_graph.pattern.decisions.txt', 'w') as wwf:
        for line in new_lines:
            wwf.write(line)
