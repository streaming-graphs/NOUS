directory = 'data/straya/'
fn = 'attack'
ext = '.csv'

lookup = {}
olf = open(directory + 'ordered_list' + '.txt', 'r')
for i, line in enumerate(olf):
    lookup[line.strip()] = str(i)

# print lookup['Backdoor'], lookup['Backdoors']
lookup['Backdoors'] = str(183)
# exit(0)

wf = open(directory + fn + '_nums' + ext, 'w')
with open(directory + fn + ext, 'r') as f:
    for line in f:
        newline = [x.strip() for x in line.split(',')]
        writeline = [lookup[x] for x in newline]
        wf.write(','.join(writeline))
        wf.write('\n')
wf.close()
