directory = 'data/straya/'
fn = 'attack'
ext = '.csv'

hist = {}
with open(directory + fn + ext, 'r') as f:
    for line in f:
        newline = [x.strip() for x in line.split(',')]
        for word in newline:
            count = hist.get(word, 0)
            hist[word] = count + 1

fn = 'normal'
with open(directory + fn + ext, 'r') as f:
    for line in f:
        newline = [x.strip() for x in line.split(',')]
        for word in newline:
            count = hist.get(word, 0)
            hist[word] = count + 1

sorted_list = [pair[0] for pair in sorted(hist.items(), key=lambda item: item[1], reverse=True)]

with open(directory + 'ordered_list_new.txt', 'w') as wf:
    for word in sorted_list:
        wf.write(word)
        wf.write('\n')
wf.close()
