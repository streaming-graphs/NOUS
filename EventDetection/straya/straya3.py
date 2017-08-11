# Creata a list of words found in attack and normal files and rank them based on their frequency
# There are ~680,000 words in the attack file, over 10million in the normal file.
# Since we will be choosing only a subset of the 50 features in downstream code, it is wise to just create this
# list for the attack file and just use a counter (explained in detail in relevant file) for the normal records.


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
