# Read all_samples
# Read vertdict
d = {}
with open('data/vertexDictionary_rels.out') as f:
    for line in f:
        key, val = line.split()
        d[key] = val

directory = 'data/wordnet/versions/'
subs = 5
pl = 7
cat = 'neg'
wf = open(directory + cat + '_samples_nums_' + str(subs) + 'f_pl' + str(pl) + '.txt', 'wb')

with open(directory + cat + '_samples_' + str(subs) + 'f_pl' + str(pl) + '.txt') as f:
    for line in f:
        words = line.split()
        nums = ''
        for word in words[:-1]:
            nums += d[word] + ' '
        if words:
            nums += words[-1]
            wf.write(nums)
            wf.write('\n')
        else:
            break

wf.close()
