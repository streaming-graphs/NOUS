#!/usr/bin/python
f = open('scopus.tsv')
papers = set()
for line in f:
  tokens = line.strip().split('\t')
  if len(tokens) != 4:
    continue
  if tokens[2] == 'PSYC':
    papers.add(tokens[0])
f.close()
print('Found ' + str(len(papers)) + ' papers')
f = open('scopus.tsv')
fout = open('psyc.tsv', 'w')
for line in f:
  tokens = line.strip().split('\t')
  if tokens[0] in papers:
    fout.write(line)
fout.close()
f.close()
