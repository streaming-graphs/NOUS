#!/usr/bin/python
f = open('pairs.csv')
fout = open('pairs.tsv', 'w')
for line in f:
  outline = line.replace(',', '\t')
  fout.write(outline)
f.close()
fout.close()
