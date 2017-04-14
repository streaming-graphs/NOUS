#!/usr/bin/python 
import sys
inp = sys.argv[1]
out = inp + ".gdf"
bip_nodes = dict()
bip_edges = set()

f = open(inp)
for line in f:
  tokens = line.split('\t')
  node_list = tokens[0].split('=>')
  domains = node_list[0].split(',')
  keywords = node_list[1].split(',')
  for d in domains:
    bip_nodes[d.upper()] = 'area'
  for k in keywords:
    bip_nodes[k] = 'keyword'
  for d in domains:
    for k in keywords:
      bip_edges.add(d.upper() + ',' + k + ',hasTopic\n')
f.close()

fout = open(out, 'w')
fout.write('nodedef> name VARCHAR,label VARCHAR, ppavm VARCHAR\n') 
for k,v in bip_nodes.items():
  fout.write(k + ',' + k + ',' + v + '\n')
fout.write('edgedef>node1 VARCHAR,node2 VARCHAR,relationship VARCHAR\n')
for e in bip_edges:
  fout.write(e)
fout.close()
