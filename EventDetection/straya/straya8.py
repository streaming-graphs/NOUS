# Attempting to visualize the UNSW graph.


import networkx as nx
import matplotlib.pyplot as plt


directory = 'data/straya/'
fn = 'attack'
ext = '.csv'

# Create graph
G = nx.DiGraph()
with open(directory + fn + ext, 'r') as f:
    for line in f:
        line_list = [x.strip() for x in line.split(',')]
        src, dest = line_list[0], line_list[2]
        if G.has_edge(src, dest):
            w = G.get_edge_data(src, dest)['weight']
            G.add_edge(src, dest, color='r', weight=w+1)
        else:
            G.add_edge(src, dest, color='r', weight=1)

list_of_nodes = set(G.nodes())
# Print how many times each edge has appeared
for edge in G.edges(data=True):
    print edge[0] + '--' + str(edge[2]['weight']) + '-->' + edge[1]

count = 0

fn = 'normal'
with open(directory + fn + ext, 'r') as f:
    for line in f:
        line_list = [x.strip() for x in line.split(',')]
        src, dest = line_list[0], line_list[2]
        if src in list_of_nodes and dest in list_of_nodes:
            if G.has_edge(src, dest):
                c = G.get_edge_data(src, dest)['color']
                w = G.get_edge_data(src, dest)['weight']
                if c == 'g':
                    G.add_edge(src, dest, color='g', weight=w+1)
                elif c == 'r':
                    G.add_edge(src, dest, color='r', weight=w+1)
                else:
                    count += 1
                    G.add_edge(src, dest, color='y', weight=w+1)
            else:
                G.add_edge(src, dest, color='g', weight=1)

print count
print '----------'

# Print how many times each edge has appeared
for edge in G.edges(data=True):
    print edge[0] + '--' + str(edge[2]['weight']) + '-->' + edge[1]

# Draw graph
pos = nx.circular_layout(G)

edges = G.edges()
colors = [G[u][v]['color'] for u, v in edges]
weights = [G[u][v]['weight'] for u, v in edges]
mw = max(weights)
weights = [x*1.0/mw for x in weights]

nx.draw(G, pos, edges=edges, edge_color=colors, width=weights, with_labels=True)
plt.savefig(directory + 'UNSW_graph.pdf')
