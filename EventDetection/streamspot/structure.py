# Get some properties of different types of graphs in the streamspot dataset


import matplotlib.pyplot as plt
import networkx as nx
import numpy as np


def save_graph(graph, file_name):
    # initialze Figure
    plt.figure(num=None, figsize=(20, 20), dpi=80)
    plt.axis('off')
    fig = plt.figure(1)
    pos = nx.spring_layout(graph)
    nx.draw_networkx_nodes(graph, pos)
    nx.draw_networkx_edges(graph, pos)
    nx.draw_networkx_labels(graph, pos)

    cut = 1.00
    xmax = cut * max(xx for xx, yy in pos.values())
    ymax = cut * max(yy for xx, yy in pos.values())
    plt.xlim(0, xmax)
    plt.ylim(0, ymax)

    plt.savefig(file_name)
    plt.close()
    del fig

directory = 'data/streamspot/data/all_divs/'
# fname = ''
fname2 = '.pdf'
dg = nx.DiGraph()

for i in [0, 150, 250, 350, 450, 550]:
    with open(directory + str(i), 'rb') as f:
        for line in f:
            edge = line.split()[:-1]
            # edge_str = ''.join([edge[1], edge[4], edge[3]])
            # print edge_str
            # exit(0)
            # if edge_str in ['awe', 'auc', 'bGc', 'aGe', 'avc', 'aHe']:
            #     continue
            dg.add_node(edge[0], type=edge[1])
            dg.add_node(edge[2], type=edge[3])
            dg.add_edge(edge[0], edge[2], type=edge[-1])
            # dg.add_edge(edge[2], edge[0], type='**' + edge[-1])
    # save_graph(dg.subgraph(dg.neighbors('2287')), str(i) + 'sg' + fname2)
    print nx.is_strongly_connected(dg), nx.is_weakly_connected(dg)
    print nx.number_strongly_connected_components(dg), nx.number_weakly_connected_components(dg)
    print len(dg.nodes())
