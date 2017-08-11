# For each graph ID, create a graph - there will be 600 graphs in total
# Do this once and store the graphs to files
# Each node will have an attribute type and a list of neighbors. Each edge will have a type too
# There will be 500 positive graphs and 100 attack graphs (IDs: 300-399)
import networkx as nx
import pickle as pkl
# from pushbullet import Pushbullet

dg = nx.DiGraph()
directory = 'data/streamspot/data/all_divs/'
# pb = Pushbullet('API Key')

for gid in range(0, 600):
    # if gid % 50 == 0:
    #     pb.push_note(str(gid) + ' graphs completed', ' ')
    fname = directory + str(gid)
    with open(fname, 'rb') as f:
        for line in f:
            edge = line.split()[:-1]
            edge_str = ''.join([edge[1], edge[4], edge[3]])
            # print edge_str
            # exit(0)
            if edge_str in ['awe', 'auc', 'bGc', 'aGe', 'avc', 'aHe']:
                continue
            dg.add_node(edge[0], type=edge[1])
            dg.add_node(edge[2], type=edge[3])
            dg.add_edge(edge[0], edge[2], type=edge[-1])
            dg.add_edge(edge[2], edge[0], type='**' + edge[-1])

    pkl.dump(dg, open(fname + 'dstar_thin.txt', 'w'))
    dg.clear()
# dg1 = pkl.load(open('data/streamspot/data/test.txt'))

# print dg.nodes(data=True)
# print dg.edges(data=True)
