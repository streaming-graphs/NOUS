# From each graph, make a random walk of user-specified path length
# Positive paths will be taken from the 500 benign ones and negative graphs from the 600 attack ones
# Run LSTM as usual
import numpy as np
import pickle as pkl
from pushbullet import Pushbullet
import sys


directory = 'data/streamspot/data/all_divs/'
pb = Pushbullet('API key')
pl = int(sys.argv[1])


def rwss(graph, start, length):
    # Take in a graph ID and a path length pl
    # Return a string of length 2pl with pl number of nodes and pl-1 number of edges and a label 0/1
    # This function takes in the node type and uses that in the path string
    sampled_path = []
    current = start
    sampled_path.append(g.node[current]['type'])
    cur_edges = graph.edges([current], data=True)
    for k in range(length):
        r = np.random.choice(range(len(cur_edges)))
        sampled_path.append(cur_edges[r][-1]['type'])
        current = cur_edges[r][1]
        sampled_path.append(g.node[current]['type'])
        cur_edges = graph.edges(cur_edges[r][1], data=True)
    return sampled_path


# def rwss_id(graph, start, length):
#     # Take in a graph ID and a path length pl
#     # Return a string of length 2pl with pl number of nodes and pl-1 number of edges and a label 0/1
#     # This function takes in the node ID and uses that in the path string
#     # We found that IDs are not the same across graphs. So using this is not very meaningful
#     sampled_path = []
#     current = start
#     sampled_path.append(current)
#     cur_edges = graph.edges([current], data=True)
#     for k in range(length):
#         r = np.random.choice(range(len(cur_edges)))
#         sampled_path.append(cur_edges[r][-1]['type'])
#         current = cur_edges[r][1]
#         sampled_path.append(current)
#         cur_edges = graph.edges(cur_edges[r][1], data=True)
#     return sampled_path


if __name__ == '__main__':
    # Create positive and negative random paths
    pos_gid = np.hstack([np.arange(0, 300, 1), np.arange(400, 600, 1)])
    neg_gid = np.arange(300, 400, 1)

    pos_samples = []
    neg_samples = []

    pb.push_note('Sample Generation Started', ' ')

    for gid in pos_gid:
        gname = directory + str(gid) + 'dstar_thin.txt'
        g = pkl.load(open(gname))
        nodes = np.random.choice(g.nodes(), 2000, False)
        for node in nodes:
            path = rwss(g, node, pl)
            path = [str(ord(x)) if len(x) == 1 else str(ord(x[-1]) + 100) for x in path]
            # for x in range(1, len(path), 2):
            #     if len(path[x]) == 1:
            #         path[x] = str(ord(path[x]))
            #     else:
            #         path[x] = str(ord(path[x][-1]) + 100)
            path.append('0')
            pos_samples.append(path)

    pb.push_note('Positive Sample Generation Ended', ' ')

    for gid in neg_gid:
        gname = directory + str(gid) + 'dstar_thin.txt'
        g = pkl.load(open(gname))
        # nodes = np.random.choice(g.nodes(), 1000, False)
        nodes = g.nodes()
        for node in nodes:
            path = rwss(g, node, pl)
            path = [str(ord(x)) if len(x) == 1 else str(ord(x[-1]) + 100) for x in path]
            # for x in range(1, len(path), 2):
            #     if len(path[x]) == 1:
            #         path[x] = str(ord(path[x]))
            #     else:
            #         path[x] = str(ord(path[x][-1]) + 100)
            path.append('1')
            neg_samples.append(path)

    all_samples = pos_samples + neg_samples
    np.random.shuffle(all_samples)

    # Write to file
    dirdotdot = 'data/streamspot/data/'
    wfname = 'all_samples_' + str(pl) + 'star_big_thin.txt'
    with open(dirdotdot + wfname, 'w') as wf:
        for sample in all_samples:
            wf.write(' '.join(sample))
            wf.write('\n')

    pb.push_note('File written', ' ')
