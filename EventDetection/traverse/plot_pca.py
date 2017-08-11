# Pre-processing
# Load pos samples
# Load neg samples
# Create 3D matrix for each such that each page is a sample
# Each page: ith row represents the embedding of the ith element in the sample

# PCA on each page -> get an (x,y) -> plot it
# Different colors for pos and neg samples
import numpy as np
from sklearn.decomposition import PCA
import matplotlib.pyplot as plt
import random


# X = np.array([[-1, -1], [-2, -1], [-3, -2], [1, 1], [2, 1], [3, 2]])
# print X
# pca = PCA(n_components=2)
# pca.fit(X)
# print pca.components_
# plt.plot(X[:, 0], X[:, 1], '.')
# plt.plot(pca.components_[1], pca.components_[0])
# plt.savefig('pca_plot.pdf')
# plt.close()
#
# Save embeddings as numpy file from walk file
# embs = []
# with open(embname) as f:
#     for line in f:
#         if len(line) <= 10:
#             temp = line.strip().split()
#             embs = np.empty([38545, int(temp[1])])
#         else:
#             temp = line.strip().split()
#             idc = int(temp[0])
#             temp = [float(x) for x in temp[1:]]
#             embs[idc-1] = temp
# np.save('embeds_64.npy', embs)

flips = 4
pl = 6
directory = 'data/wordnet/versions/'
embname = 'data/wordnet/embeds/embeds_3f_pl5.npy'
embs = np.load(embname)

pos_samples = np.genfromtxt(directory + 'pos_samples_nums_' + str(flips) + 'f_pl' + str(pl)
                            + '.txt')[:, :-1].astype('int32')
neg_samples = np.genfromtxt(directory + 'neg_samples_nums_' + str(flips) + 'f_pl' + str(pl)
                            + '.txt')[:, :-1].astype('int32')
# print pos_samples.shape
# print neg_samples.shape
pos_mat = np.empty([pos_samples.shape[0], pos_samples.shape[1], len(embs[0])])
neg_mat = np.empty([neg_samples.shape[0], neg_samples.shape[1], len(embs[0])])
pca = PCA(n_components=2)

idc = random.sample(range(0, 60000), 500)
count = 0
for sample in pos_samples[idc, :]:
    count += 1
    temp = []
    for word in sample:
        temp.append(embs[int(word)])
    pca.fit(temp)
    comps = pca.components_
    # plt.plot(comps[0, :], comps[1, :], 'g.')
    x = np.median(comps[0, :])
    y = np.median(comps[1, :])
    plt.plot(x, y, 'g.', alpha=0.5)
    # plt.annotate(str(count), xy=(x, y), xycoords='data', xytext=(-5, 5), textcoords='offset points')
    pos_mat[0] = temp

count = 0
for sample in neg_samples[idc, :]:
    count += 1
    temp = []
    for word in sample:
        temp.append(embs[int(word)])
    pca.fit(temp)
    comps = pca.components_
    # plt.plot(comps[0, :], comps[1, :], 'r.')
    x = np.median(comps[0, :])
    y = np.median(comps[1, :])
    plt.plot(x, y, 'r.', alpha=0.5)
    # plt.annotate(str(count), xy=(x, y), xycoords='data', xytext=(-5, 5), textcoords='offset points')
    neg_mat[0] = temp

plt.savefig('pca_plot_med_rnd_4f_pl6.pdf')

# for sample in pos_samples[:1000, :]:
#     # temp = []
#     # for word in sample:
#     #     temp.append(embs[int(word)])
#     pca.fit(sample)
#     comps = pca.components_
#     # plt.plot(comps[0, :], comps[1, :], 'g.')
#     plt.plot(comps[0, 0], comps[0, 1], 'g.', alpha=0.5)
#     # pos_mat[0] = temp
#
# for sample in neg_samples[:1000, :]:
#     # temp = []
#     # for word in sample:
#     #     temp.append(embs[int(word)])
#     pca.fit(sample)
#     comps = pca.components_
#     # plt.plot(comps[0, :], comps[1, :], 'r.')
#     plt.plot(comps[0, 0], comps[0, 1], 'r.', alpha=0.5)
#     # neg_mat[0] = temp
#
# plt.savefig('pca_plot_noembed_persample.pdf')

# pca.fit(pos_samples)
# comps = pca.components_
# plt.plot(comps[0, :], comps[1, :], 'g.', alpha=0.5)
#
# pca.fit(neg_samples)
# comps = pca.components_
# plt.plot(comps[0, :], comps[1, :], 'r.', alpha=0.5)
#
# plt.savefig('pca_plot_noembed.pdf')
