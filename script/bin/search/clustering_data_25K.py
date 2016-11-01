# This version implements k-means clustering algorithm on the feature set of the vertices 

import numpy as np
import pandas as pd
import pickle as pk
from scipy.cluster.vq import kmeans,vq

fname = 'data/vertexWord2Vec/vertexWord2Vec.hd5'
store = pd.HDFStore(fname, 'r')
df = store['vertexWord2Vec']
embed_matrix = df.values

print np.shape(embed_matrix)
print "Hello! Input Data Files have been loaded..."	

# Feature vectors are being concatenated
fname = 'data/vertexDegree.txt'
vertdeg = []
[n_rows,n_cols] = np.shape(embed_matrix)
print n_rows, n_cols
features_vec = np.zeros([n_rows,n_cols+1])
count = 0
for line in open(fname):     
    features_vec[count,0] = np.float(line)
    features_vec[count,1:] = embed_matrix[count,:]
    count += 1
del embed_matrix
print np.shape(features_vec)
print "Hello! Feature vector has been concatenated..."	

# Combined feature vector is whitened to have unit variance along each direction
sum_cols = np.sum(features_vec*features_vec,axis=0)
avg_cols = np.sqrt(sum_cols/n_rows)
avg_cols = np.reshape(avg_cols,(1,n_cols+1))
features_vec /= avg_cols

# Implementing k-means clustering algorithm with in-built scipy function
# clustering with k (=10) clusters
num_clusters = 25000
cluster_centroid,_ = kmeans(features_vec,num_clusters)
# assign each sample to a cluster
idx_array,_ = vq(features_vec,cluster_centroid)

# Writing the cluster-ids and cluster centorid vectors in output files
f = open("idx_cluster_025K.txt", "w")
pk.dump(idx_array, f)
f.close()

f = open("Cluster_Centroids_025K.txt", "w")
pk.dump(cluster_centroid, f)
f.close()


## Re-computing the cluster centroids for each data point or each vertex or each node
#num_cluster_objs = np.zeros((num_clusters,1))
#cluster_centroid = np.zeros((num_clusters, n_cols+1))
#for ii in xrange(num_clusters):
    #num_cluster_objs[ii,] = sum(idx_array==ii)
    #temp_id = idx_array==ii
    #cluster_centroid[ii,:] = np.sum(features_vec*temp_id, axis = 0)/num_cluster_objs[ii,]  

#print "Number of entities in each cluster..."	
#print num_cluster_objs


