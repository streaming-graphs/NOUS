import numpy as np
import tensorflow as tf
import pickle as pk
from math import sqrt
from pyspark import SparkContext, SparkConf
from pyspark.mllib.clustering import KMeans, KMeansModel
#from scipy.cluster.vq import kmeans,vq

numClusters = 2
numIter = 10
modelDir = "KMeansModel." + str(numClusters) + "." + str(numIter) 
conf = SparkConf().setAppName("kmeans")
sc = SparkContext(conf=conf)
#data = sc.textFile("/people/d3x771/projects/knowledgeGraph/PathSearchBenchmark/step7/vecEmbedYago.txt")
#parsedData = data.map(lambda line: np.array([float(x) for x in line.split(' ')]))
# Build the model (cluster the data)
#clusters = KMeans.train(parsedData, numClusters, maxIterations=numIter,
#                        runs=10, initializationMode="random")

# Evaluate clustering by computing Within Set Sum of Squared Errors
def error(point):
    center = clusters.centers[clusters.predict(point)]
    return sqrt(sum([x**2 for x in (point - center)]))

#WSSSE = parsedData.map(lambda point: error(point)).reduce(lambda x, y: x+y)
#print("Within Set Sum of Squared Error = " + str(WSSSE))

# Save and load model
#clusters.save(sc, modelDir)
sameModel = KMeansModel.load(sc, modelDir)
testLines = sc.textFile("/people/d3x771/projects/knowledgeGraph/PathSearchBenchmark/step7/vecEmbedWithIdYago.txt")
idsWithFeatures = testLines.map(lambda line: line.strip().split(";;"))
ids = idsWithFeatures.map(lambda v : v[0])
testData = idsWithFeatures.map(lambda v : np.array([float(x) for x in v[1]]))
clusterIds = testData.map(lambda v : sameModel.predict(v))
result = ids.zip(clusterIds)
result.saveAsTextFile(sc, "clusterIds")
