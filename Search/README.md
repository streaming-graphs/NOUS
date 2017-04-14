# NOUS Search: Implements following algorithms for graph search 
1) Path Finder : Finding paths between pair of entities. The Paths found are characterized by topic coherence and specificity. (user configurable)

## 2. Build and Execute Hello World Program(s):
### 2.1 Prerequisites
* Java 1.7+
* Maven
* Apache Spark 2.1 OR above
* HDFS File System (Optional)

### 2.2 Build
 Clone github repository 

` clone https://github.com/streaming-graphs/NOUS.git NOUS `
 cd [Repo_Home]/Search
 mvn package
 ```
Here `[Repo_Home]` is the path to your cloned directory `NOUS`. 

### 2.3 Run Hello World
[SPARK_HOME]/bin/spark-submit --verbose --jars "[PATH_TO_JAR]" --master [SPARK_MASTER]  --class "gov.pnnl.nous.Main" "[PATH_TO_JAR]"  <graphPath> <entityPairsFile> <outputDir> <topicsFile> <maxPathLength> <maxDegree> <topcCoherenceThreshold(Optional)>

<graphPath> : Path to a triple file (or to directory containing triples files). The vertex ids and edge labels are expected to be mapped to integer format. Each line on triple file 
contains srcId, edgeLabelId and dstId separetd by tab:
<srcId>	<edgeLabelId>	<dstId>

<entityPairsFile> : Path to a file containing entity pair per line, separated by tab 
<entityId1>	<entityId2>
<entityId3>	<entityId4>

<outputDir> : Path to an outputDir. (search code will create an output file for each entity pair in thie directory)

<maxPathLength> : Integer specifyting maximum number of iterations for paths (#edges in path)
1 => direct paths
2 => upto 1 hop and so on..
default value = 5

Optional parameters:
<maxDegree> : Integer specifying "ignore nodes with degree > maxDegree " while finding paths. 
Specify maxDegree = -1 for using all nodes 
default value = -1

<topicsFile> : Topic/Embedding matrix for each vertex in the graph, vector values are separted by ","
<nodeid>	<topic vector>
<nodeid>	<topic vector>

<topicCoherenceThreshold(Optional)> : Ignores path if any consecutive vertices have topic similarity < threshold 
default value = 0.001

Example
spark-submit --jars target/uber-graph-search-nous-0.1.jar --master "local" --class "gov.pnnl.nous.Main" target/uber-graph-search-nous-0.1.jar  examples/sample_yago.ttl examples/sample_yago_entity.ttl  outputDir examples/sample_yago_topics	5  200 0.01
