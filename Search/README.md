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

<graphPath> : Path to a triple file (separated by tab)
<srcId>	<edgeLabel>	<dstId>

<entityPairsFile> : Path to a file containing entity pair per line, separated by tab 
<entity_name_1>	<entity_name_2>
<entity_name_3>	<entity_name_4>

<outputDir> : Path to an outputDir. (search code will create an output file for each entity pair in thie directory)

<topicsFile> : Topic/Embedding matrix for each vertex in the graph
<nodeid>	<topic vector>
<nodeid>	<topic vector>

<maxPathLength> : Integer specifyting maximum number of hops for paths 
0 => direct paths
1 => upto 1 hop and so on..
default value = 5

<maxDegree> : Integer specifying "ignore nodes with degree > maxDegree " while finding paths, specify maxDegree = -1 for using all nodes 
default value = -1

<topicCoherenceThreshold(Optional)> : Ignores path if any consecutive vertices have topic similarity < threshold 
default value = 0.001

Example
spark-submit --jars target/uber-graph-search-nous-0.1.jar --master "local" --class "gov.pnnl.nous.Main" target/uber-graph-search-nous-0.1.jar  examples/sample_yago.ttl examples/sample_yago_entity.ttl  outputDir examples/sample_yago_topics	5  200 0.01
