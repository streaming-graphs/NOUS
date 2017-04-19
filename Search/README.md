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
```
` clone https://github.com/streaming-graphs/NOUS.git NOUS `
 cd [Repo_Home]/Search
 mvn package
 ```
Here `[Repo_Home]` is the path to your cloned directory `NOUS`. 

### 2.3 Run Hello World
[SPARK_HOME]/bin/spark-submit --verbose --jars "[PATH_TO_JAR]" --master [SPARK_MASTER]  --class "gov.pnnl.nous.Main" "[PATH_TO_JAR]"  <graphPath> <entityPairsFile> <outputDir> <maxPathLength> <optional:numEntitiesPerGraphLine> <optional:maxDegree> <optional:topicsFile> <optional: topicCoherenceThreshold(Optional)>

<graphPath> : Path to a triples file(or directory containing triples files). The vertex ids and edge labels are expected to be mapped to integer format. Each line on triple file 
contains srcId, edgeLabelId and dstId separetd by tab:
<srcId>	<edgeLabelId>	<dstId>

<entityPairsFile> : Path to a file containing entity pair (integer format) per line, separated by tab 
<entityId1>	<entityId2>
<entityId3>	<entityId4>

<outputDir> : Path to an outputDir. (search code will create an output file for each entity pair in thie directory)

<maxPathLength> : Integer specifyting maximum number of iterations for paths (#edges in path)
1 => direct paths
2 => upto 1 hop and so on..

Optional parameters:
<numEntitiesPerGraphLine> : Optionally NOUS-Search can also read other graph formats. Use 
* numEntitiesPerGraphLine=2 for format: <srcid> <dstid>
* numEntitiesPerGraphLine=4 for format:	<srcid> <edgeid> <dstid> <timestamp>
default value = 3

<maxDegree> : Integer specifying "ignore nodes with degree > maxDegree " while finding paths. 
Specify maxDegree = -1 for using all nodes 
default value = -1

<topicsFile> : Topic/Embedding matrix for each vertex in the graph, vector values are separted by ","
<nodeid>	<topic vector>
<nodeid>	<topic vector>
default value = "NONE"

<topicCoherenceThreshold(Optional)> : Ignores path if any consecutive vertices have topic similarity < threshold 
default value = -1

Example: To run example data, finding all paths upto length 3:
spark-submit --jars target/uber-graph-search-nous-0.1.jar --master "local" --class "gov.pnnl.nous.Main" target/uber-graph-search-nous-0.1.jar  ./examples/yago/yagoIntSample.ttl ./examples/yago/entityPairs.int.txt  ./examples/yago/output 3

Sample Output:
loading integer graph
(Number of vertices,25474)
(Number of edges,98070)
Found no filter, executing regular path enumeration
(Number of paths found between pairs,1,11532,2045)
(Number of paths found between pairs,1,16848,9)
(Number of paths found between pairs,11505,11442,32)

The output files will be generated in directory "/examples/yago/output/"  and contain paths in integer format.

### 2.4 Labels To Integer format and vice versa
Search module provides 2 custom scripts 
* 1) to convert labeled graph to integer format 
* 2) to convert path results back to labeld data format

#### 2.4.1 Labeld Graph to Integer format:
update path to input and output file in scripts/getIntGraph.py 
````
python scripts/getIntGraph.py 

will generate:

vertexDictonary.txt
and files containing mapping of each graph triple to its integer format

#### 2.4.2 Integer Paths to Labeled Paths

python scripts/getLabeledPaths.py



