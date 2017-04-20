# NOUS Search: 
Implements Path Finding  algorithms for knowledge graphs. Unlike traditional methods in 
path finding that focus on shortest paths, we focus on finding "highly coherent" and 
"specific" paths between pairs of entities. The algorithms are user configurable in minimum topic 
coherence and maximum specificity

## 2. Build and Execute Hello World Program(s):
### 2.1 Prerequisites
* Java 1.7+
* Maven
* Apache Spark 2.1 OR above
* Python 2.7+
* HDFS File System (Optional)

### 2.2 Build
 Clone github repository 
```
 git clone https://github.com/streaming-graphs/NOUS.git NOUS
 cd [Repo_Home]/Search
 mvn package
 ```
Here `[Repo_Home]` is the path to your cloned directory `NOUS`. 

### 2.3 Run Hello World
Command Line Configuration :
```
[SPARK_HOME]/bin/spark-submit --verbose --jars "[PATH_TO_JAR]" --master [SPARK_MASTER]  --class "gov.pnnl.nous.pathSearch.Int.PathSearch" "[PATH_TO_JAR]"  <graphPath> <entityPairsFile> <outputDir> <maxPathLength> <optional:numEntitiesPerGraphLine> <optional:maxDegree> <optional:topicsFile> <optional: topicCoherenceThreshold>

<graphPath> : Path to a triples file(or directory containing triples files). The vertex ids and edge labels are expected to be mapped to integer format. Each line on triple file 
contains srcId, edgeLabelId and dstId separetd by tab:
` <srcId>	<edgeLabelId>	<dstId> `

entityPairsFile : Path to a file containing entity pair (integer format) per line, separated by tab 
` <entityId1>	<entityId2> `
` <entityId3>	<entityId4> `

<outputDir> : Path to an outputDir. (search code will create an output file containing paths for each entity pair in this directory)

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
```

Example: To run example data, finding all paths upto length 3:

```
spark-submit --jars target/path_search-1.0-SNAPSHOT.jar --master "local" --class "gov.pnnl.nous.pathSearch.Int.PathSearch" target/path_search-1.0-SNAPSHOT.jar  ./examples/yago/intGraph/ ./examples/yago/entityPairs.int.txt  ./examples/yago/output/integer/ 3
```

Sample Output:
```
loading integer graph
(Number of vertices,25474)
(Number of edges,98070)
Found no filter, executing regular path enumeration
(Number of paths found between pairs,1,11532,2045)
(Number of paths found between pairs,1,16848,9)
(Number of paths found between pairs,11505,11442,32)
```

The output files will be generated in directory "/examples/yago/output/integer"  and contain paths in
following format.
```
srcId : (edgeId1-edgeDirection1) nodeId1, (edgeId2-edgeDirection2) nodeid2...
```

See next section on how to convert labeled graph to integer format and convert paths back to labeled format.

### 2.4 Scripts 
Search module provides 2 custom python scripts 
* To convert labeled graph to integer format (scripts/getIntGraph.py)
* To convert path results back to labeld data format (scripts/getLabeledPaths.py)

#### 2.4.1 Labeled Graph to Integer format:
Converts a graph triples to integer format and saves the vertex and edge dictionary.
Input graph triples of the form
```
srcLabel	edgelabel	dstlabel
```
Outputs triple of the form, and saves mapping dictionary
```
srcId	edgeId	dstId
```
To run the script
```
python scripts/getIntGraph.py <graphInDirPath> <graphOutDirPath> <dictOutDirPath>
```
To run yago example :
```
python scripts/getIntGraph.py examples/yago/origGraph/ examples/yago/intGraph/ examples/yago/
```

Converts regular yago triples in the directory "examples/yago/origGraph/ " to integer and write file under "examples/yago/intGraph/". The mapping of vertex labels and edge labels will be saved under examples/yago/vertexDictionary.txt and examples/yago/edegDictionary.txt respectively.

#### 2.4.2 Integer Paths to Labeled Paths
Use this script to convert output of NOUS-PathSearch back to labeled format:
Input paths format:
```
srcId : (edgeId1-edgeDirection1) nodeId1, (edgeId2-edgeDirection2) nodeid2...
```
Outputs Path
```
srcLabel : (edgeLabel-edgeDirection1) nodeLabel1, (edgeLabel2-edgeDirection2) nodeLabel2...
```
To run:
```
python scripts/getLabeledPaths.py <pathToDirContainingVertexEdgeDict> <inputPathsDir> <outputPathsDir>
```

To transform yago results
```
python scripts/getLabeledPaths.py examples/yago/ examples/yago/output/integer/ examples/yago/output/labeled/
```

Reads "examples/yago/vertexDictionary.txt" and "examples/yago/edgeDictionary.txt" to get dictionary.
Reads input files containing paths in integer format from directory "examples/yago/output/integer/" and write their corresponding labeled version in "examples/yago/output/labeled/".


