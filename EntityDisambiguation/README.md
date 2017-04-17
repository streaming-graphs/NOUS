# NOUS : Construction and Querying of Dynamic Knowledge Graphs
## 1. Introduction	
Entity Disambiguation algorithms map  "mention of an entity" in text to its corresponding matching vertex in the knowledge base. 
NOUS implements the parallel version of the Entity Disambiguation algorithm as described by Han et al in "Collective Entity Linking in Web Text: A Graph-based Method, SIGIR 2011"

## 2. Build and Execute Hello World Program(s):
### 2.1 Prerequisites
* Java 1.7 OR above
* Maven
* Apache Spark 2.1 OR above
* HDFS File System (Optional)

### 2.2 Build
 Clone github repository 

` clone https://github.com/streaming-graphs/NOUS.git NOUS `

 Perform maven build :
 
 ```bash
 cd [Repo_Home]/EntityDisambiguation
 mvn package
 ```
Here `[Repo_Home]` is the path to your cloned directory `NOUS`. 

### 2.3 Run Hello World
[SPARK_HOME]/bin/spark-submit --verbose --jars "[PATH_TO_JAR]" --master [SPARK_MASTER]  --class "gov.pnnl.nous.Main" "[PATH_TO_JAR]"  < <outputDir> <topicsFile> <maxPathLength> <maxDegree> <topcCoherenceThreshold(Optional)>
