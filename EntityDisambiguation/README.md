# NOUS : Construction and Querying of Dynamic Knowledge Graphs
## 1. Introduction	
Entity Linking or Disambiguation is the task maps an entity mentioned in text to the matching vertex in the graph. NOUS entity linking implements the parallel version of the Entity Disambiguation algorithm as described by Han et al in "Collective Entity Linking in Web Text: A Graph-based Method, SIGIR 2011"

## 2. Build and Execute Hello World Program(s):
### 2.1 Prerequisites
* Java 1.7 OR above
* Maven
* Apache Spark 2.1 OR above
* HDFS File System (Optional)

### 2.2 Build
 Clone github repository 

` clone https://github.com/streaming-graphs/NOUS.git NOUS `

 Perform maven build in any of the module : `triple_extractor` OR `knowledge_graph` Ex:
 
 ```bash
 cd [Repo_Home]/triple_extractor
 mvn package
 ```
Here `[Repo_Home]` is the path to your cloned directory `NOUS`. 

### 2.3 Run Hello World
Implements Entity Disambiguation as described by Han et al in "Collective Entity Linking in Web Text: A Graph-based Method, SIGIR 2011"

