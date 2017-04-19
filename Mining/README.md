# Graph Mining
This module of the NOUS provides a distributed system for online pattern
discovery in graph streams. It combines both incremental mining and parallel mining
for feasible pattern detection over graph streams. 

## 1. Introduction	
It has the following unique features that differ from conventional incremental
querying and mining systems.
 * Events as graph patterns.
 * Incremental mining.
 * Scale-up.
 * Easy-to-use.

Relevant Publications

1. Zhang, Baichuan, Sutanay Choudhury, Mohammad Al Hasan, Xia Ning, Khushbu Agarwal, Sumit Purohit, and Paola Gabriela Pesntez Cabrera. "Trust from the past: Bayesian Personalized Ranking based Link Prediction in Knowledge Graphs." SDM Workshop on Mining Networks and Graphs, 2016.
2. Sutanay Choudhury, Khushbu Agarwal, Sumit Purohit,  Baichuan Zhang, Meg Pirrung, Will Smith, Mathew Thomas. "NOUS: Construction and Querying of Dynamic Knowledge Graphs". http://arxiv.org/abs/1606.02314, 2016.

## 2. Build and Execute Hello World Program(s):
### 2.1 Prerequisites
* Java 1.7 OR above
* Maven
* Apache Spark 1.6 OR above
* HDFS File System (Optional)

### 2.1 Recommended
* Java 1.7 
* Maven
* Apache Spark 2.0 OR above
* HDFS File System (Optional)

### 2.2 Build
 Clone github repository 

` clone https://github.com/streaming-graphs/NOUS.git NOUS `

 Perform maven build in any of the module : `Mining` Ex:
 
 ```bash
 cd [Repo_Home]/Mining
 mvn package
 ```
Here `[Repo_Home]` is the path to your cloned directory `NOUS`. 

### 2.3 Run Hello World

##### 2.3.1 Graph Mining: 
This module takes triple file(s) (could be produced from NLP processing using other NOUS modules) as input and finds frequent patterns over time. User can configure the minimum support count for frequent patterns, time window and the number of iterations.

Required Input file format consist of tab separated 4 entries that corresponds to <subject> <object> <predicate> and <timestamp>. For Example:

```
7       1       77849   2010-01-01T05:01:00.000
26      2       77850   2010-01-01T05:01:00.000
26      2       77851   2010-01-01T05:01:00.000
5       3       77852   2010-01-01T05:01:00.000
5       3       77853   2010-01-01T05:01:00.000
5       3       77854   2010-01-01T05:01:00.000
```

To run test examples (needs Spark), execute 
```java
[SPARK_HOME]/bin/spark-submit --verbose --jars [PATH TO NOUS JAR] --master yarn --deploy-mode client --num-executors 8 --executor-cores 8  --executor-memory 45G  --driver-memory 45g --conf "spark.driver.extraJavaOptions=-XX:+UseCompressedOops -verbose:gc -XX:+PrintGCDetails -XX:+PrintGCTimeStamps"  --conf "spark.driver.maxResultSize=20g"  --conf spark.history.fs.logDirectory="/user/spark/applicationHistory" --class "gov.pnnl.aristotle.algorithms.DataToPatternGraph"  
[PATH TO NOUS JAR] [NOUS HOME]/Mining/conf/knowledge_graph.conf 
```

Here [NOUS HOME] is the path to the checkout out directory.

Various run-time parameters can be configured using `knowledge_graph.conf` file provided with the source code. 


### 3.2 knowledge_graph : 
knowledge_graph component of the NOUS deals with construction of in-memory property graph and execution of analytical algorithms on newly created graph. It has following modules as part of it:
#### 3.2.1 algorithms.entity 
Implements Entity Disambiguation as described by Han et al in "Collective Entity Linking in Web Text: A Graph-based Method, SIGIR 2011"

#### 3.2.2 algorithms.mining 
Implements dynamic graph mining to find closed patterns over a sliding time window
##### Input
Graph Mining Module supports different input graph formats. 
 
[dronedata.ttl](https://github.com/streaming-graphs/NOUS/blob/master/data/graphmining/dronedata.ttl) input file in the "data/graphmining" directory shows one such format. The input file has tab separated values representing <subject> <relation_ship> <object> <timestamp> <source_id>
 
 `<FAA>     <releases>        <updated UAS guidance>    2015-09-22T13:00:49+00:00       http://www.uavexpertnews.com/faa-releases-updated-uas-guidance-tells-of-new-uas-leaders/`
  
 [citeseer.ttl](https://github.com/streaming-graphs/NOUS/blob/master/data/graphmining/citeseer.lg) data set contains a selection of the CiteSeer data set (http://citeseer.ist.psu.edu/).
 
 These papers are classified into one of the following six classes:

      Agents
 	AI
 	DB
 	IR
 	M
 	HCI

 ##### Output
 
 Graph Mining Module generates output in multiple formats. One such format shows discovered patterns with it occurrence frequency.
 
 `<schumer>        <require> <technology>    <faa>     <finalize regulations>      <before  fatal drone accident> => 210`

#### 3.2.3 algorithms.pathRanking
Implements question answering using LDA based heristics to find most coherent paths linking two entities
