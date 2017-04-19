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

Required Input file format consist of tab separated 4 entries that corresponds to `<subject> <object> <predicate> and <timestamp>`. For Example:

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


##### 2.3.1 Output:

Mining code generates various files to present the output. All the output file name and file paths can be configured in `knowledge_graph.conf` file.

* `frequentPatterns.tsv`: It lists all the frequent patterns across the "window".
* `frequentPatternsPerBatch.tsv`: It lists batch-wise summary of all the frequent patterns.
* `dependencyGraph.txt`: It presents the pattern growth tree showing all the smaller patterns participating in a larger pattern.

### 2.4 Analysis:
Mining module also provides various scripts to analyze and visualize output data in the form of plots and images. The scripts do require third party tools such as 
* R
* Python
* GraphViz


Some examples are given below:

#### 2.4.1 To generate GraphViz supported format of the depenendency graph : 
` scala CreateGraphVizDotFile.scala <path to dependencyGraph.txt file>`
It will generate dependencyGraph.dot file which can be viewed in the GraphViz.

To generate GraphViz image in an eps file:

`dot -Tps <path to dependencyGraph.dot file> -o <output path to dependencyGraph.eps file>`

Example:
`dot -Tps ../output/dependencyGraph.dot -o ../output/dependencyGraph.eps`


![Dependency Graph](https://github.com/streaming-graphs/NOUS/blob/master/Mining/output/dependencyGraph.png)

#### 2.4.2 To get Top-K frequent patterns and corresponding Dependency Graph

`python getTopKFrequentPattens.py`

Edit this file to change the value of "K", and output file paths. It will generate follwoing files
* frequentPatternsPerBatchTopK.tsv
* frequentPatternsPerBatchTopKFormatted.tsv

![Top 20 Frequent Patterns](https://github.com/streaming-graphs/NOUS/blob/master/Mining/output/frequentPatternsTopK.png)
