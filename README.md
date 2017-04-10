# NOUS : Construction and Querying of Dynamic Knowledge Graphs
Automated construction of knowledge graphs remains an expensive technical challenge that 
is beyond the reach for most enterprises and academic institutions. 
NOUS is an end-to-end framework for developing custom knowledge graphs driven 
analytics for arbitrary application domains. 
The uniqueness of our system lies A) in its combination of curated KGs along with 
knowledge extracted from unstructured text, B) support for advanced trending and explanatory 
questions on a dynamic KG, and C) the ability to 
answer queries where the answer is embedded across multiple data sources.

## Introduction	
NOUS provides complete suite of capabilities needed to build a domain specific knowledge graph 
from streaming data. This includes 
1) Natural language processing(NLP), 
2) Entity and relationship mapping, 
3) Confidence Estimation using Link Prediction. 
4) Rule Learning/Trend Discovery using Frequent Graph Mining
5) Question Answering using Graph Search 

### NOUS Project Structure :

* TripleExtractor: Contains NLP code, takes text documenets as input 
and produces triples of the form (uses Stanford Core NLP)
subject, predicate, object 
* EntityDisambiguation : Entity linking to a given KB, (implements the algorithm in 
Collective Entity Linking in Web Text: A Graph-based Method, Han et al, SIGIR 2011)  
* DynamicGraphMining : Given a streaming graph, find frequent patterns
* GraphSearch : Given a attributed graph and entity pairs, return all paths 
Entity Queries: (Who/What is X) and 
"Why" Queries : (Why Company X may acquire Y, "How" Person X knows Y)
* Link Prediction: Confidence estimation of each link in the graph using Naive Bayes 

#### knowledge_graph
##### Graph Mining Module:
A major research contribution of NOUS is the development of a distributed algorithm for streaming graph mining. The algorithm accepts the stream of incoming triples as input, a window size parameter that represents the size of a sliding win- dow over the stream and reports the set of closed frequent patterns present in the window. 

Graph Mining Module supports different input graph formats mentioned above. "dronedata.ttl" input file in the "data/graphmining" directory shows one such format. The input file has tab separated values representing <subject> <relation_ship> <object> <timestamp> <source_id>

`FAA     releases        updated UAS guidance    2015-09-22T13:00:49+00:00       http://www.uavexpertnews.com/faa-releases-updated-uas-guidance-tells-of-new-uas-leaders/`

### How to build and execute NOUS:
#### Prerequisites
* Java 1.7 OR above
* Maven
* Apache Spark 1.2 OR above
* HDFS File System (Optional)

#### Build
NOUS is developed as a maven project so the easiest way to build the project is to use `mvn package`
#### Run
NOUS includes various components such as Graph Mining, Graph Profiling, Graph Search etc.
These components are executed using different syntax and parameters. Please read individual component section to find it execution syntax.

###### Graph Mining 
On a spark Cluster Graph Mining code can be run using :
`[SPARK_HOME]/bin/spark-submit --verbose --jars "[PATH_TO_NOUS_JAR]" --master [SPARK_MASTER]  --class "gov.pnnl.aristotle.algorithms.GraphMiner" [PATH_TO_NOUS_JAR]  [BASE_TYPE] [MIN_SUPPORT] [TYPE_THRESHOLD] [MAX_ITERATIONS] [INPUT_FILE_PATH `


Data
====
All data is in the data directory. The four data sets are described and credited below.

Citeseer
--------
This data set contains a selection of the CiteSeer data set (http://citeseer.ist.psu.edu/).

These papers are classified into one of the following six classes:

	Agents
	AI
	DB
	IR
	ML
	HCI
