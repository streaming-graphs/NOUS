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
1. Triple Extraction from Natural Language Text(NLP)
2. Mapping Raw Triples to Knowledge Graph
3. Confidence Estimation via Link Prediction
4. Rule Learning /Trend Discovery via Frequent Graph Mining
5. Question Answering

## Project Structure: For supported data formats and examples please see section on Data 
### triple_extractor : Code for NLP, takes text documents as input and produces triples of the form
subject, predicate, object, timestamp, documentId

### knowledge_graph : 
knowledge_graph component of the NOUS deals with construction of in-memory property graph and execution of analytical algorithms on newly created graph. It has following modules as part of it:
1. algorithms.entity: Implements Entity Disambiguation as described by Han et al in "Collective Entity Linking in Web Text: A Graph-based Method, SIGIR 2011"
2. algorithms.mining: Implements dynamic graph mining to find closed patterns over a sliding time window
3. algorithms.search: Implements question answering for entity(What/Who) and path queries (Why X did Y/How X relates to Y)


## How to build and execute NOUS:
### Prerequisites
* Java 1.7 OR above
* Maven
* Apache Spark 1.2 OR above
* HDFS File System (Optional)

### Build
NOUS is developed as a maven project so the easiest way to build the project is to use `mvn package`
### Run
NOUS includes various components such as Graph Mining, Graph Profiling, Graph Search etc.
These components are executed using different syntax and parameters. Please read individual component section to find it execution syntax.

### Run

#### Entity Disambiguation: 

TODO

##### Graph Mining:
On a spark Cluster Graph Mining code can be run using :
`[SPARK_HOME]/bin/spark-submit --verbose --jars "[PATH_TO_NOUS_JAR]" --master [SPARK_MASTER]  --class "gov.pnnl.aristotle.algorithms.GraphMiner" [PATH_TO_NOUS_JAR]  [BASE_TYPE] [MIN_SUPPORT] [TYPE_THRESHOLD] [MAX_ITERATIONS] [INPUT_FILE_PATH `


#### Graph Search:

TODO

Data
====
Example datasets to run each module is in the data directory. The four data sets are described and credited below.
#### Entity Disambiguation: 

TODO

#### Graph Mining:
A major research contribution of NOUS is the development of a distributed algorithm for streaming graph mining. The algorithm accepts the stream of incoming triples as input, a window size parameter that represents the size of a sliding win- dow over the stream and reports the set of closed frequent patterns present in the window. 

##### Inputd
Graph Mining Module supports different input graph formats mentioned above. [dronedata.ttl](https://github.com/streaming-graphs/NOUS/blob/master/data/graphmining/dronedata.ttl) input file in the "data/graphmining" directory shows one such format. The input file has tab separated values representing <subject> <relation_ship> <object> <timestamp> <source_id>

`<FAA>     <releases>        <updated UAS guidance>    2015-09-22T13:00:49+00:00       http://www.uavexpertnews.com/faa-releases-updated-uas-guidance-tells-of-new-uas-leaders/`

##### Output

Graph Mining Module generates output in multiple formats. One such format shows discovered patterns with it occurrence frequency.

`<schumer>        <require> <technology>    <faa>     <finalize regulations>      <before  fatal drone accident> => 210`


### triple_extractor:


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
