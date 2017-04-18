# Graph Mining
This component of the NOUS provides a distributed system for online pattern
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

### 2.2 Build
 Clone github repository 

` clone https://github.com/streaming-graphs/NOUS.git NOUS `

 Perform maven build in any of the module : `DynamicGraphMining` Ex:
 
 ```bash
 cd [Repo_Home]/DynamicGraphMining
 mvn package
 ```
Here `[Repo_Home]` is the path to your cloned directory `NOUS`. 

### 2.3 Run Hello World
We provide toy examples to test setup for each module under NOUS  [examples directroy](https://github.com/streaming-graphs/NOUS/blob/master/examples/). For algorithmic and implementation details please refer to `NOUS Design`  section. 

#### 2.3.1 Triple Extractor
Triple extractor supports NLP of text and supports multiple text formats (Text only, OpenGraph Protocol, JSON). To run the triple extractor example
[triple-extractor.input](https://github.com/streaming-graphs/NOUS/blob/master/examples/triple-extractor/triple-extractor.input) :

`cd [Repo_Home]/triple_extractor`

```java
java -cp target/uber-TripleParser-0.1-SNAPSHOT.jar gov.pnnl.aristotle.text.TripleParser ../examples/triple-extractor/triple-parser.input 
```

##### 2.3.2 Knowledge Graph - Graph Mining: 
This module takes triple file(s) (could be produced from NLP processing 2.3.1)  as input and finds frequent patterns over time. User can configure the minimum support count for frequent patterns, time window and number of iterations.
To run test examples (needs Spark), execute 
```java
[SPARK_HOME]/bin/spark-submit --verbose --jars "[PATH_TO_NOUS_JAR]" --master [SPARK_MASTER]  --class "gov.pnnl.aristotle.algorithms.GraphMiner" target/knowledge_graph-0.1-SNAPSHOT-jar-with-dependencies.jar rdf:type 10 5 3 ../examples/graphmining/dronedata.ttl
```

On a spark Cluster Graph Mining code can be run using :

```bash
cd [Repo_Home]/knowledge_graph
```
```java
[SPARK_HOME]/bin/spark-submit --verbose --jars "[PATH_TO_NOUS_JAR]" --master [SPARK_MASTER]  --class "gov.pnnl.aristotle.algorithms.GraphMiner" [PATH_TO_NOUS_JAR]  [BASE_TYPE] [MIN_SUPPORT] [TYPE_THRESHOLD] [MAX_ITERATIONS] [INPUT_FILE_PATH]
```
where:
```
[BASE_TYPE]      :   String value of any edge label between source and destination that is considerred as a 'type' of the source. 
Ex. Triple such as <Barack Obama> rdf:type <Person> can be identified with BASE_TYPE as "rdf:type"

[MIN_SUPPORT]    :   Positive Integer value that specify minimum frequency of any pattern to be considered as "Frequent"

[TYPE_THRESHOLD] :   Positive Integer value that specify minimum frequency of any Entity to be considered as a "type" 

[MAX_ITERATIONS] :   Positive Integer value that specify maximum number of iteration performed by graph miner component
```

#### 2.3.3 Knowledge Graph - Question Answering:
The QA module takes triple file (could be produced from NLP processing) as input and answers questions(What/Who/Why) interactively. Query parameters are entered separated by "_", as shown below:
* What/Who questions(Entity queries) are answered as query type 1 and 2 :
	* 1_X implies ` Tell me about X (e.g: 1_Google => What is Google) `
  	* 2_X_Y implies ` Tell me about X in context of Y (e.g. 2_Harry Potter_author => Who authored Harry Potter series)`
* Why/How(Path Queries) as query type 3 
  	* 3_X_Y implies ` How X relates to Y (3_Lebron James_Urban Meyer => How does Lebron James know Urban Meyer)`

To run the question answering module for test example, start the session using :
```bash
cd [Repo_Home]/knowledge_graph
```

```java
[SPARK_HOME]/bin/spark-submit --verbose --jars "[PATH_TO_NOUS_JAR]" --master [SPARK_MASTER]  --class "gov.pnnl.aristotle.algorithms.DemoDriver" target/knowledge_graph-0.1-SNAPSHOT-jar-with-dependencies.jar  ../examples/question-answering/search-input.ttl

Wait for graph to load and prompt for queries to appear, enter
1_Drone
3_CIA_Drone
3_Amazon_Drone
```

In general: 
```java
[SPARK_HOME]/bin/spark-submit --verbose --jars "[PATH_TO_NOUS_JAR]" --master [SPARK_MASTER]  --class "gov.pnnl.aristotle.algorithms.DemoDriver" [PATH_TO_NOUS_JAR]  [INPUT_FILE_PATH]
```

## 3 NOUS Design: 

NOUS code is organized in two maven projects triple_extractor and knowledge_graph. This section contains details of algorithms and code structure. 

### 3.1 triple_extractor : 

Contains NLP code, takes text documents as input and produces triples of the form
subject, predicate, object, timestamp, documentId
* Parsers:  Identify data format, filter documents in english language and extract (text, timestamp, other document metadata). The parser supports:
	* 1) ordinary text files, 2) web pages supporting OpenGraph protocol, 3) JSON 

* Triple extraction:  TripleParser.scala performs the following tasks to extract triples from every sentence of a given document: a) named entity extraction, b) co-reference resolution and c) triple extraction via Open Information Extraction (OpenIE) and Semantic Role Labeling (SRL).  We use Stanford CoreNLP as the underlying library for most tasks and EasySRL from University of Washington for Semantic Role Labeling. TripleFilter.scala a number of heuristics to extract final triples from the OpenIE/SRL output.

* Relation Extraction:  Triple extraction produces a large number of predicates (ranging into thousands), which need to be mapped to a few predicates (tens to couple hundreds).  RelationMiner.scala provides an implementation of a Distant Supervision algorithm for extracting relations.  Given a file with a list of seed subject-predicate pairs and a file containing a list of text corpus files, it will extract a set of rules for the target relation.  The rules are written out to an output file which should be reviewed by a human expert.  

Test Example:  Given a sentence `Aerialtronics is back on tour with four exhibitions in the United States and Europe in April and May, including the AUVSI Unmanned Systems 2015 trade show at the World Congress Centre in Atlanta.`", we will initially extract the following from step 4:
Named Entities: Aerialtronics, United States, Europle, April, World Congress Centre, Atlanta.
Raw triples: 
Triple1: (Aerialtronics, is back on, tour with four exhibitions).
Triple2: (World Congress Centre, in, Atlanta).
Next, we will run these rules through our filtering heuristics and rule-based relation extractors.  The first one will be rejected as we reject triples with no named entity in the subject phrase.  The second one will be mapped to (World Congress Centre, is-located, Atlanta) using a rule that says (org, in, location) => (org,is-located-location).

See Run/Example section above on instruction for running the code

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
