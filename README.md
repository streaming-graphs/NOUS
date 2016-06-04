# NOUS : Construction and Querying of Dynamic Knowledge Graphs
Automated construction of knowledge graphs remains an expensive technical challenge that
is beyond the reach for most enterprises and academic institutions.
NOUS is an end-to-end framework for developing custom knowledge graphs driven
analytics for arbitrary application domains.
The uniqueness of our system lies A) in its combination of curated KGs along with
knowledge extracted from unstructured text, B) support for advanced trending and explanatory
questions on a dynamic KG, and C) the ability to
answer queries where the answer is embedded across multiple data sources.

## 1. Introduction	
NOUS provides complete suite of capabilities needed to build a domain specific knowledge graph
from streaming data. This includes
 * Triple Extraction from Natural Language Text(NLP)
 * Mapping Raw Triples to Knowledge Graph
 * Confidence Estimation via Link Prediction
 * Rule Learning /Trend Discovery via Frequent Graph Mining
 * Question Answering

## 2. Build and Execute Hello World Program(s):
### 2.1 Prerequisites
* Java 1.7 OR above
* Maven
* Apache Spark 1.2 OR above
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
We describe how to run each NOUS component here using the test examples provided in the [examples](https://github.com/streaming-graphs/NOUS/blob/master/examples/) directory. For algorithmic and implementation details please refer to `NOUS unplugged`  section. 

Each NOUS component is executed using slightly different syntax and with relevant parameters:

#### 2.3.1 Triple Extractor
Triple extractor supports NLP for multiple text formats including text only, openGraph Protocol and Json. To run the toy example in examples directory
[triple-extractor.input](https://github.com/streaming-graphs/NOUS/blob/master/examples/triple-extractor/triple-extractor.input) :

`cd [Repo_Home]/triple_extractor`

```java
java -cp target/uber-TripleParser-0.1-SNAPSHOT.jar gov.pnnl.aristotle.text.TripleParser ../examples/triple-extractor/triple-parser.input 
```

##### 2.3.2 Graph Mining: 
This module takes the triple file produced from NLP processing as input  and generates frequent patterns over a time sliding window.
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

To run test examples, execute 
```java
[SPARK_HOME]/bin/spark-submit --verbose --jars "[PATH_TO_NOUS_JAR]" --master [SPARK_MASTER]  --class "gov.pnnl.aristotle.algorithms.GraphMiner" target/knowledge_graph-0.1-SNAPSHOT-jar-with-dependencies.jar rdf:type 10 5 3 ../examples/graphmining/dronedata.ttl
```
Please look at "Project Structure" section for details on algorithm and code design.

#### 2.3.3 Graph Search:
This module takes the triple file produced from NLP processing as input and answers questions of form What/Who/Why interactively:
* Query parameters are entered as
* queryType_Entity_OtherParaneters
* What/Who questions are answered as query type 1:
	* 1_X => Tell me about X (e.g: 1_Google => What is Google)
  	* 2_X_Y => Tell me about X in context of Y (e.g. 2_Harry Potter_author => Who authored Harry Potter series)
* How/Why Questions as query type 3:
  * 3_X_Y =>How X relates to Y (3_Lebron James_Urban Meyer => How does Lebron James know Urban Meyer)
  * 3_X_Y_Context => Why X did Y in Context K (3_Amazon_Drones_bought => Why Amazon bought drones)

On a spark Cluster the question answering module can be started using :

```bash
cd [Repo_Home]/knowledge_graph
```

```java
[SPARK_HOME]/bin/spark-submit --verbose --jars "[PATH_TO_NOUS_JAR]" --master [SPARK_MASTER]  --class "gov.pnnl.aristotle.algorithms.DemoDriver" [PATH_TO_NOUS_JAR]  [INPUT_FILE_PATH]
```

To run example
```java
[SPARK_HOME]/bin/spark-submit --verbose --jars "[PATH_TO_NOUS_JAR]" --master [SPARK_MASTER]  --class "gov.pnnl.aristotle.algorithms.DemoDriver" target/knowledge_graph-0.1-SNAPSHOT-jar-with-dependencies.jar  ../examples/question-answering/search-input.ttl

Wait for graph to load and prompt for queries to appear, enter
1_Drone
3_CIA_Drone
3_Amazon_Drone
```

## 3 NOUS Design: 

NOUS code is organized as multiple maven projects (Check out section on Build/Run and Data to build and run test examples) : 

### 3.1 triple_extractor : 

Contains NLP code, takes text documents as input and produces triples of the form
subject, predicate, object, timestamp, documentId
* Parsers:  Identify data format, filter documents in english language and extract (text, timestamp, other document metadata). The parser supports:
	* 1) ordinary text files, 2) web pages supporting OpenGraph protocol, 3) JSON 

* Triple extraction:  TripleParser.scala performs the following tasks to extract triples from every sentence of a given document: a) named entity extraction, b) co-reference resolution and c) triple extraction via Open Information Extraction (OpenIE) and Semantic Role Labeling (SRL).  We use Stanford CoreNLP as the underlying library for most tasks and EasySRL from University of Washington for Semantic Role Labeling. TripleFilter.scala a number of heuristics to extract final triples from the OpenIE/SRL output.

* Relation Extraction:  Triple extraction produces a large number of predicates (ranging into thousands), which need to be mapped to a few predicates (tens to couple hundreds).  RelationMiner.scala provides an implementation of a Distant Supervision algorithm for extracting relations.  Given a file with a list of seed subject-predicate pairs and a file containing a list of text corpus files, it will extract a set of rules for the target relation.  The rules are written out to an output file which should be reviewed by a human expert.  

See Run/Example section above on instruction for running the code

### 3.2 knowledge_graph : 
knowledge_graph component of the NOUS deals with construction of in-memory property graph and execution of analytical algorithms on newly created graph. It has following modules as part of it:
1. algorithms.entity: Implements Entity Disambiguation as described by Han et al in "Collective Entity Linking in Web Text: A Graph-based Method, SIGIR 2011"
2. algorithms.mining: Implements dynamic graph mining to find closed patterns over a sliding time window
3. algorithms.search: Implements question answering for entity(What/Who) and path queries (Why X did Y/How X relates to Y)


## 4. Data

Example datasets to run each module is in the data directory. The four data sets are described and credited below.
#### 4.1 Entity Disambiguation: 

TODO

#### 4.2 Graph Mining:
A major research contribution of NOUS is the development of a distributed algorithm for streaming graph mining. The algorithm accepts the stream of incoming triples as input, a window size parameter that represents the size of a sliding win- dow over the stream and reports the set of closed frequent patterns present in the window. 




### 4.3 triple_extractor:

Example:  Given a sentence `Aerialtronics is back on tour with four exhibitions in the United States and Europe in April and May, including the AUVSI Unmanned Systems 2015 trade show at the World Congress Centre in Atlanta.`", we will initially extract the following from step 4:
Named Entities: Aerialtronics, United States, Europle, April, World Congress Centre, Atlanta.
Raw triples: 
Triple1: (Aerialtronics, is back on, tour with four exhibitions).
Triple2: (World Congress Centre, in, Atlanta).
Next, we will run these rules through our filtering heuristics and rule-based relation extractors.  The first one will be rejected as we reject triples with no named entity in the subject phrase.  The second one will be mapped to (World Congress Centre, is-located, Atlanta) using a rule that says (org, in, location) => (org,is-located-location).

#### 4.4 Graph Search



### 5. Publicly Accessible Deliverables.

1. Zhang, Baichuan, et al. "Trust from the past: Bayesian Personalized Ranking based Link Prediction in Knowledge Graphs." arXiv preprint arXiv:1601.03778 (2016).
2. NOUS Presentation: TODO
