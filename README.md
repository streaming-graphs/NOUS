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
 * Triple Extraction from Natural Language Text(NLP)
 * Mapping Raw Triples to Knowledge Graph
 * Confidence Estimation via Link Prediction
 * Rule Learning /Trend Discovery via Frequent Graph Mining
 * Question Answering

## Build and Execute Hello World Program(s):
### Prerequisites
* Java 1.7 OR above
* Maven
* Apache Spark 1.2 OR above
* HDFS File System (Optional)

### Build
 Clone github repository 

` clone https://github.com/streaming-graphs/NOUS.git NOUS `

 Perform maven build in any of the module : `triple_extractor` OR `knowledge_graph` 

 `mvn package` 

### Run Hello World
Each NOUS component is executed using different syntax and and with relevant parameters:

#### Triple Extractor

[triple-extractor.input](https://github.com/streaming-graphs/NOUS/blob/master/examples/triple-extractor/triple-extractor.input) File is used to demonstrate this component. Please read "Data" section for more information about the input and output data.

`cd [Repo_Home]/triple_extractor`

`java -cp TripleParser-0.1-SNAPSHOT.jar gov.pnnl.aristotle.text.datasources.Plugins data/triple-extractor/triple-parser.input data/triple-extractor/triple.output`



##### Graph Mining:
On a spark Cluster Graph Mining code can be run using :

`cd [Repo_Home]/knowledge_graph`

`[SPARK_HOME]/bin/spark-submit --verbose --jars "[PATH_TO_NOUS_JAR]" --master [SPARK_MASTER]  --class "gov.pnnl.aristotle.algorithms.GraphMiner" target/knowledge_graph-0.1-SNAPSHOT-jar-with-dependencies.jar rdf:type 10 5 3 dronedata.ttl`

This component requires following arguments:

`[BASE_TYPE]      :   String value of any edge label between source and destination that is considerred as a 'type' of the source. Ex. <Barack Obama> rdf:type <Person>`

`[MIN_SUPPORT]    :   Positive Integer value that specify minimum frequency of any pattern to be considered as "Frequent"`

`[TYPE_THRESHOLD] :   Positive Integer value that specify minimum frequency of any Entity to be considered as a "type" `

`[MAX_ITERATIONS] :   Positive Integer value that specify maximum number of iteration performed by graph miner component`


`[SPARK_HOME]/bin/spark-submit --verbose --jars "[PATH_TO_NOUS_JAR]" --master [SPARK_MASTER]  --class "gov.pnnl.aristotle.algorithms.GraphMiner" [PATH_TO_NOUS_JAR]  [BASE_TYPE] [MIN_SUPPORT] [TYPE_THRESHOLD] [MAX_ITERATIONS] [INPUT_FILE_PATH `

Please look at "Project Structure" section for more information.

#### Graph Search:


## Project Structure: 

NOUS code is organized as multiple maven projects (Check out section on Build/Run and Data to build and run test examples) : 

### triple_extractor : 

Contains NLP code, takes text documents as input and produces triples of the form
subject, predicate, object, timestamp, documentId
* Parsers:  Identify format and extract data (text, timestamp, language - filters for english language, document metadata), supports:
	* 1) ordinary text files, 2) web pages supporting OpenGraph protocol, 3) JSON 

* Triple extraction:  TripleParser.scala performs the following tasks to extract triples from every sentence of a given document: a) named entity extraction, b) co-reference resolution and c) triple extraction via Open Information Extraction (OpenIE) and Semantic Role Labeling (SRL).  We use Stanford CoreNLP as the underlying library for most tasks and EasySRL from University of Washington for Semantic Role Labeling. TripleFilter.scala a number of heuristics to extract final triples from the OpenIE/SRL output.

* Relation Extraction:  Triple extraction produces a large number of predicates (ranging into thousands), which need to be mapped to a few predicates (tens to couple hundreds).  RelationMiner.scala provides an implementation of a Distant Supervision algorithm for extracting relations.  Given a file with a list of seed subject-predicate pairs and a file containing a list of text corpus files, it will extract a set of rules for the target relation.  The rules are written out to an output file which should be reviewed by a human expert.  

See Run/Example section for running the code


### knowledge_graph : 
knowledge_graph component of the NOUS deals with construction of in-memory property graph and execution of analytical algorithms on newly created graph. It has following modules as part of it:
1. algorithms.entity: Implements Entity Disambiguation as described by Han et al in "Collective Entity Linking in Web Text: A Graph-based Method, SIGIR 2011"
2. algorithms.mining: Implements dynamic graph mining to find closed patterns over a sliding time window
3. algorithms.search: Implements question answering for entity(What/Who) and path queries (Why X did Y/How X relates to Y)



TODO

Data
====
Example datasets to run each module is in the data directory. The four data sets are described and credited below.
#### Entity Disambiguation: 

TODO

#### Graph Mining:
A major research contribution of NOUS is the development of a distributed algorithm for streaming graph mining. The algorithm accepts the stream of incoming triples as input, a window size parameter that represents the size of a sliding win- dow over the stream and reports the set of closed frequent patterns present in the window. 

##### Inputd
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


### triple_extractor:

Example:  Given a sentence ``Aerialtronics is back on tour with four exhibitions in the United States and Europe in April and May, including the AUVSI Unmanned Systems 2015 trade show at the World Congress Centre in Atlanta.", we will initially extract the following from step 4:
Named Entities: Aerialtronics, United States, Europle, April, World Congress Centre, Atlanta.
Raw triples: 
Triple1: (Aerialtronics, is back on, tour with four exhibitions).
Triple2: (World Congress Centre, in, Atlanta).
Next, we will run these rules through our filtering heuristics and rule-based relation extractors.  The first one will be rejected as we reject triples with no named entity in the subject phrase.  The second one will be mapped to (World Congress Centre, is-located, Atlanta) using a rule that says (org, in, location) => (org,is-located-location).
#### Entity Disambiguation: 



### Publicly Accessible Deliverables.

1. Zhang, Baichuan, et al. "Trust from the past: Bayesian Personalized Ranking based Link Prediction in Knowledge Graphs." arXiv preprint arXiv:1601.03778 (2016).
2. NOUS Presentation: TODO
