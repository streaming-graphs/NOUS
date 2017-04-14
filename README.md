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

Relevant Publications

1. Zhang, Baichuan, Sutanay Choudhury, Mohammad Al Hasan, Xia Ning, Khushbu Agarwal, Sumit Purohit, and Paola Gabriela Pesntez Cabrera. "Trust from the past: Bayesian Personalized Ranking based Link Prediction in Knowledge Graphs." SDM Workshop on Mining Networks and Graphs, 2016.
2. Sutanay Choudhury, Khushbu Agarwal, Sumit Purohit,  Baichuan Zhang, Meg Pirrung, Will Smith, Mathew Thomas. "NOUS: Construction and Querying of Dynamic Knowledge Graphs". http://arxiv.org/abs/1606.02314, 2016.

## 2. Build and Execute Hello World Program(s):
### 2.1 Prerequisites
* Java 1.7 OR above
* Maven 3.0 or above
* Apache Spark 2.0 OR above
* HDFS File System (Optional)

### 2.2 Build
 Clone github repository 

` clone https://github.com/streaming-graphs/NOUS.git NOUS `

 Perform maven build in any of the module : `TripleExtractor` OR `Mining` Ex:
 
 ```bash
 cd [Repo_Home]/TripleExtractor
 mvn package
 ```
Here `[Repo_Home]` is the path to your cloned directory `NOUS`. 

### 2.3 Run Hello World
NOUS is organized into multiple modules that support the KB workflow. Each module 
contains README and data to run the examples.
For algorithmic and implementation details please refer to `NOUS paper in DesWeb 2017`. 

