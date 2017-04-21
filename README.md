# NOUS: Construction and Querying of Dynamic Knowledge Graphs
Automated construction of knowledge graphs (KG) remains an expensive technical challenge that 
is beyond the reach for most enterprises and academic institutions. 
NOUS is an end-to-end framework for developing custom knowledge graphs driven 
analytics for arbitrary application domains. 
The uniqueness of our system lies A) in its combination of curated KGs along with 
knowledge extracted from unstructured text, B) support for advanced trending and explanatory 
questions on a dynamic KG, and C) the ability to 
answer queries where the answer is embedded across multiple data sources.

What does NOUS mean? "The capacity to reason with experiential knowledge."  See [here](https://en.wikipedia.org/wiki/Nous) and [there](http://biblehub.com/greek/3563.htm).

## Introduction	
NOUS provides complete suite of capabilities needed to build a domain specific knowledge graph 
from streaming data. This includes 
1) Natural language processing(NLP), 
2) Entity and relationship mapping, 
3) Confidence Estimation using Link Prediction. 
4) Rule Learning/Trend Discovery using Frequent Graph Mining
5) Question Answering using Graph Search 

### NOUS Project Structure :

* TripleExtractor: Contains NLP code, takes text document as input 
and produces triples of the for
subject, predicate, object 
* EntityDisambiguation : Entity linking to a given KG, (implements the algorithm in 
Collective Entity Linking in Web Text: A Graph-based Method, Han et al, SIGIR 2011)  
* Mining : Given a streaming graph, find frequent patterns
* Search : Given a attributed graph and entity pairs, return all paths 
* Link Prediction: Confidence estimation of each link in the graph using Naive Bayes 

## How to build and execute NOUS:
### Prerequisites
* Java 1.7 OR above
* [Maven](https://maven.apache.org/install.html) 3.0 or above
* [Apache Spark](https://spark.apache.org/docs/latest/) 2.0 OR above
* [Scala](https://www.scala-lang.org/) 2.10
* HDFS File System (Optional)

### Build
 Clone github repository 

` git clone https://github.com/streaming-graphs/NOUS.git NOUS `

All NOUS modules (except LinkPrediction) are written in scala and can be compiled with maven. LinkPrediction is written in Python and can be run directly. Perform maven build in any of the module : `TripleExtractor` OR `Mining` Ex:
 
 ```bash
 cd [Repo_Home]/TripleExtractor
 mvn package
 ```
Here `[Repo_Home]` is the path to your cloned directory `NOUS`. 

### Run Hello World
NOUS is organized into multiple modules that support the KG workflow. Each module 
contains README and data to run the examples. Refer to module's README for further details.

## Publications and Presentations
1. Choudhury S, K Agarwal, S Purohit, B Zhang, M Pirrung, W Smith, and M Thomas.  2017. “NOUS: Construction and Querying of Dynamic Knowledge Graphs.” 8th International Workshop on Data Engineering meets the Semantic Web. [link](https://arxiv.org/abs/1606.02314)
2. Zhang B, S Choudhury, M Al-Hasan, X Ning, P Pesantez, S Purohit, and K Agarwal.  2016.  "Trust from the past: Bayesian Personalized Ranking based Link Prediction in Knowledge Graphs."  In 2016 SIAM Data Mining Workshop on Mining Networks and Graphs: A Big Data Analytic Challenge. [link](https://arxiv.org/abs/1601.03778)
3. Choudhury S, K Agarwal, S Purohit. 2016. "Navigating the Maps of Science" [link](https://www.slideshare.net/SutanayChoudhury/navigating-the-maps-of-science)
4. Choudhury S, C Dowling. 2014. "Benchmarking Named Entity Disambiguation approaches for Streaming Graphs." [link](https://arxiv.org/abs/1407.3751)
