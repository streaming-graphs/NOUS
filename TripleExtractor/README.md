
## Triple Extractor Introduction  

This module is used to extract entity triples in the format of (subjective, predicate, objective). For example, the triple for sentence "Tom stands on the chair" should be (Tom, stands on, the chair). So this module is designed for the  scenario where the entity triple is needed given sentences.  


##  Build and Execute :
###  Prerequisites
* Java 1.7 OR above
* Scala 2.10
* Maven
* Apache Spark 2.1.0 OR above


### Build
 Clone github repository 

` clone https://github.com/streaming-graphs/NOUS.git NOUS `

 Perform maven build in the module : `TripleExtractor` Ex:
 
 ```bash
 cd [Repo_Home]/TripleExtractor
 mvn package -DskipTests
 ```
Here `[Repo_Home]` is the path to your cloned directory `NOUS`. 

### Run Example

Triple extractor supports NLP of text and supports multiple text formats (Text only, OpenGraph Protocol, JSON). After build success, to run the triple extractor example
[triple-extractor.input](https://github.com/streaming-graphs/NOUS/tree/master/TripleExtractor/examples/triple-extractor/triple-extractor.input) :

`cd [Repo_Home]/TripleExtractor`

```java
java -cp target/uber-TripleParser-0.1-SNAPSHOT.jar gov.pnnl.aristotle.text.TripleParser ./examples/triple-extractor/triple-extractor.input 
```  

Your output on the screen should contain the original sentence, pos tag, NER output, OpenIE output, Purged output, and Final output and looks like [output1](https://github.com/streaming-graphs/NOUS/tree/master/TripleExtractor/examples/triple-extractor/output1)  

to run the triple extractor example based on more complex rules :  

`cd [Repo_Home]/TripleExtractor`

```java
java -cp target/uber-TripleParser-0.1-SNAPSHOT.jar gov.pnnl.aristotle.text.TripleParser ./examples/triple-extractor/triple-extractor.input 1 
```   

Your output on the screen shuold contain the original sentence, NER output, OpenIE output, Purged output, Final output and looks like [output2](https://github.com/streaming-graphs/NOUS/tree/master/TripleExtractor/examples/triple-extractor/output2)