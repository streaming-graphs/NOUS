### Triple Extractor  


##### Introduction  

This module is used to extract entity triples in the format of (subjective, predicate, objective). For example, the triple for sentence "Tom stands on the chair" should be (Tom, stands on, the chair). So this module is designed for the  scenario where the entity triple is needed given sentences.  


##### Build and Execute  

* Requirements: Scala 2.10, Java 8, Spark 2.1.0  
* How to build: change your directory to `your-path-to-NOUS/triple_extractor` and run `mvn package -DskipTests`  
* Usage Example: after build success, run `java -cp target/uber-TripleParser-0.1-SNAPSHOT.jar gov.pnnl.aristotle.text.TripleParser ./examples/triple-extractor/triple-extractor.input`. Your output on the screen should contain the original sentence, pos tag, NER output, OpenIE output, Purged output, and Final output. To extract triples based on more complex rules, run `java -cp target/uber-TripleParser-0.1-SNAPSHOT.jar gov.pnnl.aristotle.text.TripleParser ./examples/triple-extractor/triple-extractor.input 1`. Your output on the screen shuold contain the original sentence, NER output, OpenIE output, Purged output, Final output.  

