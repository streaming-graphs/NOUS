package gov.pnnl.aristotle.text
import gov.pnnl.aristotle.text.datasources._
import java.net._
import org.apache.spark._
import java.io.File
import java.io.PrintWriter


object GraphBuilder {
  def parseConf(configPath: String): Map[String, String] = {
    val keyValues = scala.io.Source.fromFile(configPath)
        .getLines
        .toList
        .map(_.split("="))
    var config = Map.empty[String, String]
    keyValues.foreach(p => config = config + (p(0) -> p(1))) 
    config
  }

  def main(args: Array[String]) = {
    val appConf = parseConf(args(0))
    val fileList = appConf("INPUT_LIST")
    val outPath = appConf("OUTPUT_DIR")
    val version  = System.getProperty("java.version")
    print("JAVA VERSION IS " + version)
    
    val sc = if (appConf.contains("SPARK_MASTER_URL"))
                new SparkContext(new SparkConf().setAppName("TripleParser")
                                                .setMaster(appConf("SPARK_MASTER_URL")))
             else 
                new SparkContext(new SparkConf().setAppName("TripleParser"))
   

    val inputListRDD = sc.textFile(fileList).filter(entry => {
      // new File(entry).isFile && entry.endsWith(".json")
      entry.endsWith(".json")
    })
    
    val inputFormat = appConf("INPUT_FORMAT")
    val triples = inputListRDD.flatMap(path => {
          inputFormat match {
            case "text" => SimpleDocParser.getUrlTextPairs(path).flatMap(p => TripleParser.getTriples(p._2))
            case "web" => WebCrawlParser.getUrlTextPairs(path).flatMap(p => TripleParser.getTriples(p._2))
            case "wsj" => WSJParser.getUrlTextPairs(path).flatMap(p => TripleParser.getTriples(p._2))
            case _ => {
              println(" NO MATCH to format")
              List(Triple("1", "1", "1", 0.9))
            }
            
          }
        }).cache()

    // val triples = urlTextPairs.flatMap(urlTextPair => 
        // TripleParser.getTriples(urlTextPair._2)).cache()

    println("****** FINISHED PARSING TRIPLES : " + triples.count)
    triples.saveAsTextFile(outPath)
 
  }
}
