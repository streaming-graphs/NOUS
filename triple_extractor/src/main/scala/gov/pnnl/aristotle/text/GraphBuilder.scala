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
    
    val sc = if (appConf.contains("SPARK_MASTER_URL"))
                new SparkContext(new SparkConf().setAppName("TripleParser")
                                                .setMaster(appConf("SPARK_MASTER_URL")))
             else 
                new SparkContext(new SparkConf().setAppName("TripleParser"))
   

    println("FILE1  ")
    println("NUMBER OF LINES IN FILE.LIST = " + sc.textFile("file.list").count)
    println("FILE2  ")
    println("NUMBER OF LINES IN FILE.LIST = " + sc.textFile("/projects/nous/file.list").count)

    val inputListRDD = sc.textFile(fileList).filter(entry => {
      println("FILTERING " + entry + "isFile = " + new File(entry).isFile + " isJson = " + entry.endsWith(".json"))
      // new File(entry).isFile && entry.endsWith(".json")
      entry.endsWith(".json")
    })
    
    println(" FIle list", inputListRDD.count)
    inputListRDD.collect.foreach(println(_))
    
   
    val inputFormat = appConf("INPUT_FORMAT")
    val triples = inputListRDD.flatMap(path => {
          println( " RUNNING ON" ,InetAddress.getLocalHost.getHostAddress)
          inputFormat match {
            //case "text" => SimpleDocParser.getUrlTextPairs(path).flatMap(p => TripleParser.getTriples(p._2))
            //case "web" => WebCrawlParser.getUrlTextPairs(path).flatMap(p => TripleParser.getTriples(p._2))
            /*case "wsj" => {
              println("processing WSJ")
              WSJParser.getUrlTextPairs(path).flatMap(p => List(Triple("1", "1", "1", 0.9)))
            }*/
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
