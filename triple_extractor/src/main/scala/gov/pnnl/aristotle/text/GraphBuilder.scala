package gov.pnnl.aristotle.text
import gov.pnnl.aristotle.text.datasources._
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

    val inputListRDD = sc.textFile(fileList).filter(entry => new File(entry).isFile && entry.endsWith(".json"))

    val inputFormat = appConf("INPUT_FORMAT")
    val urlTextPairs = inputListRDD.flatMap(path => {
          inputFormat match {
            case "text" => SimpleDocParser.getUrlTextPairs(path)
            case "web" => WebCrawlParser.getUrlTextPairs(path)
            case "wsj" => WSJParser.getUrlTextPairs(path)
          }
        })
    println("****************** NUMBER OF URL text pairs = " + urlTextPairs.count)
    // inputListRDD.unpersist()
    // urlTextPairs.saveAsTextFile(outPath + ".prov")
    val triples = urlTextPairs.flatMap(urlTextPair => 
        TripleParser.getTriples(urlTextPair._2))
    
    println("****** FINISHED PARSING TRIPLES : " + triples.count)
    // urlTextPairs.unpersist()
    // val pw = new java.io.PrintWriter(new File(outPath))
    // triples.foreach(triples => pw.println(triples.mkString("_")))
    // triples.foreach(t => pw.println(t))
    // pw.close()
    // tripleParser.srlOutputWriter.close()

    // println("Storing " + triples.count + " triples")
    triples.saveAsTextFile(outPath)
  }
}
