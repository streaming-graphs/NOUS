package gov.pnnl.aristotle.text.datasources
import scala.util.parsing.json._
import org.apache.tika.parser.AutoDetectParser
import org.apache.tika.Tika
import org.apache.tika.sax.BodyContentHandler
import org.apache.tika.metadata.Metadata
import org.apache.spark._
import gov.pnnl.aristotle.text.LanguageDetector
import gov.pnnl.aristotle.text.TripleParser

object WebFeatureExtractor {
  class WebSource(val path: String, val url: String) extends Serializable {
    override def toString(): String = path + "\t" + url
  }

  def getUrl(path: String): WebSource = {
    val doc = scala.io.Source.fromFile(path).mkString
    val result = JSON.parseFull(doc).getOrElse(Map.empty[String, Any])
        .asInstanceOf[Map[String, Any]]
    new WebSource(path, result("url").asInstanceOf[String])
  }
  
  def getText(path: String): String = {
    val doc = scala.io.Source.fromFile(path).mkString
    val result = JSON.parseFull(doc).getOrElse(Map.empty[String, Any])
        .asInstanceOf[Map[String, Any]]
    val lines = result("text").asInstanceOf[String]
    if (lines == null) {
      ""
    }
    else {
      lines
    }
  }

  def getFeatures(path: String): Int = {
    val doc = scala.io.Source.fromFile(path).mkString
    val result = JSON.parseFull(doc).getOrElse(Map.empty[String, Any])
        .asInstanceOf[Map[String, Any]]
    val lines = result("text").asInstanceOf[String]
    // print(lines)
    val url = result("url").asInstanceOf[String]

    val stream = new java.io.ByteArrayInputStream(lines.getBytes) 
    val ret = 1
    /* val tika = new Tika()
    println("TIKA OUTPUT = " + tika.detect(sStream))
    println(tika.parseToString(sStream))
*/

    /*val parser = new AutoDetectParser();
    val handler = new BodyContentHandler();
    val metadata = new org.apache.tika.metadata.Metadata();
    parser.parse(new java.io.FileInputStream("/mnt/openke/drone/20160101/00000151-fd48-fc55-0a11-0c0100052a29.json"), handler, metadata);
    println("TIKA OUTPUT = [" + handler.toString() + "]")

    System.exit(1)
    var ret = if (LanguageDetector.isEnglish(lines)) 1 else 0 */
    // if (LanguageDetector.isEnglish(lines)) {
      val t1 = System.currentTimeMillis
      // println("PARSING ***************************")
      val keywords = Set("release", "announced", "accident", "sell", "selling")
      var nlpProc = 0L
      var strProc = 0L
      val parts = lines.split("\n")
      for (p <- parts) {
        val tokens = p.split(" ")
        if (tokens.length > 8) {
          println("++++++++++++++++++++++++++++++++")
          println(p)
          println("********************************")
          val nlp0 = System.currentTimeMillis
          val triples = TripleParser.getTriples(p)
          val nlp1 = System.currentTimeMillis
          nlpProc += (nlp1-nlp0)
          val str0 = System.nanoTime
          var matches = 0
          for (kw <- keywords) {
            if (p.contains(kw)) matches += 1
          }
          val str1 = System.nanoTime
          println("# matches = " + matches)
          println("# triples = " + triples.size)
          strProc += (str1-str0)
          // triples.foreach(println) 
          println("--------------------------------")
        }
      }
      // println("***********************************")
      val t2 = System.currentTimeMillis
      // println("getTriples = " + (t2-t1))
    // }
    println("String matching = " + strProc)
    println("NLP processing = " + nlpProc)
    ret
    // val url = new java.net.URL(result("url").asInstanceOf[String])
    // println("*******************")
    // println(url)
    // println("*******************")
    // println("DETECT OUTPUT by TEXT: " + tika.detect(sStream))
    // println("DETECT OUTPUT by URL: " + tika.detect(new java.net.URL(url)))
/*
       .asInstanceOf[String]
       .split("\n")
       .filter(_ != "")
       .map((new Metadata(path), _))
       lines
*/
  }

  def main(args: Array[String]) = {
    val sc = new SparkContext(new SparkConf().setAppName("UrlExtractor"))
    val prefix = args(0)
    val inputList = "openke/input_lists/months/" + prefix + ".list"
    val fileList = sc.textFile(inputList)
    val file = fileList.first
    println("PROCESSING " + file)
    getFeatures(file)
    // val urlRDD = fileList.map(getFeatures)
    // val urlRDD = fileList.map(getUrl)
    // urlRDD.saveAsTextFile("openke/urldata/" + prefix)
  }

  def processLoop(args: Array[String]) = {
    // WebFeatureExtractor.getFeatures("/mnt/openke/drone/20151124/00000151-3afd-c29d-c0a8-7a01000000d7.json")
    // val sc = new SparkContext(new SparkConf().setAppName("TripleParser")
    val path = "/mnt/openke/drone/20160101/00000151-fd48-fc55-0a11-0c0100052a29.json"
    val inputList = "/home/pnnl/beta/input_lists/months/201601.list"
    var count = 0
    var numProc = 0
    // val files = scala.io.Source.fromFile(inputList).getLines.toList
    val files = List(path)
    val total = files.size
    var procTime = 0L
    for (f <- files) {
      // println("**************************" + numProc)
      val t1 = System.currentTimeMillis
      count += WebFeatureExtractor.getFeatures(f)
      val t2 = System.currentTimeMillis
      procTime += (t2-t1)
      numProc += 1
      if ((numProc % 10) == 0) {
        println("PROCESSED " + numProc + " files/" + total + " in " + procTime + "ms")
        println("ENGLISH = " + count)
        procTime = 0
      }
    }
    // val numEnglishFiles = files.map(f => WebFeatureExtractor.getFeatures(f))
    println("NUMBER OF FILES = " + files.size)
    println("ENGLISH FILES = " + numProc)
    // val fileTypeDist = fileTypes.groupBy(t => t).mapValues(_.size)
    // print(fileTypeDist)
  }
}
