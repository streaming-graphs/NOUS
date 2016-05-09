package gov.pnnl.aristotle.text.datasources
// import gov.pnnl.aristotle.text.datasources.WebFeatureExtractor
import gov.pnnl.aristotle.text.Triple
import gov.pnnl.aristotle.text.TripleParser
import scala.util.control.Breaks._
import scala.util.parsing.json._

object Plugins {

  def getJsonData(jsonPath: String): Map[String, Any] = { 
    val doc = scala.io.Source.fromFile(jsonPath).mkString
    val result = JSON.parseFull(doc).getOrElse(Map.empty[String, Any])
        .asInstanceOf[Map[String, Any]]
    result
  }

  def getTextContentDateUrl(jsonData: Map[String, Any], contentKey: String = "text", timeKey: String = "Date", urlKey: String = "url"): (String, String, String) = {
    val text = jsonData("text").asInstanceOf[String]
    // val timestamp = jsonData(timeKey).asInstanceOf[String] 
    var timestamp = ""
    if (jsonData.contains("Date")) {
      timestamp = jsonData("Date").asInstanceOf[String]
    }
    else {
      if (jsonData.contains("http_headers")) {
        val httpHeaders = jsonData("http_headers").asInstanceOf[Map[String, String]]
        timestamp = httpHeaders("Date")
      }
    }
    val url = jsonData("url").asInstanceOf[String] 
    (text, timestamp, url)
  }

  def autoDetectFormat(jsonData: Map[String, Any]): Int = {
    val openGraphFormat1 = jsonData.contains("structured_data")
    val openGraphFormat2 = jsonData.contains("open_graph")
    var format = -1
    if (!openGraphFormat2 && openGraphFormat1) {
      val sd = jsonData("structured_data").asInstanceOf[Map[String, List[Map[String, Any]]]]
      if (sd != null) format = 1
    } 
    else if (openGraphFormat2) {
      val ogData = jsonData("open_graph").asInstanceOf[Map[String, List[String]]]
      if (ogData != null) format = 2
    }
    else {
      val mimeType = jsonData("mime_type").asInstanceOf[String]
      if (mimeType == "text/html") {
        format = 0
      }
    }
    format
  }

  def getFormatDescription(format: Int): String = format match {
    case 0 => "Full Text"
    case 1 => "OpenGraph (structured_data)"
    case 2 => "OpenGraph (open_graph)"
    case _ => "Unsupported format"
  }

  def getOpenGraphContentDateUrl(jsonData: Map[String, Any], format: Int): (String, String, String, String) = {
    // val openGraphFormat1 = jsonData.contains("structured_data")
    // val openGraphFormat2 = jsonData.contains("open_graph")
    var answer = ("", "", "", "")
    // if (!openGraphFormat2 && openGraphFormat1) {
    if (format == 1) {
      // println("Processing structured_data ...")
      val sd = jsonData("structured_data").asInstanceOf[Map[String, List[Map[String, Any]]]]
      if (sd.contains("items")) {
        val properties = sd("items")(0)("properties").asInstanceOf[Map[String, List[String]]]
        val title = properties.getOrElse("http://ogp.me/ns#title", List(""))(0)
        val description = properties.getOrElse("http://ogp.me/ns#description", List(""))(0)
        val timestamp = properties.getOrElse("article:published_time", List(""))(0)
        val url = properties.getOrElse("http://ogp.me/ns#url", List(""))(0)
        answer = (title, description, timestamp, url)
      }
    }
    // else if (openGraphFormat2) {
    else if (format == 2) {
      // println("Processing open_graph ...")
      val ogData = jsonData("open_graph").asInstanceOf[Map[String, List[String]]]
      val title = ogData.getOrElse("og:title", List(""))(0)
      val desc = ogData.getOrElse("og:description", List(""))(0)
      val timestamp = jsonData("crawled_dt").asInstanceOf[String]
      val url = ogData.getOrElse("og:url", List(""))(0)
      answer = (title, desc, timestamp, url)
    }
    answer
  }

  def parseOpenGraphData(jsonData: Map[String, Any], format: Int) = {
    // (title, desc, timestamp, url) = getOpenGraphContentDateUrl(jsonPath)
    val result = getOpenGraphContentDateUrl(jsonData, format)
    if (result._3 != "" && result._1 != "") {
      println(result._3 + ", " + result._1)
    }
  }

  def getOpenGraphTriples(jsonData: Map[String, Any], format: Int): List[Triple] = {
    val result = getOpenGraphContentDateUrl(jsonData, format)
    val triples = TripleParser.getTriples(result._2).map(t => {
      Triple(t.sub, t.pred, t.obj, result._3, result._4, t.conf)
    })
    triples
  }

  def getTextTriples(jsonData: Map[String, Any]): List[Triple] = {
    val data = getTextContentDateUrl(jsonData)
    val triples = TripleParser.getTriples(data._1).map(t => {
      Triple(t.sub, t.pred, t.obj, data._2, data._3, t.conf)
    })
    triples
  }
  def parseDroneAnalystSnippets(jsonPath: String): List[Triple] = {
    // Only processing urls ending with embed
    val jsonData = getJsonData(jsonPath)
    val extractedInfo = getTextContentDateUrl(jsonData)
    val lines = extractedInfo._1.split("\n")
    var content = ""
    breakable {
      for (line <- lines) {
        if (line.contains("Continue reading")) {
          content = line
          break
        } 
      }
    }
    // println(content)
    TripleParser.getTriples(content)
  }

  def parseAmazon(jsonPath: String, id: String): List[Triple] = {
    val jsonData = getJsonData(jsonPath)
    val lines = jsonData("text").asInstanceOf[String].split("\n")
    // val lines = WebFeatureExtractor.get("text", jsonPath).split("\n")
    var title = ""
    var nextParse = ""
    val featureBuf = new scala.collection.mutable.ListBuffer[String]
    for (line <- lines) {
      if (nextParse == "") {
        if (line.startsWith("Title")) {
          nextParse = "t"
        }
      }
      else {
        if (nextParse == "t") {
          title = line
          nextParse = "f"
        } 
        else if (nextParse == "f") {
          if (line.startsWith("Show") || line.startsWith("Product") || line.startsWith("Details")) {
            nextParse = "" 
          }
          else {
            if (line != "") {
              featureBuf += line
            }
          } 
        } 
      }
    } 
    // println("PRODUCT = " + title)
    val tripleBuf = new scala.collection.mutable.ListBuffer[Triple]
    tripleBuf += Triple(id, "rdfs:label", title)
    tripleBuf += Triple(id, "rdf:type", "drone")
    
    val features = featureBuf.toList
    features.foreach(f => {tripleBuf += Triple(id, "nous:feature", f)})
    // println("FEATURES = " + featureBuf.toList)
    tripleBuf.toList
  }
    
  // def parseWikipedia(lines: Array[String]): List[Triples] = {
  
  def saveTriples(
        path: String, 
        triples: List[Triple], 
        tripleWriter: java.io.PrintWriter, 
        logger: java.io.PrintWriter) = {
    triples.foreach(t => tripleWriter.println(t))
    // val tsFormat = new java.text.SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    // val timestamp = tsFormat.format(java.util.Calendar.getInstance().getTime())
    // logger.println(timestamp + " wrote " + triples.size + " triples from " + path)
    logger.println("   number of output triples = " + triples.size)
  }

  def main(args: Array[String]) = {
    if (args.length < 2) {
      println("Missing arguments: input-file-list output-prefix")
      System.exit(1)
    }
    // val testDoc = "/mnt/openke/drone/20160107/00000152-1dba-ba87-0a11-0c010001ed03.json"
    val inputListPath = args(0)
    val outputPrefix = args(1)
    val soureList = scala.io.Source.fromFile(inputListPath).getLines.toList
    val tripleWriter = new java.io.PrintWriter(outputPrefix + ".ttl")
    val logger = new java.io.PrintWriter(outputPrefix + ".log")

    for (line <- soureList) {
      val tokens = line.split("\t")
      val jsonPath = tokens(0)
      logger.println("Processing " + jsonPath)
      if (tokens(1).startsWith("http://www.amazon.com")) {
        val urlTokens = tokens(1).split("/")
        val id = "amzn_" + urlTokens(3) 
        val triples = parseAmazon(tokens(0), id)
        saveTriples(jsonPath, triples, tripleWriter, logger)
      }
      else if (tokens(1).startsWith("http://droneanalyst.com") && tokens(1).endsWith("embed")) {
        val triples = parseDroneAnalystSnippets(jsonPath)
        saveTriples(jsonPath, triples, tripleWriter, logger)
      }
      else {
        val jsonData = getJsonData(jsonPath)
        val format = autoDetectFormat(jsonData)
        logger.println("   detected format: " + getFormatDescription(format))
        if (format == 0) {
          val triples = getTextTriples(jsonData)
          saveTriples(jsonPath, triples, tripleWriter, logger)
        }
        else if (format == 1 || format == 2) {
          val triples = getOpenGraphTriples(jsonData, format)
          saveTriples(jsonPath, triples, tripleWriter, logger)
        }
      }
    }
    tripleWriter.close
    logger.close
  }
}
