package gov.pnnl.aristotle.text.datasources
import gov.pnnl.aristotle.text.datasources.WebFeatureExtractor
import gov.pnnl.aristotle.text.Triple
import gov.pnnl.aristotle.text.TripleParser

object Plugins {
  def parseAerialtronics(jsonPath: String): List[Triple] = {
    val results = WebFeatureExtractor.getMap(jsonPath)
    val sd = results("structured_data").asInstanceOf[Map[String, List[Map[String, Any]]]]
    val properties = sd("items")(0)("properties").asInstanceOf[Map[String, List[String]]]
    val description = properties("http://ogp.me/ns#description")(0)
    val timestamp = properties("article:published_time")(0)
    val url = properties("http://ogp.me/ns#url")(0)
    val triples = TripleParser.getTriples(description).map(t => {
      Triple(t.sub, t.pred, t.obj, timestamp, url, t.conf)
    })
    triples
/*
    val description = sd("properties")("http://ogp.me/ns#description").asInstanceOf[String]
    val timestamp = sd("properties")("http://ogp.me/ns#updated_time").asInstanceOf[String]
    println("description = " + description)
    println("timestamp = " + timestamp)
    */
  }

  def parseAmazon(jsonPath: String, id: String): List[Triple] = {
    val lines = WebFeatureExtractor.get("text", jsonPath).split("\n")
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

  def main(args: Array[String]) = {
    // val testDoc = "/mnt/openke/drone/20160107/00000152-1dba-ba87-0a11-0c010001ed03.json"
    val soureList = scala.io.Source.fromFile(args(0)).getLines.toList
    for (line <- soureList) {
      val tokens = line.split("\t")
      if (tokens(1).startsWith("http://www.amazon.com")) {
        val urlTokens = tokens(1).split("/")
        val id = "amzn_" + urlTokens(3) 
        val triples = parseAmazon(tokens(0), id)
        triples.foreach(println)
      }
      else if (tokens(1).startsWith("http://www.aerialtronics.com/20")) {
        val triples = parseAerialtronics(tokens(0))
        triples.foreach(println)
        // triples.foreach(t=> println("[" + t.sub + " -> " + t.pred + " -> " + t.obj + "]"))
      }
    }
  }
}
