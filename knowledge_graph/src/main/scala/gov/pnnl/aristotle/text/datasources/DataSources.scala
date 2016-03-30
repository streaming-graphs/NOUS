package gov.pnnl.aristotle.text.datasources
import scala.util.parsing.json._

class Metadata(val source: String)

object SimpleDocParser {
  def getUrlTextPairs(path: String): Array[(Metadata, String)] = {
    val lines = scala.io.Source.fromFile(path).getLines.toArray
        .filter(_ != "")
        .map((new Metadata(path), _))
    lines
  }
}

object WSJParser {
  def getUrlTextPairs(path: String): Array[(Metadata, String)] = {
    val doc = scala.io.Source.fromFile(path).mkString
    val result = JSON.parseFull(doc).getOrElse(Map.empty[String, Any])
        .asInstanceOf[Map[String, Any]]
    val metadata = result.getOrElse("meta", Map.empty[String, String])
        .asInstanceOf[Map[String, String]]
    val url = metadata.getOrElse("canonical", "N/A")
    val lines = result.getOrElse("content", "")
        .asInstanceOf[String]
        .split("\n")
        .filter(_ != "")
        .map((new Metadata(url), _))
    lines
  }
}
