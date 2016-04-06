package gov.pnnl.aristotle.utils

import scala.io.Source
import scala.xml.XML
import util.control.Breaks._
import scala.collection.mutable.HashMap
import scala.collection.immutable.Vector

class Query(qid: String, qphrase: String, qfilepath: String, eid: String, elabel:String){
  var query_id: String = qid
  var query_phrase : String = qphrase 
  var query_context_file : String = qfilepath
  var entity_id_string: String = eid
  var entity_label: String = elabel
}

object TAC_Utils {

  def GetQueryContext(dirPath: String, filePath: String): String ={
    return ReadQueryContextFile(dirPath + "//" + filePath) //queryObj.query_context_file)
  }
  
  def ReadQueryContextFile(filename: String): String ={
   val lines = Source.fromFile(filename).getLines().filterNot(line => line.startsWith("<"))
   return lines.mkString(" ").replaceAll("\"", " ")
  }

  def ReadEntityIdLabelFile(filename:String) : HashMap[String, String] ={
    var entityIdToLabel = new HashMap[String, String]
    var line: String = ""
    try{
        for (line <- Source.fromFile(filename).getLines()) {
           if (line.startsWith("@") || line.startsWith("#") || line.isEmpty() ) { ; }
           else {
              val arr = line.split('\t')
              if(arr.length != 2) { print("Invalid line format" + line) }
              else{
                var entity = arr(0)
                var label = arr(1)
                //println(entity +"->"+ label)
                 if(entityIdToLabel.contains(entity)) print("Dupilcate entity id found", entity, label)
                 else entityIdToLabel.put(entity, label)
             }
           }
        }
      } catch {
        case ex: Exception => println("problem reading entity to label list file")
      }
    return entityIdToLabel
  }
  
  def ReadAllQueriesXML(queryListFile: String, numQueries: Int): xml.NodeSeq ={
    val allQueries = XML.loadFile(queryListFile)
    val limitQueries = (allQueries \\ "query")
    return limitQueries.dropRight(limitQueries.length-numQueries)
  }
}