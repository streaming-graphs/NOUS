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
  
 // def readEntityId
  
  def ReadAllQueriesXML(queryListFile: String, numQueries: Int): xml.NodeSeq ={
    val allQueries = XML.loadFile(queryListFile)
    val limitQueries = (allQueries \\ "query")
    return limitQueries.dropRight(limitQueries.length-numQueries)
  }
  
 /* def readAllQueries(queryListFile: String, numQueries: Int, entityToLabelFile: String): Array[Query] ={
    
    val entityIdToLabelMap = readEntityIdLabelFile(entityToLabelFile)
    var queryList = new Array[Query](numQueries) 
    var queryId: String = ""
    var queryPhrase: String = ""
    var docId: String = ""
    var entityId: String = ""
    var entityLabel: String = ""
    var i = 0

    for (lineFull <- Source.fromFile(queryListFile).getLines()) {
        val line = lineFull.trim()
        if(i >= numQueries) return queryList
        if (line.startsWith("<query id")) {
          val pos = line.indexOf("=")
          queryId = line.substring(pos, line.length)
         
        } else if(line.startsWith("<name>") && line.endsWith("</name")) {
          val pos1 = line.indexOf(">")
          val pos2 = line.lastIndexOf("<")
          queryPhrase = line.substring(pos1+1, pos2)
        }else if(line.startsWith("<docid>") && line.endsWith("</docid")) {
          val pos1 = line.indexOf(">")
          val pos2 = line.lastIndexOf("<")
          docId = line.substring(pos1+1, pos2)
        }else if(line.startsWith("<entity>") && line.endsWith("</entity")) {
          val pos1 = line.indexOf(">")
          val pos2 = line.lastIndexOf("<")
          entityId = line.substring(pos1+1, pos2)
          entityLabel  = entityIdToLabelMap(entityId)
        }else if(line.startsWith("</query")) {
          val queryObj = new Query(queryId, queryPhrase, docId, entityId, entityLabel)
          queryList(i) = queryObj
          i+=1
        } else if(line.startsWith("</kbpentlink") || line.startsWith("<kbpentlink") || line.startsWith("?xml")) {}
        else
          print("Unknown format:" + line)
          return queryList
         
      }

    return queryList
  }
  * /
  */ 
}