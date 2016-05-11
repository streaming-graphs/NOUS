package gov.pnnl.aristotle.utils

import org.apache.spark.{SparkContext,SparkConf}
import scala.io.Source
import scala.collection.Map
import org.apache.spark.rdd._
import scala.Array.canBuildFrom
import scala.collection.LinearSeq
import scala.collection.immutable.Vector
import java.nio.file.{Paths, Files}



class MentionData(val entityType: String, val initialEvidenceWeight: Double) { 
  
  override def toString() :String = {
    entityType + ";" + initialEvidenceWeight.toString
  }
}

class NLPTriple(val subj : String, val pred : String, val obj : String) extends Serializable
class NLPTripleWithTimeWithUrl(val subj : String, val pred : String, val obj : String, 
    val ts: String, val url: String) extends Serializable

object NLPTripleParser {
  
  type EntityLabel = String
  type EntityTypeMap = Map[EntityLabel, MentionData]
  
  def main(args: Array[String]) : Unit = {
    val sparkConf = new SparkConf().setAppName("PathSearch")
    val sc = new SparkContext(sparkConf)
   
    println("starting from main")
    if(args.length !=1) {      println("Usage <pathToTriplesFIle>" )
      exit
    } 
    //Assumption:format of triple file
    // Each line is either a triple separated by "\t" or 
    // the line is a  ";"
    // ";" indicates end of a line/paragraph/block (entities with this block will be considered together during disambiguation
    val allTriples: List[List[NLPTriple]] = readTriples(args(0), sc)
    println("No of blocks=" + allTriples.size)
    //println("size of each block=")
    //allTriples.foreach(v => println(v.size))
    
    // For each set of entities in one block
    for(triplesInBlock <- allTriples){
      val entityTypeMap = getEntitiesWithTypeMapFromTriples(triplesInBlock)
      println("No of unique entities in block", entityTypeMap.size)
      entityTypeMap.foreach(v => println(v._1 +"->"+ v._2))
      println("end of block")
    }
  }
  
  def getEntitiesWithTypeMapFromTriples(sentenceTriples: List[NLPTriple]) : EntityTypeMap = {
    var entityTypeMap : EntityTypeMap = Map.empty 
    //allEntities Map contains list of entities in sentence along with their type (or "", if not available)
    //Note : if an entity has multiple "IS-A" identified, the map will retain the last one
    //val numberISAPred = sentenceTriples.count(_.pred.toUpperCase() == "IS-A")
    
    // Find number of entities in the list of triples 
    val allEntities = sentenceTriples.flatMap(triple => Set(triple.obj, triple.subj)).filter(mention => {
      val uMention = mention.toUpperCase
      (uMention != "LOC" && uMention != "ORG" && uMention != "PER" && uMention != "MISC" && uMention != "NONE")
    }).toSet
    println("All Entities =", allEntities.toString)
    val numberOfUniqueEntities =  allEntities.size
     
    
    for(triple <- sentenceTriples){
      
      if(triple.pred == "IS-A"){
       val yagoType : String = predicateTypeMapper.typeMap(triple.obj)
       entityTypeMap = entityTypeMap.+((triple.subj, new MentionData(yagoType, 1.0/numberOfUniqueEntities)))
      } else {
        if(!entityTypeMap.contains(triple.subj))
          entityTypeMap = entityTypeMap.+((triple.subj, new MentionData("", 1.0/numberOfUniqueEntities)))     
        if(!entityTypeMap.contains(triple.obj))
          entityTypeMap = entityTypeMap.+((triple.obj, new MentionData("", 1.0/numberOfUniqueEntities)))
      }
    }
    return entityTypeMap
  }
  
  def readTriples(filepath: String, sc: SparkContext): List[List[NLPTriple]] = {
    var entityListParagraph: List[List[NLPTriple]] = List.empty
    var entityListLine : List[NLPTriple] = List.empty
    
    for(line <- Source.fromFile(filepath).getLines) {
      if(line.length > 0) {
        if(line.head == ';'){       
          entityListParagraph = entityListParagraph.::(entityListLine)
          entityListLine  = List.empty
        }
        else
        {
          //println(line)
          val arr = line.split("\t")
          if(arr.length ==3 || arr.length ==4)  {
            val triple: NLPTriple = new NLPTriple(arr(0).trim, arr(1).trim, arr(2).trim)
            entityListLine = entityListLine.::(triple)
           //println("number of triples for this paragraph", entityListLine.size)
          }
          else
            println("length is less than 3", line)
        }
      }
    }
    return entityListParagraph
  }

  def getEntitiesWithTypeMapFromTriplesWithTimeWithUrl(triplesInBlock: 
      List[NLPTripleWithTimeWithUrl]): EntityTypeMap = {
      var entityTypeMap : EntityTypeMap = Map.empty 
    //allEntities Map contains list of entities in sentence along with their type (or "", if not available)
    //Note : if an entity has multiple "IS-A" identified, the map will retain the last one
    //val numberISAPred = sentenceTriples.count(_.pred.toUpperCase() == "IS-A")
    
    // Find number of entities in the list of triples 
    val allEntities = triplesInBlock.flatMap(triple => Set(triple.obj, triple.subj)).filter(mention => {
      val uMention = mention.toUpperCase
      (uMention != "LOC" && uMention != "ORG" && uMention != "PER" && uMention != "MISC" && uMention != "NONE")
    }).toSet
    println("All Entities =", allEntities.toString)
    val numberOfUniqueEntities =  allEntities.size
     
    
    for(triple <- triplesInBlock){
      
      if(triple.pred == "IS-A"){
       val yagoType : String = predicateTypeMapper.typeMap(triple.obj)
       entityTypeMap = entityTypeMap.+((triple.subj, new MentionData(yagoType, 1.0/numberOfUniqueEntities)))
      } else {
        if(!entityTypeMap.contains(triple.subj))
          entityTypeMap = entityTypeMap.+((triple.subj, new MentionData("", 1.0/numberOfUniqueEntities)))     
        if(!entityTypeMap.contains(triple.obj))
          entityTypeMap = entityTypeMap.+((triple.obj, new MentionData("", 1.0/numberOfUniqueEntities)))
      }
    }
    return entityTypeMap
  }
  
  
  def readTriplesWithTimestampWithUrl(filepath: String, sc: SparkContext): 
  Map[String, List[NLPTripleWithTimeWithUrl]] = {
     var entityListParagraph: List[List[NLPTripleWithTimeWithUrl]] = List.empty
    var entityListLine : List[NLPTripleWithTimeWithUrl] = List.empty
    
    for(line <- Source.fromFile(filepath).getLines) {
      if(line.length > 0 && !line.startsWith("#")) {
        //println(line)
        val arr = line.split("\t")
        if(arr.length == 5)  {
          val triple: NLPTripleWithTimeWithUrl = new NLPTripleWithTimeWithUrl(arr(0).trim, arr(1).trim, arr(2).trim, arr(3).trim, arr(4).trim)
          entityListLine = entityListLine.::(triple)
        }
      }
    }
    entityListLine.groupBy(_.url)
    /*   
    val allTriplesWithTimeWithUrl = sc.textFile(filepath).
    filter(line => line.length() > 0 && !line.startsWith("#")).
    map(line => line.split("\t")).filter(arr => arr.length == 5).
    map(arr => new NLPTripleWithTimeWithUrl(arr(0).trim, arr(1).trim, arr(2).trim, 
                arr(3).trim, arr(4).trim))
    
    
    println("Number of valid triples =", allTriplesWithTimeWithUrl.count)
    val allTriplesGroupedByDoc = allTriplesWithTimeWithUrl.groupBy(_.url)
    return allTriplesGroupedByDoc
  
  * 
  */
    }
  
  
}