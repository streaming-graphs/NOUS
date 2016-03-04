package gov.pnnl.aristotle.utils



import org.apache.spark.{SparkContext,SparkConf}
import scala.io.Source
import scala.collection.Map

import scala.Array.canBuildFrom
import scala.collection.LinearSeq
import scala.collection.immutable.Vector
import java.nio.file.{Paths, Files}

class Triple(subject_ : String, predicate_ : String, object_ : String) {
 val subj = subject_
 val pred = predicate_
 val obj = object_
}



object ReadTriples {
  type EntityLabel = String
  type EntityTypeMap = Map[EntityLabel, String]
  def main(args: Array[String]) : Unit = {
    val sparkConf = new SparkConf().setAppName("PathSearch").setMaster("local")
    val sc = new SparkContext(sparkConf)
   
    println("starting from main")
    if(args.length !=1) {      println("Usage <pathToTriplesFIle>" )
      exit
    } 
    //Assumption:format of triple file
    // Each line is either a triple separated by "\t" or 
    // the line is a  ";"
    // ";" indicates end of a line/paragraph/block (entities with this block will be considered together during disambiguation
    val allTriples: List[List[Triple]] = readTriples(args(0), sc)
    println("No of blocks=" + allTriples.size)
    println("size of each block=")
    allTriples.foreach(v => println(v.size))
    
    // For each set of entities in one block
    for(triplesInBlock <- allTriples){
      val entityTypeMap = getEntitiesWithTypeMapFromTriples(triplesInBlock)
      println("No of unique entities in block", entityTypeMap.size)
      entityTypeMap.foreach(v => println(v._1 +"->"+ v._2))
      println("end of block")
    }
  }
  
  def getEntitiesWithTypeMapFromTriples(sentenceTriples: List[Triple]) : EntityTypeMap = {
    var entityTypeMap : EntityTypeMap = Map.empty 
    //allEntities Map contains list of entities in sentence along with their type (or "", if not available)
    //Note : if an entity has multiple "IS-A" identified, the map will retain the last one
    for(triple <- sentenceTriples){
      
      if(triple.pred == "IS-A"){
       val yagoType : String = predicateTypeMapper.typeMap(triple.obj)
       entityTypeMap = entityTypeMap.+((triple.subj, yagoType))
      } else {
        if(!entityTypeMap.contains(triple.subj))
          entityTypeMap = entityTypeMap.+((triple.subj, ""))     
        if(!entityTypeMap.contains(triple.obj))
          entityTypeMap = entityTypeMap.+((triple.obj, ""))
      }
    }
    
    return entityTypeMap
  }

  def readTriples(filepath: String, sc: SparkContext): List[List[Triple]] = {
    var entityListParagraph: List[List[Triple]] = List.empty
    var entityListLine : List[Triple] = List.empty
    
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
          if(arr.length ==3)  {
            val triple: Triple = new Triple(arr(0).trim, arr(1).trim, arr(2).trim)
            entityListLine = entityListLine.::(triple)
           //println("number of triples for this paragraph", entityListLine.size)
          }
          else
            println("length is not 3")
        }
      }
    }
    println("returning", entityListParagraph.size )
    return entityListParagraph
  }
}