package gov.pnnl.aristotle.utils


import scala.io.Source
import scala.xml.XML
import util.control.Breaks._
import scala.collection.mutable.HashMap
import scala.collection.mutable.Set
import scala.collection.immutable.Vector


class NYT_Utils {
  
  def ParseTSVFile(filename: String): List[(String, Set[String])] = {
    var allEntityContext = List.empty[(String, Set[String])]
    var line: String = ""
    try{
        for (line <- Source.fromFile(filename).getLines()) {
           if (line.startsWith("@") || line.startsWith("#") || line.isEmpty() ) { ; }
           else {
              val arr = line.split('\t')
              if(arr.length < 3) { 
                //println("No Facets in line" + line.toString()) 
                }
              else { 
                val entityContext = GetEntityAndContext(arr.drop(2))
                allEntityContext = allEntityContext ++ entityContext
                }
           }
        }
    }catch {
        case ex: Exception => println("problem reading entity to label list file", line)
        ex.printStackTrace()
    }
    print("entity list length" , allEntityContext.length)
    return allEntityContext
  }
  
  def GetEntityAndContext( facet_list : Array[String] ) : List[(String, Set[String])] = {

    var entitiesWithContext = List.empty[(String, Set[String])]
    for(facet <- facet_list) {
      val facet_type_and_descr: Array[String] = facet.split(":")
      if(facet_type_and_descr.length == 2) {
     
        val facet_type = facet_type_and_descr.apply(0)
        val facet_val = facet_type_and_descr.apply(1)
        if(facet_type=="per_facet" || facet_type == "org_facet" || facet_type == "geo_facet"){
          var temp_context = Set.empty[String]
          for(c <- facet_list) {
            val arr = c.split(":");
            temp_context += GetFormattedEntity(arr.apply(1))
          }
          val entity = GetFormattedEntity(facet_val)
          val context =  temp_context - entity
          //println("Adding entity: " + entity + ", context=" , context.toString)
          val addEntry:List[(String, Set[String])] = List((entity, context))
          entitiesWithContext = entitiesWithContext++addEntry
        }
      }
    }
    //println(" adding ", entitiesWithContext.length , "entries")
    return entitiesWithContext
  }
             
  def GetFormattedEntity(entity: String) :String ={
    
    if(entity.contains(',') == true) {
      val pos = entity.indexOf(',')
      val lastname = entity.substring(0, pos)
      var firstname = ""
      if(pos+2 < entity.length()) {
        firstname = entity.substring(pos+2, entity.length())
      }
      return (firstname+ "_" + lastname)
    } 
    else
      return entity    
  }
  /*
  def AddFacet(facet_val: String, facet_context: Set[String], entityContext:List[(String, Set[String])] ) = {
    println("Adding entity: " + facet_val + ", context=" , facet_context.toString)
    val x = (facet_val, facet_context)
    entityContext::List(x)
  }
  */
}