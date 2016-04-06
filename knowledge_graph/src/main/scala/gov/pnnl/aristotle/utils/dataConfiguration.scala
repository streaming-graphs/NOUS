package gov.pnnl.aristotle.utils

import scala.collection.Map
import org.apache.spark.graphx._
import org.apache.spark.rdd._




object KGraphProp {
  
  val edgeLabelNeighbourEntity = "linksTo"
  val edgeLabelNodeType = "rdf:type"
  val edgeLabelNodeAlias : List[String]= List("rdfs:label", "skos:prefLabel", "isPreferredMeaningOf")
  val aliasSep: String = " <Alias> "
   
  //val ontologyTree : Graph[String, String]
    
    //TODO:KHUSHBU (REWRITE THIS TO CONSIDER TYPE HEIRRACHY)
  def compareTypesUsingHeirarchy(suggestedType: String , nodeActualType: String): Double = {
    if(nodeActualType.contains(suggestedType))
      return 1 
    else
      return 0
      //return(compareTypesUsingHeirarchy(suggestedType, ontologyTree.getParent(nodeActualType)))
  }

  def getOntologyMap(graph: Graph[String, String]): VertexRDD[(String, List[String])] = {
    val initialMsg: List[String] = List.empty
    val maxIterations = 1
    val activeDirection = EdgeDirection.Out
    
    val pregelGraph = graph.mapVertices((id, label) => List.empty[String])

    val messages =  pregelGraph.pregel[List[String]](initialMsg, maxIterations, activeDirection)(
           (vid, oldMessage, newMessage) => newMessage ++ oldMessage, 
           (triplet) => {
             if(triplet.attr == "rdf:type") 
               Iterator((triplet.srcId, triplet.dstAttr))
             else 
               Iterator.empty}, 
           (msg1, msg2) => msg1 ++ msg2)
           
      val vertexWithType = messages.vertices
      graph.vertices.innerJoin(vertexWithType)((vid, label, typeList) => (label, typeList))
           
  }
  
}

object predicateTypeMapper{
  val typeMap : Map[String, String] = Map(("PER", "person"), ("LOC", "location"), ("ORG", "company"), ("MISC",""))
}