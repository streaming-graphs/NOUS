package gov.pnnl.aristotle.utils

import org.apache.spark._
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import org.apache.spark.rdd.RDD
import scala.collection.Set
import scala.collection.immutable.TreeSet
//import scala.xml.XML
//import gov.pnnl.fcsd.datasciences.graphBuilder.nlp.semanticParsers.SennaSemanticParser
import scala.Array.canBuildFrom

abstract class NbrData extends Serializable {
} 

class NbrEdge( val dstId: Long, val dstAttr: String, val edgeAttr : String, 
    val isOutgoing: Boolean = true) extends NbrData {
  def toString(nodeLabel: String, maxEntityLabelSize: Int = Int.MaxValue): String = {
     if(isOutgoing)
           nodeLabel + "\t" + edgeAttr + "\t" + dstAttr
          else 
           dstAttr + "\t" + edgeAttr + "\t" + nodeLabel     
  }
}

class NbrDataWithNbrAttr[VD](val dstId: Long, val dstAttr: VD)  extends NbrData {}

class NbrDataId
object NodeProp {
 
  def getNodeType(g: Graph[String, String], id: Array[Long] = Array.empty) : VertexRDD[String] = {  
      return g.aggregateMessages[String]( edge => { 
        if(id.isEmpty|| id.contains(edge.srcId)){
          if(edge.attr.toLowerCase() == KGraphProp.edgeLabelNodeType.toLowerCase())  
           edge.sendToSrc(edge.dstAttr)
        }},
        (a, b) => a +"__"+ b
        )
  }
  
  def getNodeAlias(g: Graph[String, String], id: Array[Long] = Array.empty) : VertexRDD[String] = {  
      return g.aggregateMessages[String]( edge => { 
        if(id.isEmpty || id.contains(edge.srcId)) {
          for( aliasPredicate <- KGraphProp.edgeLabelNodeAlias) {
            if(aliasPredicate.toLowerCase() == edge.attr.toLowerCase())
              edge.sendToSrc(edge.dstAttr) 
          }
        }       
      },
      (a, b) => a +";"+ b
      )
  }

  // Given a list of vertex id in graph , get neighbour list that link to this node with a given relation
  // Do we need to define a filter on neighbours that can contribute to this count?
  //def getWikiLinks(id: Array[Long], relationLabel: String, validSrcNodesProperty: String, isOutgoing: Boolean, g: Graph[String, String] ): VertexRDD[Array[Long]] = 
  def getWikiLinks(idList: Array[Long], relationLabel: String, isOutgoing: Boolean, g: Graph[String, String] ): VertexRDD[Array[Long]] = 
  {
   
    var nbrlist : VertexRDD[Array[Long]] = null
    if(isOutgoing) {
      nbrlist = g.aggregateMessages[Array[Long]](
        //edge => { if((edge.attr == relationLabel) & (edge.srcAttr == validSrcNodesProperty)) {edge.sendToSrc(Array(edge.dstId))}},
        edge => { if(edge.attr.toLowerCase() == relationLabel.toLowerCase()) {edge.sendToSrc(Array(edge.dstId))}},
        (a, b) => a ++ b
        ).filter(v=> idList.contains(v._1)); 
    } else {
      nbrlist = g.aggregateMessages[Array[Long]](
        //edge => { if(edge.attr == relationLabel & edge.srcAttr == validSrcNodesProperty) {edge.sendToDst(Array(edge.srcId))}},
        edge => { if(edge.attr.toLowerCase() == relationLabel.toLowerCase()) {edge.sendToDst(Array(edge.srcId))}},
        (a, b) => a ++ b
        ).filter(v=> idList.contains(v._1)); 
    } 
    // is of form (vertexid =id, Array(nbrs_of_node_id_with_label_relationLabel))
    return nbrlist;
 } 
  
  //Sumit: method to find all predicates of a node,
  /// Do you mean to use allTriplesFromNodes.aggregateMessages
 def getValidRelationsForNode(idList: RDD[(VertexId, String)], g: Graph[String, String]):  VertexRDD[TreeSet[String]] ={
   val alltriplesFromNodes = idList.map(v => g.triplets.filter(t => (t.srcAttr.toLowerCase() == v._2.toLowerCase())))
   //val alltriplesFromNodes = g.triplets.filter(t => ))
   return g.aggregateMessages[TreeSet[String]](
        edge => { edge.sendToSrc(TreeSet(edge.attr)) ;
        edge.sendToDst(TreeSet(edge.attr));
        },
        (a, b) => a ++ b
        )
  }
  
  def getNbrLabels(id: Array[Long], relationLabel: List[String], isOutgoing: Boolean, g: Graph[String, String] ): VertexRDD[Array[String]] = {
  /* Given an array of vertex ids, collects labels of 1 hop neighbors, the list is filtered based on 
   *  edge types as specified in "relationLabel"
   *  and incoming or outgoing edges as specified in "isOutgoing" flag 
   */ 
    var nbrlist : VertexRDD[Array[String]] = null
    if(isOutgoing) {
      nbrlist = g.aggregateMessages[Array[String]](
        edge => { if(relationLabel.contains(edge.attr) && id.contains(edge.srcId)) {edge.sendToSrc(Array(edge.dstAttr))}},
        (a, b) => a ++ b
        )//.filter(v=> id.contains(v._1)); 
    } else {
      nbrlist = g.aggregateMessages[Array[String]](
        edge => { if(relationLabel.contains(edge.attr) && id.contains(edge.dstId)) {edge.sendToDst(Array(edge.srcAttr))}},
        (a, b) => a ++ b
        )//.filter(v=> id.contains(v._1)); 
    } 
    // is of form (vertexid =id, Array(nbrs_of_node_id_with_label_relationLabel))
    return nbrlist;
  }
  
  /* collect labels for one hop neighbours (incoming and outgoing)for entire graph */
  def getOneHopNbrLabels( g: Graph[String, String]) : VertexRDD[Set[String]] = {
    val nbrlist: VertexRDD[Set[String]] =  g.aggregateMessages[Set[String]](
        edge => { edge.sendToSrc(Set(edge.dstAttr.toLowerCase()))
                  edge.sendToDst(Set(edge.srcAttr.toLowerCase()))
        }, 
        (a,b) => a ++ b
    )
    return nbrlist;
  }
  
  /* collect one hop nbr labels for only vertices specified in id list */
  def getOneHopNbrLabels( g: Graph[String, String],  id: Array[Long]) : VertexRDD[Set[String]] = {
    
    val nbrlist: VertexRDD[Set[String]] =  g.aggregateMessages[Set[String]](
        edge => { 
          if(id.contains(edge.srcId) || id.contains(edge.dstId)) {
          edge.sendToSrc(Set(edge.dstAttr))
          edge.sendToDst(Set(edge.srcAttr))
          }
        }, 
        (a,b) => a ++ b
    )
   
    return nbrlist;
  }
    /* collect one hop nbr ids, labels, and edge label,  for only vertices specified in id list */
 /* def getOneHopNbrEdgeLabels( g: Graph[String, String],  id: Array[Long]) : VertexRDD[Set[LabelPair]] = {
    
    val nbrlist: VertexRDD[Set[LabelPair]] =  g.aggregateMessages[Set[LabelPair]](
        edge => { 
          if(id.contains(edge.srcId) || id.contains(edge.dstId)) {
            val labelForSrc = new LabelPair(edge.attr, edge.dstId, edge.dstAttr)
            val labelForDst = new LabelPair(edge.attr, edge.srcId, edge.srcAttr)
          edge.sendToSrc(Set(labelForSrc))
          edge.sendToDst(Set(labelForDst))
          }
        }, 
        (a,b) => a ++ b
    )
   
    return nbrlist;
  }
  * 
  */
      /* collect one hop nbr ids, labels, and edge label,  for only vertices specified in id list */
  def getOneHopNbrIds( g: Graph[String, String],  id: Array[Long]) : VertexRDD[Array[Long]] = {
    
    val nbrlist: VertexRDD[Array[Long]] =  g.aggregateMessages[Array[Long]](
        edge => { 
          if(id.contains(edge.srcId) || id.contains(edge.dstId)) {
          edge.sendToSrc(Array(edge.dstId))
          edge.sendToDst(Array(edge.srcId))
          }
        }, 
        (a,b) => a ++ b
    )
   
    return nbrlist;
  }
       /* collect one hop nbr ids, for all graph*/
  def getOneHopNbrIdsLabels( g: Graph[String, String]) : VertexRDD[Set[(Long, String, String)]] = {
    
    val nbrlist: VertexRDD[Set[(Long, String, String)]] =  g.aggregateMessages[Set[(Long, String, String)]](
        edge => {         
          edge.sendToSrc(Set((edge.dstId, edge.dstAttr, edge.attr)))
          edge.sendToDst(Set((edge.srcId, edge.srcAttr, edge.attr)))
        }, 
        (a,b) => a ++ b
    )
    print(" collected one hop nbrs for nodes:", nbrlist.count)
    return nbrlist;
  } 
  
  def getOneHopNbrIdsLabels( g: Graph[String, String], filterRelations:Set[String]) : VertexRDD[Set[(Long, String, String)]] = {
    
    val nbrlist: VertexRDD[Set[(Long, String, String)]] =  g.aggregateMessages[Set[(Long, String, String)]](
        edge => {         
          if(!filterRelations.contains(edge.attr) || filterRelations.isEmpty) {
            edge.sendToSrc(Set((edge.dstId, edge.dstAttr, edge.attr)))
        	edge.sendToDst(Set((edge.srcId, edge.srcAttr, edge.attr)))
          }
        }, 
        (a,b) => a ++ b
    )
    print(" collected one hop nbrs for nodes:", nbrlist.count)
    return nbrlist;
  } 
  
  def getOneHopNbrEdges( g: Graph[String, String], filterRelations:Set[String]) : VertexRDD[Set[NbrEdge]] = {
    
    val nbrlist: VertexRDD[Set[NbrEdge]] =  g.aggregateMessages[Set[NbrEdge]](
        edge => {         
          if(!filterRelations.contains(edge.attr) || filterRelations.isEmpty) {
            edge.sendToSrc(Set(new NbrEdge(edge.dstId, edge.dstAttr, edge.attr, true)))
        	edge.sendToDst(Set(new NbrEdge(edge.srcId, edge.srcAttr, edge.attr, false)))
          }
        }, 
        (a,b) => a ++ b
    )
    print(" collected one hop nbrs for nodes:", nbrlist.count)
    return nbrlist;
  } 

  def getOneHopNbrIdsLabels( g: Graph[String, String], id: Array[Long], 
      filterRelations:Set[String] = Set.empty) : VertexRDD[Set[(Long, String)]] = {
    val nbrlist: VertexRDD[Set[(Long, String)]] =  g.aggregateMessages[Set[(Long, String)]](
        edge => {         
          if(id.contains(edge.srcId) && (!filterRelations.contains(edge.attr) || filterRelations.isEmpty)) {
            edge.sendToSrc(Set((edge.dstId, edge.dstAttr)))
          }       
          if(id.contains(edge.dstId) && (!filterRelations.contains(edge.attr) || filterRelations.isEmpty)) {
            edge.sendToDst(Set((edge.srcId, edge.srcAttr)))
          }      
        }, 
        (a,b) => a ++ b
    )
    println(" collected one hop nbrs for nodes:", nbrlist.count)
    return nbrlist;
  } 
  
  def getOneHopNbrsEdgeLabels( g: Graph[String, String], id: Long, relationLabel: Set[String]) : VertexRDD[Set[(Long, String, String)]] = {    
    val nbrlist: VertexRDD[Set[(Long, String, String)]] =  g.aggregateMessages[Set[(Long, String, String)]](
        edge => {         
          if(id == edge.srcId && relationLabel.contains(edge.attr)) {
            edge.sendToSrc(Set((edge.dstId, edge.dstAttr, edge.attr)))
          }
          if(id == edge.dstId && relationLabel.contains(edge.attr)) {
           edge.sendToDst(Set((edge.srcId, edge.srcAttr, edge.attr)))
          }
        }, 
        (a,b) => a ++ b
    )
    println(" collected one hop nbrs for nodes:", nbrlist.count)
    return nbrlist;
    
  } 
  
  
  
  /* collect the labels of 2 hop nbrs (incoming and outgoing) for entire graph*/
  def getTwoHopNbrLabels(g: Graph[String, String]) : VertexRDD[Set[String]] = {
    
    val oneHopNbrs :VertexRDD[Set[String]]= getOneHopNbrLabels(g)
   
   // Set up a new graph , with Set[neighbour_labels + my_label] as node property 
   val newGraphListNbrs : Graph[Set[String], String] = g.outerJoinVertices(oneHopNbrs) {
      case(id, label, Some(nbrlist)) => nbrlist + label
      case(id, label, None) =>  Set(label)
    }
    
    val twoHopNbrs = newGraphListNbrs.aggregateMessages[Set[String]](edge => 
      { edge.sendToSrc(edge.dstAttr)
        edge.sendToDst(edge.srcAttr)
      },
      (a, b) => a ++ b  
    )
    
    return twoHopNbrs
  }
}
