package gov.pnnl.aristotle.algorithms

import org.apache.spark._
import org.apache.spark.Aggregator
import scala.collection.{Set, Map}
import scala.util.Sorting
import scala.math.Ordering

import org.apache.log4j.Logger
import org.apache.log4j.Level
import gov.pnnl.aristotle.utils.{NLPTripleParser}
import java.io.{BufferedWriter, OutputStreamWriter, FileOutputStream}
import gov.pnnl.aristotle.utils._

class LocalEdge[ED](val srcId: Long, val edgeAttr: ED, val dstId: Long)
class LocalDirectedEdge[ED](val edgeAttr: ED, val nodeid: Long, val outgoing: Boolean = true){
  override def toString(): String = {
    if(outgoing)
      " < " + edgeAttr.toString + "-> " + nodeid.toString 
    else 
      " <- " + edgeAttr.toString + "> " + nodeid.toString 
  }
}

class LocalGraph[VD, ED](val vertices: Map[Long, VD], 
    val edges: Map[Long, List[LocalDirectedEdge[ED]]]) {
  
  def printGraph(): Unit = {
    println
    println("Vertices : ", vertices.size)
    vertices.foreach(v => println(v._1, v._2.toString))
    
    println
    println("Edges : ", edges.size)
    edges.foreach(edge => println(edge._1, edge._2.toString + ";"))  
  }
 /* 
  def createLocalGraph(): collection.mutable.Map[Long, (VD, List[LocalDirectedEdge[ED]])]  = {
        
    val mappedVertices : collection.mutable.Map[Long, (VD, List[LocalDirectedEdge[ED]])] = vertices.map((keyValue) =>
      (keyValue._1, (keyValue._2 , List.empty[LocalDirectedEdge[ED]])))
        
    for(edge <- edges) {
      
      val srcData = mappedVertices.get(edge.srcId) match {
        case Some(nodeData) => nodeData
        case None => {
          println("Errror: While parsing edge list : Cannot find the vertexId in in vertexMap")
          exit
        }
      }
      val dstData = mappedVertices.get(edge.dstId) match {
        case Some(nodeData) => nodeData
        case None => {
          println("Errror: While parsing edge list : Cannot find the vertexId in in vertexMap")
          exit
        }
      }
      val srcEdgeList = srcData._2.::(new LocalDirectedEdge(edge.edgeAttr, edge.dstId, true))
      val dstEdgeList = dstData._2.::(new LocalDirectedEdge(edge.edgeAttr, edge.srcId, false))
        
      mappedVertices.updated(edge.srcId, (srcData._1, srcEdgeList))
      mappedVertices.updated(edge.dstId, (srcData._1, dstEdgeList))
    }
    
    mappedVertices
  }
  */
  
}