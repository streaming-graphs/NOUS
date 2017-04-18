package gov.pnnl.nous

import org.apache.spark._
import scala.collection.Map
import gov.pnnl.nous.utils._

object ColEntityTypes {
  //type ReferentGraph = LocalGraph[LabelWithWt, Double]

  type Mention = String
  type SimScore = Double
  type NodeId = Long 
  type Entity = (NodeId, String)
}

  class LabelWithWt(val label: String, val wt : Double){
    override def toString(): String ={
      label + ";" + wt.toString
    }
  }
  
  class LocalEdge[ED](val srcId: Long, val edgeAttr: ED, val dstId: Long)
  
  class LocalDirectedEdge[ED](val edgeAttr: ED, val nodeid: Long, val outgoing: Boolean = true){
    override def toString(): String = {
      if(outgoing)
        " < " + edgeAttr.toString + "-> " + nodeid.toString 
      else 
        " <- " + edgeAttr.toString + "> " + nodeid.toString 
    }
  }

abstract class LocalGraph[VD, ED]() {
  
    val vertices: Map[Long, VD]
    val edges: Map[Long, List[LocalDirectedEdge[ED]]] 
     
    def printGraph(): Unit = {
      println
      println("Vertices : ", vertices.size)
      vertices.foreach(v => println(v._1, v._2.toString))
    
      println
      println("Edges : ")
      edges.foreach(edge => println(edge._1, edge._2.toString + ";"))  
    }
  
  }


