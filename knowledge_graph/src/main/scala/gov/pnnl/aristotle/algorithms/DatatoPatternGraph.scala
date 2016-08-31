package gov.pnnl.aristotle.algorithms

import org.apache.spark.graphx.VertexRDD
import org.apache.spark.graphx.Graph
import gov.pnnl.aristotle.algorithms.mining.datamodel.KGEdgeInt



object DataToPatternGraph {
  
  val maxPatternSize: Int = 4
  type PatternEdge = (Int, Int, Int)
  type Pattern = Array[PatternEdge]
   
  type InstanceEdge = (Long, Long , Long)
  type PatternInstance = Array[InstanceEdge]
  
  type DataGraph = Graph[Int, KGEdgeInt]
  type DataGraphNodeId = Long
  type PatternGraph = Graph[PatternInstanceNode, DataGraphNodeId]
  type Label = Int
  type LabelWithTypes = (Label, List[Int])
  
  class PatternInstanceNode(/// Constructed from hashing all nodes in pattern
    val patternInstMap : Array[(PatternEdge, InstanceEdge)],
    val timestamp: Long) extends Serializable {

    val id = getid()
    
    def getid(): Long = {
      val patternInstHash: Int = patternInstMap.map(patternEdgeAndInst =>  {
           val pattern = patternEdgeAndInst._1.hashCode
           val inst = patternEdgeAndInst._2.hashCode
           (pattern, inst).hashCode
         }).hashCode()
        patternInstHash 
    }
    
    def getPattern: Pattern = {
      patternInstMap.map(_._1)
    }
    
    def getInstance : PatternInstance = {
      patternInstMap.map(_._2)
    }
    
    def getAllNodesInPatternInstance() : Array[DataGraphNodeId] = {
     val instance: PatternInstance = patternInstMap.map(_._2)
     val nodeids = Array[DataGraphNodeId](instance.size*2)
     for(i <- Range(0, instance.length-1, 2)) {
       nodeids(i) = instance(i)._1
       nodeids(i+1) = instance(i)._2
     }
     nodeids
   }
}
  
  def main(args: Array[String]): Unit = {
    if (args.length != 4) {
      println("Usage <dataGraph> <OutDir> <typeEdge> <support>")
      exit
    } 
    val sc = SparkContextInitializer.sc
    val graphFile = args(0)
    val outDir = args(1)
    val typePred = args(2).toInt
    val support = args(3).toInt
    
    val dataGraph: DataGraph = ReadHugeGraph.getGraphFileTypeInt(graphFile, sc)
    val patternGraph: PatternGraph  = getPatternGraph(dataGraph, typePred, support)
    patternGraph.vertices.saveAsTextFile(outDir + "/" + "GIP/vertices")
  }
  
  def getPatternGraph(dataGraph: DataGraph, typePred: Int, support: Int): PatternGraph = {
    val typedGraph: Graph[LabelWithTypes, KGEdgeInt] = getTypedGraph(dataGraph, typePred)
    val edgesWithPattern = typedGraph.triplets.map(triple => {
      val pattern = (triple.srcAttr, triple.attr.getlabel, triple.dstAttr)
    })
    
    
  }
  
   def getTypedGraph(g: DataGraph, typePred: Int): Graph[LabelWithTypes, KGEdgeInt] =
   {
     // Get Node Types
     val typedVertexRDD: VertexRDD[List[Int]] = g.aggregateMessages[List[Int]](edge => {
       if (edge.attr.getlabel == (typePred)) 
         edge.sendToSrc(List(edge.dstAttr))
         },
         (a, b) => a ++ b)
     // Join Node Original Data With NodeType Data
     g.outerJoinVertices(typedVertexRDD) {
       case (id, label, Some(nbr)) => (label, nbr)
       case (id, label, None) => (label, List.empty[Int])
       }
   }
 
}