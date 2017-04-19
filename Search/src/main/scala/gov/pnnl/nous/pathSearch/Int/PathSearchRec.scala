package gov.pnnl.nous.pathSearch.Int

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.log4j.Logger
import org.apache.log4j.Level
import org.apache.spark.rdd._
import org.apache.spark.graphx._
import java.io._
import gov.pnnl.nous.utils.ReadGraph
import org.apache.spark.rdd.RDD.rddToPairRDDFunctions
import PathSearchIntDataTypes._

class PathSearchRec[VD](val adjMap: Map[VertexId , Iterable[IntEdge]], 
    val nodeFeatureMap :  Map[VertexId , VD], val pathFilter : PathFilter[VD], val maxIter : Int) {
  
  def FindPathsInt(src: VertexId, dest: VertexId, 
      currIter: Int, nodesSoFar: List[VertexId]): List[Path] = {
    
    var allPaths: List[Path] = List.empty
    if(src == dest || currIter >= maxIter)
      return allPaths
    
    val srcEdges : Iterable[IntEdge] = adjMap.get(src).get
    val directEdges: List[Path]  = srcEdges.filter(v => v._1 == dest).map(v => List(v)).toList
    allPaths = allPaths++directEdges
    
    val otherNbrEdges = srcEdges.filter(v => v._1 != dest)
    for(nbrEdge <- otherNbrEdges) {
      val nbrid: VertexId = nbrEdge._1
      if(nodeFeatureMap.size > 0) {
        // If the node has any features and may be using a path filter
        val nbrData: VD = nodeFeatureMap.get(nbrid).get
        val prevNode: VertexId = nodesSoFar.last
        val prevNodeData: VD = nodeFeatureMap.get(prevNode).get
        if(!nodesSoFar.contains(nbrid) && 
          pathFilter.isValidNode(nbrData) && 
          pathFilter.isValidPair(prevNodeData, nbrData)){
          val newpath =  nodesSoFar.::(nbrid)
          val nbrPaths = FindPathsInt(nbrid, dest, currIter+1 , newpath)
          val newPaths:  List[Path]  = nbrPaths.mapConserve(v => nbrEdge::v)
          allPaths = allPaths.++(newPaths)
        }
      } else {
        // No features avaible on nodes => nothing to filter
        if(!nodesSoFar.contains(nbrid)) {
          val newpath =  nodesSoFar.::(nbrid)
          val nbrPaths:  List[Path]  = FindPathsInt(nbrid, dest, currIter+1,  newpath)
          val newPaths:  List[Path]  = nbrPaths.mapConserve(v => nbrEdge::v)
          allPaths = allPaths.++(newPaths)
        }
      }
    }
    return allPaths    
  }
      
}