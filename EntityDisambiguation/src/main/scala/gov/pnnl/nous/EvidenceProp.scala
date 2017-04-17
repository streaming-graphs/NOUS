package gov.pnnl.nous


import scala.collection.Map
import breeze.linalg._
import breeze.numerics._
import ColEntityTypes._

object EvidenceProp{
  
  /* Given a Referent Graph , return matching entities */
  def runColInference(refGraph : ReferentGraph,
      verticesToIndexMap:  Map[Long, Int] , 
      numMentions: Int, lambda: Double) : 
  Map[Int, (Int, SimScore)] = {
    
    //  [N*1] matrix containing initial score for each vertex
    val initialEvidence = getInitialEvidence(refGraph, verticesToIndexMap)
    
    // [N*N] matrix containing edge weights between vertices (0 otherwise)
    val evidencePropMatrix = initEvidencePropMatrix(refGraph, verticesToIndexMap)
    
    // [N*1] inferred evidence matrix
    val inferredEvidence : DenseMatrix[Double] = runBeliefProp(initialEvidence, evidencePropMatrix, lambda)
    println("Inferred Evidence")
    println(inferredEvidence)
    
    //[M*N] mention to Entity weights
    // Note : Initial Evidence Propagation Matrix is constructed as 
    // T[i, j] = weight of edge going from j to i
    // We transpose back for calculating final scores
    val mentionToEntityMatrix: DenseMatrix[Double] = evidencePropMatrix.t(0 to numMentions-1, ::)
    val numTotalVertices: Int = refGraph.vertices.size
    
    var finalMatchesIndexed = scala.collection.mutable.Map.empty[Int, (Int, SimScore)]
    for(mentionIndex <- 0 to numMentions-1){
      val mentionEntityComp = mentionToEntityMatrix(mentionIndex, ::)  
      var maxScore: Double = Double.NegativeInfinity
      var matchingEntityId = -1
      for(j <- numMentions to numTotalVertices-1) {
        val newScore = mentionEntityComp(j)*inferredEvidence(j, 0)
        //println("mentionIndex, entityIndex, score", mentionIndex, j, mentionEntityComp(j), inferredEvidence(j, 0), newScore)
        if(newScore > maxScore){
          maxScore = newScore
          matchingEntityId = j
          //println("changinng score for mention to ", mentionIndex, j, newScore)
        }
      } 
      finalMatchesIndexed.+=((mentionIndex, (matchingEntityId, maxScore)))
    }
    finalMatchesIndexed
  }
    

 
  
  /* create a vector containing vertices.evienceWt 
   * evidence[0.... NumMentions-1] => mention.wt
   * evidence[NumMentions ...... NumMentions+NumENtities-1] => entityWt=>0
   * 
   */ 
  def getInitialEvidence(refGraph: ReferentGraph, verticesToIndexMap: Map[Long, Int]): 
  DenseMatrix[Double] = {
    
    val numVertices = refGraph.vertices.size 
    val initialEvidence = DenseMatrix.zeros[Double](numVertices, 1)
    for(vertex <- refGraph.vertices){
      val vId = vertex._1
      //println("getting index for ",vId)
      val vIndex = verticesToIndexMap.get(vId).get 
      initialEvidence(vIndex, 0) = vertex._2.wt
    }
    initialEvidence
   }
  
  
  /* create a evidence propagation matrix such that 
   * evidenceProp[i, j] => edgeWt(nodeIndex_j, nodeIndex_i)
   * nodeIndex are mapped from the given map
   */
  def initEvidencePropMatrix(refGraph: ReferentGraph, verticesToIndexMap: Map[Long, Int]):
  DenseMatrix[Double]= {
    
    val numVertices = refGraph.vertices.size 
    val evidencePropMatrix = DenseMatrix.zeros[Double](numVertices, numVertices)
    for(edgesForSrc <- refGraph.edges){
      val srcId = edgesForSrc._1
      val srcIndex = verticesToIndexMap.get(srcId).get
      val allEdgesSrc = edgesForSrc._2
      for(srcEdge <- allEdgesSrc) {
        val dstId = srcEdge.nodeid
        val dstIndex = verticesToIndexMap.get(dstId).get
        evidencePropMatrix(dstIndex, srcIndex) = srcEdge.edgeAttr
      } 
    }
    evidencePropMatrix  
  }
  
  /* Implements 
   * Result = lambda * (Inverse(I - cT)) * initialEvidence
   * where:
   * lambda = fraction of back propagation 
   * I : Identity matrix 
   * T : evidence Propagation Matrix
   * c : 1- lambda
   */
  def runBeliefProp(initialEvidence: DenseMatrix[Double], 
      evidencePropMatrix: DenseMatrix[Double], lambda: Double): DenseMatrix[Double] = {  
    val nodeCount = initialEvidence.rows
    
   // println("Initial evidence")
   // println(initialEvidence)
    val eyeN = DenseMatrix.eye[Double](nodeCount)
    val tmp1 = (1-lambda)*evidencePropMatrix
    val tmp2 = lambda * inv(eyeN - tmp1)
    val result = tmp2*initialEvidence
    result 
  }
  
}
