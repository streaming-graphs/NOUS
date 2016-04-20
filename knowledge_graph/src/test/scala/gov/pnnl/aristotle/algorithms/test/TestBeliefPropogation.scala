package gov.pnnl.aristotle.algorithms.test;

import org.scalatest._
import org.apache.spark._
import org.apache.log4j.Logger
import org.apache.log4j.Level
import scala.collection.Map
import breeze.linalg._

import gov.pnnl.aristotle.algorithms.entity.EvidenceProp


object TestBeliefPropogation {
  
  def isEqualDouble(d1 : Double, d2: Double, absilon: Double = 0.00001) : Boolean = {
    Math.abs(d1-d2) < absilon
  }
  
  def main(args: Array[String]): Unit = {
    
    val sparkConf = new SparkConf().setAppName("EntityDisamb").setMaster("local")
    val sc = new SparkContext(sparkConf)
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)
    println("starting from main")
    
  
    val lambda = 0.1
    val numMentions = 3
    val numEntities =  6
    val numVertices = numMentions + numEntities
    
    var initialEvidence = DenseMatrix.zeros[Double](numVertices, 1)
    initialEvidence(0, 0) = 0.3
    initialEvidence(1, 0)  = 0.45
    initialEvidence(2, 0) = 0.25
    initialEvidence(3, 0) = 1/6
    
    
    val evidencePropMatrix =  DenseMatrix.zeros[Double](numVertices, numVertices)
    evidencePropMatrix(3,0) = 0.13/0.14
    evidencePropMatrix(4,0) = .01/0.14
    
    evidencePropMatrix(8, 1) = 1.0
    evidencePropMatrix(5, 2) = 0.03/0.23
    evidencePropMatrix(6, 2) = 0.08/0.23
    evidencePropMatrix(7, 2) = 0.12/0.23
    evidencePropMatrix(6, 3) = 1.0
    evidencePropMatrix(3, 6) = 0.82/1.48
    evidencePropMatrix(8, 6) = 0.66/1.48
    evidencePropMatrix(6, 8) = 1.0

    
    val newEvidence = EvidenceProp.runBeliefProp(initialEvidence,
      evidencePropMatrix, lambda)
    
    assert(newEvidence.rows == numVertices)  
    assert(newEvidence.cols == 1)

    val mentionToEntityMatrix: DenseMatrix[Double] = 
     evidencePropMatrix.t(0 to numMentions-1, ::)
    var finalMatchesIndexed = 
     scala.collection.mutable.Map.empty[Int, (Int, Double)]
    for(mentionIndex <- 0 to numMentions-1){
      val mentionEntityComp = mentionToEntityMatrix(mentionIndex, ::)  
      var maxScore: Double = Double.NegativeInfinity
      var matchingEntityId = -1
      for(j <- numMentions to numVertices-1) {
        val newScore = mentionEntityComp(j)*newEvidence(j, 0)
        println("mentionIndex, entityIndex, score", 
            mentionIndex, j, mentionEntityComp(j), newEvidence(j, 0), newScore)
        if(newScore > maxScore){
          maxScore = newScore
          matchingEntityId = j
          println("changinng score for mention to ", mentionIndex, j, newScore)
        }
      } 
      finalMatchesIndexed.+=((mentionIndex, (matchingEntityId, maxScore)))
    }
    println("Final indexed matches")
    finalMatchesIndexed.foreach(keyValue => 
      println(keyValue._1, keyValue._2._1, keyValue._2._2))
  
  
  val mappedInst0 = finalMatchesIndexed.get(0).get
  val mappedInst1 = finalMatchesIndexed.get(1).get
  val mappedInst2 = finalMatchesIndexed.get(2).get
  
  assert((mappedInst0._1 == 3) && isEqualDouble(mappedInst0._2, 0.18617091284531634))
  assert((mappedInst1._1 == 8) && isEqualDouble(mappedInst1._2, 0.18169196787503433))
  assert((mappedInst2._1 == 6) && isEqualDouble(mappedInst2._2, 0.12236223829896108))
  }
}