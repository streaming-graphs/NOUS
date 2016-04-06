package gov.pnnl.aristotle.utils

import scala.io.Source

object Math_Utils {
  
  def klDivergence(p : Array[Double], q: Array[Double], defaultDiv: Double=0.0): Double = {
    var divergence = 0.0
    if(p.length != q.length){
      println("Cannot calculate divergence of these distribnutions of unequal length")
      return Double.PositiveInfinity
    }
    val size = p.length
    for(i <- 0 until size) {
      if(p(i) != 0 && q(i) !=0) {
        divergence += p(i) * Math.log(p(i)/q(i))
      } else if (q(i) == 0) {
        divergence += p(i) * Math.log(p(i)/defaultDiv) 
      }
    }  
    divergence/Math.log(2)
  }
  
  def jensenShannonDiv(p : Array[Double], q: Array[Double]): Double = {
    if(p.length != q.length){
      println("Cannot calculate divergence of these distribnutions of unequal length")
      return Double.PositiveInfinity
    }
    
    val average =  new Array[Double](p.length)
   
    for(i <- 0 to p.length){
      average(i) = (p(i) + q(i))/2
    }
    
    return (klDivergence(p,average) + klDivergence(q, average))/2
  }
  
  def sqDistance(p : Array[Double], q: Array[Double]): Double = {
    if(p.length != q.length){
      println("Cannot calculate divergence of these distribnutions of unequal length")
      return Double.PositiveInfinity
    }   
    var sqDistance = 0.0
    for(i <- 0 until p.length) {
        sqDistance += (p(i) - q(i)) * (p(i) - q(i))
    }
    return Math.sqrt(sqDistance)
  }
    
  def jaccardCoeff[T](dist1: Set[T], dist2: Set[T]): Double = {
    if(dist1.size == 0 && dist2.size == 0) return 1
    val nIntersection = dist1.intersect(dist2).size
    val nUnion = dist1.union(dist2).size
    (nIntersection/nUnion)
  }
}