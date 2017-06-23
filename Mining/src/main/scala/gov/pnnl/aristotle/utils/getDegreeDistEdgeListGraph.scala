/**
 *
 * @author puro755
 * @dJun 21, 2017
 * @Mining
 */
package gov.pnnl.aristotle.utils

import gov.pnnl.aristotle.algorithms.SparkContextInitializer
import org.apache.spark.graphx.GraphLoader

/**
 * @author puro755
 *
 */
object getDegreeDistEdgeListGraph {

  def main(args: Array[String]): Unit = {
    
    val sc = SparkContextInitializer.sc
    val filePath = "/Users/puro755/OneDrive - PNNL/Projects/DARPA_MAA/code/socialmediadata/intGraph/tweet_graph_small.csv"
    val newGraph = GraphLoader.edgeListFile(sc,filePath)
    val degDist = newGraph.degrees.map(v=>v._2)
    
    val serializedDegDist = degDist.map(d=>d.toString)
    serializedDegDist.saveAsTextFile("degDist")
    
    
  }

}