package gov.pnnl.aristotle.algorithms.mining.datamodel

import org.apache.spark.graphx.Graph

/**
 * @author puro755
 *
 */

trait NousKG[A,B] extends Serializable {

  val input_graph : Graph[A,B]
  
  
}