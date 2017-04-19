/**
 *
 */
package gov.pnnl.aristotle.algorithms.mining.datamodel

import org.apache.spark.graphx.Graph

/**
 * @author puro755
 *
 */
case class NousKGV1(input_graph : Graph[KGNodeV1,KGEdge]) extends NousKG[KGNodeV1,KGEdge] 
{

}  