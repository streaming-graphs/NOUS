/**
 *
 */
package gov.pnnl.aristotle.algorithms

import org.apache.spark.rdd.RDD
import org.apache.spark.graphx.Graph

/**
 * @author puro755
 *
 */
//TODO : add  getter 'setter' for it
class WindowState(subgraph_index : RDD[(String,Set[PatternInstance])],
    dependency_graph : Graph[PGNode, Int],
    redundent_patterns_graph :Graph[PGNode, Int],
    closed_patterns_graph : Graph[PGNode, Int],
    promising_patterns_graph : Graph[PGNode, Int],
    var pattern_trend : Map[String,List[(Int,Int)]]
) extends Serializable {
	def getsubgraph_index : RDD[(String,Set[PatternInstance])] = return subgraph_index 
    def getdependency_graph : Graph[PGNode, Int] = return dependency_graph
    def getredundent_patterns_graph :Graph[PGNode, Int] = return redundent_patterns_graph
    def getclosed_patterns_graph : Graph[PGNode, Int] = return closed_patterns_graph
    def getpromising_patterns_graph : Graph[PGNode, Int] = return promising_patterns_graph
	
	
}