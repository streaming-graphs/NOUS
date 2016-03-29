/**
 *
 */
package gov.pnnl.aristotle.algorithms

import java.io.Serializable
import org.apache.spark.graphx.Graph

/**
 * @author puro755
 *
 */
class PatternDependencyGraph extends Serializable {

	var graph : Graph[PGNode, Int] = null
	

}