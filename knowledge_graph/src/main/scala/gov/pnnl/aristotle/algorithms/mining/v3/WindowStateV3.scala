/**
 *
 * @author puro755
 * @dJul 5, 2016
 * @knowledge_graph
 */
package gov.pnnl.aristotle.algorithms.mining.v3

import org.apache.spark.graphx.Graph
import org.apache.spark.rdd.RDD
import org.apache.spark.graphx.VertexId
import scalaz.Scalaz._
import gov.pnnl.aristotle.algorithms.mining.datamodel.KGEdge
import gov.pnnl.aristotle.algorithms.mining.datamodel.KGNodeV2Flat
import gov.pnnl.aristotle.algorithms.mining.datamodel.PatternDependencyGraph
import org.apache.spark.graphx.Graph.graphToGraphOps
import gov.pnnl.aristotle.algorithms.mining.datamodel.PGNode
import org.apache.spark.graphx.Edge
import org.apache.spark.SparkContext._
/**
 * @author puro755
 *
 */
class WindowStateV3 {

  var window_graph: Graph[KGNodeV2Flat, KGEdge] = null
  var gDep = new PatternDependencyGraph
  def mergeBatchGraph(batch_graph: Graph[KGNodeV2Flat, KGEdge]) =
    {
    if(window_graph == null) {
      WindowStateV3.this.window_graph = batch_graph
      
    } else
    {
        val vertices_rdd: RDD[(VertexId, Map[String, Long])] =
          batch_graph.vertices.map(f => (f._1, f._2.getpattern_map))
        window_graph = WindowStateV3.this.window_graph.joinVertices[Map[String, Long]](vertices_rdd)((id, kgnode, new_data) => {
          if (kgnode != null)
            new KGNodeV2Flat(kgnode.getlabel,
              kgnode.getpattern_map |+| new_data, List.empty)
          else
            new KGNodeV2Flat("", Map.empty, List.empty)
        })
      }

    }
  
    def updateGDep(batch_graph: Graph[KGNodeV2Flat, KGEdge], TYPE: String) =
    {
      val patternRDD =
        batch_graph.aggregateMessages[Set[(String, Long)]](
          edge => {
            if (edge.attr.getlabel.equalsIgnoreCase(TYPE) == false)
              if ((edge.srcAttr != null) && (edge.srcAttr.getpattern_map != null))
                edge.srcAttr.getpattern_map.foreach(pattern =>
                  edge.sendToSrc(Set((pattern._1, pattern._2))))
          }, (pattern1OnNodeN, pattern2OnNodeN) => {
            pattern1OnNodeN |+| pattern2OnNodeN
          })

      val tmp_commulative_RDD =
        get_Pattern_RDDV2Flat(batch_graph,TYPE)

      val new_dependency_graph_vertices_support: RDD[(VertexId, PGNode)] =
        tmp_commulative_RDD.map(pattern =>
          (pattern._1.hashCode().toLong,
            new PGNode(pattern._1, pattern._2)))

      //Edge(P1.hascode,P2.hashcode,1)
      // "1" is just an edge type which represent "part-of"
      // int is used to save the space 		
      val new_dependency_graph_edges: RDD[Edge[Int]] =
        patternRDD.flatMap(vertex =>
          {
            var tmp: Set[Edge[Int]] = Set.empty
            for (p <- vertex._2) {
              // double quote | is treated a OP operator and have special meaning
              // so use '|'
              val setNodes = p._1.split('|')
              val b = setNodes.filter(arr_entry =>
                !arr_entry.equalsIgnoreCase(p._1.replaceAll("\\|", "")))
                .map(arr_entry => Edge(arr_entry.hashCode().toLong,
                  p._1.replaceAll("\\|", "\t").hashCode().toLong, 1))
              tmp = tmp ++ b
            }
            tmp
          })

      this.gDep.graph = Graph(new_dependency_graph_vertices_support, new_dependency_graph_edges)

    }
  
    /*
     * TODO: should be moved from window state
     */
      def get_Pattern_RDDV2Flat(graph: Graph[KGNodeV2Flat, KGEdge],TYPE:String): RDD[(String, Long)] =
    {
      /*
     *  collect all the pattern at each node
     */
      val patternRDD =
        graph.aggregateMessages[Set[(String, Long)]](
          edge => {
            if (edge.attr.getlabel.equalsIgnoreCase(TYPE) == false)
              if ((edge.srcAttr != null) && (edge.srcAttr.getpattern_map != null))
                edge.srcAttr.getpattern_map.foreach(pattern =>
                  edge.sendToSrc(Set((pattern._1, pattern._2))))
          }, (pattern1OnNodeN, pattern2OnNodeN) => {
            pattern1OnNodeN |+| pattern2OnNodeN
          })
      println("patternRDD size is " + patternRDD.count)

      /*
       * Collects all the patterns across the graph
       */
      val new_dependency_graph_vertices_RDD: RDD[(String, Long)] =
        patternRDD.flatMap(vertex => vertex._2.map(entry =>
          (entry._1.replaceAll("\\|", "\t"), entry._2)))

      /*
       * Returns the RDD with each pattern-key and its support
       */
      return new_dependency_graph_vertices_RDD.reduceByKey((a, b) => a + b)
    }
}