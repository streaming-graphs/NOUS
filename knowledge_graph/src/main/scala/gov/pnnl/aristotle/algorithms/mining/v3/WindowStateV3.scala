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
import gov.pnnl.aristotle.algorithms.mining.datamodel.VertexProperty
import org.apache.spark.graphx.EdgeDirection
import org.apache.spark.graphx.VertexRDD
/**
 * @author puro755
 *
 */
class WindowStateV3 {

  var window_graph: Graph[KGNodeV2Flat, KGEdge] = null
  var gDep = new PatternDependencyGraph

  /*
   * Merge batch with window graph
   */
  def mergeBatchGraph(batch_graph: Graph[KGNodeV2Flat, KGEdge]) =
    {
      if (window_graph == null) {
        WindowStateV3.this.window_graph = batch_graph

      } else {
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

  /*
   * create vertices rdd of dependency graph
   */
  def getDepGraphVertexRDD(pattern_per_node_RDD: VertexRDD[Set[(String, Long)]], TYPE: String): RDD[(VertexId, PGNode)] =
    {
      //Create vertices in the dep graph
      val tmp_commulative_RDD =
        get_Pattern_RDDV2Flat(pattern_per_node_RDD, TYPE)

      val new_dependency_graph_vertices_support: RDD[(VertexId, PGNode)] =
        tmp_commulative_RDD.map(pattern =>
          (pattern._1.hashCode().toLong,
            new PGNode(pattern._1.trim(), pattern._2, -1)))
      return new_dependency_graph_vertices_support
    }

  /*
   * create edge rdd of dependency graph
   */
  def getDepGraphEdgeRDD(pattern_per_node_RDD: VertexRDD[Set[(String, Long)]]): RDD[Edge[Int]] =
    {
      val new_dependency_graph_edges: RDD[Edge[Int]] = pattern_per_node_RDD.flatMap(vertex =>
        {
          var tmp: Set[Edge[Int]] = Set.empty
          for (p <- vertex._2) {
            // double quote | is treated a OP operator and have special meaning
            // so use '|'
            val setNodes = p._1.replaceAll("\\|+", "\\|").split('|')
            var b: Array[Edge[Int]] = Array.empty
            if (setNodes.length > 1) {
              b = setNodes.map(arr_entry => Edge(arr_entry.trim().hashCode().toLong,
                p._1.replaceAll("\\|+", "\\|").replaceAll("\\|", "\t").replaceAll("\t+", "\t").trim().hashCode().toLong, -2))
            }
            tmp = tmp ++ b
          }
          tmp
        })
      return new_dependency_graph_edges

    }

  def getNewDepGraph(batch_graph: Graph[KGNodeV2Flat, KGEdge], TYPE: String): Graph[PGNode, Int] =
    {

      //Edge(P1.hascode,P2.hashcode,1)
      // "1" is just an edge type which represent "part-of"
      // int is used to save the space 
      //Create Edges in the dep graph
      // it has commulative count of all patterns on each vertex
      val pattern_per_node_RDD =
        batch_graph.aggregateMessages[Set[(String, Long)]](
          edge => {
            if (edge.attr.getlabel.equalsIgnoreCase(TYPE) == false)
              if ((edge.srcAttr != null) && (edge.srcAttr.getpattern_map != null))
                edge.srcAttr.getpattern_map.foreach(pattern =>
                  edge.sendToSrc(Set((pattern._1, pattern._2))))
          }, (pattern1OnNodeN, pattern2OnNodeN) => {
            pattern1OnNodeN |+| pattern2OnNodeN
          })

      val new_dependency_graph_vertices_support = getDepGraphVertexRDD(pattern_per_node_RDD, TYPE)
      val new_dependency_graph_edges = getDepGraphEdgeRDD(pattern_per_node_RDD)

      return Graph(new_dependency_graph_vertices_support, new_dependency_graph_edges)
    }
  
  def setRedundantTag(new_graph : Graph[PGNode,Int]) : Graph[PGNode,Int] =
  {
      val redundant_nodes = new_graph.subgraph(vpred = (vid, attr) => {
        attr.getptype == 0
      }).aggregateMessages[Int](edge => {
        if (edge.srcAttr.getsupport == edge.dstAttr.getsupport) edge.sendToSrc(2)
        else edge.sendToSrc(0)
      }, (status1, status2) => {
        math.max(status1, status2)
      })

      return new_graph.outerJoinVertices(redundant_nodes) {
        case (id, pnode, Some(nbr)) => new PGNode(pnode.getnode_label, pnode.getsupport, math.max(pnode.getptype, nbr))
        case (id, pnode, None) => {
          if ((pnode == null) || (pnode.getnode_label == null) || (pnode.getsupport == null) || (pnode.getptype == null)) {
            println("null")
            new PGNode("test", -1, -1)
          } else
            new PGNode(pnode.getnode_label, pnode.getsupport, pnode.getptype)
        }
      }
    }
  
  def setFrequentClosedPromisingTag(SUPPORT:Int) : Graph[PGNode,Int] =
  {
      //Set their status as closed, redundant, promising
      val dep_vertex_rdd = this.gDep.graph.aggregateMessages[Int](
        edge => {
          if (edge.srcAttr.getsupport < SUPPORT) edge.sendToSrc(-1)
          else {
            if (edge.srcAttr.getsupport == edge.dstAttr.getsupport) edge.sendToSrc(1)
            else
              edge.sendToSrc(0)
          }
          if (edge.dstAttr.getsupport < SUPPORT) edge.sendToDst(-1)
          else edge.sendToDst(1)

        }, (status1, status2) => {
          math.min(status1, status2)
        })

      //val only_frequent_dep_vertex_rdd = dep_vertex_rdd.filter(pred)  
      val new_graph_infquent =
        this.gDep.graph.outerJoinVertices(dep_vertex_rdd) {
          case (id, pnode, Some(nbr)) => new PGNode(pnode.getnode_label, pnode.getsupport, math.max(pnode.getptype, nbr))
          case (id, pnode, None) => if ((pnode == null) || (pnode.getnode_label == null) || (pnode.getsupport == null) || (pnode.getptype == null)) {
            println("null")
            new PGNode("test", -1, -1)
          } else
            new PGNode(pnode.getnode_label, pnode.getsupport, pnode.getptype)
        }

      return new_graph_infquent.subgraph(vpred = (vid, attr) => attr.getptype > -1)
    }
  
  def updateGDep(batch_graph: Graph[KGNodeV2Flat, KGEdge], TYPE: String, SUPPORT: Int) =
    {

    	this.gDep.graph = getNewDepGraph(batch_graph, TYPE)

    	//Tag every node in the dependency graph with In-frequent, closed, and promising tag
      this.gDep.graph = this.gDep.graph.subgraph(vpred = (vid, attr) => attr != null)

      val new_graph = setFrequentClosedPromisingTag(SUPPORT)

      // Set redundant tag
      this.gDep.graph = setRedundantTag(new_graph)
        
    }

  def saveDepG() {
    //Collect all super graph on the source (up to size 3 iterations) 
    val newGraph = this.gDep.graph.pregel[List[VertexProperty]](List.empty[VertexProperty],
      3, EdgeDirection.In)(
        (id, dist, newDist) =>
          {
            val newpdgnode = new PGNode(dist.getnode_label, dist.getsupport, dist.getptype)
            newpdgnode.pgprop = dist.pgprop ++ newDist
            newpdgnode
          }, // Vertex Program
        triplet => { // Send Message
          val newprop = new VertexProperty(triplet.dstId, triplet.dstAttr.getnode_label)
          Iterator((triplet.srcId, List(newprop)))
        },
        (a, b) => a ++ b // Merge Message
        )

    val pattern_dep_graph = newGraph.vertices.mapValues(f => {
      val prop_name_list = f.pgprop.map(prop => prop.property_label)
      (f.getnode_label, prop_name_list.length, prop_name_list)
    })
    pattern_dep_graph.saveAsTextFile("DependencyGraphSummary" + System.nanoTime())
    val dgep_v_rdd = this.gDep.graph.vertices.map(v => (v._1, v._2.getnode_label, v._2.getsupport, v._2.getptype))
    dgep_v_rdd.saveAsTextFile("DependencyGraphVertices" + System.nanoTime())
    val dgep_e_rdd = this.gDep.graph.triplets.map(t=>(t.srcAttr.getnode_label+"#"+t.srcAttr.getptype, t.dstAttr.getnode_label+"#"+t.srcAttr.getptype))
    dgep_e_rdd.saveAsTextFile("DependencyGraphEdges" + System.nanoTime())
  }

  /*
     * TODO: should be moved from window state
     */
  def get_Pattern_RDDV2Flat(pattern_per_node_RDD: VertexRDD[Set[(String, Long)]], TYPE: String): RDD[(String, Long)] =
    {
      /*
       * Collects all the patterns across the graph
       */
      val new_dependency_graph_vertices_RDD: RDD[(String, Long)] =
        pattern_per_node_RDD.flatMap(vertex => vertex._2.map(entry =>
          (entry._1.replaceAll("\\|+", "\\|").replaceAll("\\|", "\t").replaceAll("\t+", "\t").trim, entry._2)))

      /*
       * Returns the RDD with each pattern-key and its support
       */
      val res = new_dependency_graph_vertices_RDD.reduceByKey((a, b) => a + b)
      //println("resutling patterkey-count size" + res.count)
      //res.collect.foreach(f=> println(f._1 + "   : " + f._2))
      return res
    }
}