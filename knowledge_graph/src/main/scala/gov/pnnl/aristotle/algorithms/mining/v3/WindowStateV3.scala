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
import gov.pnnl.aristotle.algorithms.mining.datamodel.KGEdgeInt
import gov.pnnl.aristotle.algorithms.mining.datamodel.KGNodeV2FlatInt
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

  var window_graph: Graph[KGNodeV2FlatInt, KGEdgeInt] = null
  var gDep = new PatternDependencyGraph

  /*
   * Merge batch with window graph
   */
  def mergeBatchGraph(batch_graph: Graph[KGNodeV2FlatInt, KGEdgeInt]) =
    {
      if (window_graph == null) {
        WindowStateV3.this.window_graph = batch_graph

      } else {
        val vertices_rdd: RDD[(VertexId, Map[List[Int], Long])] =
          batch_graph.vertices.map(f => (f._1, f._2.getpattern_map))
        
         window_graph = WindowStateV3.this.window_graph.joinVertices[Map[List[Int], Long]](vertices_rdd)((id, kgnode, new_data) => {
          if (kgnode != null)
            new KGNodeV2FlatInt(kgnode.getlabel,
              kgnode.getpattern_map |+| new_data, List.empty)
          else
            new KGNodeV2FlatInt(-1, Map.empty, List.empty)
        })
        
      }
      //window_graph.vertices.collect.foreach(f => println("final window graph sp " , f._1, " : " , f._2))
    }

  /*
   * create vertices rdd of dependency graph
   */
  def getDepGraphVertexRDD(all_pattern_data: RDD[(List[Int], Long)], TYPE: Int): 
  RDD[(VertexId, PGNode)] =
    {
      val new_dependency_graph_vertices_support: RDD[(VertexId, PGNode)] =
        all_pattern_data.filter(pattern=>pattern._1.filterNot(elm => elm == -1).size != 0).map(pattern =>
          (pattern._1.filterNot(elm => elm == -1).hashCode(),
            new PGNode(pattern._1.filterNot(elm => elm == -1), pattern._2, -1)))
      return new_dependency_graph_vertices_support.distinct
    }

  /*
   * create edge rdd of dependency graph
   * TODO: input needs not to be pattern_per_node_RDD and then flat map on that.
   */
  def getDepGraphEdgeRDD(all_pattern_data: RDD[(List[Int], Long)]): RDD[Edge[Int]] =
    {
      val new_dependency_graph_edges: RDD[Edge[Int]] = all_pattern_data.flatMap(vertex =>
        {
          var tmp: Set[Edge[Int]] = Set.empty
          
            // double quote | is treated a OP operator and have special meaning
            // so use '|'
            //val setNodes = p._1.replaceAll("\\|+", "\\|").split('|')
            val sep_index = vertex._1.indexOf(-1)
            val twoLists = vertex._1.splitAt(sep_index)
            //If done want to generate base pattern for some reason, check (twoLists._2.size != 1)
            if ((twoLists._1.size != 0) && (twoLists._2.size != 0) && (twoLists._1.size % 3 == 0) && (twoLists._2.filterNot(elm => elm == -1).size % 3 == 0)) {
              tmp = tmp ++ Array(Edge(twoLists._1.hashCode(), vertex._1.filterNot(elm => elm == -1).hashCode(), 1))
              tmp = tmp ++ Array(Edge(twoLists._2.filterNot(elm => elm == -1).hashCode(), vertex._1.filterNot(elm => elm == -1).hashCode(), 1))
            }
          
          tmp
        })
      return new_dependency_graph_edges.distinct

    }

  def getNewDepGraph(batch_graph: Graph[KGNodeV2FlatInt, KGEdgeInt], TYPE: Int): Graph[PGNode, Int] =
    {
      //Edge(P1.hascode,P2.hashcode,1)
      // "1" is just an edge type which represent "part-of"
      // int is used to save the space 
      //Create Edges in the dep graph
      // it has commulative count of all patterns on each vertex
    
      val pattern_rdd =
    		  batch_graph.vertices.flatMap(vertex =>
      {
        vertex._2.getpattern_map.map(pattern => (pattern._1, pattern._2)).toSet
      })
      val all_pattern_data = pattern_rdd.reduceByKey((a, b) => a + b)
      val new_dependency_graph_vertices_support = getDepGraphVertexRDD(all_pattern_data, TYPE)
      
      val new_dependency_graph_edges = getDepGraphEdgeRDD(all_pattern_data)
      val res =  Graph(new_dependency_graph_vertices_support, new_dependency_graph_edges)
            
      return res
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
            new PGNode(List.empty, -1, -1)
          } else
            new PGNode(pnode.getnode_label, pnode.getsupport, pnode.getptype)
        }
      }
    }
  
  def setFrequentClosedPromisingTag(graph :Graph[PGNode, Int],SUPPORT:Int) : Graph[PGNode,Int] =
  {
      
    //Set their status as closed, redundant, promising
      val dep_vertex_rdd = graph.aggregateMessages[Int](
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
        if(dep_vertex_rdd.count == 0)
        {
          //set all nodes  as closed.
          val new_graph = this.gDep.graph.mapVertices((id, attr) => {
            new PGNode(attr.getnode_label, attr.getsupport, 1)
          })
          return new_graph
        }
      //val only_frequent_dep_vertex_rdd = dep_vertex_rdd.filter(pred)  
      val new_graph_infquent =
        graph.outerJoinVertices(dep_vertex_rdd) {
          case (id, pnode, Some(nbr)) => new PGNode(pnode.getnode_label, pnode.getsupport, math.max(pnode.getptype, nbr))
          case (id, pnode, None) => if ((pnode == null) || (pnode.getnode_label == null) || (pnode.getsupport == null) || (pnode.getptype == null)) {
            println("null")
            new PGNode(List.empty, -1, -1)
          } else
            new PGNode(pnode.getnode_label, pnode.getsupport, pnode.getptype)
        }

      return new_graph_infquent.subgraph(vpred = (vid, attr) => attr.getptype > -1)
    }
  
  def updateGDep(batch_graph: Graph[KGNodeV2FlatInt, KGEdgeInt], TYPE: Int, SUPPORT: Int) =
    {

	
    var updatedGDep = getNewDepGraph(batch_graph, TYPE)
    	//Tag every node in the dependency graph with In-frequent, closed, and promising tag
    	updatedGDep = updatedGDep.subgraph(vpred = (vid, attr) => attr != null)

    	val new_graph = setFrequentClosedPromisingTag(updatedGDep,SUPPORT)
    	// Set redundant tag
        updatedGDep = setRedundantTag(new_graph)
        //this.gDep.graph
        if(this.gDep.graph == null)
          this.gDep.graph = updatedGDep
        else
        	this.gDep.graph = Graph(this.gDep.graph.vertices.union(updatedGDep.vertices).distinct,
        			this.gDep.graph.edges.union(updatedGDep.edges).distinct)
    }

  def saveDepG() {
    //Collect all super graph on the source (up to size 3 iterations) 
    val newGraph = this.gDep.graph.pregel[List[List[Int]]](List.empty,
      3, EdgeDirection.In)(
        (id, dist, newDist) =>
          {
            val newpdgnode = new PGNode(dist.getnode_label, dist.getsupport, dist.getptype)
            newpdgnode.pgprop = dist.pgprop ++ newDist
            newpdgnode
          }, // Vertex Program
        triplet => { // Send Message
          val newprop = triplet.dstAttr.getnode_label
          Iterator((triplet.srcId, List(newprop)))
        },
        (a, b) => a ++ b // Merge Message
        )

    val pattern_dep_graph = newGraph.vertices.mapValues(f => {
      val prop_name_list = f.pgprop.map(prop => prop)
      (f.getnode_label, prop_name_list.length, prop_name_list)
    })
    pattern_dep_graph.saveAsTextFile("DependencyGraphSummary" + System.nanoTime())
    val dgep_v_rdd = this.gDep.graph.vertices.map(v => (v._1, v._2.getnode_label, v._2.getsupport, v._2.getptype))
    dgep_v_rdd.saveAsTextFile("DependencyGraphVertices" + System.nanoTime())
    val dgep_e_rdd = this.gDep.graph.triplets.map(t => (t.srcAttr.getnode_label + "#" + t.srcAttr.getptype, t.dstAttr.getnode_label + "#" + t.srcAttr.getptype))
    dgep_e_rdd.saveAsTextFile("DependencyGraphEdges" + System.nanoTime())
  }

  /*
     * TODO: should be moved from window state
     */
  def get_Pattern_RDDV2Flat(pattern_per_node_RDD: RDD[(VertexId,Set[(List[Int], Long)])],
      TYPE: String): RDD[(List[Int], Long)] =
    {
      /*
       * Collects all the patterns across the graph
       */
      val new_dependency_graph_vertices_RDD: RDD[(List[Int], Long)] =
        pattern_per_node_RDD.flatMap(vertex => vertex._2.map(entry =>
          (entry._1, entry._2)))

      /*
       * Returns the RDD with each pattern-key and its support
       */
      val res = new_dependency_graph_vertices_RDD.reduceByKey((a, b) => a + b)
      return res
    }
}