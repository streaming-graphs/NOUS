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
  
    def updateGDep(batch_graph: Graph[KGNodeV2Flat, KGEdge], TYPE: String, SUPPORT:Int) =
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
            new PGNode(pattern._1.trim(), pattern._2,-1)))
            //new_dependency_graph_vertices_support.collect.foreach(p=>println("pattern NODES " + p._1 + " " + p._2.getnode_label + " " + p._2.getsupport + " " + p._2.getptype))

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
              val setNodes = p._1.replaceAll("\\|+", "\\|").split('|')
              var b :  Array[Edge[Int]] = Array.empty
              if(setNodes.length > 1)
              {
                //setNodes.foreach(a => println("edge array entry is "+ a))
               b = setNodes.map(arr_entry => Edge(arr_entry.trim().hashCode().toLong,
                  p._1.replaceAll("\\|+", "\\|").replaceAll("\\|", "\t").trim().hashCode().toLong, -2)) 
              }
//              val b = setNodes.filter(arr_entry =>
//                !arr_entry.equalsIgnoreCase(p._1.replaceAll("\\|", "")))
//                .map(arr_entry => Edge(arr_entry.hashCode().toLong,
//                  p._1.replaceAll("\\|", "\t").hashCode().toLong, -2))
              tmp = tmp ++ b
            }
            tmp
          })
//new_dependency_graph_edges.collect.foreach(e=> println("EDGE 0000 is " +e.srcId + "  dst " +e.dstId + "  rel " +e.attr))
      this.gDep.graph = Graph(new_dependency_graph_vertices_support, new_dependency_graph_edges)
      //this.gDep.graph.triplets.collect.foreach(t=> println("scr"+t.srcAttr+"dest"+t.dstAttr+"rel"+t.attr))
      println("graph size 0 " + this.gDep.graph.edges.count)
      //Tag every node in the dependency graph with
      // Set In-frequent, closed, and promising tag
      val dep_vertex_rdd = this.gDep.graph.aggregateMessages[Int](
          edge => {
//            println("in edge")
//          println("node src" + edge.toString())
//          println("node src1" + edge.toEdgeTriplet.srcId)
//          println("node src2" + edge.toEdgeTriplet.dstId)
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
      println("rdd size 1 " + dep_vertex_rdd.count)   
      val new_graph =
      this.gDep.graph.outerJoinVertices(dep_vertex_rdd) {
        case (id, pnode, Some(nbr)) => new PGNode(pnode.getnode_label, pnode.getsupport, math.max(pnode.getptype,nbr))
        case (id, pnode, None) => new PGNode(pnode.getnode_label, pnode.getsupport, pnode.getptype)
      }
      println("graph size" + new_graph.vertices.count)
       // Set redundant tag
      val redundant_nodes = new_graph.subgraph(vpred = (vid, attr) => {
        attr.getptype == 0
      }).aggregateMessages[Int](edge => {
      		if (edge.srcAttr.getsupport == edge.dstAttr.getsupport) edge.sendToSrc(1)
      		else edge.sendToSrc(0)
      }, (status1, status2) => {
            math.max(status1, status2)
          })
     println("rdd size 2" + redundant_nodes.count)
     
    this.gDep.graph  =
      new_graph.outerJoinVertices(redundant_nodes) {
        case (id, pnode, Some(nbr)) => new PGNode(pnode.getnode_label, pnode.getsupport, math.max(pnode.getptype,nbr))
        case (id, pnode, None) => new PGNode(pnode.getnode_label, pnode.getsupport, pnode.getptype)
      }
          println("graph  size 2" + this.gDep.graph.vertices.count)
  
     //this.gDep.graph.triplets.collect.foreach(t=> println(t.toString)) 
    }
  
    def saveDepG()
    {
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
    pattern_dep_graph.saveAsTextFile("DependencyGraphPatterns" + System.nanoTime())

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
          (entry._1.replaceAll("\\|+", "\\|").replaceAll("\\|", "\t").trim, entry._2)))

      /*
       * Returns the RDD with each pattern-key and its support
       */
      return new_dependency_graph_vertices_RDD.reduceByKey((a, b) => a + b)
    }
}