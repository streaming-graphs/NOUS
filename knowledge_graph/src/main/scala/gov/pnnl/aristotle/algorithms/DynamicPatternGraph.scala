package gov.pnnl.aristotle.algorithms

import org.apache.spark.graphx.Graph
import java.io.PrintWriter
import org.apache.spark.graphx.VertexRDD
import org.apache.spark.graphx.VertexId
import scalaz.Scalaz._
import gov.pnnl.aristotle.algorithms.GraphProfiling
import org.apache.spark.SparkContext
import org.apache.spark.graphx.EdgeContext
import scala.Array.canBuildFrom
import org.apache.spark.graphx.EdgeRDD
import org.apache.spark.graphx.Edge
import org.apache.spark.rdd.RDD
import java.io.File
import java.lang.Boolean
import org.apache.spark.storage.StorageLevel
import org.apache.spark.SparkContext._
import scala.util.control.Breaks._
import java.util.regex.Pattern
import scala.collection.mutable.Stack
import akka.dispatch.Foreach

class DynamicPatternGraph(var minSup: Int) extends Serializable {

  val TYPE: String = "rdf:type"
  var SUPPORT: Int = minSup
  var input_graph: Graph[KGNode, KGEdge] = null
  def init(graph: Graph[String, KGEdge], writerSG: PrintWriter): DynamicPatternGraph = {

    /*
     * Get all the rdf:type dst node information on the source node
     */
    val typedVertexRDD: VertexRDD[Map[String, Map[String, Int]]] =
      GraphProfiling.getTypedVertexRDD_Temporal(graph,
        writerSG, minSup, this.TYPE)

    // Now we have the type information collected in the original graph
    val typedAugmentedGraph: Graph[(String, Map[String, Map[String, Int]]), KGEdge] = GraphProfiling.getTypedAugmentedGraph_Temporal(graph,
      writerSG, typedVertexRDD)

    /*
     * Create RDD where Every vertex has all the 1 edge patterns it belongs to
     * Ex: Sumit: (person worksAt organizaion) , (person friendsWith person)
     */
    val nonTypedVertexRDD: VertexRDD[Map[String, scala.collection.immutable.Set[PatternInstance]]] =
      getNonTypeVertexRDD(typedAugmentedGraph)

    /*
     *    get the updated graph after joining non-type vertex RDD with typed 
     *    graphs received in last step.
     */
    val updatedGraph: Graph[KGNode, KGEdge] =
      typedAugmentedGraph.outerJoinVertices(nonTypedVertexRDD) {
        case (id, (label, something), Some(nbr)) => new KGNode(label, nbr)
        case (id, (label, something), None) => new KGNode(label, Map())
      }

    /*
     *  Filter : all nodes that don't have even a single pattern
     */
    val subgraph_with_pattern = filterNonPatternSubGraph(updatedGraph)
    println("subgraph size" + subgraph_with_pattern.triplets.count)

    /*
     * update sink nodes and push source pattern at sink node with no destination information
     * so that when we compute support of that pattern, it will not change the value.
     * get all nodes which are destination of any edge
     */
    val updateGraph_withsink = updateGraphWithSink(subgraph_with_pattern)

    val result = null
    this.input_graph = GraphPatternProfiler.fixGraph(GraphPatternProfiler.
      get_Frequent_Subgraph(updateGraph_withsink, result, SUPPORT));
    return this

  }

  def filterNonPatternSubGraph(updatedGraph: Graph[KGNode, KGEdge]): Graph[KGNode, KGEdge] =
    {
      return updatedGraph.subgraph(epred =>
        ((epred.attr.getlabel.equalsIgnoreCase(TYPE) != true) &&
          ((epred.srcAttr.getpattern_map.size != 0) ||
            (epred.dstAttr.getpattern_map.size != 0))))

    }
  def reducePatternsOnNode(a: Map[String, Set[PatternInstance]],
    b: Map[String, Set[PatternInstance]]): Map[String, Set[PatternInstance]] =
    {
      return a |+| b
    }

  def updateGraphWithSink(subgraph_with_pattern: Graph[KGNode, KGEdge]): Graph[KGNode, KGEdge] =
    {
      val all_dest_nodes =
        subgraph_with_pattern.triplets.map(triplets => (triplets.dstId, triplets.dstAttr)).distinct
      val all_source_nodes =
        subgraph_with_pattern.triplets.map(triplets => (triplets.srcId, triplets.srcAttr)).distinct
      val all_sink_nodes =
        all_dest_nodes.subtractByKey(all_source_nodes).map(sink_node => sink_node._2.getlabel).collect

      val graph_with_sink_node_pattern: VertexRDD[Map[String, Set[PatternInstance]]] =
        subgraph_with_pattern.aggregateMessages[Map[String, Set[PatternInstance]]](
          edge => {
            if (edge.attr.getlabel.equalsIgnoreCase(TYPE) == false) {
              if (all_sink_nodes.contains(edge.dstAttr.getlabel)) {
                val srcnodepattern = edge.srcAttr.getpattern_map
                val dstNodeLable: String = edge.dstAttr.getlabel
                val srcNodeLable: String = edge.srcAttr.getlabel
                srcnodepattern.foreach(s => {
                  if (s._1.contains(edge.attr.getlabel)) // TODO: fix this weak comparison 
                  {
                    val pattern_instances = s._2
                    pattern_instances.foreach(an_instance => {
                      // because it is one edge pattern yet, so no need to a loop
                      if (an_instance.get_instacne.head._2 == dstNodeLable.hashCode())
                        edge.sendToDst(Map(s._1 -> Set()))
                    })
                  }
                })
              }
            }
          },
          (pattern1NodeN, pattern2NodeN) => {
            reducePatternsOnNode(pattern1NodeN, pattern2NodeN)
          })

      val updateGraph_withsink: Graph[KGNode, KGEdge] =
        subgraph_with_pattern.outerJoinVertices(graph_with_sink_node_pattern) {
          case (id, kg_node, Some(nbr)) =>
            new KGNode(kg_node.getlabel, kg_node.getpattern_map |+| nbr)
          case (id, kg_node, None) =>
            new KGNode(kg_node.getlabel, kg_node.getpattern_map)
        }

      return updateGraph_withsink
    }

  def getNonTypeVertexRDD(typedAugmentedGraph: Graph[(String, Map[String, Map[String, Int]]), KGEdge]): VertexRDD[Map[String, scala.collection.immutable.Set[PatternInstance]]] =
    {
      return typedAugmentedGraph.aggregateMessages[Map[String, scala.collection.immutable.Set[PatternInstance]]](
        edge => {
          if (edge.attr.getlabel.equalsIgnoreCase(TYPE) == false) {
            // Extra info for pattern
            if (edge.srcAttr._2.contains("nodeType") &&
              (edge.dstAttr._2.contains("nodeType"))) {
              val dstnodetype =
                edge.dstAttr._2.getOrElse("nodeType", Map("unknownOOT" -> 1))
              val srcnodetype =
                edge.srcAttr._2.getOrElse("nodeType", Map("unknownOOT" -> 1))
              val dstNodeLable: String =
                edge.dstAttr._1
              val srcNodeLable: String = edge.srcAttr._1
              srcnodetype.foreach(s => {
                dstnodetype.foreach(d => {
                  val patternInstance =
                    edge.attr.getlabel.toString() + "\t" + dstNodeLable
                  edge.sendToSrc(Map(s._1 + "\t" + edge.attr.getlabel + "\t" +
                    d._1 + "|"
                    -> Set(new PatternInstance(scala.collection.immutable.
                      Set((srcNodeLable.hashCode(),
                        dstNodeLable.hashCode()))))))
                })
              })
            }
          }
        },
        (pattern1NodeN, pattern2NodeN) => {
          reducePatternsOnNode(pattern1NodeN, pattern2NodeN)
        })
    }
  def trim(now: Int, windowSize: Int) = {

    //        input_graph = input_graph.subgraph(epred => epred.attr.getdatetime 
    //            > (now - windowSize))
  }

  def merge(gBatch: DynamicPatternGraph, sc: SparkContext): DynamicPatternGraph = {

    if (this.input_graph == null) {
      this.input_graph = gBatch.input_graph
    }

    val vertices_rdd: RDD[(VertexId, Map[String, Set[PatternInstance]])] =
      gBatch.input_graph.vertices.map(f => (f._1, f._2.getpattern_map))

    val new_array: Array[VertexId] = gBatch.input_graph.vertices.map(a_vetext => a_vetext._1).toArray

    val new_window_graph = this.input_graph.subgraph(vpred = (vid, attr) => {
      new_array.contains(vid)
    }).joinVertices[Map[String, Set[PatternInstance]]](vertices_rdd)((id,
      kgnode, new_data) => {
      if (kgnode != null)
        new KGNode(kgnode.getlabel, kgnode.getpattern_map |+| new_data)
      else
        new KGNode("", Map.empty)
    })

    val result = new DynamicPatternGraph(minSup)
    result.input_graph = new_window_graph
    return result
  }

  def joinPatterns(gDep: PatternDependencyGraph, writerSG: PrintWriter,
    level: Int): Graph[KGNode, KGEdge] = {

    // Now mine the smaller graph and return the graph with larger patterns
    return getAugmentedGraphNextSize(this.input_graph,
      writerSG: PrintWriter, level)
  }

  /**
   * This method mines the smaller graph and return the graph with larger patterns
   * Every graph pattern grows using 3 types of joins
   * 1. Self Instance Join : two n-size instances of a Pattern (P) on a vertex
   *    are joined to create bigger pattern (PP) of 2*n size
   * 2. Self Pattern Join : two instances of size (n1, n2) of two different patterns
   * 		P1 and P2 on a vertex are joined to create  bigger pattern P1P2 of size n1+n2
   * 3. Edge Pattern Join:  two instances of size (n1, n2) of two different patterns
   * 		P1 and P2 on source and destination vertices are joined to create  bigger
   *    pattern P1P2 of size n1+n2 on the source vertex
   */

  def getAugmentedGraphNextSize(nThPatternGraph: Graph[KGNode, KGEdge],
    writerSG: PrintWriter,
    iteration_id: Int): Graph[KGNode, KGEdge] =
    {

      var t0 = System.currentTimeMillis();
      var t1 = System.currentTimeMillis();
      /*
	   * TODO: isomorphic patterns like these:
	   * 6 (person       friends_with    person          person  likes   product         person  works_at        company         company makes   nt              nt      is_produced_in  state               state   famous_for      national_park   ,3)
		 * 6 (person       likes   product         person  works_at        company         company makes   nt              nt      is_produced_in  state           state   famous_for      national_park               person  friends_with    person  ,3)
		 * 6 (person       friends_with    person          person  works_at        company         company makes   nt              nt      is_produced_in  state           state   famous_for      natio    nal_park   ,3)
	   * 
	   * 
	   */

      // Step 1: First do self join of existing n-size patterns to create 2*n patterns.
      val g1 = GraphPatternProfiler.self_Instance_Join_Graph(nThPatternGraph,
        iteration_id)

      //Step 2 : Self Join 2 different patterns at a node to create 2*n size pattern
      var res = nThPatternGraph.vertices.map(v => v._2.getInstanceCount)
      val newGraph = GraphPatternProfiler.self_Pattern_Join_Graph(nThPatternGraph,
        iteration_id)
      writerSG.flush()

      /*
      *  STEP 3: instead of aggregating entire graph, map each edgetype
      */
      val nPlusOneThPatternGraph = edge_Pattern_Join_Graph(newGraph, writerSG,
        iteration_id)

      return nPlusOneThPatternGraph
    }

  /**
   *   find_Incorrect_Patterns methods is an auxiliary method used to report any
   *   inconsistencies between the patterns-key and its instances size. If some
   *   sub-graphs has loops in it, there is a possibility that the pattern-key size
   *   will be of longer length than the instance size.
   */
  def find_Incorrect_Patterns(newGraph: Graph[KGNode, KGEdge],
    writerSG: PrintWriter, iteration_id: Int) {
    /*
       * Find incorrect pattern instance so far
       * checking 
       */
    var incorrect_pattern = -1
    val accum = GraphMiner.sc.accumulator(0, "My Accumulator")
    newGraph.vertices.values.foreach(v => v.getpattern_map.foreach(f => f._2.foreach(ins => {
      if (f._1.count(_ == 'E') != ins.get_instacne.size) {
        println("and equall " + f._1.count(_ == 'E') + "  " +
          f._1.toString() + "\t" + ins.get_instacne.size + ins.toString)
        accum += 1
      }

    })))
  }

  /**
   * edge_Pattern_Join_Graph perform 3rd type of the join out of 3 possible join
   * operations available in the graph mining module.
   *
   * Input to this method is an edge. All the patterns available at the destination
   * of the edge are moved to the source vertex after appending the current edge
   * to destination pattern instance and 1-edge pattern of the current edge to the
   * pattern-key at the destination.
   *
   *
   */
  def edge_Pattern_Join_Graph(newGraph: Graph[KGNode, KGEdge],
    writerSG: PrintWriter, iteration_id: Int): Graph[KGNode, KGEdge] = {
    var t0 = System.currentTimeMillis();
    writerSG.flush()
    val nPlusOneThPatternVertexRDD =
      newGraph.aggregateMessages[Map[String, Set[PatternInstance]]](
        edge => {
          if (edge.attr.getlabel.equalsIgnoreCase(TYPE) == false)
            sendPatternToNode(edge, iteration_id)
        }, (pattern1OnNodeN, pattern2OnNodeN) => {
          reducePatternsOnNode(pattern1OnNodeN, pattern2OnNodeN)
        })

    val nPlusOneThPatternGraph = joinPatternGraph(newGraph,
      nPlusOneThPatternVertexRDD)

    return nPlusOneThPatternGraph
  }

  def joinPatternGraph(nThPatternGraph: Graph[KGNode, KGEdge],
    nPlusOneThPatternVertexRDD: VertexRDD[Map[String, Set[PatternInstance]]]): Graph[KGNode, KGEdge] =
    {
      val update_tmp_graph =
        nThPatternGraph.outerJoinVertices(nPlusOneThPatternVertexRDD) {
          case (id, a_kgnode, Some(nbr)) =>
            new KGNode(a_kgnode.getlabel, a_kgnode.getpattern_map |+| nbr)
          case (id, a_kgnode, None) =>
            new KGNode(a_kgnode.getlabel, a_kgnode.getpattern_map)
        }
      val result = null
      return update_tmp_graph
    }

  /**
   * sendPatternToNode method is the core method call by edge_Pattern_Join_Graph
   * method. It iterates through all the patterns and their instances at both the
   * source and destination of the edge. It augment all the patterns at the desti-
   * nation by appending current edge to it. It sends this newly created pattern
   * and its instance to the source vertex using spark GraphX message passing.
   */
  def sendPatternToNode(edge: EdgeContext[KGNode, KGEdge, Map[String, Set[PatternInstance]]],
    iteration_id: Int) {

    /*
     * Get source and Destination patterns.
     */
    val allSourceNodePatterns = edge.srcAttr.getpattern_map;
    val allDestinationNodePatterns = edge.dstAttr.getpattern_map

    if ((allSourceNodePatterns.size > 0)
      && (allDestinationNodePatterns.size > 0)) {
      allSourceNodePatterns.foreach(sr =>
        allDestinationNodePatterns.foreach(dst =>
          {
            sr._2.foreach(sr_instance => {
              dst._2.foreach(dst_instance => {

                /*
                 * Only the one edge patterns at the source which end at the 
                 * destination vertex are eligible to be joined to every pattern
                 * instance at destination.
                 * 
                 * Ex. <Foo knows Bar> can be joined with any pattern at <Bar>
                 * vertex.
                 */
                if ((sr_instance.get_instacne.size == 1) &&
                  (sr_instance.get_instacne.head._2 ==
                    edge.dstAttr.getlabel.hashCode())) {
                  {
                    //save triple pattern as it appears on the source and destination
                    GraphPatternProfiler.sendPattern(sr._1, dst._1,
                      sr_instance, dst_instance, edge)
                  }
                }
              })
            })
          }))
    }
  }

  def updateGDep(gDep: PatternDependencyGraph): Graph[PGNode, Int] =
    {
      val patternRDD =
        this.input_graph.aggregateMessages[Set[(String, Int)]](
          edge => {
            if (edge.attr.getlabel.equalsIgnoreCase(TYPE) == false)
              if ((edge.srcAttr != null)
                && (edge.srcAttr.getpattern_map != null))
                edge.srcAttr.getpattern_map.foreach(pattern =>
                  edge.sendToSrc(Set((pattern._1, pattern._2.size))))
          }, (pattern1OnNodeN, pattern2OnNodeN) => {
            pattern1OnNodeN |+| pattern2OnNodeN
          })

      val tmp_commulative_RDD =
        GraphPatternProfiler.get_Pattern_RDD(this.input_graph)

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

      return Graph(new_dependency_graph_vertices_support,
        new_dependency_graph_edges)

    }

  def mergeBatchGraph(batch_graph: Graph[KGNode, KGEdge]): Graph[KGNode, KGEdge] =
    {
      val vertices_rdd: RDD[(VertexId, Map[String, Set[PatternInstance]])] =
        batch_graph.vertices.map(f => (f._1, f._2.getpattern_map))

      val new_window_graph = this.input_graph.joinVertices[Map[String, Set[PatternInstance]]](vertices_rdd)((id, kgnode, new_data) => {
        if (kgnode != null)
          new KGNode(kgnode.getlabel, kgnode.getpattern_map |+| new_data)
        else
          new KGNode("", Map.empty)
      })

      return new_window_graph
    }

}