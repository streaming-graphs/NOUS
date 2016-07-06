package gov.pnnl.aristotle.algorithms.mining

import org.apache.spark.graphx.Graph
import java.io.PrintWriter
import org.apache.spark.graphx.VertexRDD
import org.apache.spark.graphx.VertexId
import scalaz.Scalaz._
import org.apache.spark.SparkContext
import org.apache.spark.graphx.EdgeContext
import scala.Array.canBuildFrom
import org.apache.spark.graphx.Edge
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext._
import scala.util.control.Breaks._
import gov.pnnl.aristotle.algorithms.mining.datamodel.KGEdge
import gov.pnnl.aristotle.algorithms.mining.datamodel.KGNodeV1
import gov.pnnl.aristotle.algorithms.mining.datamodel.KGNodeV2
import gov.pnnl.aristotle.algorithms.mining.datamodel.PGNode
import gov.pnnl.aristotle.algorithms.mining.datamodel.PatternDependencyGraph
import gov.pnnl.aristotle.algorithms.mining.datamodel.PatternInstance
import org.apache.spark.graphx.Graph.graphToGraphOps
import gov.pnnl.aristotle.algorithms.mining.datamodel.KGNodeV2Flat
import gov.pnnl.aristotle.algorithms.GraphMiner


class DynamicPatternGraphV2Flat(var minSup: Int) 
extends DynamicPatternGraph[KGNodeV2Flat, KGEdge] {
 
  var TYPE: String = "rdf:type"
  var SUPPORT: Int = minSup
  var type_support :Int = 2
  var type_profile_rdd :RDD[(String,List[String])]= null
  var input_graph: Graph[KGNodeV2Flat, KGEdge] = null
  
  
  def get_type_profile_rdd(typedAugmentedGraph: Graph[(String, Map[String, Map[String, Int]]), 
      KGEdge]) : RDD[(String,List[String])] = 
  {
      type_profile_rdd = typedAugmentedGraph.vertices.flatMap(vertex =>
        {
          vertex._2._2.getOrElse("nodeType", Map()).map(a_type => (a_type._1, List(vertex._2._1)))
        }).reduceByKey((a, b) => a |+| b)
      type_profile_rdd
    }
  
  
  def getTypedGraph(graph: Graph[String, KGEdge],
      writerSG: PrintWriter)
  	: Graph[(String, Map[String, Map[String, Int]]), 
      KGEdge] =
  {
      val typedVertexRDD: VertexRDD[Map[String, Map[String, Int]]] =
        GraphProfiling.getTypedVertexRDD_Temporal(graph,
          writerSG, type_support, this.TYPE)

      // Now we have the type information collected in the original graph
      val typedAugmentedGraph: Graph[(String, Map[String, Map[String, Int]]), KGEdge] 
      = GraphProfiling.getTypedAugmentedGraph_Temporal(graph,
        writerSG, typedVertexRDD)
      return typedAugmentedGraph
    }
  
  /*
   * takes raw graph as input and returns a graph with one-edge patterns 
   * on the source of the pattern.
   */
  override def init(graph: Graph[String, KGEdge], writerSG: PrintWriter,basetype:String,
      type_support : Int): DynamicPatternGraphV2Flat = {

    /*
     * Get all the rdf:type dst node information on the source node
     */
    this.TYPE = basetype
    this.type_support = type_support
    println("***************support is " + SUPPORT)

    // Now we have the type information collected in the original graph
    val typedAugmentedGraph: Graph[(String, Map[String, Map[String, Int]]), 
      KGEdge] = getTypedGraph(graph, writerSG)
    

    /*
     * Create RDD where Every vertex has all the 1 edge patterns it belongs to
     * Ex: Sumit: (person worksAt organizaion) , (person friendsWith person)
     * 
     * Read method comments
     * ....
     * ....
     * ....So in some sense, the methods' name is misleading. TODO: fix the name
     */
    val nonTypedVertexRDD: VertexRDD[Map[String, 
      Long]] =
        getOneEdgePatterns(typedAugmentedGraph)
    //nonTypedVertexRDD.collect.foreach(f=>writerSG.println("non-type"+f.toString))
    val updatedGraph: Graph[KGNodeV2Flat, KGEdge] =
      typedAugmentedGraph.outerJoinVertices(nonTypedVertexRDD) {
        case (id, (label, something), Some(nbr)) => new KGNodeV2Flat(label, nbr,List.empty)
        case (id, (label, something), None) => new KGNodeV2Flat(label, Map(),List.empty)
      }
    /*
     *  Filter : all nodes that don't have even a single pattern
     */
    writerSG.flush()
    /*
     * update sink nodes and push source pattern at sink node with no destination information
     * so that when we compute support of that pattern, it will not change the value.
     * get all nodes which are destination of any edge
     */
    val updateGraph_withsink = updateGraphWithSinkV2(updatedGraph)
    println("sink graph done")
    val result = null
    this.input_graph = GraphPatternProfiler.get_Frequent_SubgraphV2Flat(updateGraph_withsink, result, SUPPORT)
    return this

  }

  def filterNonPatternSubGraphV2(updatedGraph :Graph[KGNodeV2Flat,KGEdge])
  :Graph[KGNodeV2Flat,KGEdge]=
  {
    return updatedGraph.subgraph(epred =>
      ((epred.attr.getlabel.equalsIgnoreCase(TYPE) != true) &&
        ((epred.srcAttr.getpattern_map.size != 0) || 
            (epred.dstAttr.getpattern_map.size != 0))))
    
  }
  def reducePatternsOnNode(a: Map[String, Set[PatternInstance]], b: Map[String, Set[PatternInstance]]): Map[String, Set[PatternInstance]] =
    {
      return a |+| b
    }
  
  def reducePatternsOnNodeV2(a: Map[String, Long], b: Map[String, Long]): Map[String, Long] =
    {
    return a |+| b
    }
  
  def updateGraphWithSinkV2(subgraph_with_pattern : 
      Graph[KGNodeV2Flat,KGEdge]) :
  Graph[KGNodeV2Flat, KGEdge] =
  {
      val all_dest_nodes =
        subgraph_with_pattern.triplets.map(triplets => (triplets.dstId, triplets.dstAttr)).distinct
      val all_source_nodes =
        subgraph_with_pattern.triplets.map(triplets => (triplets.srcId, triplets.srcAttr)).distinct
      val all_sink_nodes =
        all_dest_nodes.subtractByKey(all_source_nodes).map(sink_node => sink_node._2.getlabel).collect

      val graph_with_sink_node_pattern: VertexRDD[Map[String, Long]] =
        subgraph_with_pattern.aggregateMessages[Map[String, Long]](
          edge => {
            if (edge.attr.getlabel.equalsIgnoreCase(TYPE) == false) {
              if (all_sink_nodes.contains(edge.dstAttr.getlabel)) {
                val srcnodepattern = edge.srcAttr.getpattern_map
                val dstNodeLable: String = edge.dstAttr.getlabel
                val srcNodeLable: String = edge.srcAttr.getlabel
                if (srcNodeLable != null && dstNodeLable != null) {
                  srcnodepattern.foreach(s => {
                    if (s._1.contains(edge.attr.getlabel)) // TODO: fix this weak comparison 
                    {
                      edge.sendToDst(Map(s._1 -> 0))
                      val pattern_instances = s._2
                    }
                  })
                }
              }
            }
          },
          (pattern1NodeN, pattern2NodeN) => {
            reducePatternsOnNodeV2(pattern1NodeN, pattern2NodeN)
          })

      val updateGraph_withsink: Graph[KGNodeV2Flat, KGEdge] = 
        subgraph_with_pattern.outerJoinVertices(graph_with_sink_node_pattern) {
        case (id, kg_node, Some(nbr)) => new KGNodeV2Flat(kg_node.getlabel, kg_node.getpattern_map |+| nbr,List.empty)
        case (id, kg_node, None) => new KGNodeV2Flat(kg_node.getlabel, kg_node.getpattern_map,List.empty)
      }

      return updateGraph_withsink
    }
  
def getNonTypeVertexRDD(typedAugmentedGraph: Graph[(String, 
    Map[String, Map[String, Int]]), KGEdge]) : VertexRDD[Map[String, 
      scala.collection.immutable.Set[PatternInstance]]] =
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

/*
 * This method takes a typed graph whre each node has all the types they 
 * belong to. Output of this file is a Vertex RDD which has all the possible
 * 1-edge patterns (at their source). So if the source which label "user5" has 2 
 * types namely "person" and "user5" ( because of its degree) and destination 
 * say "company10" also has two types "company" and "company10". The result will
 * be a RDD where "user5" will have 4 1-edge patterns "person worksat company",
 * "person worksat company10", "user5 worksat company", and "user5 worksat company10".
 * 
 * So in some sense, the methods' name is misleading. TODO: fix the name
 */

def getOneEdgePatterns(typedAugmentedGraph: Graph[(String, 
    Map[String, Map[String, Int]]), KGEdge]) : VertexRDD[Map[String, 
      Long]] =
{
      return typedAugmentedGraph.aggregateMessages[Map[String, Long]](
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
                    -> 1))
                })
              })
            }
          }
        },
        (pattern1NodeN, pattern2NodeN) => {
          reducePatternsOnNodeV2(pattern1NodeN, pattern2NodeN)
        })
    }

  def trim(now: Int, windowSize: Int) = {

    //        input_graph = input_graph.subgraph(epred => epred.attr.getdatetime 
    //            > (now - windowSize))
  }

  /**
   * takes gBatch as input which is 'mined' batch graph and merge it with 
   * existing window graph. 
   */
  def merge(gBatch: DynamicPatternGraphV2Flat, sc: SparkContext): DynamicPatternGraphV2Flat = {

    if (this.input_graph == null) {
      this.input_graph = gBatch.input_graph
      val result = new DynamicPatternGraphV2Flat(minSup)
      result.input_graph = this.input_graph
      return result

    } else {
      val vertices_rdd: RDD[(VertexId, Map[String, Long])] =
        gBatch.input_graph.vertices.map(f => (f._1, f._2.getpattern_map))

      val new_array: Array[VertexId] = gBatch.input_graph.vertices.map(a_vetext => a_vetext._1).toArray

      val new_window_graph = this.input_graph.subgraph(vpred = (vid, attr) => {
        new_array.contains(vid)
      }).joinVertices[Map[String, Long]](vertices_rdd)((id,
        kgnode, new_data) => {
        if (kgnode != null)
          new KGNodeV2Flat(kgnode.getlabel, kgnode.getpattern_map |+| new_data, List.empty)
        else
          new KGNodeV2Flat("", Map.empty, List.empty)
      })
      
      val result = new DynamicPatternGraphV2Flat(minSup)
      result.input_graph = new_window_graph
      return result
    }

  }

  def joinPatterns(gDep: PatternDependencyGraph, writerSG: PrintWriter,
    level: Int): Graph[KGNodeV2Flat, KGEdge] = {

    // Now mine the smaller graph and return the graph with larger patterns
    return getAugmentedGraphNextSizeV2(this.input_graph,
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
     
  def getAugmentedGraphNextSizeV2(nThPatternGraph: Graph[KGNodeV2Flat, KGEdge],
    writerSG: PrintWriter,
    iteration_id: Int): Graph[KGNodeV2Flat, KGEdge] =
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
      // TODO: This creats many issues. so disable it for now
      t0 = System.currentTimeMillis();
      val g1 = nThPatternGraph//GraphPatternProfiler.self_Instance_Join_GraphV2Flat(nThPatternGraph, iteration_id)
      t1 = System.currentTimeMillis();
			//      writerSG.println("#Time to do self_instance join graph" + " =" + (t1 - t0) * 1e-3 +
			//        "s and" + "#TSize of self_instance update graph" + " =" + g1.vertices.first.toString)
			//      writerSG.flush()

      //Step 2 : Self Join 2 different patterns at a node to create 2*n size pattern
      var res = nThPatternGraph.vertices.map(v => (v._1,v._2.getInstanceCount))
      t0 = System.currentTimeMillis();
      val newGraph = GraphPatternProfiler.self_Pattern_Join_GraphV2Flat(g1, iteration_id)
      t1 = System.currentTimeMillis();
      println("#Time to do self_pattern join graph" +
        " =" + (t1 - t0) * 1e-3 + "s and " +
        "#TSize of self_pattern update graph" + " =" + newGraph.vertices.first.toString)
      writerSG.flush()
      res = nThPatternGraph.vertices.map(v => (v._1,v._2.getInstanceCount))
 
      //println("Second maximum number of instances on any node " + res.values.collect.max)
      

      /*
      *  STEP 3: instead of aggregating entire graph, map each edgetype
      */
      t0 = System.currentTimeMillis();
      val newGraph1 = filterNonPatternSubGraphV2(newGraph)
      newGraph.unpersist(true) //Blocking call
      val nPlusOneThPatternGraph = edge_Pattern_Join_GraphV2(newGraph1, writerSG, iteration_id)
      t1 = System.currentTimeMillis();
      newGraph1.unpersist(true) //Blocking call
      writerSG.println("#Time to do edge_join  graph" + " =" +
        (t1 - t0) * 1e-3 + "seconds," + nPlusOneThPatternGraph.vertices.count)

      return nPlusOneThPatternGraph
    }

  /**
   *   find_Incorrect_Patterns methods is an auxiliary method used to report any
   *   inconsistencies between the patterns-key and its instances size. If some
   *   sub-graphs has loops in it, there is a possibility that the pattern-key size
   *   will be of longer length than the instance size.
   */
  def find_Incorrect_Patterns(newGraph: Graph[KGNodeV1, KGEdge],
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
  def edge_Pattern_Join_GraphV2(newGraph1: Graph[KGNodeV2Flat, KGEdge],
    writerSG: PrintWriter, iteration_id: Int): Graph[KGNodeV2Flat, KGEdge] = {
    var t0 = System.currentTimeMillis();
    writerSG.flush()
    val newGraph = newGraph1.subgraph(epred =>
      ((epred.attr.getlabel.equalsIgnoreCase(TYPE) != true) &&
        (epred.srcAttr.getpattern_map.size != 0) & 
            (epred.dstAttr.getpattern_map.size != 0)))

    if(iteration_id >3 )
    {
       val nPlusOneThPatternVertexRDD =
      newGraph.subgraph(epred =>
      ((epred.attr.getlabel.equalsIgnoreCase(TYPE) != true) &&
        ((epred.srcAttr.getpattern_map.size != 0) || 
            (epred.dstAttr.getpattern_map.size != 0)))).aggregateMessages[Long](
        edge => {
          //if (edge.attr.getlabel.equalsIgnoreCase(TYPE) == false)
          println("edge" + edge.srcAttr)
            edge.sendToSrc(1)
        }, (pattern1OnNodeN, pattern2OnNodeN) => {
          println("reduce msg")
          pattern1OnNodeN + pattern2OnNodeN
        })
        
        //val a = nPlusOneThPatternVertexRDD.mapValues(f=>f).reduce((a,b)=>a+b)
        val b : Double = nPlusOneThPatternVertexRDD.map(f=>f._2).sum
        println("total number of mesages are "+b)
       //System.exit(1)
    }

   
    val nPlusOneThPatternVertexRDD =
      newGraph.aggregateMessages[Map[String, Long]](
        edge => {
          //if (edge.attr.getlabel.equalsIgnoreCase(TYPE) == false)
          println("sending edge msg")  
          sendPatternToNodeV2(edge, iteration_id)
            
        }, (pattern1OnNodeN, pattern2OnNodeN) => {
          println("reducsing edge msg")
          reducePatternsOnNodeV2(pattern1OnNodeN, pattern2OnNodeN)
        })
    var t1 = System.currentTimeMillis();
    println("#Time to do get nPlusOneThPatternVertexRDD"
      + " =" + (t1 - t0) * 1e-3 + "s and" + nPlusOneThPatternVertexRDD.count)

    t0 = System.currentTimeMillis();
    val nPlusOneThPatternGraph = joinPatternGraphV2(newGraph, nPlusOneThPatternVertexRDD)
    t1 = System.currentTimeMillis();
    println("#Time to do edge_join  graph" + " =" + (t1 - t0) * 1e-3 + "seconds," +
      "#TSize of  edge_join update graph" + " =" + newGraph.vertices.count)

    return nPlusOneThPatternGraph
  }

  def joinPatternGraphV2(nThPatternGraph: Graph[KGNodeV2Flat, KGEdge],
    nPlusOneThPatternVertexRDD: VertexRDD[Map[String, Long]]):
    Graph[KGNodeV2Flat, KGEdge] =
    {
      val update_tmp_graph =
        nThPatternGraph.outerJoinVertices(nPlusOneThPatternVertexRDD) {
          case (id, a_kgnode, Some(nbr)) =>
            new KGNodeV2Flat(a_kgnode.getlabel, 
                a_kgnode.getpattern_map |+| nbr,List.empty)
          case (id, a_kgnode, None) =>
            new KGNodeV2Flat(a_kgnode.getlabel, 
                a_kgnode.getpattern_map,List.empty)
        }
      val result = null
      return update_tmp_graph
    }

  def getPatternSize(patternKey:String) : Int =
  {
    val tocken_count = patternKey.trim().split("\t").length 
    if(tocken_count % 3 == 0)
      return tocken_count / 3
      else 
        return -1
  }
  /**
   * sendPatternToNode method is the core method call by edge_Pattern_Join_Graph
   * method. It iterates through all the patterns and their instances at both the
   * source and destination of the edge. It augment all the patterns at the desti-
   * nation by appending current edge to it. It sends this newly created pattern
   * and its instance to the source vertex using spark GraphX message passing.
   */
  def sendPatternToNodeV2(edge: EdgeContext[KGNodeV2Flat, KGEdge, Map[String, Long]],
    iteration_id: Int) {

    /*
     * Get source and Destination patterns.
     */
    val allSourceNodePatterns = edge.srcAttr.getpattern_map;
    val allDestinationNodePatterns = edge.dstAttr.getpattern_map
//    		println("s = " + edge.srcAttr.getlabel + " s size " + allDestinationNodePatterns.size + 
//    		    " and d= " +   edge.dstAttr.getlabel + " d size " + allDestinationNodePatterns.size )
    if ((allSourceNodePatterns.size > 0) && (allDestinationNodePatterns.size > 0)) {
      allSourceNodePatterns.foreach(sr =>
        {
        if(getPatternSize(sr._1) == 1)
        {
                  //println("pattern size "+getPatternSize(sr._1) + "  in " + iteration_id)
        	allDestinationNodePatterns.foreach(dst =>
          {
            {
              //Ex. <Foo knows Bar> can be joined with any pattern at <Bar>
              if(sr._1.split("\\t")(2).equals(dst._1.split("\\t")(0)))
              {
                
                GraphPatternProfiler.sendPatternV2Flat(sr._1, dst._1,
                      sr._2, dst._2, edge)
              }
            }
          })
        }
        }

        )
    }
    //println("done sending msg")
  }

  def updateGDep(gDep: PatternDependencyGraph): Graph[PGNode, Int] =
    {
      val patternRDD =
        this.input_graph.aggregateMessages[Set[(String, Long)]](
          edge => {
            if (edge.attr.getlabel.equalsIgnoreCase(TYPE) == false)
              if ((edge.srcAttr != null) && (edge.srcAttr.getpattern_map != null))
                edge.srcAttr.getpattern_map.foreach(pattern =>
                  edge.sendToSrc(Set((pattern._1, pattern._2))))
          }, (pattern1OnNodeN, pattern2OnNodeN) => {
            pattern1OnNodeN |+| pattern2OnNodeN
          })

      val tmp_commulative_RDD =
        GraphPatternProfiler.get_Pattern_RDDV2Flat(this.input_graph)

      val new_dependency_graph_vertices_support: RDD[(VertexId, PGNode)] =
        tmp_commulative_RDD.map(pattern =>
          (pattern._1.hashCode().toLong,
            new PGNode(pattern._1, pattern._2,-1)))

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

      return Graph(new_dependency_graph_vertices_support, new_dependency_graph_edges)

    }
  
  def mergeBatchGraph(batch_graph : Graph[KGNodeV2Flat,KGEdge]) :
  Graph[KGNodeV2Flat,KGEdge] =
  {
    val vertices_rdd: RDD[(VertexId, Map[String, Long])] =
        batch_graph.vertices.map(f => (f._1, f._2.getpattern_map))

      val new_window_graph = this.input_graph.joinVertices[Map[String, 
        Long]](vertices_rdd)((id, kgnode, new_data) => {
        if (kgnode != null)
          new KGNodeV2Flat(kgnode.getlabel, 
              kgnode.getpattern_map |+| new_data,List.empty)
        else
          new KGNodeV2Flat("", Map.empty,List.empty)
      })

      return new_window_graph
  }
  
}