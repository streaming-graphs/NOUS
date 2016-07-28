/**
 *
 * @author puro755
 * @dJul 5, 2016
 * @knowledge_graph
 */
package gov.pnnl.aristotle.algorithms.mining.v3

import org.apache.spark.graphx.Graph
import gov.pnnl.aristotle.algorithms.mining.datamodel.KGEdgeInt
import gov.pnnl.aristotle.algorithms.mining.datamodel.KGNodeV2FlatInt
import java.io.PrintWriter
import gov.pnnl.aristotle.algorithms.mining.GraphProfiling
import org.apache.spark.graphx.VertexRDD
import gov.pnnl.aristotle.algorithms.mining.datamodel.PGNode
import org.apache.spark.rdd.RDD
import scalaz.Scalaz._
import org.apache.spark.SparkContext._
import gov.pnnl.aristotle.algorithms.mining.datamodel.PatternDependencyGraph
import gov.pnnl.aristotle.algorithms.mining.datamodel.PatternGraph
import org.apache.spark.graphx.EdgeContext
import org.apache.spark.SparkContext
import org.apache.spark.graphx.VertexId
import org.apache.spark.graphx.Edge
import gov.pnnl.aristotle.algorithms.mining.datamodel.VertexProperty
import gov.pnnl.aristotle.algorithms.mining.GraphPatternProfiler
import gov.pnnl.aristotle.algorithms.mining.datamodel.KGEdgeInt
import gov.pnnl.aristotle.algorithms.mining.datamodel.KGNodeV2FlatInt

/**
 * @author puro755
 *
 */
class CandidateGeneration(val minSup: Int) extends Serializable{

  var TYPE: Int = 0
  var SUPPORT: Int = minSup
  var type_support: Int = 2
  var input_graph: Graph[KGNodeV2FlatInt, KGEdgeInt] = null
  val batch_id_map : Map[Int,(Long,Long)] = Map.empty

  def init(sc : SparkContext, graph: Graph[Int, KGEdgeInt], writerSG: PrintWriter, basetype: Int,
    type_support: Int): CandidateGeneration = {

    /*
     * Get all the rdf:type dst node information on the source node
     */
    this.TYPE = basetype
    this.type_support = type_support
    println("***************support is " + SUPPORT)

    // Now we have the type information collected in the original graph
    val typedAugmentedGraph: Graph[(Int, Map[Int, Int]), KGEdgeInt] = getTypedGraph(graph, writerSG)
    /*
     * Create RDD where Every vertex has all the 1 edge patterns it belongs to
     * Ex: Sumit: (person worksAt organizaion) , (person friendsWith person)
     * 
     * Read method comments
     * ....
     * ....
     * 
     */
    val nonTypedVertexRDD: VertexRDD[Map[List[Int], Long]] = getOneEdgePatterns(typedAugmentedGraph)
    val updatedGraph: Graph[KGNodeV2FlatInt, KGEdgeInt] =
      typedAugmentedGraph.outerJoinVertices(nonTypedVertexRDD) {
        case (id, (label, something), Some(nbr)) => new KGNodeV2FlatInt(label, nbr, List.empty)
        case (id, (label, something), None) => new KGNodeV2FlatInt(label, Map(), List.empty)
      }

    writerSG.flush()
    /*
     * update sink nodes and push source pattern at sink node with no destination information
     * so that when we compute support of that pattern, it will not change the value.
     * get all nodes which are destination of any edge
     */
    val updateGraph_withsink = updateGraphWithSinkV2(updatedGraph)
    val result = null
    this.input_graph = get_Frequent_SubgraphV2Flat(sc,updatedGraph, result, SUPPORT)
    return this
  }

  def getTypedGraph(graph: Graph[Int, KGEdgeInt],
    writerSG: PrintWriter): Graph[(Int, Map[Int, Int]), KGEdgeInt] =
    {
      val typedVertexRDD: VertexRDD[Map[Int, Int]] =
        GraphProfiling.getTypedVertexRDD_Temporal(graph,
          writerSG, type_support, this.TYPE.toInt)
      // Now we have the type information collected in the original graph
      val typedAugmentedGraph: Graph[(Int, Map[Int, Int]), KGEdgeInt] 
    		  = GraphProfiling.getTypedAugmentedGraph_Temporal(graph,
        writerSG, typedVertexRDD)
      return typedAugmentedGraph
    }

  def getOneEdgePatterns(typedAugmentedGraph: Graph[(Int,  
    Map[Int, Int]), KGEdgeInt]): VertexRDD[Map[List[Int], Long]] =
    {
      return typedAugmentedGraph.aggregateMessages[Map[List[Int], Long]](
        edge => {
          if (edge.attr.getlabel != TYPE ) {
            // Extra info for pattern
            if ((edge.srcAttr._2.size > 0) &&
              (edge.dstAttr._2.size > 0)) {
              val dstnodetype = edge.dstAttr._2
              val srcnodetype = edge.srcAttr._2
              srcnodetype.foreach(s => {
                dstnodetype.foreach(d => {
                  edge.sendToSrc(Map(List(s._1, edge.attr.getlabel,
                    d._1, -1)
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
  def reducePatternsOnNodeV2(a: Map[List[Int], Long], b: Map[List[Int], Long]): Map[List[Int], Long] =
    {
      return a |+| b
    }

  def updateGraphWithSinkV2(subgraph_with_pattern: Graph[KGNodeV2FlatInt, KGEdgeInt]): Graph[KGNodeV2FlatInt, KGEdgeInt] =
    {
        val vertices_with_sink_status: VertexRDD[Int] =
        subgraph_with_pattern.aggregateMessages[Int](
          edge => {
            edge.sendToSrc( 1 )
            edge.sendToDst( 0 )
          },
          ( nodetype1, nodetype2 ) => nodetype1 + nodetype2 )
        
        val all_sink_nodes_rdd = vertices_with_sink_status.filter(v=>v._2==0)
        
        val graph_with_sink_info:  Graph[KGNodeV2FlatInt, KGEdgeInt] =
        subgraph_with_pattern.outerJoinVertices(all_sink_nodes_rdd) {
          case ( id, kg_node, Some( nbr ) ) => new KGNodeV2FlatInt( kg_node.getlabel, kg_node.getpattern_map, kg_node.getProperties ++ List( new VertexProperty( 1, "sinknode".hashCode() ) ) )
          case ( id, kg_node, None )        => new KGNodeV2FlatInt( kg_node.getlabel, kg_node.getpattern_map, kg_node.getProperties )
        }
        
      val sink_only_graph = graph_with_sink_info.subgraph(vpred = (vid, attr) => {
        !attr.getVertextPropLableArray.contains("sinknode".hashCode())
      })
      
      val graph_with_sink_node_pattern: VertexRDD[Map[List[Int], Long]] =
        sink_only_graph.aggregateMessages[Map[List[Int], Long]](
          edge => {
            if (edge.attr.getlabel != TYPE) {
                val srcnodepattern = edge.srcAttr.getpattern_map
                val dstNodeLable: Int = edge.dstAttr.getlabel
                val srcNodeLable: Int = edge.srcAttr.getlabel
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
          },
          (pattern1NodeN, pattern2NodeN) => {
            reducePatternsOnNodeV2(pattern1NodeN, pattern2NodeN)
          })

      val updateGraph_withsink: Graph[KGNodeV2FlatInt, KGEdgeInt] =
        subgraph_with_pattern.outerJoinVertices(graph_with_sink_node_pattern) {
          case (id, kg_node, Some(nbr)) => new KGNodeV2FlatInt(kg_node.getlabel, kg_node.getpattern_map |+| nbr, List.empty)
          case (id, kg_node, None) => new KGNodeV2FlatInt(kg_node.getlabel, kg_node.getpattern_map, List.empty)
        }

      return updateGraph_withsink
    }

  def joinPatterns(writerSG: PrintWriter,
    level: Int): Graph[KGNodeV2FlatInt, KGEdgeInt] = {

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

  def getAugmentedGraphNextSizeV2(nThPatternGraph: Graph[KGNodeV2FlatInt, KGEdgeInt],
    writerSG: PrintWriter,
    iteration_id: Int): Graph[KGNodeV2FlatInt, KGEdgeInt] =
    {

      // Step 1: First do self join of existing n-size patterns to create 2*n patterns.
      // This creates many scalability issues without increase in the result quality.
      // so disable it for now
      val g1 = nThPatternGraph //GraphPatternProfiler.self_Instance_Join_GraphV2Flat(nThPatternGraph, iteration_id)

      //Step 2 : Self Join 2 different patterns at a node to create 2*n size pattern
      val newGraph = self_Pattern_Join_GraphV2Flat(g1, iteration_id)


      /*
      *  STEP 3: instead of aggregating entire graph, map each edgetype
      */
      val nPlusOneThPatternGraph = edge_Pattern_Join_GraphV2(newGraph, writerSG, iteration_id)
      newGraph.unpersist(true) //Blocking call
      return nPlusOneThPatternGraph
    }

  def self_Pattern_Join_GraphV2Flat(updateGraph_withsink: Graph[KGNodeV2FlatInt, KGEdgeInt],
    join_size: Int): Graph[KGNodeV2FlatInt, KGEdgeInt] = {
    val newGraph = updateGraph_withsink.mapVertices((id, attr) => {

      /*
       * Initialize some local objects
       */
      var thisnodename = attr.getlabel
      var thisnodemap = attr.getpattern_map
      var selfJoinPatternstmp: List[(List[Int], Long)] = List.empty
      var joinedPattern: Map[List[Int], Long] = Map.empty

      /*
       * Create a list of the pattern. List provides an ordering of the pattern
       */
      attr.getpattern_map.foreach(sr1 =>
        {
          selfJoinPatternstmp = selfJoinPatternstmp :+ sr1
        })
        
      val selfJoinPatterns = selfJoinPatternstmp

      /*
       * Pre-compute the join-able patterns to avoid double join and 
       * wrong support calculation. 
       * DFS key provides localized unique keys but it is still possible to construct same pattern key just by changing
       * the 'glue' location. 'glue' use used to construct dependency graph.
       *  
       * Ex:
       * 
       * A: List(-1078222292, 160090837, 118807), and ,List(-1078222292, 160090837, -1237882651, -1078222292, 160090837, -361161128)  
       * B: List(-1078222292, 160090837, -361161128, -1078222292, 160090837, 118807), and ,List(-1078222292, 160090837, -1237882651)
       * C: List(-1078222292, 160090837, -1237882651, -1078222292, 160090837, 118807), and ,List(-1078222292, 160090837, -361161128))
       * 
       * based on lexicographic order edge with -1237882651 is 0, -361161128 is 1, and 118807 is 2
       * 
       * A and C will lead to same graph key with same 'glue' index : 0 glue 1 2
       * B will lead to same key but with different 'glue' index : 0 1 glue 2
       * 
       * If we compare keys ignoring 'glue' we can find out that both they keys are same, and we need to join only 
       * one of A,B,OR C.
       */
      var normalized_join : Map[List[Int],(List[Int], List[Int] ,List[Int], Long, Long)] = Map.empty
      // normalized_bigger_pattern -> bigger_pattern_with_glue, smallerpattern1, smallerpattern2, smallerpattern1_support, smallerpattern2_support
      for (i <- 0 until selfJoinPatterns.length) {
        for (j <- i + 1 until selfJoinPatterns.length) {
        	 /*
           * Adding various Filter Heuristics based on the graph structure, and
           * entity type.
           * IF two sub-grpahs are joined, they are joined based on their DFS lexicographic order
           */
          val smallpattern1 = selfJoinPatterns(i)._1.filterNot(elm => elm == -1)
          val smallpattern1_support : Long = selfJoinPatterns(i)._2
          val smallpattern2 = selfJoinPatterns(j)._1.filterNot(elm => elm == -1)
          val smallpattern2_support : Long = selfJoinPatterns(j)._2
          if (!smallpattern2.contains(smallpattern1) &&
            !smallpattern1.contains(smallpattern2) &&
            (!FilterHeuristics.checkcross_join(smallpattern1, smallpattern2)) &&
            (FilterHeuristics.non_overlapping(smallpattern1, smallpattern2)) &&
            //(FilterHeuristics.compatible_join(selfJoinPatterns(i)._1, selfJoinPatterns(j)._1)) &&
            (!FilterHeuristics.redundant_join(smallpattern1, smallpattern2,join_size))) {

            val pattern = (smallpattern1 mkString ("\t")) + "|\t" + (smallpattern2 mkString ("\t")) + "|"
            //send something like: -1078222292 160090837 -1237882651| -1078222292 160090837 -361161128|   
            val pg = new PatternGraph()
            pg.ConstructPatternGraph(pattern)
            val startedge = smallpattern2.toArray
            var dfspath = pg.DFS(startedge(0).toString())
            var fixedpath = dfspath.split("\t").flatMap(str => {
              if (str.endsWith("|"))
                List(str.replaceAll("\\|", ""), "-1")
              else List(str)
            })
            var dfspathlist = fixedpath.map(_.trim.toInt).toList
            if (dfspathlist(dfspathlist.size - 1) == -1)
              dfspathlist = dfspathlist.slice(0, dfspathlist.size - 1)
              
            normalized_join += (dfspathlist.filterNot(elm => elm == -1) 
                -> (dfspathlist,smallpattern1, smallpattern2 , smallpattern1_support, smallpattern2_support)) 
          }
          
          
          
        }
      }  
     
      /*
       *  Now only for join-able patters, 
       */  
       normalized_join.foreach(bigger_pattern =>{
         joinedPattern = joinedPattern + (bigger_pattern._2._1 -> (bigger_pattern._2._4 * bigger_pattern._2._4))
       }) 

      new KGNodeV2FlatInt(attr.getlabel, joinedPattern |+| attr.getpattern_map, List.empty)

    })

    return newGraph
  }

  /*
   * Return a subgraph with at-least one pattern on the source or destination. 'type' edge is also removed
   * from the graph because it is collected as node properties.
   */
  def filterNonPatternSubGraphV2(updatedGraph: Graph[KGNodeV2FlatInt, KGEdgeInt]): Graph[KGNodeV2FlatInt, KGEdgeInt] =
    {
      return updatedGraph.subgraph(epred =>
        ((epred.attr.getlabel != TYPE)))
		/*
		 * &&
		          ((epred.srcAttr.getpattern_map.size != 0) &&
		            (epred.dstAttr.getpattern_map.size != 0)))
		 */
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
  def edge_Pattern_Join_GraphV2(newGraph1: Graph[KGNodeV2FlatInt, KGEdgeInt],
    writerSG: PrintWriter, iteration_id: Int): Graph[KGNodeV2FlatInt, KGEdgeInt] = {
    var t0 = System.currentTimeMillis();
    writerSG.flush()

    val nPlusOneThPatternVertexRDD =
      newGraph1.aggregateMessages[Map[List[Int], Long]](
        edge => {
          sendPatternToNodeV2(edge, iteration_id)
        }, (pattern1OnNodeN, pattern2OnNodeN) => {
          reducePatternsOnNodeV2(pattern1OnNodeN, pattern2OnNodeN)
        })
    val nPlusOneThPatternGraph = joinPatternGraphV2(newGraph1, nPlusOneThPatternVertexRDD)
    
    return nPlusOneThPatternGraph
  }

  def sendPatternToNodeV2(edge: EdgeContext[KGNodeV2FlatInt, KGEdgeInt, Map[List[Int], Long]],
    iteration_id: Int) {

    /*
     * Get source and Destination patterns.
     */
    val allSourceNodePatterns = edge.srcAttr.getpattern_map;
    val allDestinationNodePatterns = edge.dstAttr.getpattern_map
    if ((allSourceNodePatterns.size > 0) && (allDestinationNodePatterns.size > 0)) {
      allSourceNodePatterns.foreach(sr =>
        {
          /*
           * Edge Patterns grow one edge at a time
           */
          if (getPatternSize(sr._1) == 1) {
            allDestinationNodePatterns.foreach(dst =>
              {
                {
                  //Ex. <Foo knows Bar> can be joined with any pattern at <Bar>
                  if (sr._1(2) == (dst._1(0))) {

                    sendPatternV2Flat(sr._1, dst._1,
                      sr._2, dst._2, edge)
                  }
                }
              })
          }
        })
    }
  }

  def sendPatternV2Flat(pattern1: List[Int], pattern2: List[Int],
    instance1Dst: Long, instance2Dst: Long,
    edge: EdgeContext[KGNodeV2FlatInt, KGEdgeInt, Map[List[Int], Long]]) {

    var bigger_instance: Int = 0
    edge.sendToSrc(Map(pattern1 ++ List(-1) ++ pattern2 -> instance2Dst))
  }

  //Helper fuction to make sure it is a 1-edge pattern
  def getPatternSize(patternKey: List[Int]): Int =
    {
      val tocken_count = patternKey.length
      if (tocken_count % 3 == 0)
        return tocken_count / 3
      else
        return -1
    }

 

  def joinPatternGraphV2(nThPatternGraph: Graph[KGNodeV2FlatInt, KGEdgeInt],
    nPlusOneThPatternVertexRDD: VertexRDD[Map[List[Int], Long]]): Graph[KGNodeV2FlatInt, KGEdgeInt] =
    {
      val update_tmp_graph =
        nThPatternGraph.outerJoinVertices(nPlusOneThPatternVertexRDD) {
          case (id, a_kgnode, Some(nbr)) =>
            new KGNodeV2FlatInt(a_kgnode.getlabel,
              a_kgnode.getpattern_map |+| nbr, List.empty)
          case (id, a_kgnode, None) =>
            new KGNodeV2FlatInt(a_kgnode.getlabel,
              a_kgnode.getpattern_map, List.empty)
        }
      val result = null
      return update_tmp_graph
    }

  /*
   * helper function to clean window graph
   * It is possible to use actual epoc time instaed of batchid 
   * 	
   */
  def trim(now: Int, windowSize: Int) = {

    val cutoff : (Long,Long) = batch_id_map.getOrElse((now-windowSize), (-1, -1))
    if(input_graph != null)
    	input_graph = input_graph.subgraph(epred => (epred.attr.getdatetime <  cutoff._1))
  }

  
  def getMinMaxTime()
  {
    val min = this.input_graph.edges.map(e=>e.attr.getdatetime).reduce((time1,time2) => math.min(time1, time2))
    val max = this.input_graph.edges.map(e=>e.attr.getdatetime).reduce((time1,time2) => math.max(time1, time2))
    (min,max)
  }
  
  
  /**
   * takes gBatch as input which is 'mined' batch graph and merge it with
   * existing window graph.
   */
  def getInterSectionGraph(gBatch: CandidateGeneration, sc: SparkContext): CandidateGeneration = {

    if (this.input_graph == null) {
      this.input_graph = gBatch.input_graph
      val result = new CandidateGeneration(minSup)
      result.input_graph = this.input_graph
      return result

    } else {
      val vertices_rdd: RDD[(VertexId, Map[List[Int], Long])] =
        this.input_graph.vertices.map(f => (f._1, f._2.getpattern_map))
      val new_window_graph = gBatch.input_graph.joinVertices[Map[List[Int], Long]](vertices_rdd)((id,
        kgnode, new_data) => {
        if (kgnode != null)
          new KGNodeV2FlatInt(kgnode.getlabel, kgnode.getpattern_map |+| new_data, List.empty)
        else
          new KGNodeV2FlatInt(-1, Map.empty, List.empty)
      })

      val result = new CandidateGeneration(minSup)
      result.input_graph = new_window_graph
      return result
    }

  }

      def get_Frequent_SubgraphV2Flat(sc:SparkContext,subgraph_with_pattern: Graph[KGNodeV2FlatInt, KGEdgeInt],
    result: Graph[PGNode, Int], SUPPORT: Int): Graph[KGNodeV2FlatInt, KGEdgeInt] =
    {
      /*
	 * This method returns output graph with frequnet patterns only. 
	 * TODO : need to clean the code
	 */
      //println("checking frequent subgraph")
      var commulative_subgraph_index: Map[String, Int] = Map.empty;

      //TODO : Sumit: Use this RDD instead of the Map(below) to filter frequent pattersn
      val pattern_support_rdd: RDD[(List[Int], Long)] =
        subgraph_with_pattern.vertices.flatMap(vertex =>
          {
            vertex._2.getpattern_map.map(f => (f._1, f._2)).toSet
          })
      val tmpRDD = pattern_support_rdd.reduceByKey((a, b) => a + b)
      val frequent_pattern_support_rdd = tmpRDD.filter(f => ((f._2 >= SUPPORT) | (f._2 == -1)))

      val pattern_vertex_rdd: RDD[(List[Int], Set[Long])] =
        subgraph_with_pattern.vertices.flatMap(vertex =>
          {
            vertex._2.getpattern_map.map(f => (f._1, Set(vertex._1))).toSet
          }).reduceByKey((a, b) => a++b) // append vertexids on each node
      val join_frequent_node_vertex = frequent_pattern_support_rdd.leftOuterJoin(pattern_vertex_rdd)
      val vertex_pattern_reverse_rdd = join_frequent_node_vertex.flatMap(pattern_entry 
          => (pattern_entry._2._2.getOrElse(Set.empty).map(v_id => (v_id, Set(pattern_entry._1))))).reduceByKey((a, b) => a ++ b)
      
      //originalMap.filterKeys(interestingKeys.contains)        
      val frequent_graph: Graph[KGNodeV2FlatInt, KGEdgeInt] =
        subgraph_with_pattern.outerJoinVertices(vertex_pattern_reverse_rdd) {
          case (id, kg_node, Some(nbr)) => new KGNodeV2FlatInt(kg_node.getlabel, kg_node.getpattern_map.filterKeys(nbr.toSet), kg_node.getProperties)
          case (id, kg_node, None) => new KGNodeV2FlatInt(kg_node.getlabel, Map.empty, kg_node.getProperties)
        }

      val validgraph = frequent_graph.subgraph(epred =>
        ((epred.srcAttr.getpattern_map != null) || (epred.dstAttr.getpattern_map != null)))
      return validgraph;
    }
 

}
