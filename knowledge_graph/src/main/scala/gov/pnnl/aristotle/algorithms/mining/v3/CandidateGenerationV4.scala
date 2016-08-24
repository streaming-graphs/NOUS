/**
 *
 * @author puro755
 * @dAug 22, 2016
 * @knowledge_graph
 */
package gov.pnnl.aristotle.algorithms.mining.v3

import java.io.Serializable
import org.apache.spark.graphx.Graph
import gov.pnnl.aristotle.algorithms.mining.datamodel.KGNodeV4
import gov.pnnl.aristotle.algorithms.mining.datamodel.KGEdgeInt
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext
import java.io.PrintWriter
import org.apache.spark.graphx.VertexRDD
import gov.pnnl.aristotle.algorithms.mining.datamodel.PatternInstance
import scalaz.Scalaz._
import org.apache.spark.graphx.Edge
import gov.pnnl.aristotle.algorithms.mining.GraphProfiling
import gov.pnnl.aristotle.algorithms.mining.datamodel.PatternInstance
import gov.pnnl.aristotle.algorithms.mining.datamodel.PatternInstanceNode
import gov.pnnl.aristotle.algorithms.mining.datamodel.PatternInstanceNode
import gov.pnnl.aristotle.algorithms.mining.datamodel.PatternInstanceNode

/**
 * @author puro755
 *
 */
class CandidateGenerationV4(val minSup: Int) extends Serializable {

  var TYPE: Int = 0
  var SUPPORT: Int = minSup
  var type_support: Int = 2
  var input_gpi: Graph[KGNodeV4, KGEdgeInt] = null
  val batch_id_map : Map[Int,(Long,Long)] = Map.empty
  println("**************CREATING GRAPH************")
  
  
    def init(sc : SparkContext, graph: Graph[Int, KGEdgeInt], writerSG: PrintWriter, basetype: Int,
    type_support: Int): CandidateGenerationV4 = {

    /*
     * Get all the rdf:type dst node information on the source node
     */
    this.TYPE = basetype
    this.type_support = type_support
    println("***************support is " + SUPPORT)

    // Now we have the type information collected in the original graph
    val typedAugmentedGraph: Graph[(Int, Map[Int, Int]), KGEdgeInt] = getTypedGraph(graph, writerSG)
    
    
    /*
     * Create GIP from this graph
     * 
     * Every node has following structure:
     * (Long,(List[Int],Set[PatternInstance],Long)
     * Long: VertexId
     * List[Int]: pattern key
     * Set[PatternInstance]: pattern instances around the local edge 
     * Long : timestamp of that pattern edge
     */
    val all_gip_nodes = typedAugmentedGraph.triplets.flatMap(triple=>{
      
      //Local Execution on a triple edge; but needs source and destination
      val source_node = triple.srcAttr
      val destination_node = triple.dstAttr
      val time_stamp = triple.attr.getdatetime
      
      val all_src_types = source_node._2.keys
      val all_dst_types = destination_node._2.keys
      var all_local_gip_vertices: 
      scala.collection.mutable.Set[(Long,PatternInstanceNode)] 
    		  = scala.collection.mutable.Set.empty
      for(src_type <- all_src_types)
        for(dst_type <- all_dst_types)
        {
          val gip_v_key = (src_type+dst_type).hashCode.toLong
          val gip_v_label = List(src_type,triple.attr.getlabel)
          val gip_v_instance = Set(new 
              PatternInstance(
                  Set((triple.srcAttr._1,triple.dstAttr._1))))
          all_local_gip_vertices += ((gip_v_key,new PatternInstanceNode(gip_v_label,gip_v_instance,time_stamp)))
        }
      all_local_gip_vertices
    })
    
    
    
    
    /*
     * Create RDD where Every vertex has all the 1 edge patterns it belongs to
     * Ex: Sumit: (person worksAt organizaion) , (person friendsWith person)
     * 
     * Read method comments
     * ....
     * ....
     * 
     */
    val nonTypedVertexRDD: VertexRDD[Array[(List[Int], Set[PatternInstance])]] = getOneEdgePatterns(typedAugmentedGraph)    

    //val nonTypedVertexRDD : VertexRDD[Array[(List[Int], Long)]] =  getCondensedVRDD(nonTypedVertexRDD_flat)

    /*
     * Create Vertices of the GIP
     * GIP is a property graph so every nodes keeps some extra information
     * Starting with pattern_edge, and data_edge 
     * NOTE: map vertices can not provide timestamp, must be constructed from edge
    val gip_vertices = nonTypedVertexRDD.flatMap(vertex=>{
      vertex._2.map(a_pattern
          =>((a_pattern._1.toString()+a_pattern._2.toString).hashCode().toLong,(a_pattern._1,a_pattern._2)))
    })
    */
    /*
     * Create Edges of the GIP
     */
    val gip_edge = nonTypedVertexRDD.flatMap(vertex=>{
      var all_gip_vertices: scala.collection.mutable.Set[Long] = scala.collection.mutable.Set.empty
      vertex._2.map(a_pattern => all_gip_vertices +=
        ((a_pattern._1.toString() + a_pattern._2.toString).hashCode().toLong))
      val all_gip_vertices_list = all_gip_vertices.toList
      // make a list to get an order becase we need to cross join them
      // in next step.
      var i = 0
      var j = 0
      var local_edges: scala.collection.mutable.Set[Edge[Int]] = scala.collection.mutable.Set.empty
      for (i <- 0 to all_gip_vertices_list.length - 1) {
        for (j <- i to all_gip_vertices_list.length - 1) {
          if (all_gip_vertices_list(i) != all_gip_vertices_list(j))
            local_edges += Edge(all_gip_vertices_list(i), all_gip_vertices_list(j), 1)
        }
      }
      local_edges
    })
    
    val gip =  Graph(all_gip_nodes,gip_edge)
    writerSG.flush()
    return this
  }
  
    def getTypedGraph(graph: Graph[Int, KGEdgeInt],
    writerSG: PrintWriter): Graph[(Int, Map[Int, Int]), KGEdgeInt] =
    {
      val typedVertexRDD: VertexRDD[Map[Int, Int]] =
        GraphProfiling.getTypedVertexRDD_Temporal(graph,
          writerSG, type_support, this.TYPE.toInt)
      // Now we have the type information collected in the original graph
      val typedAugmentedGraph: Graph[(Int, Map[Int, Int]), KGEdgeInt] = GraphProfiling.getTypedAugmentedGraph_Temporal(graph,
        writerSG, typedVertexRDD)
      return typedAugmentedGraph
    }
    
      def getOneEdgePatterns(typedAugmentedGraph: Graph[(Int,  
    Map[Int, Int]), KGEdgeInt]): VertexRDD[Array[(List[Int],Set[PatternInstance])]] =
    {
      return typedAugmentedGraph.aggregateMessages[Array[(List[Int], Set[PatternInstance])]](
        edge => {
          if (edge.attr.getlabel != TYPE) {
            // Extra info for pattern
            if ((edge.srcAttr._2.size > 0) &&
              (edge.dstAttr._2.size > 0)) {
              val dstnodetype = edge.dstAttr._2
              val srcnodetype = edge.srcAttr._2
              srcnodetype.foreach(s => {
                dstnodetype.foreach(d => {
                  var pattern_instance: scala.collection.immutable.Set[(Int, Int)] = Set.empty
                  edge.sendToSrc(Array(List(s._1, edge.attr.getlabel,
                    d._1, -1)
                    -> Set(new PatternInstance(pattern_instance))))
                  edge.sendToDst(Array(List(s._1, edge.attr.getlabel,
                    d._1, -1)
                    -> Set(new PatternInstance(pattern_instance))))
                })
              })
            }
          }
        },
        (pattern1NodeN, pattern2NodeN) => {
          reducePatternsOnNodeV2(pattern1NodeN, pattern2NodeN)
        })
    }
  def reducePatternsOnNodeV2(a: Array[(List[Int], Set[PatternInstance])], b: Array[(List[Int], Set[PatternInstance])]): 
	  Array[(List[Int], Set[PatternInstance])] =
    {
      
    return a ++ b
    //This may leads to a large array on node with same pattern. For now, it can be used to 
    // create GIP. 
    //Option 2: call getCondensedVRDD
    
    }
  
  
  
  def maintainWindow(input_gpi: Graph[(Long,PatternInstanceNode), Int], cutoff_time : Long) 
  : Graph[(Long,PatternInstanceNode), Int] =
	{
		return input_gpi.subgraph(vpred = (vid,attr) => {
		  attr._2.timestamp > cutoff_time
		})
	}
  
  
}