/**
 *
 * @author puro755
 * @dAug 22, 2016
 * @knowledge_graph
 */
package gov.pnnl.aristotle.algorithms.mining.v4

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
import org.apache.spark.rdd.RDD
import gov.pnnl.aristotle.algorithms.mining.datamodel.PatternInstanceNode
import org.apache.spark.rdd.RDD.rddToPairRDDFunctions
import scala.Array.canBuildFrom
import scala.Int.int2long

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
  type LabelWithTypes = (Int, List[Int])
  
  println("**************CREATING GRAPH************")
  
  
    def init(sc : SparkContext, graph: Graph[Int, KGEdgeInt], writerSG: PrintWriter, basetype: Int,
    type_support: Int, winodow_GIP : Graph[PatternInstanceNode, Long]):  Graph[PatternInstanceNode, Long] = {

    /*
     * Get all the rdf:type dst node information on the source node
     */
    this.TYPE = basetype
    this.type_support = type_support
    println("***************support is " + SUPPORT)

    // Now we have the type information collected in the original graph
    // val typedAugmentedGraph: Graph[(Int, Map[Int, Int]), KGEdgeInt] = getTypedGraph(graph, writerSG)
    val typedAugmentedGraph: Graph[LabelWithTypes, KGEdgeInt] = getTypedGraphNoMap(graph, writerSG)
    /*
     * Create RDD where Every vertex has all the 1 edge patterns it belongs to
     * Ex: Sumit: (person worksAt organizaion) , (person friendsWith person)
     * 
     * Read method comments
     * ....
     * ....
     * 
     */
//    val oneEdgePatternOnVertexRDD: RDD[(Long,Array[(List[Int], PatternInstance)])] = 
//      getOneEdgePatternsRDDNoMap(typedAugmentedGraph)    
//    
    val gipVertices = getGIPVerticesNoMap(typedAugmentedGraph)
   
    val gipEdge = getGIPEdges(gipVertices)
    
    val new_GIP =  Graph(gipVertices,gipEdge)
    
    
     /*
     * If the current GIP is null, i.e. batch is = 0;
     * Return the newly created GIP as the 'current_GIP'
     */
    if(winodow_GIP == null)
    {
      return new_GIP
    }

    /*
     * Otherwise make a union of the newGIP and window_GIP
     */
    return Graph(winodow_GIP.vertices.union(new_GIP.vertices).distinct,
        winodow_GIP.edges.union(new_GIP.edges).distinct)
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
    
    def getTypedGraphNoMap(graph: Graph[Int, KGEdgeInt],
    writerSG: PrintWriter): Graph[LabelWithTypes, KGEdgeInt] =
    {
      val typedVertexRDD: VertexRDD[List[Int]] =
        GraphProfiling.getTypedVertexRDD_TemporalNoMap(graph,
          writerSG, type_support, this.TYPE.toInt)
      // Now we have the type information collected in the original graph
          // join the vertices with type information
      val typedAugmentedGraph: Graph[LabelWithTypes, KGEdgeInt] = 
        GraphProfiling.getTypedAugmentedGraph_TemporalNoMap(graph,
        writerSG, typedVertexRDD)
        
      return typedAugmentedGraph
    }
 
 def getGIPEdges(gip_vertices: RDD[(Long,PatternInstanceNode)]) :
 RDD[Edge[Long]] =
 {
     /*
     * Create Edges of the GIP
     * 
     * getGIPVertices has similar code to find Vertices, but for Vertices, we need to carry much more data.
     * Also if we try to store fat data on every vertex, it may become bottleneck
     * 
     */
   val gipPatternIdPerDataNode  = gip_vertices.flatMap(patterVertex => Iterable((patterVertex._2.getSourceId,patterVertex._1),
       (patterVertex._2.getDestinationId,patterVertex._1))).groupByKey()
   
       val gipEdges = gipPatternIdPerDataNode.flatMap(gipNode=>{
         val edgeList = gipNode._2.toList
         val dataGraphVertexId = gipNode._1
         var local_edges : scala.collection.mutable.ArrayBuffer[Edge[Long]]  = scala.collection.mutable.ArrayBuffer()
         for(i <- 0 to  (edgeList.size-2))
         {
           for(j <- i+1 to  (edgeList.size-1))
           {
             local_edges += Edge(edgeList(i), edgeList(j), dataGraphVertexId)
           }
         }
        local_edges
       })


      return gipEdges
    }
 
 
 
 def getGIPVerticesNoMap(typedAugmentedGraph: Graph[LabelWithTypes, KGEdgeInt] ) :
 RDD[(Long,PatternInstanceNode)] =
 {
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
   val validTriples = typedAugmentedGraph.triplets.filter(triple=>triple.attr.getlabel != TYPE).
      filter(triple=>triple.attr.getlabel != TYPE).filter(triple=>triple.srcAttr._2.size > 0)
      .filter(triple=>triple.dstAttr._2.size > 0)
      
    val allGIPNodes : RDD[(Long,PatternInstanceNode)]= 
      validTriples
      .map(triple => {

      //Local Execution on a triple edge; but needs source and destination
      val source_node = triple.srcAttr
      val destination_node = triple.dstAttr
     // val time_stamp = triple.attr.getdatetime
      val pred_type = triple.attr.getlabel
      val src_type = source_node._2.head
      val dst_type = destination_node._2.head
      val gip_v_key = List(src_type, pred_type, dst_type, triple.srcId, triple.dstId).hashCode.toLong
      val gip_pattern = List(src_type, pred_type, dst_type)
      val gip_instance_edge = new PatternInstance(Set((triple.srcAttr._1, triple.dstAttr._1)))
      val gip_v_map = Map((src_type->triple.srcAttr._1),(dst_type -> triple.dstAttr._1))
      val timestamp = triple.attr.getdatetime
      val gip_v_label :List[Int] = List(src_type, pred_type, dst_type, triple.srcAttr._1, triple.dstAttr._1)
     val pattern = (gip_v_key, new PatternInstanceNode(gip_v_label, 
         gip_pattern, gip_instance_edge,gip_v_map, timestamp) )
     pattern 
    })
    return allGIPNodes
 }
    
 def getGIPVertices(typedAugmentedGraph: Graph[(Int, Map[Int, Int]), KGEdgeInt] ) :
 RDD[(Long,PatternInstanceNode)] =
 {
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
    val allGIPNodes : RDD[(Long,PatternInstanceNode)]= 
      typedAugmentedGraph.triplets.filter(triple=>triple.attr.getlabel != TYPE).flatMap(triple => {

      //Local Execution on a triple edge; but needs source and destination
      val source_node = triple.srcAttr
      val destination_node = triple.dstAttr
      val time_stamp = triple.attr.getdatetime
      val all_src_types = source_node._2.keys
      val all_dst_types = destination_node._2.keys

      var all_local_gip_vertices: scala.collection.mutable.Set[(Long, PatternInstanceNode)] = scala.collection.mutable.Set.empty
      
      for (src_type <- all_src_types)
        for (dst_type <- all_dst_types) {
          val gip_v_key = List(src_type , triple.attr.getlabel, dst_type, triple.srcAttr._1, triple.dstAttr._1).hashCode
          val gip_v_patternedge = List(src_type , triple.attr.getlabel, dst_type)
          val gip_v_label = List(src_type , triple.attr.getlabel, dst_type, triple.srcAttr._1, triple.dstAttr._1)
          val gip_v_instance = new PatternInstance(Set((triple.srcAttr._1, triple.dstAttr._1)))
          val gip_v_map = Map((src_type -> triple.srcAttr._1), (dst_type -> triple.dstAttr._1))
          all_local_gip_vertices += ((gip_v_key, new PatternInstanceNode(gip_v_label, 
              gip_v_patternedge,gip_v_instance, 
              gip_v_map,time_stamp)))
        }
      all_local_gip_vertices
    })
    return allGIPNodes
 }
    
 def getOneEdgePatternsRDD(typedAugmentedGraph: Graph[(Int,  
    Map[Int, Int]), KGEdgeInt]): RDD[(Long, Array[(List[Int],PatternInstance)])] =
    {

      //val src_rdd = typedAugmentedGraph.triplets.groupBy(triple=>triple.srcId)
      val result = typedAugmentedGraph.triplets.flatMap(triple => {
        var localarrayList: scala.collection.mutable.ListBuffer[(List[Int], PatternInstance)] = scala.collection.mutable.ListBuffer.empty

        val dstnodetype = triple.dstAttr._2.keys
        val srcnodetype = triple.srcAttr._2.keys
        srcnodetype.foreach(s => {
          dstnodetype.foreach(d => {
            var pattern_instance: scala.collection.immutable.Set[(Int, Int)] = Set((triple.srcAttr._1, triple.dstAttr._1))
            localarrayList += ((List(s, triple.attr.getlabel, d), new PatternInstance(pattern_instance)))
            //localarrayList += ((List(s, edge.attr.getlabel, d, new PatternInstance(pattern_instance)))
          })
        })
        Iterable((triple.srcId, localarrayList.toArray),
          (triple.dstId, localarrayList.toArray))
      }).reduceByKey((a, b) => a ++ b)
      return result

    }
 
    def getOneEdgePatternsRDDNoMap(typedAugmentedGraph: Graph[LabelWithTypes, KGEdgeInt]): 
    RDD[(Long, Array[(List[Int],PatternInstance)])] =
    {

      //val src_rdd = typedAugmentedGraph.triplets.groupBy(triple=>triple.srcId)
      val result = typedAugmentedGraph.triplets.flatMap(triple => {
        var localarrayList: scala.collection.mutable.ListBuffer[(List[Int], PatternInstance)] = scala.collection.mutable.ListBuffer.empty

        val d = triple.dstAttr._2.head
        val s = triple.srcAttr._2.head
     
        var pattern_instance: scala.collection.immutable.Set[(Int, Int)] = Set((triple.srcAttr._1, triple.dstAttr._1))
       localarrayList += ((List(s, triple.attr.getlabel, d), new PatternInstance(pattern_instance)))
            //localarrayList += ((List(s, edge.attr.getlabel, d, new PatternInstance(pattern_instance)))
         
        Iterable((triple.srcId, localarrayList.toArray),
          (triple.dstId, localarrayList.toArray))
      }).reduceByKey((a, b) => a ++ b)
      return result

    }
   
  def reducePatternsOnNodeV2(a: Array[(List[Int], PatternInstance)], 
      b: Array[(List[Int], PatternInstance)]): 
	  Array[(List[Int], PatternInstance)] =
    {
      
    return a ++ b
    //This may leads to a large array on node with same pattern. For now, it can be used to 
    // create GIP. 
    //Option 2: call getCondensedVRDD
    
    }
  
  
  
  def maintainWindow(input_gpi: Graph[PatternInstanceNode, Int], cutoff_time : Long) 
  : Graph[PatternInstanceNode, Int] =
	{
		return input_gpi.subgraph(vpred = (vid,attr) => {
		  attr.timestamp > cutoff_time
		})
	}
  
  def computeMinImageSupport(input_gpi : Graph[PatternInstanceNode, Long])
	  :RDD[(List[Int],Int)] =
  {

      /*
     * A flat RDD like:
     * (P1,person,sp)
     * (P1,person,sc)
     * (P1,org,pnnl)
     * (P1,org,pnnl)
     */
    if(input_gpi == null)
      println("null")

      val sub_pattern_key_rdd = input_gpi.vertices.flatMap(vertex => {
        vertex._2.pattern_instance_map.map(pattern_instance_pair => {
          ((vertex._2.pattern_edge, pattern_instance_pair._1, pattern_instance_pair._2))
        })
      }).distinct
      //.reduceByKey((sub_pattern_instance_count1, sub_pattern_instance_count2) => sub_pattern_instance_count1 + sub_pattern_instance_count2)

      
      val mis_rdd = sub_pattern_key_rdd.map(key=>{
        ((key._1, key._2),1)
         /*
         * ((P1,person) , 1) from (P1,person,sp)
         * ((P1,person) , 1) from (P1,person,sc)
         * ((P1,org) , 1) from (P1,org,pnnl)
         * 
         */

      }).reduceByKey((unique_instance1_count,unique_instance2_count) 
          => unique_instance1_count + unique_instance2_count)
      
     /*
     * Input is 'mis_rdd' which is a Cumulative RDD like:
     * P1:person, 2
     * P1:org, 1
     * 
     * Output is patternSup which gets minimum of all P1:x 
     * so return (P1, 1)
     */
      val patternSup: RDD[(List[Int], Int)] = mis_rdd.map(sup_pattern_key => {
        //Emitt (P1, 2) and (P1 1)
       (sup_pattern_key._1._1,sup_pattern_key._2)
      }).reduceByKey((full_pattern_instace_count1, full_pattern_instace_count2) => {
        /*
       * Not using Math lib to because it loads entire dir for min function.
       * Also seen it failing in cluster mode.
       */
        if (full_pattern_instace_count1 < full_pattern_instace_count2)
          full_pattern_instace_count1
        else
          full_pattern_instace_count2
      })

      return patternSup
    }
  
}
