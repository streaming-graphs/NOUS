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
import java.io.File
import java.lang.Boolean
import org.apache.spark.SparkContext._
import scala.util.control.Breaks._
import java.util.regex.Pattern
import org.apache.commons.math3.util.ArithmeticUtils
import gov.pnnl.aristotle.algorithms.mining.datamodel.BatchState
import gov.pnnl.aristotle.algorithms.mining.datamodel.KGEdge
import gov.pnnl.aristotle.algorithms.mining.datamodel.KGNodeV1
import gov.pnnl.aristotle.algorithms.mining.datamodel.KGNodeV2
import gov.pnnl.aristotle.algorithms.mining.datamodel.PGNode
import gov.pnnl.aristotle.algorithms.mining.datamodel.PatternInstance
import gov.pnnl.aristotle.algorithms.mining.datamodel.WindowState
import gov.pnnl.aristotle.algorithms.mining.datamodel.KGNodeV2Flat
import gov.pnnl.aristotle.algorithms.mining.datamodel.PatternConstraint
import gov.pnnl.aristotle.algorithms.mining.datamodel.PatternConstraint
import gov.pnnl.aristotle.algorithms.mining.datamodel.PatternGraph
import gov.pnnl.aristotle.algorithms.mining.datamodel.KGNodeV2FlatInt
import gov.pnnl.aristotle.algorithms.mining.datamodel.KGEdgeInt

object GraphPatternProfiler {

  val writerSGLog: PrintWriter = new PrintWriter(new File("Log.txt"));
  def main(args: Array[String]): Unit = {}
  //val TYPE= "IS-A"
  var TYPE = "rdf:type"
  //var SUPPORT = 220; // usual definition of support 
  var TYPE_THRESHOLD = 5000; // translating degree to a type

  def fixGraph(graph: Graph[KGNodeV1, KGEdge]): Graph[KGNodeV1, KGEdge] =
    {
      val newGraph: Graph[KGNodeV1, KGEdge] = graph.mapVertices((id, attr) => {
        var joinedPattern: Map[String, Set[PatternInstance]] = Map.empty
        val vertex_pattern_map = attr.getpattern_map
        //            val vertext_label = edge.srcAttr._1
        //if (vertex_pattern_map.size > 0) {
        vertex_pattern_map.map(vertext_pattern =>
          {
            if (vertext_pattern._1.contains('|'))
              joinedPattern = joinedPattern + ((vertext_pattern._1.
                replaceAll("\\|", "\t") -> vertext_pattern._2))
            else
              joinedPattern = joinedPattern + ((vertext_pattern._1 ->
                vertext_pattern._2))
          })
        new KGNodeV1(attr.getlabel, joinedPattern)
        //}

      })
      return newGraph
    }

    def fixGraphV2(graph: Graph[KGNodeV2, KGEdge]):
    Graph[KGNodeV2, KGEdge] =
    {
      val newGraph: Graph[KGNodeV2, KGEdge] = graph.mapVertices((id, attr) => {
        var joinedPattern: Map[String, Long] = Map.empty
        val vertex_pattern_map = attr.getpattern_map
        //            val vertext_label = edge.srcAttr._1
        //if (vertex_pattern_map.size > 0) {
        vertex_pattern_map.map(vertext_pattern =>
          {
            if (vertext_pattern._1.contains('|'))
              joinedPattern = joinedPattern + ((vertext_pattern._1.
                replaceAll("\\|", "\t") -> vertext_pattern._2))
            else
              joinedPattern = joinedPattern + ((vertext_pattern._1 ->
                vertext_pattern._2))
          })
        new KGNodeV2(attr.getlabel, joinedPattern,List.empty)
        //}

      })
      return newGraph
    }

def fixGraphV2Flat(graph: Graph[KGNodeV2FlatInt, KGEdgeInt]):
    Graph[KGNodeV2FlatInt, KGEdgeInt] =
    {
      val newGraph: Graph[KGNodeV2FlatInt, KGEdgeInt] = graph.mapVertices((id, attr) => {
        var joinedPattern: Array[(List[Int], Long)] = Array.empty
        val vertex_pattern_map = attr.getpattern_map
        vertex_pattern_map.map(vertext_pattern =>
          {
            if (vertext_pattern._1.contains(-1))
              joinedPattern = joinedPattern ++ Array((vertext_pattern._1.filterNot(elm=> elm == -1)
                 , vertext_pattern._2))
            else
              joinedPattern = joinedPattern ++ Array((vertext_pattern._1 ,
                vertext_pattern._2))
          })
        new KGNodeV2FlatInt(attr.getlabel, joinedPattern,List.empty)
        //}

      })
      return newGraph
    }

  
  def getListOfPattern(graph: Graph[(String, Map[String, Set[Int]]), String]) =
    {
      val subgraph_index_updated = graph.aggregateMessages[Map[String, Int]](
        edge => {
          if (edge.attr.equalsIgnoreCase(TYPE) == false) {
            val vertex_pattern_map = edge.srcAttr._2
            val vertext_label = edge.srcAttr._1
            if (vertex_pattern_map.size > 0) {
              vertex_pattern_map.map(vertext_pattern =>
                {
                  edge.sendToSrc(Map(vertext_pattern._1 ->
                    vertext_pattern._2.size))
                })
            }
          }
        }, (pattern1OnNodeN, pattern2OnNodeN) => {
          pattern1OnNodeN |+| pattern2OnNodeN
        })
      subgraph_index_updated.saveAsTextFile("patternDump")
    }
  // graph: incoming batch
  // 

  def updateCloedPatternDependencyGraph(result: 
      (RDD[(String, Set[PatternInstance])], Graph[PGNode, Int])): 
      (Graph[PGNode, Int], Graph[PGNode, Int], Graph[PGNode, Int]) =
    {
      val dependency_graph = result._2

      val closed_pattern_rdd =
        dependency_graph.aggregateMessages[Boolean](
          edge => {
            edge.sendToSrc(edge.srcAttr.getsupport == edge.dstAttr.getsupport)
          }, (pattern1OnNodeN, pattern2OnNodeN) => {
            //math.max(pattern1OnNodeN,pattern2OnNodeN)
            pattern1OnNodeN || pattern2OnNodeN
          })
      // -1 : redundant patterns
      // -2 : closed pattern
      // -3 promising pattern

      var validgraph: Graph[(PGNode, Int), Int] =
        dependency_graph.outerJoinVertices(closed_pattern_rdd) {
          case (id, (something), Some(nbr)) => {
            if (nbr == false) (something, -2)
            else (something, -1)
          }
          case (id, (something), None) => (something, -3)
        }

      val redundant_graph = filterGraph(validgraph, -1);
      val closed_graph = filterGraph(validgraph, -2)
      val promising_graph = filterGraph(validgraph, -3)

      return (redundant_graph, closed_graph, promising_graph)
    }
  def filterGraph(validgraph: Graph[(PGNode, Int), Int], 
      filter_value: Int): Graph[PGNode, Int] =
    {
      val redundant_graph = validgraph.subgraph(vpred = (vid, attr) =>
        attr._2 == filter_value)
      val redundant_nodes = redundant_graph.vertices
      //because the validgraph has extra int value {-1,-2,-3} which is 
      // redundant if we already filder based on "filter_value" 
      var redundant_graph2: Graph[PGNode, Int] = 
        redundant_graph.outerJoinVertices(redundant_nodes) {
        case (id, (something), Some(nbr)) => (nbr._1)
        //case (id, (something), None  ) => (something._1,something._2)
      }
      return redundant_graph2
    }

  def printPattern(graph: Graph[KGNodeV1, KGEdge],
    writerSG: PrintWriter, id: Int) {
    val tmpRDD = get_Pattern_RDD(graph)
    //tmpRDD.collect.foreach(f => writerSG.println(id + " " + f.toString))
    writerSG.flush();
  }

  /**
   * get_pattern method collects all the pattern and their support across the graph
   * and return only the frequent patterns.
   */
  def get_pattern(graph: Graph[KGNodeV1, KGEdge],
    writerSG: PrintWriter, id: Int, SUPPORT: Int): RDD[(String, Int)] =
    {
      val tmpRDD = get_Pattern_RDD(graph)
      println("Total Instances Found" + tmpRDD.map(f => f._2).reduce((a, b) => a + b))
      //tmpRDD.collect.foreach(f => writerSG.println("All:\t"  +f._1 + "\t" + f._2))

      val frq_tmpRDD = tmpRDD.filter(f => f._2 > SUPPORT)
      //println("number of patterns " + tmpRDD.filter(f => f._2 > SUPPORT).count)

      //frq_tmpRDD.collect.foreach(f => writerSG.println("Frq:\t" +f._1 + "\t" + f._2))
      writerSG.flush()
      return frq_tmpRDD

    }

    /**
   * get_sorted_pattern method collects all the pattern and their support across the graph
   * and return only the frequent patterns.
   */
  def get_sorted_pattern(graph: Graph[KGNodeV1, KGEdge],
    writerSG: PrintWriter, id: Int, SUPPORT: Int): RDD[(String, Int)] =
    {
      val tmpRDD_non_sorted = get_Pattern_RDD(graph)
      val tmpRDD = tmpRDD_non_sorted.sortBy(f => f._2)
      println("Total Instances Found" + tmpRDD.map(f => f._2).reduce((a, b) => a + b))
      //tmpRDD.collect.foreach(f => writerSG.println("All:\t"  +f._1 + "\t" + f._2))

      val frq_tmpRDD = tmpRDD.filter(f => f._2 > SUPPORT)
      //println("number of patterns " + tmpRDD.filter(f => f._2 > SUPPORT).count)

      //frq_tmpRDD.collect.foreach(f => writerSG.println("Frq:\t" +f._1 + "\t" + f._2))
      writerSG.flush()
      return frq_tmpRDD

    }

    def get_sorted_patternV2(graph: Graph[KGNodeV2, KGEdge],
    writerSG: PrintWriter, id: Int, SUPPORT: Int): RDD[(String, Long)] =
    {
      val tmpRDD_non_sorted = get_Pattern_RDDV2(graph)
      val tmpRDD = tmpRDD_non_sorted.sortBy(f => f._2)
      //println("Total Instances Found" + tmpRDD.map(f => f._2).reduce((a, b) => a + b))
      //tmpRDD.collect.foreach(f => writerSG.println("All:\t"  +f._1 + "\t" + f._2))

      val frq_tmpRDD = tmpRDD.filter(f => f._2 > SUPPORT)
      println("number of patterns " + tmpRDD.filter(f => f._2 > SUPPORT).count)

      //frq_tmpRDD.collect.foreach(f => writerSG.println("Frq:\t" +f._1 + "\t" + f._2))
      writerSG.flush()
      return frq_tmpRDD

    }
  
 def get_sorted_patternV2Flat(graph: Graph[KGNodeV2FlatInt, KGEdgeInt],
    writerSG: PrintWriter, id: Int, SUPPORT: Int): RDD[(List[Int], Long)] =
    {
      val tmpRDD_non_sorted = get_Pattern_RDDV2Flat(graph)
      //val tmpRDD = tmpRDD_non_sorted.sortBy(f => f._2) it also fails with ExecutorLostFailure mag5.out
      //println("Total Instances Found" + tmpRDD.map(f => f._2).reduce((a, b) => a + b))
      //tmpRDD_non_sorted.collect.foreach(f => println("All:\t"  +f._1 + "\t" + f._2))

      val frq_tmpRDD = tmpRDD_non_sorted.filter(f => f._2 >= SUPPORT)

      writerSG.flush()
      return frq_tmpRDD

    }
 //
 def get_pattern_node_association_V2Flat(graph: Graph[KGNodeV2FlatInt, KGEdgeInt],
    writerSG: PrintWriter, id: Int, SUPPORT: Int): RDD[(List[Int], Set[(Int,Int)])] =
    {
      val v_degree = graph.degrees //.map(v => (v._1,v._2))
      val v_rdd_raw = graph.vertices
      
      val v_rdd_raw_joined = v_degree.innerZipJoin(v_rdd_raw)((id, degree, vnode) => (degree, vnode))
      //.innerZipJoin(v_rdd_raw)((id, degree, vnode) => (degree, vnode))
      //v_rdd_raw_joined
      val v_rdd: RDD[((Int, Int), Iterable[List[Int]])] = v_rdd_raw_joined.map(v =>
        ((v._2._1, v._2._2.getlabel), v._2._2.getpattern_map.map(patter=>patter._1)))
      val vb = v_rdd.map(v => v._1)

      val pattrn_rdd = v_rdd.flatMap(v => {
        var pattern_set: Set[(List[Int], Set[(Int, Int)])] = Set.empty
        v._2.map(p_string => pattern_set = pattern_set + ((p_string, Set(v._1))))
        pattern_set
      }).reduceByKey((a, b) => a |+| b)
      pattrn_rdd
    }
 
  def get_node_pattern_association_V2Flat(graph: Graph[KGNodeV2FlatInt, KGEdgeInt],
    writerSG: PrintWriter, id: Int, SUPPORT: Int): RDD[(Int, Set[List[Int]])] =
    {
      return graph.vertices.map(v => 
        (v._2.getlabel, v._2.getpattern_map.map(pattern=>pattern._1).toSet)).filter(v=>v._2.size > 0)
    }
  
 //get_patter
 /**
 * Auxiliary method called by get_pattern method to collect all the pattern and 
 * their support across the graph and return only the frequent patterns.
 */

  def get_Pattern_RDD(graph: Graph[KGNodeV1, KGEdge]) : RDD[(String,Int)] =
  {
    /*
     *  collect all the pattern at each node
     */  
    val patternRDD =
        graph.aggregateMessages[Set[(String, Int)]](
          edge => {
            if (edge.attr.getlabel.equalsIgnoreCase(TYPE) == false)
              if ((edge.srcAttr != null) && (edge.srcAttr.getpattern_map != null))
                edge.srcAttr.getpattern_map.foreach(pattern =>
                  edge.sendToSrc(Set((pattern._1, pattern._2.size))))
          }, (pattern1OnNodeN, pattern2OnNodeN) => {
            pattern1OnNodeN |+| pattern2OnNodeN
          })
      println("patternRDD size is " + patternRDD.count)

      /*
       * Collects all the patterns across the graph
       */
      val new_dependency_graph_vertices_RDD: RDD[(String, Int)] =
        patternRDD.flatMap(vertex => vertex._2.map(entry =>
          (entry._1.replaceAll("\\|", "\t"), entry._2)))

      /*
       * Returns the RDD with each pattern-key and its support
       */
      return new_dependency_graph_vertices_RDD.reduceByKey((a, b) => a + b)
    }
  
    def get_Pattern_RDDV2(graph: Graph[KGNodeV2, KGEdge]) 
    : RDD[(String,Long)] =
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
  
 def get_Pattern_RDDV2Flat(graph: Graph[KGNodeV2FlatInt, KGEdgeInt]) 
    : RDD[(List[Int],Long)] =
  {
    /*
     *  collect all the pattern at each node
     */
     val pattern_support_rdd: RDD[(List[Int], Long)] =
        graph.vertices.flatMap(vertex =>
          {
            vertex._2.getpattern_map.map(f => (f._1, f._2)).toSet
          })
      return pattern_support_rdd.reduceByKey((a, b) => a + b)
    }
    

  def edge_Pattern_Join_Graph(newGraph: Graph[KGNodeV1, KGEdge],
    result: Graph[PGNode, Int],
    writerSG: PrintWriter, SUPPORT: Int): Graph[KGNodeV1, KGEdge] = {

    var t0 = System.currentTimeMillis();
    val nPlusOneThPatternVertexRDD =
      newGraph.aggregateMessages[Map[String, Set[PatternInstance]]](
        edge => {
          if (edge.attr.getlabel.equalsIgnoreCase(TYPE) == false)
            sendPatternToNode(edge)
        }, (pattern1OnNodeN, pattern2OnNodeN) => {
          reducePatternsOnNode(pattern1OnNodeN, pattern2OnNodeN)
        })
    var t1 = System.currentTimeMillis();

    t0 = System.currentTimeMillis();
    val nPlusOneThPatternGraph = joinPatternGraph(newGraph,
      nPlusOneThPatternVertexRDD, result, SUPPORT)
    t1 = System.currentTimeMillis();

    return nPlusOneThPatternGraph
  }


def self_Pattern_Join_GraphV2(updateGraph_withsink: Graph[KGNodeV2, KGEdge],
    join_size: Int): Graph[KGNodeV2, KGEdge] = {
    val newGraph = updateGraph_withsink.mapVertices((id, attr) => {

      /*
       * Initialize some local objects
       */
      var selfJoinPatternstmp: List[(String, Long)] = List.empty
      var joinedPattern: Map[String, Long] = Map.empty

      /*
       * Create a list of the pattern. List provides an ordering of the pattern
       */
      attr.getpattern_map.foreach(sr1 =>
        {
          selfJoinPatternstmp = selfJoinPatternstmp ++ List(sr1);
        })
      val selfJoinPatterns = selfJoinPatternstmp

      /*
       * For each pattern-instance of each pattern at both the source and 
       * destination, iterate over them and join them if they are disjointed 
       * instances.
       */

      for (i <- 0 until selfJoinPatterns.length) {
        for (j <- i + 1 until selfJoinPatterns.length) {
          //TODO: Another issue becasue of not knowing the exact instances is that 
          // if in the step 1, 2 instances of "user5   buys    product" are used 
          // to form a 2 edge pattern "user5   buys    product user5   buys    product"
          // with support 1. But now in step 2, it will try to join with 
          // sub-pattern "user5   buys    product" and form a 3-edge pattern
          // "user5   buys    product user5   buys    product user5   buys    product"
          // SO i am adding a sub-string check which is not always correct.
          if (!selfJoinPatterns(j)._1.contains(selfJoinPatterns(i)._1) &&
            !selfJoinPatterns(i)._1.contains(selfJoinPatterns(j)._1)) {
            var t0 = System.currentTimeMillis()
            joinedPattern = joinedPattern + (selfJoinPatterns(j)._1 + "|" +
              selfJoinPatterns(i)._1 -> (selfJoinPatterns(j)._2 * selfJoinPatterns(i)._2))
            var t1 = System.currentTimeMillis()
          }
        }
      }
      new KGNodeV2(attr.getlabel, joinedPattern |+| attr.getpattern_map,List.empty)

    })

    return newGraph
  }
  
def checkcross_join(pattern1 :String, pattern2 :String) : Boolean = 
{
  return !(pattern1.split("\t")(0).equalsIgnoreCase(pattern2.split("\t")(0)))
}

def compatible_join(pattern1 :String, pattern2 :String) : Boolean = 
{
  //if(pattern1.contains("type:person\tfriends_with"))
    //println("found")
  val basetpe : Set[String] = Set("type:person","type:company","type:product","type:sup_mt")
  val pattern1_array = pattern1.split("\t")
  val pattern2_array = pattern2.split("\t")
  //check non-compatible first edge
  if((pattern1_array(1).equalsIgnoreCase(pattern2_array(1))) 
      && (pattern1_array(2).startsWith("type:") && !pattern2_array(2).startsWith("type:"))) return false
  //type:person works_at        type:company      type:company        makes   type:sup_mt5
  //type:person works_at        o5      o5  makes       type:sup_mt5    :4
  else if((pattern1_array(1).equalsIgnoreCase(pattern2_array(1))) 
      && (!pattern1_array(2).startsWith("type:") && pattern2_array(2).startsWith("type:"))) return false
  //check non-compatible last edge
  //type:person     buys    type:product            type:person     works_at        type:company
  //type:person     works_at        o5              o5      makes   type:su    p_mt5    :4
  else if((pattern1_array(pattern1_array.length - 2).equalsIgnoreCase(pattern2_array(1)))   
      && ((pattern1_array(pattern1_array.length - 1).startsWith("type:") && !pattern2_array(2).startsWith("type:"))
          || (!pattern1_array(pattern1_array.length - 1).startsWith("type:") && pattern2_array(2).startsWith("type:")))) return false
  //pattern 2 originates from this node and its first edge is an instance edge of pattern 1's last edge  
  else if((pattern1_array(1).equalsIgnoreCase(pattern2_array(pattern2_array.length - 2)))   
      && ((pattern1_array(2).startsWith("type:") && !pattern2_array(pattern2_array.length - 1).startsWith("type:")) ||
          (!pattern1_array(2).startsWith("type:") && pattern2_array(pattern2_array.length - 1).startsWith("type:")))) return false
      
  else if(pattern1_array.last.startsWith("type:") && pattern2_array.last.startsWith("type:")) return true
  else if(pattern1_array.last.startsWith("type:") && !basetpe.contains(pattern2_array.last)) return true
  else if(!pattern1_array.last.startsWith("type:") && !basetpe.contains(pattern2_array.last)) return true
  return false
}

def non_overlapping(pattern1 :String, pattern2 :String) : Boolean =
{
      val pattern1array = pattern1.replaceAll("\t+", "\t").split("\t")
      val pattern2array = pattern2.replaceAll("\t+", "\t").split("\t")
      val p1a_length = pattern1array.length
      val p2a_length = pattern2array.length
      
      if(pattern1.contains("person\tworks_at\tcompany"))
      {
       //println("found")
      }
      
      if(p1a_length %3 !=0 || p2a_length %3 !=0)
      {
      	println(pattern1 + " wrong formatting and " + pattern2)
      	//System.exit(1)
      }
      //check 4 combinations of 'boundary-edge' overlap
      // a1b1, a1bn, anb1, anbn
      if (
          
        (pattern1array(0).equalsIgnoreCase(pattern2array(0)) &&
        pattern1array(1).equalsIgnoreCase(pattern2array(1)) &&
        pattern1array(2).equalsIgnoreCase(pattern2array(2))) ||
        
        (pattern1array(0).equalsIgnoreCase(pattern2array(p2a_length-3)) &&
        pattern1array(1).equalsIgnoreCase(pattern2array(p2a_length-2)) &&
        pattern1array(2).equalsIgnoreCase(pattern2array(p2a_length-1))) ||
        
        
        (pattern1array(p1a_length-3).equalsIgnoreCase(pattern2array(0)) &&
        pattern1array(p1a_length-2).equalsIgnoreCase(pattern2array(1)) &&
        pattern1array(p1a_length-1).equalsIgnoreCase(pattern2array(2))) ||
        
        (pattern1array(p1a_length-3).equalsIgnoreCase(pattern2array(p2a_length-3)) &&
        pattern1array(p1a_length-2).equalsIgnoreCase(pattern2array(p2a_length-2)) &&
        pattern1array(p1a_length-1).equalsIgnoreCase(pattern2array(p2a_length-1))) 
      
      
      ) return false
      return true
}

    
  def getSinkPatternOnNode(attr: KGNodeV1): Iterable[String] =
    {
      return attr.getpattern_map.filter(p => (p._2.size == 0)).map(f => f._1)
    }

    
        def getSinkPatternOnNodeV2(attr: KGNodeV2): Iterable[String] =
    {
      return attr.getpattern_map.filter(p => (p._2 == 0)).map(f => f._1)
    }
        
  def self_Instance_Join_Graph(updateGraph_withsink: Graph[KGNodeV1, KGEdge],
    join_size: Int): Graph[KGNodeV1, KGEdge] = {
    val g1 = updateGraph_withsink.mapVertices((id, attr) => {
      var joinedPattern: Map[String, Set[PatternInstance]] = Map.empty
      var all_instance: Set[PatternInstance] = Set.empty

      //join all sink patterns
      val allsink_patterns = getSinkPatternOnNode(attr)
      if (allsink_patterns.size > 0) {
        val empty_key_power_set: Set[String] = allsink_patterns.toSet
        val powe = empty_key_power_set.subsets(join_size)
        powe.foreach(f =>
          {
            var key: String = ""
            var i = -2;
            val set_size = f.size
            f.foreach(a_pattern_label => { key = key + "\t" + a_pattern_label })
            key = key.trim()
            joinedPattern = joinedPattern + (key -> Set())
          })

      }

      /*
      * Join non-sink nodes
      */
      attr.getpattern_map.foreach(sr => {
        all_instance = sr._2
        val power_set = all_instance.subsets(join_size)
        breakable {
          power_set.foreach(a_set => {
            if ((a_set.size > 0) && (a_set.size == join_size)) {
              val set_size = a_set.size
              var key: String = sr._1;
              var i = -2;
              for (i <- 1 to set_size) {
                ///key = key + "|" + sr._1
              }
              joinedPattern = joinedPattern + ((key -> (a_set)))
            }
          })
        }

      })
      new KGNodeV1(attr.getlabel, joinedPattern |+| attr.getpattern_map)
    })
    return g1;
  }


   
    
 def self_Instance_Join_GraphV2(updateGraph_withsink: Graph[KGNodeV2, KGEdge],
    join_size: Int): Graph[KGNodeV2, KGEdge] = {
    val g1 = updateGraph_withsink.mapVertices((id, attr) => {
      var joinedPattern: Map[String, Long] = Map.empty
      var all_instance: Long = 0

      //join all sink patterns
      val allsink_patterns = getSinkPatternOnNodeV2(attr)
      if (allsink_patterns.size > 0) {
        val empty_key_power_set: Set[String] = allsink_patterns.toSet
        val powe = empty_key_power_set.subsets(join_size)
        powe.foreach(f =>
          {
            var key: String = ""
            var i = -2;
            val set_size = f.size
            f.foreach(a_pattern_label => { key = key + "\t" + a_pattern_label })
            key = key.trim()
            joinedPattern = joinedPattern + (key -> 0)
          })

      }

      /*
      * Join non-sink nodes
      */
      attr.getpattern_map.filter(sr=>(sr._2 > 0) && (sr._2 < 20)).foreach(sr => {
        all_instance = sr._2
        
        //println("all" + all_instance)
        //TODO : all_instance is long but helper function needs an int....will generate wrong 
        // result
        if(all_instance - join_size > 0)
        {
        	        val numerof_new_instances = ArithmeticUtils.factorial(all_instance.toInt) / (
            ArithmeticUtils.factorial(join_size) * ArithmeticUtils.factorial(
                (all_instance.toInt - join_size)))
	        var key: String = sr._1;
	      var i = -2;
	      for (i <- 1 to join_size) {
	       key = key + "|" + sr._1
	      }
	      joinedPattern = joinedPattern + ((key -> numerof_new_instances.toInt))

        }


      })
      new KGNodeV2(attr.getlabel, joinedPattern |+| attr.getpattern_map,List.empty)
    })
    return g1;
  }
    
 

  def joinPatternGraph(nThPatternGraph: Graph[KGNodeV1, KGEdge],
    nPlusOneThPatternVertexRDD: VertexRDD[Map[String, Set[PatternInstance]]],
    result: Graph[PGNode, Int], SUPPORT: Int): Graph[KGNodeV1, KGEdge] =
    {
      val update_tmp_graph =
        nThPatternGraph.outerJoinVertices(nPlusOneThPatternVertexRDD) {
          case (id, a_kgnode, Some(nbr)) =>
            new KGNodeV1(a_kgnode.getlabel, a_kgnode.getpattern_map |+| nbr)
          case (id, a_kgnode, None) =>
            new KGNodeV1(a_kgnode.getlabel, a_kgnode.getpattern_map)
        }
      return get_Frequent_Subgraph(update_tmp_graph, result, SUPPORT);
      //return update_tmp_graph
    }

  def reducePatternsListOnNode(a: Map[String, Set[Int]], b: Map[String, Set[Int]]): Map[String, Set[Int]] =
    {
      return a |+| b
    }

  def reducePatternsListOnNodeInt(a: Map[String, Int], b: Map[String, Int]): Map[String, Int] =
    {
      return a |+| b
    }
  def reducePatternsOnNode(a: Map[String, Set[PatternInstance]], b: Map[String, Set[PatternInstance]]): Map[String, Set[PatternInstance]] =
    {
      println("reduce")
      return a |+| b
    }
  def reducePatternsOnNodeInt(a: Map[String, Int], b: Map[String, Int]): Map[String, Int] =
    {
      return a |+| b
    }
  def nodeHasBothPatterns(node_pattern_graph: Map[String, Set[String]], source_label: String,
    source_pattern: String, destination_pattern: String): Boolean =
    {
      return (node_pattern_graph.getOrElse(source_label,
        Set()).contains(source_pattern) &&
        node_pattern_graph.getOrElse(source_label,
          Set()).contains(destination_pattern))
    }

  def getFusiblePattern(pattern2: String): String =
    {
      //println("dst pattern is "+pattern2)
      val m = Pattern.
        compile("(\\s*[^\\s]+\\s+[^\\s]+\\s+[^\\s]+\\s*)$").matcher(pattern2);
      if (m.find()) {
        return m.group(0).trim()
      } else
        return pattern2

    }

  /**
   *
   * Non sorted pattern keys are used for incrementally increasing
   * (in one direction) patterns one edge at a time
   *
   */
  def sendPattern(pattern1: String, pattern2: String,
    instance1Dst: PatternInstance, instance2Dst: PatternInstance,
    edge: EdgeContext[KGNodeV1, KGEdge, Map[String, Set[PatternInstance]]]) {

    var bigger_instance: scala.collection.immutable.Set[(Int, Int)] = Set.empty
    instance1Dst.get_instacne.foreach(edge =>
      bigger_instance += (edge))
    instance2Dst.get_instacne.foreach(edge =>
      bigger_instance += (edge))
    val new_pattern_instance = new PatternInstance(bigger_instance)

    edge.sendToSrc(Map(pattern1 + "|" + pattern2 -> Set(new_pattern_instance)))
  }

  def sendPatternV2(pattern1: String, pattern2: String,
    instance1Dst: Long, instance2Dst: Long,
    edge: EdgeContext[KGNodeV2, KGEdge, Map[String, Long]]) {

    var bigger_instance: Int = 0
     edge.sendToSrc(Map(pattern1 + "|" + pattern2 -> instance2Dst))
  }

 def sendPatternV2Flat(pattern1: String, pattern2: String,
    instance1Dst: Long, instance2Dst: Long,
    edge: EdgeContext[KGNodeV2Flat, KGEdge, Map[String, Long]]) {

    var bigger_instance: Int = 0
     edge.sendToSrc(Map(pattern1 + "|" + pattern2 -> instance2Dst))
  }
  
  //sorted pattern keys are saved to handle isomorphic sub-graphs.
  def sendSortedPattern(pattern1: String, pattern2: String,
    instance1Dst: Int, instance2Dst: Int,
    edge: EdgeContext[(String, Map[String, Set[Int]]), String, Map[String, Set[Int]]]) {
    //save sorted triple pattern
    if (pattern1.hashCode() >= pattern2.hashCode()) {
      edge.sendToSrc(Map(pattern1 + "|" + pattern2 -> Set(instance2Dst)))
    } else {
      edge.sendToSrc(Map(pattern2 + "|" + pattern1 -> Set(instance1Dst)))
    }
  }

  def isFusiblePatterns(pattern1: String, pattern2: String): Boolean =
    {
      //TODO : Check for Exact similar instance
      if ((pattern1.replaceAll("\\|", "").startsWith(pattern2.replaceAll("\\|", "")))
        || (pattern2.replaceAll("\\|", "").startsWith(pattern1.replaceAll("\\|", "")))
        || (pattern1.replaceAll("\\|", "").endsWith(pattern2.replaceAll("\\|", "")))
        || (pattern2.replaceAll("\\|", "").endsWith(pattern1.replaceAll("\\|", "")))) {
        // TODO :check for exact sub-instance matching before joining.
        return true
      }
      return false;
    }
  def sendPatternToNode(edge: EdgeContext[KGNodeV1, KGEdge, Map[String, Set[PatternInstance]]]) {
    val allSourceNodePatterns = edge.srcAttr.getpattern_map;
    val allDestinationNodePatterns = edge.dstAttr.getpattern_map

    if ((allSourceNodePatterns.size > 0) && (allDestinationNodePatterns.size > 0)) {
      /*
       * Graph Join Operations.
       * 
       * 1. common node to join
       * 2. graph bijection
       * 3. graph frequency
       * 4. closed graph pattern
       * 5. Sub-graph isomorphism .....
       * TBD...
       */
      // next size pattern obtained by joining patterns on neighbours.

      allSourceNodePatterns.foreach(sr =>
        allDestinationNodePatterns.foreach(dst =>
          {
            /*
	         * barclays	expects	people
	         * people	invest	financial markets, 
	         * people	invest	angellist,
	         * people	invest	northeast, 
	         * people	invest	exchange
	         * 
	         * sr=(org	expects	people,Set(people))
	         * dst=(people	invest	org,Set(financial markets,angellist, northeast, exchange))
	         * 
	         */
            sr._2.foreach(sr_instance => {
              dst._2.foreach(dst_instance => {

                //	            if((sr_instance.get_instacne.size == 1) 
                //	                && (sr_instance.get_instacne.head._2 == edge.dstAttr.getlabel.hashCode()))
                if ((sr_instance.get_instacne.size == 1) &&
                  (sr_instance.get_instacne.head._2 == edge.dstAttr.getlabel.hashCode())) //(sr_instance.get_instacne.intersect(dst_instance.get_instacne) == Set.empty))
                  {
                  { //save triple pattern as it appears on the source and destination
                    sendPattern(sr._1, dst._1, sr_instance, dst_instance, edge)
                  }

                }
              })
            })
          }))
      //self join patter with 1 more edge
      // make a (ordered) list to do permutation
      // it is 4 level loop but on each node, number of pattern will be 
      // finite.

    }
    //println("sendPatternToNode done");
  }


  //VertexRDD[Map[key is a pattern stored as list of strings, 
  // value is a map  : Map[vertex labely as key , list of all destination nodes in this pattern]

  // Return value is a graph where every vertex stores (String,Map[String, Set[String]])
  // The first entry is the vertex label.
  // The second entry is a map.  Keys of the map represents a pattern (e.g. P1, P8) and the values
  // are instances of that pattern (Sumit, buys, Hybrid)



  def get_Frequent_Subgraph(subgraph_with_pattern: Graph[KGNodeV1, KGEdge],
    result: Graph[PGNode, Int], SUPPORT: Int): Graph[KGNodeV1, KGEdge] =
    {
      /*
	 * This method returns output graph only with frequnet subgraph. 
	 * TODO : need to clean the code
	 */
      println("checking frequent subgraph")
      var t0 = System.nanoTime()
      var commulative_subgraph_index: Map[String, Int] = Map.empty;

      //TODO : Sumit: Use this RDD instead of the Map(below) to filter frequent pattersn
      val pattern_support_rdd: RDD[(String, Int)] =
        subgraph_with_pattern.vertices.flatMap(vertex =>
          {
            vertex._2.getpattern_map.map(f => (f._1, f._2.size)).toSet
            //          var tmp: Set[(String, Int)] = Set.empty
            //          for (p <- vertex._2.getpattern_map) {
            //
            //            //TODO : for now keep tracking all the frequent sink patterns
            //            // Need to keep track for their support. needs data structure 
            //            // change. few more than expected patterns are getting tracked now
            //            if (p._2.size == 0) {
            //              val new_node = (p._1.replaceAll("\\|", "\t"), -1) // -1 reports a sink pattern
            //              tmp = tmp + new_node
            //            }
            //
            //            // double quote | is treated a OP operator and have special meaning
            //            // so use '|'
            //            //val valid_pattern_instance = p._2 - 0
            //            println("in flatmpa")
            //            val valid_pattern_instance = p._2
            //            if (valid_pattern_instance.size > 0) {
            //              val new_node = (p._1.replaceAll("\\|", "\t"), valid_pattern_instance.size)
            //              tmp = tmp + new_node
            //            }
            //          }
            //          tmp
          })
      println("before reduce " + pattern_support_rdd.count)
      pattern_support_rdd.collect.foreach(f=> println(f.toString))
      val tmpRDD = pattern_support_rdd.reduceByKey((a, b) => a + b)
      println("after reduce " + tmpRDD.count)
      tmpRDD.collect.foreach(f=> println(f.toString))
      val frequent_pattern_support_rdd = tmpRDD.filter(f => ((f._2 >= SUPPORT) | (f._2 == -1)))
      var t1 = System.nanoTime()
      println("time to calculate rdd frequnet pattern in nanao" + (t1 - t0) * 1e-9 + "seconds," + "frequnet pattern rdd size " + frequent_pattern_support_rdd.count)
      val frequent_patterns = frequent_pattern_support_rdd.keys.collect
      println()
      var tt0 = System.nanoTime()
      //      subgraph_with_pattern.vertices.collect.foreach(index_map => {
      //        index_map._2.getpattern_map.foreach(index => {
      //          if (index._2 == Set(0)) {
      //            commulative_subgraph_index = commulative_subgraph_index + (index._1 ->
      //              (commulative_subgraph_index.getOrElse(index._1, 0) + index._2.size))
      //          }
      //          val valid_pattern_instance = index._2
      //          if (valid_pattern_instance.size > 0)
      //            commulative_subgraph_index = commulative_subgraph_index + (index._1 ->
      //              (commulative_subgraph_index.getOrElse(index._1, 0) + index._2.size))
      //        })
      //      })
      //      commulative_subgraph_index = 
      //        commulative_subgraph_index.filter(v => v._2 >= SUPPORT)
      var tt1 = System.nanoTime()
      println("commulative_subgraph_index size" + commulative_subgraph_index.size + "time = " + (tt1 - tt0) * 1e-9 + "seconds,")
      val newGraph: Graph[KGNodeV1, KGEdge] =
        subgraph_with_pattern.mapVertices((id, attr) => {
          var joinedPattern: Map[String, Set[PatternInstance]] = Map.empty
          val vertex_pattern_map = attr.getpattern_map
          vertex_pattern_map.map(vertext_pattern =>
            {
              
              if (frequent_patterns.contains(vertext_pattern._1))
                joinedPattern = joinedPattern + ((vertext_pattern._1 -> vertext_pattern._2))
            })
          new KGNodeV1(attr.getlabel, joinedPattern)

        })

      val validgraph = newGraph.subgraph(epred =>
        ((epred.srcAttr.getpattern_map != null) || (epred.dstAttr.getpattern_map != null)))
      return validgraph;
    }
 
     
    
 def get_Frequent_SubgraphV2(subgraph_with_pattern: Graph[KGNodeV2, KGEdge],
    result: Graph[PGNode, Int], SUPPORT: Int): Graph[KGNodeV2, KGEdge] =
    {
      /*
	 * This method returns output graph only with frequnet subgraph. 
	 * TODO : need to clean the code
	 */
      println("checking frequent subgraph")
      var t0 = System.nanoTime()
      var commulative_subgraph_index: Map[String, Int] = Map.empty;

      //TODO : Sumit: Use this RDD instead of the Map(below) to filter frequent pattersn
      val pattern_support_rdd: RDD[(String, Long)] =
        subgraph_with_pattern.vertices.flatMap(vertex =>
          {
            vertex._2.getpattern_map.map(f => (f._1, f._2)).toSet
          })
      println("before reduce " + pattern_support_rdd.count)
      pattern_support_rdd.collect.foreach(f=> println(f.toString))
      val tmpRDD = pattern_support_rdd.reduceByKey((a, b) => a + b)
      println("after reduce " + tmpRDD.count)
      tmpRDD.collect.foreach(f=> println(f.toString))
      val frequent_pattern_support_rdd = tmpRDD.filter(f => ((f._2 >= SUPPORT) | (f._2 == -1)))
      var t1 = System.nanoTime()
      println("time to calculate rdd frequnet pattern in nanao" + (t1 - t0) * 1e-9 + "seconds," + "frequnet pattern rdd size " + frequent_pattern_support_rdd.count)
      val frequent_patterns = frequent_pattern_support_rdd.keys.collect
      println()
      var tt0 = System.nanoTime()
      var tt1 = System.nanoTime()
      println("commulative_subgraph_index size" + commulative_subgraph_index.size + "time = " + (tt1 - tt0) * 1e-9 + "seconds,")
      val newGraph: Graph[KGNodeV2, KGEdge] =
        subgraph_with_pattern.mapVertices((id, attr) => {
          var joinedPattern: Map[String, Long] = Map.empty
          val vertex_pattern_map = attr.getpattern_map
          vertex_pattern_map.map(vertext_pattern =>
            {
              
              if (frequent_patterns.contains(vertext_pattern._1))
                joinedPattern = joinedPattern + ((vertext_pattern._1 -> vertext_pattern._2))
            })
          new KGNodeV2(attr.getlabel, joinedPattern,List.empty)

        })

      val validgraph = newGraph.subgraph(epred =>
        ((epred.srcAttr.getpattern_map != null) || (epred.dstAttr.getpattern_map != null)))
      return validgraph;
    }
    
}