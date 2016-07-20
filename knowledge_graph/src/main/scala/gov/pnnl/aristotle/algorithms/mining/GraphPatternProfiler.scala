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
import gov.pnnl.aristotle.algorithms.mining.GraphProfiling
import gov.pnnl.aristotle.algorithms.mining.datamodel.KGNodeV2Flat
import gov.pnnl.aristotle.algorithms.mining.datamodel.PatternConstraint
import gov.pnnl.aristotle.algorithms.mining.datamodel.PatternConstraint
import gov.pnnl.aristotle.algorithms.mining.datamodel.PatternGraph

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

      def fixGraphV2Flat(graph: Graph[KGNodeV2Flat, KGEdge]):
    Graph[KGNodeV2Flat, KGEdge] =
    {
      val newGraph: Graph[KGNodeV2Flat, KGEdge] = graph.mapVertices((id, attr) => {
        var joinedPattern: Map[String, Long] = Map.empty
        val vertex_pattern_map = attr.getpattern_map
        //            val vertext_label = edge.srcAttr._1
        //if (vertex_pattern_map.size > 0) {
        vertex_pattern_map.map(vertext_pattern =>
          {
            if (vertext_pattern._1.contains('|'))
              joinedPattern = joinedPattern + ((vertext_pattern._1.
                replaceAll("\\|", "\t").replaceAll("\t+", "\t") -> vertext_pattern._2))
            else
              joinedPattern = joinedPattern + ((vertext_pattern._1.replaceAll("\t+", "\t") ->
                vertext_pattern._2))
          })
        new KGNodeV2Flat(attr.getlabel, joinedPattern,List.empty)
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

  def PERCOLATOR(batch_state: BatchState, sc: SparkContext, writerSG: PrintWriter,
    TYPE: String, SUPPORT: Int, TYPE_THRESHOLD: Int,
    iteration_limit: Int,
    window_state: WindowState) // Sumit -> P1, Mark -> P1
    : WindowState =
    {
      /*
     * Override defaults parameters with the command line parameters  
     */
      this.TYPE = TYPE;
      //this.SUPPORT = SUPPORT;
      this.TYPE_THRESHOLD = TYPE_THRESHOLD;

      /*
     * Create data structures required to keep track of 
     */
      var subgraph_index = window_state.getsubgraph_index 
      // tracks: P1: [(Mark, Prius), (Sumit, Prius)]
      var dependency_graph = window_state.getdependency_graph

      /*
     * Delete stale batches.
     * TODO: create a method for this
     * TODO: validate the code
     */
      val stale_pattern_id = batch_state.id - 4

      var pattern_trend: Map[String, List[(Int, Int)]] = window_state.pattern_trend
      //TODO: A RDD CAN BE THINK Instead of Map
      var stale_pattern: Map[String, Int] = Map.empty
  
      //update the pattern_trend Map
      pattern_trend.foreach(pattern_trend_entry => {
        if (stale_pattern.contains(pattern_trend_entry._1))
          pattern_trend += (pattern_trend_entry._1 ->
            pattern_trend.getOrElse(pattern_trend_entry._1,
              pattern_trend_entry._2).tail)
      })

      /*
     * Update All the patterns and Dependency Graph. Remove all the support values in stale patterns
	 *
     * Initialized local objects based on current window state
     */
      // Graph[(pattern-key,number-of-instances), -1/-2/-3] -{1,2,3} is redundant as of now. TODO: remove
      var closed_patterns_graph: Graph[PGNode, Int] =
        window_state.getclosed_patterns_graph;
      var new_closed_graph: Graph[PGNode, Int] = null;
      if (closed_patterns_graph != null) {
        new_closed_graph = closed_patterns_graph.mapVertices((id, attr) => {
          if (stale_pattern.contains(attr.getnode_label))
            new PGNode(attr.getnode_label,
              attr.getsupport - stale_pattern.getOrElse(attr.getnode_label, 0),-1)
          else new PGNode("", -1,-1)
        })
      }

      var redundent_patterns_graph: Graph[PGNode, Int] =
        window_state.getredundent_patterns_graph;
      var new_redundent_patterns_graph: Graph[PGNode, Int] = null
      if (redundent_patterns_graph != null) {
        new_redundent_patterns_graph =
          redundent_patterns_graph.mapVertices((id, attr) => {
            if (stale_pattern.contains(attr.getnode_label))
              new PGNode(attr.getnode_label,
                attr.getsupport - stale_pattern.getOrElse(attr.getnode_label, 0),-1)
            else new PGNode("", -1,-1)
          })
      }

      var promising_patterns_graph: Graph[PGNode, Int] =
        window_state.getpromising_patterns_graph;
      var new_promising_patterns_graph: Graph[PGNode, Int] = null;
      if (promising_patterns_graph != null) {
        new_promising_patterns_graph =
          promising_patterns_graph.mapVertices((id, attr) => {
            if (stale_pattern.contains(attr.getnode_label))
              new PGNode(attr.getnode_label,
                attr.getsupport - stale_pattern.getOrElse(attr.getnode_label, 0),-1)
            else new PGNode("", -1,-1)
          })
      }

      val new_dependency_graph = dependency_graph.mapVertices((id, attr) => {
        if (stale_pattern.contains(attr.getnode_label))
          new PGNode(attr.getnode_label,
            attr.getsupport - stale_pattern.getOrElse(attr.getnode_label, 0),-1)
      })

      //    var promising_patterns_graph: Graph[PGNode, Int] = window_state.getpromising_patterns_graph;
      //    var redundent_patterns_graph: Graph[PGNode, Int] = window_state.getredundent_patterns_graph;
      //    var closed_patterns_graph: Graph[PGNode, Int] = window_state.getclosed_patterns_graph;
      //    
      var run_stat: Map[Int, List[Double]] = Map();
      var pattern_results = (redundent_patterns_graph,
        closed_patterns_graph, promising_patterns_graph)
      var input_v_size = batch_state.getinputgraph.vertices.count
      var input_e_size = batch_state.getinputgraph.edges.count
      var result = (subgraph_index, dependency_graph)
      var frequent_instance: Long = 0
      var frequent_instances: Long = 0
      var redundant_instances: Long = 0
      var closed_instances : Long = 0
      var promising_instances : Long = 0

      /*
     * Get base pattern graph
    */
      var t0 = System.nanoTime();
      val oneEdgePatternGraph: Graph[KGNodeV1, KGEdge] =
        getBasePatternGraph(batch_state.getinputgraph,
          writerSG, pattern_results._2, SUPPORT)
      batch_state.getinputgraph.unpersist(true)
      result = updateFSMap(oneEdgePatternGraph, sc,
        subgraph_index, dependency_graph)
      //      oneEdgePatternGraph.vertices.collect.foreach(f =>
      //        writerSG.println("graph 1 : " + f.toString))
      pattern_results = updateCloedPatternDependencyGraph(result)
      var fixed_grph = fixGraph(oneEdgePatternGraph)
      var t1 = System.nanoTime();
      printPattern(fixed_grph, writerSG, 1)

      /*
     * Instrumentation for base pattern mining
     * Support calculation
     */
      var t_i0 = System.nanoTime();
      if (result._2.vertices.count > 0)
        frequent_instances = result._2.vertices.map(f =>
          f._2.getsupport).reduce((a, b) => a + b)
      if (pattern_results._1.vertices.count > 0)
        redundant_instances = pattern_results._1.vertices.map(f =>
          f._2.getsupport).reduce((a, b) => a + b)

      if (pattern_results._2.vertices.count > 0)
        closed_instances = pattern_results._2.vertices.filter(f =>
          f._2.getsupport > 0).map(f => f._2.getsupport).reduce((a, b) => a + b)

      if (pattern_results._3.vertices.count > 0)
        promising_instances = pattern_results._3.vertices.map(f =>
          f._2.getsupport).reduce((a, b) => a + b)

      var t_i1 = System.nanoTime();

      writerSG.println("\n" + 1 + "###Time to calculate pattern instance count" +
        " =" + (t_i1 - t_i0) * 1e-9 + "s")
      writerSG.flush();
      run_stat += (1 -> List(input_v_size, input_e_size,
        (t1 - t0) * 1e-9, result._2.vertices.count, frequent_instances,
        pattern_results._1.vertices.count, redundant_instances,
        pattern_results._2.vertices.count, closed_instances,
        pattern_results._3.vertices.count, promising_instances))

      /*
	 *  Get two size pattern graph.
	 *  >2 size pattern graphs are recusively mined in the section below
	 *  
	 */
      t0 = System.nanoTime();
      var updatedGraph2Edge = getAugmentedGraphNextSize(fixed_grph,
        pattern_results._2, writerSG, 2, SUPPORT)
      oneEdgePatternGraph.unpersist(true);
      result = updateFSMap(updatedGraph2Edge, sc, result._1, result._2)
      //      updatedGraph2Edge.vertices.collect.foreach(f =>
      //        writerSG.println("graph 2 : " + f.toString))
      pattern_results = updateCloedPatternDependencyGraph(result)
      fixed_grph = fixGraph(updatedGraph2Edge)
      printPattern(fixed_grph, writerSG, 2)

      /*
     * Instrumentation for 2 size pattern mining
     * Support calculation
     */
      input_v_size = fixed_grph.vertices.count
      input_e_size = fixed_grph.edges.count
      t1 = System.nanoTime();
      t_i0 = System.nanoTime();
      if (result._2.vertices.count > 0)
        frequent_instances = result._2.vertices.map(f =>
          f._2.getsupport).reduce((a, b) => a + b)
      if (pattern_results._1.vertices.count > 0)
        redundant_instances = pattern_results._1.vertices.map(f =>
          f._2.getsupport).reduce((a, b) => a + b)
      if (pattern_results._2.vertices.count > 0)
        closed_instances = pattern_results._2.vertices.filter(f =>
          f._2.getsupport > 0).map(f => f._2.getsupport).reduce((a, b) => a + b)
      if (pattern_results._3.vertices.count > 0)
        promising_instances = pattern_results._3.vertices.map(f =>
          f._2.getsupport).reduce((a, b) => a + b)
      t_i1 = System.nanoTime();
      writerSG.println("\n" + 2 + "###Time to calculate pattern instance count" +
        " =" + (t_i1 - t_i0) * 1e-9 + "s")
      writerSG.flush();

      run_stat += (2 -> List(input_v_size, input_e_size,
        (t1 - t0) * 1e-9, result._2.vertices.count, frequent_instances,
        pattern_results._1.vertices.count, redundant_instances,
        pattern_results._2.vertices.count, closed_instances,
        pattern_results._3.vertices.count, promising_instances))

      /*
	 *    Continue mining the resulting graph
	 *    To Debug the code, 'iteration' counter check the number of 
	 *    rounds a graph is minined.
	 *    
	 *    It is also possible to stop the mining when the number of closed
	 *    patterns stopped increasing 
	 */
      var last_closed_pattern_count = pattern_results._2.vertices.count
      var iteration = 3;
      var closed_fixed_graph = fixed_grph;
      breakable {
        while (1 == 1) {
          println("********* " + iteration)
          if (iteration == iteration_limit) break;

          /*
         * Mine the graph based on current input graph.
         * Update the data structures to keep track of patterns found 
         * so far
         */
          writerSG.println("in iteration" + iteration)
          writerSG.flush();

          t0 = System.nanoTime();
          var next_pattern_graph = getAugmentedGraphNextSize(closed_fixed_graph,
            pattern_results._2, writerSG, iteration, SUPPORT)
//          next_pattern_graph.vertices.collect.foreach(f =>
//            writerSG.println("graph " + iteration + " : " + f.toString))

          input_v_size = closed_fixed_graph.vertices.count
          input_e_size = closed_fixed_graph.edges.count
          fixed_grph.unpersist(true)
          result = updateFSMap(next_pattern_graph, sc, result._1, result._2)
          //result._1.collect.foreach(f=>writerSG.println("resN : "+f.toString))
          pattern_results = updateCloedPatternDependencyGraph(result)
          fixed_grph = fixGraph(next_pattern_graph)
          closed_fixed_graph = get_Closed_Subgraph(fixed_grph, result);

          /*
         * Instrumentation Code
         * Support calculation
         */
          t1 = System.nanoTime();
          val t_i0 = System.nanoTime();
          if (result._2.vertices.count > 0)
            frequent_instance = result._2.vertices.filter(v =>
              v._2 != null).map(f => f._2.getsupport).reduce((a, b) => a + b)
          //println("freqqunt instace" +frequent_instance)
          if (pattern_results._1.vertices.count > 0)
            redundant_instances = pattern_results._1.vertices.map(f =>
              f._2.getsupport).reduce((a, b) => a + b)
          if (pattern_results._2.vertices.count > 0)
            closed_instances = pattern_results._2.vertices.filter(f =>
              f._2.getsupport > 0).map(f => f._2.getsupport).reduce((a, b) => a + b)
          if (pattern_results._3.vertices.count > 0)
            promising_instances = pattern_results._3.vertices.map(f =>
              f._2.getsupport).reduce((a, b) => a + b)
          val t_i1 = System.nanoTime();
          writerSG.println("\n" + iteration +
            "###Time to calculate pattern instance count" +
            " =" + (t_i1 - t_i0) * 1e-9 + "s")
          writerSG.flush();

          run_stat += (iteration -> List(input_v_size, input_e_size,
            (t1 - t0) * 1e-9, result._2.vertices.count, frequent_instances,
            pattern_results._1.vertices.count, redundant_instances,
            pattern_results._2.vertices.count, closed_instances,
            pattern_results._3.vertices.count, promising_instances))
          iteration = iteration + 1

          printPattern(closed_fixed_graph, writerSG, iteration)
          //if (pattern_results._2.vertices.count <= last_closed_pattern_count) break

        }
      }

      val pattern_in_this_batch = get_pattern(closed_fixed_graph, writerSG, iteration, SUPPORT)
      pattern_in_this_batch.collect.foreach(pattern => {
        val updated_trend: List[(Int, Int)] =
          pattern_trend.getOrElse(pattern._1,
            List[(Int, Int)]()) :+ (batch_state.id, pattern._2)
        pattern_trend = pattern_trend + (pattern._1 -> updated_trend)
      })

      writerSG.println("Iteration\tinput_graph_size_v\tinput_graph_size_e\trun_time\t#freqent\t#freqent_instance\t#redundant\t#redundant_instance\t#closed\t#closed_instance\t#promising\t#promising_instance\n")
      run_stat.foreach(iteration_state => {
        writerSG.print(iteration_state._1 + "\t")
        iteration_state._2.foreach(stat => writerSG.print(stat + "\t"))
        writerSG.println("")
      })

      val mb = 1024 * 1024
      val runtime = Runtime.getRuntime
      writerSG.println("** Used Memory:  " + (runtime.totalMemory - runtime.freeMemory) / mb)
      writerSG.println("** Free Memory:  " + runtime.freeMemory / mb)
      writerSG.println("** Total Memory: " + runtime.totalMemory / mb)
      writerSG.println("** Max Memory:   " + runtime.maxMemory / mb)
      return new WindowState(result._1, result._2, pattern_results._1,
        pattern_results._2, pattern_results._3, pattern_trend)
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
  
 def get_sorted_patternV2Flat(graph: Graph[KGNodeV2Flat, KGEdge],
    writerSG: PrintWriter, id: Int, SUPPORT: Int): RDD[(String, Long)] =
    {
      val tmpRDD_non_sorted = get_Pattern_RDDV2Flat(graph)
      //val tmpRDD = tmpRDD_non_sorted.sortBy(f => f._2) it also fails with ExecutorLostFailure mag5.out
      //println("Total Instances Found" + tmpRDD.map(f => f._2).reduce((a, b) => a + b))
      //tmpRDD.collect.foreach(f => writerSG.println("All:\t"  +f._1 + "\t" + f._2))

      val frq_tmpRDD = tmpRDD_non_sorted.filter(f => f._2 > SUPPORT)

      writerSG.flush()
      return frq_tmpRDD

    }
 //
 def get_pattern_node_association_V2Flat(graph: Graph[KGNodeV2Flat, KGEdge],
    writerSG: PrintWriter, id: Int, SUPPORT: Int): RDD[(String, Set[(Int,String)])] =
    {
      val v_degree = graph.degrees //.map(v => (v._1,v._2))
      val v_rdd_raw = graph.vertices
      val v_rdd_raw_joined = v_degree.innerZipJoin(v_rdd_raw)((id, degree, vnode) => (degree, vnode))
      //.innerZipJoin(v_rdd_raw)((id, degree, vnode) => (degree, vnode))
      //v_rdd_raw_joined
      val v_rdd: RDD[((Int, String), Iterable[String])] = v_rdd_raw_joined.map(v =>
        ((v._2._1, v._2._2.getlabel), v._2._2.getpattern_map.keys))
      val vb = v_rdd.map(v => v._1)

      val pattrn_rdd = v_rdd.flatMap(v => {
        var pattern_set: Set[(String, Set[(Int, String)])] = Set.empty
        v._2.map(p_string => pattern_set = pattern_set + ((p_string, Set(v._1))))
        pattern_set
      }).reduceByKey((a, b) => a |+| b)
      pattrn_rdd
    }
 
  def get_node_pattern_association_V2Flat(graph: Graph[KGNodeV2Flat, KGEdge],
    writerSG: PrintWriter, id: Int, SUPPORT: Int): RDD[(String, Set[String])] =
    {
      return graph.vertices.map(v => 
        (v._2.getlabel, v._2.getpattern_map.keys.toSet)).filter(v=>v._2.size > 0)
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
  
        def get_Pattern_RDDV2Flat(graph: Graph[KGNodeV2Flat, KGEdge]) 
    : RDD[(String,Long)] =
  {
    /*
     *  collect all the pattern at each node
     */
     val pattern_support_rdd: RDD[(String, Long)] =
        graph.vertices.flatMap(vertex =>
          {
            vertex._2.getpattern_map.map(f => (f._1.replaceAll("\\|", "\t"), f._2)).toSet
          })
      return pattern_support_rdd.reduceByKey((a, b) => a + b)
    }
    
  /*
   * This Method update global data structure keeping track of Window state
   * 
   */
  def updateFSMap(graph: Graph[KGNodeV1, KGEdge],
    sc: SparkContext,
    subgraph_index: RDD[(String, scala.collection.immutable.Set[PatternInstance])],
    dependency_graph: Graph[PGNode, Int]): (RDD[(String, Set[PatternInstance])], Graph[PGNode, Int]) =
    {
      println("start subgraph_index size is" + subgraph_index.count);

      /*
       * say P1 is "person worksAt org"
       * ansd P2 is "org locatedIn loc"
       * RDD[(P1: {(sumit.hashcode,pnnl.hashcode),(sutanay.hashcode, pnnl.hashcode)}
       *     (P2: {(pnnl.hascode,richland.hashcode),(google.hashcode, paloalto.hashcode)})
       */
      //(pattern_map._1,pattern_map._2.map(f=>(anode._2.getlabel.hashCode(),f)))))
      val sub_graph_udpated = graph.vertices.flatMap(anode => {
        (anode._2.getpattern_map.map(pattern_map =>
          (pattern_map._1, pattern_map._2.map(f => f))))
      }).reduceByKey((a, b) => a |+| b)

      /*
       * Update Dependency Graph
       * 
       * Incoming graph has pattern joined using pipe 
       * symbol i.e "|"
       * split based on this pipe to get children patterns and then create 2 new 
       * edges from both children patterns to bigger 'parent' pattern
       */

      val existin_dependency_graph_edges: RDD[Edge[Int]] = dependency_graph.edges

      val existin_dependency_graph_vertices: RDD[(VertexId, PGNode)] = dependency_graph.vertices

      //VertexRDD[(P1,9),(P2,25)(P4,20)]
      val patternRDD =
        graph.aggregateMessages[Set[(String, Int)]](
          edge => {
            if (edge.attr.getlabel.equalsIgnoreCase(TYPE) == false)
              edge.srcAttr.getpattern_map.foreach(pattern =>
                edge.sendToSrc(Set((pattern._1, pattern._2.size))))
          }, (pattern1OnNodeN, pattern2OnNodeN) => {
            pattern1OnNodeN |+| pattern2OnNodeN
          })

      val new_dependency_graph_vertices_RDD: RDD[(String, Int)] =
        patternRDD.flatMap(vertex =>
          {
            var tmp: Set[(String, Int)] = Set.empty
            for (p <- vertex._2) {
              // double quote | is treated a OP operator and have special meaning
              // so use '|'
              val new_node = (p._1.replaceAll("\\|", "\t"), p._2)
              tmp = tmp + new_node
            }
            tmp
          })

      /*
         * get the commulative RDD with its support
         * //RDD[(P1,250),(P2,400)(P4,200)]
         */
      val tmp_commulative_RDD =
        new_dependency_graph_vertices_RDD.reduceByKey((a, b) => a + b)

      ////RDD[(P1.hascode,(P1,250)),(P2.hashcode ,(P2,400)) , (P4.hashcode ,(P4,200)) ]
      val new_dependency_graph_vertices_support: RDD[(VertexId, PGNode)] = 
        tmp_commulative_RDD.map(pattern 
            => (pattern._1.hashCode().toLong, new PGNode(pattern._1, pattern._2,-1)))

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

      val updated_edges =
        existin_dependency_graph_edges.union(new_dependency_graph_edges)
      val updated_vertices =
        existin_dependency_graph_vertices.
          union(new_dependency_graph_vertices_support).reduceByKey((a, b) =>
            new PGNode(a.getnode_label, a.getsupport + b.getsupport,-1))
      //TODO : creating new graph loose the previous indexing, so change it to spark JOIN 

      var updated_dependency_graph =
        Graph(updated_vertices.distinct.filter(f => f != null),
          updated_edges.distinct.filter(f => f != null))
      updated_dependency_graph = updated_dependency_graph.subgraph(vpred =
        (vid, attr) => attr != null)
      return (sub_graph_udpated.union(subgraph_index),
        updated_dependency_graph)
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

  def self_Pattern_Join_Graph(updateGraph_withsink: Graph[KGNodeV1, KGEdge],
    join_size: Int): Graph[KGNodeV1, KGEdge] = {
    val newGraph = updateGraph_withsink.mapVertices((id, attr) => {

      /*
       * Initialize some local objects
       */
      var selfJoinPatternstmp: List[(String, Set[PatternInstance])] = List.empty
      var joinedPattern: Map[String, Set[PatternInstance]] = Map.empty

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
        for (j <- i until selfJoinPatterns.length) {
          println("doing self join")
          var t0 = System.currentTimeMillis()
          selfJoinPatterns(i)._2.foreach(src_instance => {
            selfJoinPatterns(j)._2.foreach(dst_instance => {
                        println("inside self join")

              if (src_instance.equals(dst_instance))
                joinedPattern = joinedPattern
              // Dont join to itself i.e. pattern with same instances
              else {
                if (src_instance.get_instacne.
                  intersect(dst_instance.get_instacne) == Set.empty) {
                  joinedPattern = joinedPattern + appendPattern(src_instance,
                    dst_instance, selfJoinPatterns(i)._1, selfJoinPatterns(j)._1)
                } else {
                  /*
                   * NOTE : Update: of we only add non-disjoint graphs, 
                   * we don't need these cases and fusion.
                   */
                }
              } //else ends
            })
          })
          var t1 = System.currentTimeMillis()
        }
      }
      new KGNodeV1(attr.getlabel, joinedPattern |+| attr.getpattern_map)

    })

    return newGraph
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
    
        def self_Pattern_Join_GraphV2Flat(updateGraph_withsink: Graph[KGNodeV2Flat, KGEdge],
    join_size: Int): Graph[KGNodeV2Flat, KGEdge] = {
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
       * 
       * TODO: because we are not tracking the actual instances, there is not way to 
       * distinguish that follwoing pattern and its support is wrong:
       *  person   buys        product         user5   buys    product  -> 4 
       *  because where the person is same as user5.
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
            !selfJoinPatterns(i)._1.contains(selfJoinPatterns(j)._1) && 
            (!checkcross_join(selfJoinPatterns(i)._1,selfJoinPatterns(j)._1)) &&
            (non_overlapping(selfJoinPatterns(i)._1,selfJoinPatterns(j)._1)) &&
            (compatible_join(selfJoinPatterns(i)._1,selfJoinPatterns(j)._1))
          ) 
          {

            val pattern: String = selfJoinPatterns(j)._1.trim() + "|" + "\t" +
              selfJoinPatterns(i)._1.trim() + "|"
            val pg = new PatternGraph()
            ///println("pattern is for dfs "+pattern)
            pg.ConstructPatternGraph(pattern)
            var dfspath = pg.DFS(selfJoinPatterns(j)._1.split("\t")(0))
            if (dfspath.endsWith("|"))
            {
              val ind = dfspath.lastIndexOf("|")
              dfspath = dfspath.substring(0, ind)
              
            }
          	joinedPattern = joinedPattern + (dfspath -> (selfJoinPatterns(i)._2 * selfJoinPatterns(j)._2))
//              if (GraphOrdering.comparegraph(selfJoinPatterns(j)._1, selfJoinPatterns(i)._1) < 0) {
//                var t0 = System.currentTimeMillis()
//                joinedPattern = joinedPattern + (selfJoinPatterns(j)._1 + "|" +
//                  selfJoinPatterns(i)._1 -> (selfJoinPatterns(j)._2 * selfJoinPatterns(i)._2))
//                var t1 = System.currentTimeMillis()
//              } else if (GraphOrdering.comparegraph(selfJoinPatterns(j)._1, selfJoinPatterns(i)._1) > 0) {
//                var t0 = System.currentTimeMillis()
//                joinedPattern = joinedPattern + (selfJoinPatterns(i)._1 + "|" +
//                  selfJoinPatterns(j)._1 -> (selfJoinPatterns(i)._2 * selfJoinPatterns(j)._2))
//                var t1 = System.currentTimeMillis()
//              }

          }
        }
      }
      new KGNodeV2Flat(attr.getlabel, joinedPattern |+| attr.getpattern_map,List.empty)

    })

    return newGraph
  }
    
  def appendPattern(src_instance: PatternInstance, dst_instance: PatternInstance,
    srcPatternKey: String, dstPatternKey: String): (String, Set[PatternInstance]) =
    {

      var bigger_instance: scala.collection.immutable.Set[(Int, Int)] = Set.empty
      src_instance.get_instacne.foreach(edge =>
        bigger_instance += (edge))
      dst_instance.get_instacne.foreach(edge =>
        bigger_instance += (edge))
      val new_pattern_instance = new PatternInstance(bigger_instance)
      return (srcPatternKey + "|" +
        dstPatternKey -> (Set(new_pattern_instance))) //Set(0)))

    }

  def getSinkPatternOnNode(attr: KGNodeV1): Iterable[String] =
    {
      return attr.getpattern_map.filter(p => (p._2.size == 0)).map(f => f._1)
    }

    def getSinkPatternOnNodeV2Flat(attr: KGNodeV2Flat): Iterable[String] =
    {
      return attr.getpattern_map.filter(p => (p._2 == 0)).map(f => f._1)
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

    def self_Instance_Join_GraphV2Flat(updateGraph_withsink: Graph[KGNodeV2Flat, KGEdge],
    join_size: Int): Graph[KGNodeV2Flat, KGEdge] = {
    val g1 = updateGraph_withsink.mapVertices((id, attr) => {
      var joinedPattern: Map[String, Long] = Map.empty
      var all_instance: Long = 0

      //join all sink patterns
      val allsink_patterns = getSinkPatternOnNodeV2Flat(attr)
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
      new KGNodeV2Flat(attr.getlabel, joinedPattern |+| attr.getpattern_map,List.empty)
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
    
  def getAugmentedGraphNextSize(nThPatternGraph: Graph[KGNodeV1, KGEdge],
    result: Graph[PGNode, Int], writerSG: PrintWriter,
    iteration_id: Int, SUPPORT: Int): Graph[KGNodeV1, KGEdge] =
    {

      var t0 = System.currentTimeMillis();
      var t1 = System.currentTimeMillis();
      /*
	   * TODO: isomorphic patterns like these:
	   * 6 (person       friends_with    person          person  likes   product         person  works_at        company         company makes   nt              nt      is_produced_in  state               state   famous_for      national_park   ,3)
		6 (person       likes   product         person  works_at        company         company makes   nt              nt      is_produced_in  state           state   famous_for      national_park               person  friends_with    person  ,3)
		6 (person       friends_with    person          person  works_at        company         company makes   nt              nt      is_produced_in  state           state   famous_for      natio    nal_park   ,3)
	   * 
	   * 
	   */

      // Step 1: First do self join of existing n-size patterns to create n+1-size patterns.
      // To avoid pattern explosion because of creating different representation of same 
      // sub-graph, self-joined pattern are saved in sorted order of sub-graph.
      //    
      // for P = {1,2,3,....n} create PP { {1,2},  PP {1,3} } PPP {{1,2,3},{1,2,4}}, PPPP {{1,2,3,4},{1,2,3,5}}...patterns
      // i.e. power set of the patterns.
      //val g1 = self_Instance_Join_Graph(updateGraph_withsink,iteration_id)
      t0 = System.currentTimeMillis();
      val g1 = self_Instance_Join_Graph(nThPatternGraph, iteration_id)
      t1 = System.currentTimeMillis();
      writerSG.println("#TSize of self_instance update graph" + " =" + g1.vertices.count)
      writerSG.println("#Time to do self_instance join graph" + " =" + (t1 - t0) * 1e-3 + "s")

      //Step 2 : Self Join 2 different patterns at a node to create n+1 size pattern
      t0 = System.currentTimeMillis();
      val newGraph = self_Pattern_Join_Graph(nThPatternGraph, iteration_id)
      t1 = System.currentTimeMillis();
      writerSG.println("#TSize of self_pattern update graph" + " =" + newGraph.vertices.count)
      writerSG.println("#Time to do self_pattern join graph" + " =" + (t1 - t0) * 1e-3 + "s")

      /*
     *  STEP 3: instead of aggregating entire graph, map each edgetype
     */
      println("**in step 3")
      t0 = System.currentTimeMillis();
      val nPlusOneThPatternGraph = edge_Pattern_Join_Graph(newGraph, result, writerSG, SUPPORT)
      t1 = System.currentTimeMillis();
      writerSG.println("#TSize of  edge_join update graph" + " =" + nPlusOneThPatternGraph.vertices.count)
      writerSG.println("#Time to do edge_join  graph" + " =" + (t1 - t0) * 1e-3 + "s")

      return nPlusOneThPatternGraph
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

  def getBasePatternGraph(graph: Graph[String, KGEdge], writerSG: PrintWriter,
    result: Graph[PGNode, Int], SUPPORT: Int): Graph[KGNodeV1, KGEdge] = {

    //Get all the rdf:type dst node information on the source node
    var t0 = System.currentTimeMillis();
    val typedVertexRDD: VertexRDD[Map[String, Int]] = 
      GraphProfiling.getTypedVertexRDD_Temporal(graph, writerSG, TYPE_THRESHOLD, this.TYPE)

    // Now we have the type information collected in the original graph
    val typedAugmentedGraph: Graph[(String, Map[String, Int]), KGEdge] 
    		= GraphProfiling.getTypedAugmentedGraph_Temporal(graph, writerSG, typedVertexRDD)
    var t1 = System.currentTimeMillis();
    writerSG.println("#Time to prepare base typed graph" + " =" + (t1 - t0) * 1e-3 + "s")
    //typedAugmentedGraph.vertices.collect.foreach(v => writerSG.println("graph 0" + v.toString))
    /*
     * Create RDD where Every vertex has all the 1 edge patterns it belongs to
     * Ex: Sumit: (person worksAt organizaion) , (person friendsWith person)
     */

    val nonTypedVertexRDD: 
    VertexRDD[Map[String, scala.collection.immutable.Set[PatternInstance]]] = 
      typedAugmentedGraph.aggregateMessages[Map[String, 
        scala.collection.immutable.Set[PatternInstance]]](
      edge => {
        if (edge.attr.getlabel.equalsIgnoreCase(TYPE) == false) {
          // Extra info for pattern
          if ((edge.srcAttr._2.size > 0) 
              && (edge.dstAttr._2.size > 0)) {
            val dstnodetype = edge.dstAttr._2
            val srcnodetype = edge.srcAttr._2
            val dstNodeLable: String = edge.dstAttr._1
            val srcNodeLable: String = edge.srcAttr._1
            srcnodetype.foreach(s => {
              dstnodetype.foreach(d => {
                val patternInstance = 
                  edge.attr.getlabel.toString() + "\t" + dstNodeLable
                edge.sendToSrc(Map(s._1 + "\t" + edge.attr.getlabel + "\t" + d._1 + "|" 
                    -> Set(new PatternInstance(scala.collection.immutable.Set((
                        srcNodeLable.hashCode(), dstNodeLable.hashCode()))))))

              })
            })
          }
        }
      },
      (pattern1NodeN, pattern2NodeN) => {
        reducePatternsOnNode(pattern1NodeN, pattern2NodeN)
      })

    t0 = System.currentTimeMillis();
    val updateGraph: Graph[KGNodeV1, KGEdge] = 
      typedAugmentedGraph.outerJoinVertices(nonTypedVertexRDD) {
      case (id, (label, something), Some(nbr)) => new KGNodeV1(label, nbr)
      case (id, (label, something), None) => new KGNodeV1(label, Map())
    }
    t1 = System.currentTimeMillis();

    /*
     *  Filter : all nodes that dont have even a single pattern
     */
    val subgraph_with_pattern = updateGraph.subgraph(epred =>
      ((epred.attr.getlabel.equalsIgnoreCase(TYPE) != true) &&
        ((epred.srcAttr.getpattern_map.size != 0) 
            || (epred.dstAttr.getpattern_map.size != 0))))
    println("subgraph size" + subgraph_with_pattern.triplets.count)

    /*
       * update sink nodes and push source pattern at sink node with no destination information
    	 so that wehn we compute support of that pattern, it will not change the value.
         get all nodes which are destination of any edge
       */
    val all_dest_nodes = subgraph_with_pattern.triplets.map(triplets 
        => (triplets.dstId, triplets.dstAttr)).distinct
    val all_source_nodes = subgraph_with_pattern.triplets.map(triplets 
        => (triplets.srcId, triplets.srcAttr)).distinct
    val all_sink_nodes = 
      all_dest_nodes.subtractByKey(all_source_nodes).map(sink_node 
          => sink_node._2.getlabel).collect

    val graph_with_sink_node_pattern: 
    VertexRDD[Map[String, Set[PatternInstance]]] 
    = subgraph_with_pattern.aggregateMessages[Map[String, Set[PatternInstance]]](
      edge => {
        if (edge.attr.getlabel.equalsIgnoreCase(TYPE) == false) {
          if (all_sink_nodes.contains(edge.dstAttr.getlabel)) {
            val srcnodepattern = edge.srcAttr.getpattern_map
            val dstNodeLable: String = edge.dstAttr.getlabel
            val srcNodeLable: String = edge.srcAttr.getlabel
            srcnodepattern.foreach(s => {
              //val patternInstance = srcNodeLable +"\t"+ edge.attr.toString() +"\t"+dstNodeLable
              if (s._1.contains(edge.attr.getlabel)) // TODO: fix this weak comparison 
              {
                val pattern_instances = s._2
                pattern_instances.foreach(an_instance => {
                  // because it is one edge pattern yet, so no need to a loop
                  if (an_instance.get_instacne.head._2 == dstNodeLable.hashCode())
                    //edge.sendToDst(Map(s._1 -> Set(an_instance)))
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

    val updateGraph_withsink: Graph[KGNodeV1, KGEdge] = 
      subgraph_with_pattern.outerJoinVertices(graph_with_sink_node_pattern) {
      case (id, kg_node, Some(nbr)) 
      => new KGNodeV1(kg_node.getlabel, kg_node.getpattern_map |+| nbr)
      case (id, kg_node, None) 
      => new KGNodeV1(kg_node.getlabel, kg_node.getpattern_map)
    }
      if(SUPPORT != -1)
      {
      	    return get_Frequent_Subgraph(updateGraph_withsink, result, SUPPORT);
      }
      else
            updateGraph_withsink

  }

  def get_Closed_Subgraph(updateGraph: Graph[KGNodeV1, KGEdge],
    result: (RDD[(String, Set[PatternInstance])], 
        Graph[PGNode, Int])): Graph[KGNodeV1, KGEdge] =
    {

      // updatedGraph is already a frequent graph
      // Get nodes with at least one frequent pattern
      //    
      var commulative_subgraph_index: Map[String, Int] = Map.empty;
      //check if depedency graph has any 2edge pattern. There is no point check 
      // dependency graph for initial round of mining {0,1}
      val valid_dep_graph = result._2.vertices.filter(v 
          => v._2 != null).filter(veretex 
              => (veretex._2.getnode_label.split("\t").length > 3))
      if (valid_dep_graph.count > 1) {
        // We have dependency graph.
        result._2.vertices.filter(v => v._2 != null).collect.foreach(index => {
          commulative_subgraph_index = 
            commulative_subgraph_index + (index._2.getnode_label ->  
            (commulative_subgraph_index.getOrElse(index._2.getnode_label, 0) 
                + index._2.getsupport.toInt)
           )
        })

      } else {

      }

      //Filter : remove all non frequent instances. After this vertex will ONLY have frequent pattern 
      // and its instances

      var resulting_subgraph_index_rdd: VertexRDD[Map[String, Set[PatternInstance]]] =
        updateGraph.aggregateMessages[Map[String, Set[PatternInstance]]](
          edge => {
            if (edge.attr.getlabel.equalsIgnoreCase(TYPE) == false) {
              //if frequent pattern-> send to source
              val srcPatterns = edge.srcAttr.getpattern_map;
              //check each pattern on this node
              srcPatterns.foreach(pattern => {
                if (commulative_subgraph_index.contains(pattern._1.replaceAll("\\|", "")))
                  edge.sendToSrc(Map(pattern._1 -> pattern._2))
              })
            }
          },
          (pattern1NodeN, pattern2NodeN) => {
            reducePatternsOnNode(pattern1NodeN, pattern2NodeN)
          })

      var validgraph: Graph[KGNodeV1, KGEdge] = 
        updateGraph.outerJoinVertices(resulting_subgraph_index_rdd) {
        case (id, a_kgnode, Some(nbr)) => new KGNodeV1(a_kgnode.getlabel, nbr)
        case (id, a_kgnode, None) => new KGNodeV1(a_kgnode.getlabel, Map())
      }
      validgraph = validgraph.subgraph(epred =>
        ((epred.srcAttr.getpattern_map.size != 0)
          || (epred.dstAttr.getpattern_map.size != 0)))

      return validgraph;

    }

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
 
    def get_Frequent_SubgraphV2Flat(sc:SparkContext,subgraph_with_pattern: Graph[KGNodeV2Flat, KGEdge],
    result: Graph[PGNode, Int], SUPPORT: Int): Graph[KGNodeV2Flat, KGEdge] =
    {
      /*
	 * This method returns output graph with frequnet patterns only. 
	 * TODO : need to clean the code
	 */
      println("checking frequent subgraph")
      var commulative_subgraph_index: Map[String, Int] = Map.empty;

      //TODO : Sumit: Use this RDD instead of the Map(below) to filter frequent pattersn
      val pattern_support_rdd: RDD[(String, Long)] =
        subgraph_with_pattern.vertices.flatMap(vertex =>
          {
            vertex._2.getpattern_map.map(f => (f._1, f._2)).toSet
          })
      val tmpRDD = pattern_support_rdd.reduceByKey((a, b) => a + b)
      val frequent_pattern_support_rdd = tmpRDD.filter(f => ((f._2 >= SUPPORT) | (f._2 == -1)))

      val pattern_vertex_rdd: RDD[(String, Set[Long])] =
        subgraph_with_pattern.vertices.flatMap(vertex =>
          {
            vertex._2.getpattern_map.map(f => (f._1, Set(vertex._1))).toSet
          }).reduceByKey((a, b) => a++b) // append vertexids on each node
      val join_frequent_node_vertex = frequent_pattern_support_rdd.leftOuterJoin(pattern_vertex_rdd)
      val vertex_pattern_reverse_rdd = join_frequent_node_vertex.flatMap(pattern_entry 
          => (pattern_entry._2._2.getOrElse(Set.empty).map(v_id => (v_id, Set(pattern_entry._1))))).reduceByKey((a, b) => a ++ b)
      
      //originalMap.filterKeys(interestingKeys.contains)        
      val frequent_graph: Graph[KGNodeV2Flat, KGEdge] =
        subgraph_with_pattern.outerJoinVertices(vertex_pattern_reverse_rdd) {
          case (id, kg_node, Some(nbr)) => new KGNodeV2Flat(kg_node.getlabel, kg_node.getpattern_map.filterKeys(nbr.toSet), kg_node.getProperties)
          case (id, kg_node, None) => new KGNodeV2Flat(kg_node.getlabel, Map.empty, kg_node.getProperties)
        }

      val validgraph = frequent_graph.subgraph(epred =>
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