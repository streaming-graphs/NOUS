//package gov.pnnl.aristotle.algorithms
//import org.apache.spark._
//import org.apache.spark.graphx._
//import org.apache.spark.rdd.RDD
//import org.apache.spark.rdd.RDD
//import scala.collection.Set
//import org.apache.log4j.Logger
//import org.apache.log4j.Level
//import gov.pnnl.aristotle.algorithms._
//import gov.pnnl.aristotle.utils.NodeProp
//import gov.pnnl.aristotle.utils.MatchStringCandidates
//import java.io.PrintWriter
//import java.io.File
//import gov.pnnl.aristotle.algorithms.mining.DynamicPatternGraphV2Flat
//import gov.pnnl.aristotle.algorithms.mining.GraphProfiling
//import org.apache.spark.SparkContext._
//import scalaz.Scalaz._
//import gov.pnnl.aristotle.algorithms.mining.GraphPatternProfiler
//import gov.pnnl.aristotle.algorithms.mining.datamodel.PatternDependencyGraph
//import scala.util.control.Breaks._
//import org.apache.hadoop.conf.Configuration
//import org.apache.hadoop.fs.FileSystem
//import org.apache.spark.SparkConf
//import org.apache.spark.SparkContext
//import org.apache.hadoop.mapreduce.lib.input.{FileSplit, TextInputFormat}
//import org.apache.spark.rdd.{NewHadoopRDD}
//import org.apache.spark.{SparkConf, SparkContext}
//import org.apache.hadoop.io.LongWritable
//import org.apache.hadoop.io.Text

//object Driver {
//    
//  val sparkConf = new SparkConf().setAppName("Driver").setMaster("local")
//    val sc = new SparkContext(sparkConf)
//    Logger.getLogger("org").setLevel(Level.OFF)
//     Logger.getLogger("akka").setLevel(Level.OFF)
//     println("starting from main")
//     val writerSG = new PrintWriter(new File("GraphMiningDriver.txt"))
//    
//  def main(args: Array[String]): Unit = {
//    
//    
//    
//    if(args.length < 1) println("Not enough arguments, Usage:<pathToGraphFile(s)>")
//    
//    val graphFilename :String = args(4)
//    println("Reading graph")
//    val multi_edge_graph = ReadHugeGraph.getTemporalGraph(graphFilename, sc)
//	  val dp_grpah = new DynamicPatternGraphV2Flat(args(1).toInt)
//    val typed_graph = dp_grpah.getTypedGraph(multi_edge_graph, writerSG)
//    val typerdd = typed_graph.vertices.flatMap(vertex =>
//        {
//          vertex._2._2.map(a_type => (a_type._1, List(vertex._2._1)))
//        }).reduceByKey((a, b) => a |+| b)
//
//     val gBatch = dp_grpah.init(multi_edge_graph, 
//          writerSG,args(0),args(2).toInt)
//
//    val pattern_rdd = get_defaultPatternRDD(args)
//    var mode = "listen"
//    while(mode.toLowerCase() != "exit"){
//       
//      val userInput: Array[String] = readLine("\nEnter mode -profile [-type <type>]|[-entity <entity>]  OR -trend  <entity|relation>  OR -trend  firstK <number> <entity|relation>\n").
//      stripPrefix(" ").stripSuffix(" ").split(" ")
//      
//       mode = userInput(0).toLowerCase()
//       if(mode =="-profile" && userInput.length >= 2) {
//          var filename: String =  "sampleProfileOutput.txt"
//          val writerSG = new PrintWriter(new File(filename))
//     	 if(userInput(1) == "-type") {
//     	   val profileArgs = userInput.slice(2,userInput.length).mkString(" ")
// 	       GraphProfiling.showNodeTypeProfile(typerdd, profileArgs)
// 	     } else if(userInput(1) == "-entity") {
//     	   val profileArgs = userInput.slice(2,userInput.length).mkString(" ")
//     	   val vertexrdd = gBatch.input_graph.vertices.filter(v => v._2.getlabel.contains(profileArgs) && v._2.getpattern_map.size > 0)
//     	   val entity_anser_rdd  = vertexrdd.map(v => (v._1,v._2.getlabel,v._2.getpattern_map.keys)).collect
//     	   entity_anser_rdd.foreach(e=>println("*** Profile of input entity : \nid=" 
//     	       +e._1 + " \nlabel=" + e._2 + "\nbase pattern=" + e._3 +"\n***End***\n"))
//     	 } else {
//     	    // check for both
////     	   val profileArgs = userInput(1)
//// 	       val typemap = GraphProfiling.getAugmentedTypeMap(g, writerSG);
//// 	       GraphProfiling.showNodeTypeProfile(typemap, profileArgs)
//// 	       val augGraph = GraphProfiling.getAugmentedGraph(g, writerSG)
//// 	       GraphProfiling.showNodeProfile(augGraph, profileArgs)
//     	 }
//      } else if (mode == "-trend")
//      {
//        if(userInput(1) == "firstK")
//        {
//          try {
//            val first = userInput(2).toInt
//            val profileArgs = userInput.slice(3, userInput.length).mkString(" ")
//            val related_patterns = pattern_rdd.filter(p => p._1.contains(profileArgs)).collect.take(first)
//            //val top10 = LASGraphMiner.topK(related_patterns, 10)
//            related_patterns.foreach(p => {
//              println("*** Related patterns :" + p.toString)
//
//            })
//
//          } catch {
//            case ex: java.lang.Exception => {
//              println("Invalid Number Format ")
//            }
//            
//          }
//        }
//        val profileArgs = userInput.slice(1,userInput.length).mkString(" ")
//        val related_patterns = pattern_rdd.filter(p=>  p._1.contains(profileArgs))
//        //val top10 = LASGraphMiner.topK(related_patterns, 10)
//        related_patterns.foreach(p=>{
//          println("*** Related patterns :" + p.toString)
//          
//        })
//      }
//      
////      else if(mode == "-search"  && userInput.length == 2) {
////         val queryLabel =  userInput(1)
////         val nodesWithStringMatchesRDD : RDD[(VertexId, String)] = MatchStringCandidates.getMatchesRDD(queryLabel, g)
////         println("NUm Nodes that match candidate string:" + queryLabel + "=" + nodesWithStringMatchesRDD.count)
////         val candidates = nodesWithStringMatchesRDD.map(v => v._1).collect()
////
////         val nbrLabels : VertexRDD[Set[String]] =  NodeProp.getOneHopNbrLabels(g).filter(v=> candidates.contains(v._1))
////         
////         val candidateLabels:VertexRDD[String] = g.vertices.filter(v =>  candidates.contains(v._1))
////         val verticesLabelsNeighbours: VertexRDD[(String, Set[String])] = candidateLabels.innerZipJoin(nbrLabels)((id,label,nbrlist) => (label, nbrlist))
////         
////         verticesLabelsNeighbours.foreach(v=>  println("ENTITY: " + v._2._1 + ", Neighbour Labels = "  + v._2._2.toString))
////         
////   } else if( mode == "-path" && userInput.length >= 3) {
////         val entityLabel1 = userInput(1)
////         val entityLabel2 = userInput(2)
////         val entityLabels = Array(entityLabel1, entityLabel2)
////         var maxLength = 4
////         println("Trying to find " + maxLength + "  length path between "+ entityLabel1 , entityLabel2 )
////         val all2EdgePaths: List[List[(Long, String, String)]] = PathSearch.FindPaths(entityLabels, g, sc)
////
////         for(path <- all2EdgePaths){
////           for(edge <- path){ print(edge._2 + "->") }
////           println()
////         }
////         
////    }
//    else {
//      println("Invalid arguments", args)
//    }
//  }
//  println("Exiting..")
//}
//  
//  //REMOVE THIS AFTER LAS DEMO
//    def get_defaultPatternRDD(args : Array[String]) :  RDD[(String, Long)] =
//  {
//    val fc = classOf[TextInputFormat]
//    val kc = classOf[LongWritable]
//    val vc = classOf[Text]
//    val text = sc.newAPIHadoopFile(args(5), fc, kc, vc, sc.hadoopConfiguration)
//    val default_pattern_rdd: RDD[(String, Long)] = text.asInstanceOf[NewHadoopRDD[LongWritable, Text]]
//      .mapPartitionsWithInputSplit((inputSplit, iterator) => {
//        val file = inputSplit.asInstanceOf[FileSplit]
//        iterator.map(tup => {
//          val path_arr = file.getPath.toString().split("/")
//          val day = path_arr(path_arr.length - 2)
//          val fields = ReadHugeGraph.getFieldsFromPatternLine(tup._2.toString())
//          (fields(0), fields(1).toLong)
//        })
//      })
//      
//      return default_pattern_rdd
//  }
//  
//  
//    def pseudomain(args: Array[String]) : RDD[(String, Long)] =
//  {
//    
//
//    var t_sc0 = System.nanoTime();
//    var pattern_in_this_batch : RDD[(String, Long)] = null
//    /*
//	 * Read multiple files. for now, each file is treated as a new batch.
//	 */
//    val minSup = args(1).toInt;
//    val format: String = args(4)
//    val number_of_input_files = args.size
//    val windowSize = 5
//    var batch_id = -1;
//    var gWin = new DynamicPatternGraphV2Flat(minSup)
//    var gDep = new PatternDependencyGraph
//    var pattern_trend: Map[String, List[(Int, Int)]] = Map.empty
//
//    /*
//     * Read all the files/folder one-by-one and construct an input graph
//     */
//    for (i <- 4 to number_of_input_files - 1) {
//
//      /*
//       * Count the start time of the batch processing
//       */
//      val t_b0 = System.nanoTime();
//      var t0 = System.nanoTime();
//      val input_graph = ReadHugeGraph.getTemporalGraph(args(i), sc)
//      var t1 = System.nanoTime();
//      println("#Time to load the  graph" + " =" + (t1 - t0) * 1e-9 + "seconds," +
//        "#First Vertex is" + " =" )
//      
//        
//        
//      /*
//       *  gBatch is a pre-processed version of input graph. It has 1 edge 
//       *  pattern on each vertex. all the other information is removed from the
//       *  vertex. 
//       */
//      val gBatch = new DynamicPatternGraphV2Flat(minSup).init(input_graph, 
//          writerSG,args(0),args(2).toInt)
//      //gBatch.input_graph.vertices.collect.foreach(f => writerSG.println("base vertex " +f.toString))
//      gWin.trim(i, windowSize)
//      val batch_window_intersection_graph = gWin.merge(gBatch, sc)
//      var level = 0;
//      val iteration_limit: Int = args(3).toInt
////      batch_window_intersection_graph.input_graph.vertices.collect.foreach(f
////          =>writerSG.println("first batch graph ver"+f.toString))
//      writerSG.flush()
//      breakable {
//        while (1 == 1) {
//          level = level + 1
//          println(s"#####Iteration ID $level and interation_limit is $iteration_limit")
//          val t_i0 = System.nanoTime()
//          if (level == iteration_limit) break;
//
//          /*
//           * calculate the join time for the main mining operation
//           */
//          val t_j0 = System.nanoTime()
//          batch_window_intersection_graph.input_graph =
//            batch_window_intersection_graph.joinPatterns(gDep, writerSG, level)
//          val t_j1 = System.nanoTime()
//          println("#Time to get join the patterns" + " =" + (t_j1 - t_j0) * 1e-9 + "seconds," +
//            "#TSize of first vertext of  window graph" + " =")
//
//          /*
//           * Update the dependency graph 
//           */
//          val t_d0 = System.nanoTime()
//          //gDep.graph = gWin.updateGDep(gDep)
//          val t_d1 = System.nanoTime()
//          println("#Time to get update dependency graph" + " =" + (t_d1 - t_d0) * 1e-9 + "seconds," +
//            "#TSize of first vertext of  dependency graph" + " =")
//
//          println("updating gDep DONE")
//          val t_i1 = System.nanoTime()
//          println("#Time to mine the iteration" + " =" + (t_i1 - t_i0) * 1e-9 + "seconds," +
//            "#TSize of  pattern_trend map" + " =" + pattern_trend.size)
//
//          /*
//             * Update the current graph by removing special purpose '|' symbols 
//             * in the pattern keys. This symbol is used to identify which small
//             * patterns participate in construction a bigger pattern. 
//             */
//          batch_window_intersection_graph.input_graph =
//            GraphPatternProfiler.get_Frequent_SubgraphV2Flat(null,
//              GraphPatternProfiler.fixGraphV2Flat(batch_window_intersection_graph.input_graph), null, minSup)
////        batch_window_intersection_graph.input_graph.vertices.collect.foreach(f
////          =>writerSG.println(s"interation $level batch graph ver"+f.toString))
//        }
//      }
// 
////      batch_window_intersection_graph.input_graph.vertices.collect.foreach(f =>
////      writerSG.println("final batch graph is"+f.toString)  
////      )
//      /*
//       * Now merger the mined intersection graph with original window
//       */
//      gWin.input_graph = gWin.mergeBatchGraph(batch_window_intersection_graph.input_graph)
//      pattern_in_this_batch = GraphPatternProfiler.get_sorted_patternV2Flat(gWin.input_graph,
//        writerSG, 2, args(1).toInt)
//        println("received frequent pattern rdd size"+pattern_in_this_batch.count)
//      //pattern_in_this_batch.collect.foreach(f => writerSG.println(s"pattern_"+FilenameUtils.getBaseName(args(4)) +"= " + f.toString))
//      writerSG.flush()
//      val t_b1 = System.nanoTime();
//      println("#Time to mine the batch" + " =" + (t_b1 - t_b0) * 1e-9 + "seconds," +
//        "#TSize of  edge_join update graph" + " =" + pattern_trend.size)
//    }
//    var t_sc1 = System.nanoTime();
//    println("#Time to load the  graph" + " =" + (t_sc1 - t_sc0) * 1e-9 + "seconds," +
//      "#TSize of  edge_join update graph" + " =" + pattern_trend.size)
//    
//     /*
//     * Stop the spark context
//     */ 
//   //sc.stop
//  return pattern_in_this_batch
//  }
//}
