package gov.pnnl.aristotle.algorithms

import org.apache.spark.graphx.VertexRDD
import org.apache.spark.graphx.Graph
import gov.pnnl.aristotle.algorithms.mining.datamodel.KGEdgeInt
import org.apache.spark.rdd.RDD
import org.apache.spark.graphx.Edge
import org.apache.log4j.Logger
import org.apache.log4j.Level
import scala.io.Source
import org.joda.time.format.DateTimeFormat
import org.ini4j.Wini
import java.io.File
import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.graphx.EdgeTriplet
import com.google.inject.spi.Dependency
import org.apache.spark.graphx.EdgeDirection
import scala.util.Random
import scala.util.control.Breaks._
import java.io.PrintWriter



object DataToPatternGraph {
  
  val maxPatternSize: Int = 4
  type SinglePatternEdge = (Long, Long, Long)
  type Pattern = Array[SinglePatternEdge]
  type PatternId = List[SinglePatternEdge] //TODO : Will Change it to Long
  type SingleInstanceEdge = (Long, Long , Long)
  type PatternInstance = Array[SingleInstanceEdge]
  
  type DataGraph = Graph[Int, KGEdgeInt]
  type DataGraphNodeId = Long
  type PatternGraph = Graph[PatternInstanceNode, DataGraphNodeId]
  type DependencyGraph = Graph[DependencyNode, Long]
  type Label = Int
  type LabelWithTypes = (Label, List[Int])
  type PType = Int
  
  val writerSG = new PrintWriter(new File("GraphMiningOutputV4.txt")) 
  class DependencyNode(val pattern: Pattern) extends Serializable
  {
    val id: Long  = getPatternId(pattern)
    var ptype: PType = 1 // By Default everyone is considered closed. Later they are tagged accordingly.
    var support :  Int = 0
    /*
     * ptype:
     * -1 : Infrequent
     *  0 : Promising
     *  1 : Closed
     *  2 : Redundant
     */
    
  }
  class PatternInstanceNode(/// Constructed from hashing all nodes in pattern
    val patternInstMap : Array[(SinglePatternEdge, SingleInstanceEdge)],
    val timestamp: Long) extends Serializable {
    val id = getPatternInstanceNodeid(patternInstMap)
    
    
    
    def getPattern: Pattern = {
      patternInstMap.map(_._1)
    }
    
    def getInstance : PatternInstance = {
      patternInstMap.map(_._2)
    }
    
    def getAllVertexInInstance : Array[Long] = {
     patternInstMap.map(_._2).flatMap(singleInstance => Iterator(singleInstance._1, singleInstance._3))
    }
    
    /*
     * An SingleInstanceEdge is (Long, Long , Long)
     */
    def getAllSourceInstances : Array[Long] = {
      patternInstMap.map(_._2._1)
    }

     /*
     * An SingleInstanceEdge is (Long, Long , Long)
     */
    def getAllDestinationInstances : Array[Long] = {
      patternInstMap.map(_._2._3)
    }
    
    def getAllNodesInPatternInstance() : Array[DataGraphNodeId] = {
     val instance: PatternInstance = patternInstMap.map(_._2)
     val nodeids = Array[DataGraphNodeId](instance.size*2)
     for(i <- Range(0, instance.length-1, 2)) {
       nodeids(i) = instance(i)._1
       nodeids(i+1) = instance(i)._2
     }
     nodeids
   }
}
  
  def getPatternId(patternNode :
      Pattern): Long = {
      val patternInstHash: Int = patternNode.toList.map(patternEdge =>  {
           patternEdge.hashCode
         }).hashCode()
         patternInstHash 
    }
  
  def getPatternInstanceNodeid(patternInstMap :
      Array[(SinglePatternEdge, SingleInstanceEdge)]): Long = {
      val patternInstHash: Int = patternInstMap.map(patternEdgeAndInst =>  {
           val pattern = patternEdgeAndInst._1.hashCode
           val inst = patternEdgeAndInst._2.hashCode
           (pattern, inst).hashCode
         }).hashCode()
        patternInstHash 
    }
  
  
  def printNOUSRuntime(outString:String,batchId:Int)
  {
    println("\n**NOUS_RUN_START&bid="+batchId+"&outstring="+outString+"&NOUS_RUN_END**\n")
  }
  def main(args: Array[String]): Unit = {
    if (args.length != 1) {
      println("Usage : <configuration file path>")
      exit
    } 
    val confFilePath = args(0)
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)

    //var runtime_summary : Map[String,String] = Map.empty 
    
    val sc = SparkContextInitializer.sc

    /*
     * Read configuration parameters.
     */
    val ini = new Wini(new File(confFilePath));
    val pathOfBatchGraph = ini.get("run", "batchInfoFilePath");
    val outDir = ini.get("run", "outDir")
    val typePred = ini.get("run", "typeEdge").toInt
    val misSupport = ini.get("run", "misSupport").toInt
    val startTime = ini.get("run", "startTime")
    val dateTimeFormatPattern = ini.get("run","dateTimeFormatPattern")
    val batchSizeInTime = ini.get("run", "batchSizeInTime")
    val windowSizeInBatchs = ini.get("run", "windowSizeInBatch").toInt
    val maxIterations = log2(ini.get("run", "maxPatternSize").toInt)
    val supportScallingFactor = ini.get("run", "supportScallingFactor").toInt
    val debugId = ini.get("run", "debugId").toInt
    val frqPatternFilePath = ini.get("output", "frqPatternFilePath")
    val frqPatternPerBatchFilePath = ini.get("output", "frqPatternPerBatchFilePath")
    val depGraphFilePath = ini.get("output","depGraphFilePath")
    
    /*
     * Print all configuration variable to re-produce the experiment
     */
    println(ini.toString())
    
    /*
     * Some instrumentation variables. Should be deleted before release
     */
    var graphConstructionDebug = false
    var graphGrowthDebug = false
    if (debugId == 1)
      graphConstructionDebug = true
    if (debugId == 2)
      graphGrowthDebug = true
      
    /*
     * Initialize various global parameters.
     */
    val batchSizeInMilliSeconds = getBatchSizerInMillSeconds(batchSizeInTime)
    var currentBatchId = getBatchId(startTime, batchSizeInTime) - 1 
    // subtract one as it is increased by one as the first thing in mining loop
    var windowPatternGraph: PatternGraph = null
    var dependencyGraph: DependencyGraph = null
    
    
    /*
     * (BatchId, (PatternId, Support))
     */
    var frequentPatternInWindowPerBatch : RDD[(Int, (PatternId, Int))] = null
    
    /*
     * infrequentPatternInWinodw records global count of every Frequent pattern
     */
    var frequentPatternInWindow : RDD[(PatternId, Int)] = null
    
    
    /*
     * 
     */
    var infrequentPatternInBatch : RDD[(PatternId, Int)] = null
    /*
     * infrequentPatternInWinodw records global count of every inFrequent pattern
     */
    var infrequentPatternInWinodw : RDD[(PatternId, Int)] = null
     
    /*
     * (BatchId, (PatternId, Support))
     */
    var infrequentPatternInWinodwPerBatch : RDD[(Int, (PatternId, Int))] = null
    
    
    /*
     * Read the files/folder one-by-one and construct an input graph
     */
    println("**Before reading file, base currentBatchId is ", currentBatchId)
    breakable{
      
    val allFiles = new java.io.File(pathOfBatchGraph).listFiles.filter(_.getName.endsWith(".CSV"))
    println("total files " , allFiles.length)
    for (graphFileobj <- allFiles) {
      val graphFile = graphFileobj.getPath()
    	var t0_batch = System.nanoTime()
    	
      currentBatchId = currentBatchId + 1
      var t0 = System.nanoTime()
      val incomingDataGraph: DataGraph = ReadHugeGraph.getTemporalGraphIntGDELT(graphFile, sc, batchSizeInMilliSeconds,dateTimeFormatPattern).cache
      println("graph sie " , incomingDataGraph.vertices.count)
      if(graphConstructionDebug)
      {
        val numEdges = incomingDataGraph.edges.count
        printNOUSRuntime("# DataGraph Edges " + numEdges, currentBatchId)
        var t1 = System.nanoTime()
        printNOUSRuntime("Time to construct data graph and count number of edges " + (t1 - t0) * 1e-9 + "seconds", currentBatchId)
        
      }
      if(graphGrowthDebug) 
      {
        val numVertices = incomingDataGraph.vertices.count
        val numEdges = incomingDataGraph.edges.count
        var t1 = System.nanoTime()
        printNOUSRuntime("Data Graph Vertices " + numVertices, currentBatchId)
        printNOUSRuntime("Data Graph Edges " + numEdges, currentBatchId)
        printNOUSRuntime("Time to construct data graph and count number of edges and vertices " + (t1 - t0) * 1e-9 + "seconds", currentBatchId)

      }
      
      t0=System.nanoTime()
      val incomingPatternGraph: PatternGraph = getPatternGraph(incomingDataGraph, typePred).cache
      
      println("file done")
      if(graphConstructionDebug)
      {
        val numGIPEdges = incomingPatternGraph.edges.count
        printNOUSRuntime("# GIPGraph Edges " + numGIPEdges, currentBatchId)
        var t1 = System.nanoTime()
        printNOUSRuntime("Time to construct GIP graph and count number of edges " + (t1 - t0) * 1e-9 + "seconds" , currentBatchId)
      }
      if(graphGrowthDebug)
      {
        val numGIPVertices = incomingPatternGraph.vertices.count
        val numGIPEdges = incomingPatternGraph.edges.count
        printNOUSRuntime("GIP Graph Vertices " + numGIPVertices, currentBatchId)
        printNOUSRuntime("GIP Graph Edges " + numGIPEdges, currentBatchId)
        var t1 = System.nanoTime()
        printNOUSRuntime("Time to construct GIP graph and count number of edges and vertices " + (t1 - t0) * 1e-9 + "seconds" , currentBatchId)

      }
      
      /*
       *  Support for Sliding Window : First thing we do is to get our new Window
       *  graph that will be mined.
       *  
       *  NOTE: WE NEED TO GET THE INCREMENTAL WINDOW GRPAH. CURRENT APPROACH IS FAULTY
       *  BECUASE IT JUST MERGE EXISTING WINDOW AND NEW BATCH TO CREATE NEW WINDOW
       *  GRAPH. WHEN THE NEW WINDOW GRAPH IS MINED,IT REPEATS THE  THE PATTERN 
       *  MINING FOR ALL HISTORICAL NODES, WHICH IS WRONG
       *  
       *  ALSO, THE BATCHWISE CALCULATION OF FREQUENT AND INFREQENT PATTERS ARE 
       *  ALSO WRONG AS THEY ARE ALWAYS CUMULATIVE (TOUGHT TO SAY, THAT EVEN THAT 
       *  CALCLUCATION IS EXACT.)
       *
       *	If the windowPatternGraph is null, i.e. it is the first batch.
       *	Return the newly created incomingPatternGraph as the 'windowPatternGraph'
       * 
       * <don't see the code>The one optimization we have done here is that we have added only the 
       * frequent batchgraph into window graph.</don't see the code> 
       * 
     	 */

      if (windowPatternGraph == null) {
        windowPatternGraph = incomingPatternGraph
      } else {
        /*
		    * Remove out-of-window edges and nodes
		    */
        t0 = System.nanoTime()
        //Get valid window graph. Remove older edges
        windowPatternGraph = maintainWindow(windowPatternGraph, currentBatchId, windowSizeInBatchs)

        /*
      	 * Make a union of the incomingPatternGraph and windowPatternGraph
      	 * windowPatternGraph is the one used in mining
      	 */

        val newVertices = windowPatternGraph.vertices.union(incomingPatternGraph.vertices).cache
        val newEdges = windowPatternGraph.edges.union(incomingPatternGraph.edges).cache
        
        if(graphGrowthDebug)
        {
          var numWinNodes = windowPatternGraph.vertices.count
          var numWinEdges = windowPatternGraph.edges.count
          printNOUSRuntime("Window Graph Vertices Before Windodw Join" + numWinNodes, currentBatchId)
          printNOUSRuntime("Window Graph Edges Before Windodw Join" + numWinEdges, currentBatchId)
        }
        
        windowPatternGraph = Graph(newVertices,newEdges).cache
        
        if(graphGrowthDebug || graphConstructionDebug)
        {
          var numWinNodes = windowPatternGraph.vertices.count
          var numWinEdges = windowPatternGraph.edges.count
          printNOUSRuntime("Window Graph Vertices After Windodw Join" + numWinNodes, currentBatchId)
          printNOUSRuntime("Window Graph Edges After Windodw Join" + numWinEdges, currentBatchId)
          var t1 = System.nanoTime()
          printNOUSRuntime("Time to merge Graphs from this batch and window " + (t1 - t0) * 1e-9 + "seconds", currentBatchId)

        }
    		

      }

      /*
       * Now start the Mining
       */
      var allPatterns = computeMinImageSupport(windowPatternGraph).cache
      //allPatterns.filter(f=>f._2 > 1).collect.foreach(f=>println("patterh ", f._1, f._2))
      var frequentPatternsInIncrementalBatch = getFrequentPatterns(allPatterns, misSupport).cache
      printNOUSRuntime("all frequent pattern of size 1 " + frequentPatternsInIncrementalBatch.count, currentBatchId)
      // println("Sum of all frequent pattern of size 1", frequentPatternsInIncrementalBatch.values.sum)
      // frequentPatternsInIncrementalBatch.collect.foreach(f=> println(f._1.toList, f._2))
      /*
       * Get Updated Frequent pattern in the window.
       * NOTE : We always use window level stats to do mining
       * NOTE : Removed this step because it recordes duplicate patterns
       * in join loop.
       * We have moved this code to outside join loop, so window is updated
       * at the end of batch processing.
       * frequentPatternInWindow = updateFrequentPatternInWindow(frequentPatternInBatch,frequentPatternInWindow)
       */

      
      // Keep record of frequent pattern in this batch only
      frequentPatternInWindowPerBatch = updateFrequentInFrequentPatternsInWindowPerBatch(frequentPatternsInIncrementalBatch,
        frequentPatternInWindowPerBatch, currentBatchId)

      infrequentPatternInBatch = getInfrequentPatterns(allPatterns, misSupport)

      /*
       * infrequent pattern are not carried forwarded in the join process so it is required
       * to update the window level information about infrequent patterns at this point.
       */
      infrequentPatternInWinodw = updateInfrequentPatternInWindow(infrequentPatternInBatch, infrequentPatternInWinodw)

      /*
       * We want to know the change in the infrequent pattern per batch.
       * So the next line of  code update a data structure 
       */
      infrequentPatternInWinodwPerBatch = updateFrequentInFrequentPatternsInWindowPerBatch(infrequentPatternInBatch, infrequentPatternInWinodwPerBatch, currentBatchId)

      /*
       * Add a sampling scheme for each pattern
       * NOTE:disabled
       * val freqPatternSamplingScheme = getSamplingSchemeForFrequentPatterns(frequentPatternInBatch,supportScallingFactor, misSupport)
       */
      
      /*
       * Broadcast all the frequent patterns.
		   * assumption is that number of frequent pattern will not be HUGE
		   */
      var frequentPatternBroacdCasted: Broadcast[Array[(PatternId, Int)]] = sc.broadcast(frequentPatternsInIncrementalBatch.collect)
      
      try {
        
        /*
         * Get a graph with only edges that participate in a frequent pattern
         */
        windowPatternGraph =
          getMISFrequentGraph(windowPatternGraph, sc, frequentPatternBroacdCasted)
        
        if(graphGrowthDebug)
          {
          val numMISWinNodes = windowPatternGraph.vertices.count
          val numMISWinEdges = windowPatternGraph.edges.count
          printNOUSRuntime("MIS Window Graph Vertices " + numMISWinNodes, currentBatchId)
          printNOUSRuntime("MIS Window Graph Edges " + numMISWinEdges, currentBatchId)

        }

      } catch {
        case e: Exception => println("*** FINISHED WITHOUT FINDING BIGGER PATTERNS **")
      }

      /*
       * TODO: Mine the windowPatternGraph by pattern Join
       */
      var currentIteration = 1
      breakable {
        while (currentIteration <= maxIterations) {
          println("iteration ", currentIteration, s"finding 2^$currentIteration max-size pattern")
          currentIteration = currentIteration + 1

          //1 .join graph
          t0=System.nanoTime()
          /*
           * update pattern graph: Get +1 size pattern
           * update dep graph : get new level of tree ( i.e. new larger pattern leaf nodes)
           */
          val joinResult: (PatternGraph, DependencyGraph) = joinGraph(windowPatternGraph, dependencyGraph, 
              currentIteration,frequentPatternBroacdCasted,sc,currentBatchId)
          if(graphGrowthDebug || graphConstructionDebug)
          {
            val joinWinEdges = joinResult._1.edges.count
            var t1 = System.nanoTime()
            printNOUSRuntime("Num Edges After GIP Join" + joinWinEdges, currentBatchId)
            printNOUSRuntime("Time to join Graph with edge count" + (t1 - t0) * 1e-9 + "seconds", currentBatchId)
          }
          
          windowPatternGraph = joinResult._1
          dependencyGraph = joinResult._2

          //2. get new frequent Patterns, union them with existing patterns and broadcast
          try{
            allPatterns = computeMinImageSupport(windowPatternGraph)
          }
          catch{
            case e: Exception => println("*** computeMinImageSupport failed  **")
          }
          frequentPatternsInIncrementalBatch = getFrequentPatterns(allPatterns, misSupport).cache
          if (frequentPatternsInIncrementalBatch == null) {
            println(" ###BREAKING FROM THE LOOP")
            break
          }
          try{
          	printNOUSRuntime("all frequent pattern found with count in joins " + frequentPatternsInIncrementalBatch.count, currentBatchId)
          	//println("Sum of all frequent pattern of size 2", frequentPatternsInIncrementalBatch.values.sum)
          }catch{
            case e: Exception => println("*** frequentPatternsInIncrementalBatch count failed  **")
            
          }
          frequentPatternBroacdCasted = sc.broadcast(frequentPatternsInIncrementalBatch.collect)
          
          //if(frequentPatternBroacdCasted == null) break
          /*
         * It should be noted that only the infrequent patterns are used to udpate window level information inside this
         * loop, Not the frequent one. It is so because infrequent pattern are not carried forward in the join process,
         * they are lost after each iteration.  
         * 
         * In the contrary, the frequent patterns are always part of the GIP graph in next iteration. So the window is
         * updated at the end of last iteration.
         */
          infrequentPatternInBatch = getInfrequentPatterns(allPatterns, misSupport)
          infrequentPatternInWinodw = updateInfrequentPatternInWindow(infrequentPatternInBatch, infrequentPatternInWinodw)
          infrequentPatternInWinodwPerBatch = updateFrequentInFrequentPatternsInWindowPerBatch(infrequentPatternInBatch, infrequentPatternInWinodwPerBatch, currentBatchId)

         /*
         * update status of all patterns in the depenency graph
         */
          dependencyGraph = updateGDepStatus(dependencyGraph, sc, frequentPatternBroacdCasted)

          //Get redundant patterns
          val redundantPatterns = getRedundantPatterns(dependencyGraph)
          var redundantPatternsBroacdCasted: Broadcast[Array[(PatternId, Int)]] = sc.broadcast(redundantPatterns.collect)

          //Filter frequent pattern and get all non-redundant frequent patterns.
          val nonreduncantFrequentPattern = frequentPatternsInIncrementalBatch.subtract(redundantPatterns)
          var nonreduncantFrequentPatternBroacdCasted: Broadcast[Array[(PatternId, Int)]] = sc.broadcast(nonreduncantFrequentPattern.collect)

          //3. Get new graph
          try {
            windowPatternGraph =
              getMISFrequentGraph(windowPatternGraph, sc, nonreduncantFrequentPatternBroacdCasted)
          } catch {
            case e: Exception => println("*** FINISHED WITHOUT FINDING BIGGER PATTERNS **")
          }

          //4. trim the graph to remove orphan nodes (degree = 0)
          //windowPatternGraph = trimGraph(windowPatternGraph, sc, frequentPatternBroacdCasted)
        }
      }

      
      frequentPatternInWindow = updateFrequentPatternInWindow(frequentPatternsInIncrementalBatch, frequentPatternInWindow)
      if(graphGrowthDebug || graphConstructionDebug )
      	  {
        val newnumMISWinNodes = windowPatternGraph.vertices.count
        val newnumMISWinEdges = windowPatternGraph.edges.count
        printNOUSRuntime("MIS Window Graph Vertices At End of Batch Processing" + newnumMISWinNodes, currentBatchId)
        printNOUSRuntime("MIS Window Graph Edges At End of Batch Processing" + newnumMISWinEdges, currentBatchId)

        var t1_batch = System.nanoTime()
        printNOUSRuntime("Batch Processing Time "
          + (t1_batch - t0_batch) * 1e-9 + "seconds" + " num Edges end of batch " + newnumMISWinEdges, currentBatchId)

      }

   
    }
    
    }
   frequentPatternInWindowPerBatch.saveAsTextFile("frequentPatternInWindowPerBatch")
   frequentPatternInWindow.saveAsTextFile("frequentPatternInWindow")
  }

  def customPrintList(input : List[Any]) : String =
  {
    return input.toString.replaceAll("List", "").replaceAll("\\)\\)", ")").replaceAll("\\(\\(", "(")
  }
  def updateFrequentPatternInWindow(frequentPatternInBatch : RDD[(PatternId, Int)],
      frequentPatternInWindow : RDD[(PatternId, Int)]) : RDD[(PatternId, Int)] =
  {
    if(frequentPatternInWindow == null)
        return frequentPatternInBatch
        else
          return frequentPatternInWindow.union(frequentPatternInBatch)
          .reduceByKey((windowcount,batchcount) => windowcount + batchcount)
          
  }
  
  def updateFrequentInFrequentPatternsInWindowPerBatch(frequentPatternInBatch : RDD[(PatternId, Int)], 
      frequentPatternInWindowPerBatch : RDD[(Int, (PatternId, Int))], batchId : Int) : RDD[(Int, (PatternId, Int))] =
  {
    val patternPerBatch = frequentPatternInBatch.map(pattern=>(batchId, pattern))
    if(frequentPatternInWindowPerBatch == null)
        return patternPerBatch
        else
          return frequentPatternInWindowPerBatch.union(patternPerBatch)
     /*
      * NOTE : Not reducing the same key patterns and get all batches at one place
      * As this function will be called multiple time and we can save compute time 
      * by calculating it only when required.
      */
          
  }
  
  def updateInfrequentPatternInWindow(infrequentPatternInBatch : RDD[(PatternId, Int)],
      infrequentPatternInWinodw : RDD[(PatternId, Int)]) : RDD[(PatternId, Int)] =
  {
    if(infrequentPatternInWinodw == null)
        return  infrequentPatternInBatch
        else
          return infrequentPatternInWinodw.union(infrequentPatternInBatch).
          reduceByKey((windowcount,batchcount) => windowcount + batchcount)
      
  }
  def getSamplingSchemeForFrequentPatterns(frequentPattern : RDD[(PatternId, Int)],
      supportScallingFactor : Int, misSupport : Int ) : RDD[(PatternId, Int,Double)] =
  {
    frequentPattern.map(pattern => {
      var extraInstances: Double = 0.0
      val cutOffInstance: Int = (supportScallingFactor * misSupport)
      if (pattern._2 > supportScallingFactor * misSupport)
        extraInstances = pattern._2 - cutOffInstance

      // calculate the probability to retain that pattern edge

      (pattern._1,
        pattern._2, (1 - (extraInstances / pattern._2)))
    })
  }
  
  def getFrequentPatterns(patternsWithMIS :RDD[(PatternId, Int)],misSupport :Int) 
  	:RDD[(PatternId, Int)] =
  {
    patternsWithMIS.filter(pattern_entry 
      => pattern_entry._2 >= misSupport)
  }

  def getInfrequentPatterns(patternsWithMIS :RDD[(PatternId, Int)],misSupport :Int) 
  	:RDD[(PatternId, Int)] =
  {
    patternsWithMIS.filter(pattern_entry 
      => pattern_entry._2 < misSupport)
  }
    
  def getCommulativePatternCount(patternCountEachBatch : RDD[(Long, (PatternId,Int))],misSupport :Int):
  RDD[(PatternId,Int)] =
  {
    val commulativePatternCount = patternCountEachBatch.map(batchEntry => {
      (batchEntry._2._1, batchEntry._2._2)
    }).reduceByKey((countInBatch1, countInBatch2) => countInBatch1 + countInBatch2)
    
    return commulativePatternCount
  }
  def getRedundantPatterns(dependencyGraph:DependencyGraph) : RDD[(PatternId,Int)] =
  {
    val redundantPaterns  = 
      dependencyGraph.vertices.filter(gdepNode 
          => gdepNode._2.ptype == 2).map(depNode=>{
        (depNode._2.pattern.toList,depNode._2.support)
      })
    
      return redundantPaterns
  }
  
  def updateGDepStatus(dependencyGraph:DependencyGraph,sc:SparkContext,
    frequentPatternBC: Broadcast[Array[(PatternId,Int)]]) : DependencyGraph =
  {

      val allfrequentPatterns = frequentPatternBC.value
      val allfrequentPatternsMap = allfrequentPatterns.toMap
      val new_graph = dependencyGraph.mapVertices((id, attr) => {
        attr.support = allfrequentPatternsMap.getOrElse(attr.pattern.toList, -1)
        attr
      }).cache
      val frequentGDepGraph = new_graph.subgraph(vpred = (vid, attr) => attr.support != -1).cache

      /*
       * From Pregal Doc in http://spark.apache.org/docs/latest/graphx-programming-guide.html
       * Notice that Pregel takes two argument lists (i.e., graph.pregel(list1)(list2)). The first argument list 
       * contains configuration parameters including the initial message, the maximum number of iterations, and the edge
       * direction in which to send messages (by default along out edges). The second argument list contains the user 
       * defined functions for receiving messages (the vertex program vprog), computing messages (sendMsg), and 
       * combining messages mergeMsg.
       * 
       */
      val newGraph = frequentGDepGraph.pregel[List[Int]](List.empty,
        3, EdgeDirection.In)(
          (id, dist, newDist) =>
            {
              /*
					     * ptype:
					     * -1 : Infrequent
					     *  0 : Promising
					     *  1 : Closed
					     *  2 : Redundant
					     */
              if (!newDist.contains(2))
              {
                /*
                 * If this nodes has got NO 2s that means its support is NOT same to ANY OF
                 * children. So it is a closed node. to the type is 1
                 */
                dist.ptype = 1
              }
              else
              {
                /*
                 * Else means there is some OR ALL 2s.  
                 */
                if(!newDist.contains(1))
                {
                  /*
                   * There are ALL 2s.
                   * If this nodes has got all 2s that means its support is same to its ALL
                   * children. So it is a redundant node. to the type is 2
                   */
                	dist.ptype = 2
                }else
                {
                  /*
                   * There are some 2 and some 1 so for some children, it has same support and for some
                   * it does not have same support => it is a promising node. so the type value is 0
                   */
                  dist.ptype = 0
                }
                
              }
                
              dist
            }, // Vertex Program
          triplet => { // Send Message
            if (triplet.srcAttr.support == triplet.dstAttr.support) {
              Iterator((triplet.srcId, List(2)))
              /*
               * If all the 2s are send to source, then it means 
               * source support is always same as its destination (bigger 
               * pattern) => it is a redundant node
               */
            } else
              Iterator((triplet.srcId, List(1)))
          },
          (a, b) => a ++ b // Merge Message
          )
      newGraph
    }
  def joinGraph(windowPatternGraph: PatternGraph , dependencyGraph:DependencyGraph, currentIteration:Int,
      frequentPatternBroacdCasted: Broadcast[Array[(PatternId, Int)]],sc:SparkContext, currentBatchId: Int) :
  (PatternGraph,DependencyGraph) = 
  {
      /*
       * Get new pattern graph with +1 size pattern nodes
       */
    	val newwindowPatternGraph = getUpdateWindowPatternGraph(windowPatternGraph, dependencyGraph,currentIteration,
          frequentPatternBroacdCasted,sc, currentBatchId)
      val newDependencyGraph = getUpdatedDepGraph(windowPatternGraph, dependencyGraph)

      return (newwindowPatternGraph, newDependencyGraph)
  }
  
  def getUpdatedDepGraph(windowPatternGraph: PatternGraph , dependencyGraph:DependencyGraph)
  : DependencyGraph =
  {
    /*
     * For every edges between two patternNodes leads to 3 nodes and 2 edges
     * out of the 3 nodes, we have 2 parent nodes and one child node
     * 2 edges are the one between each parent and the child
     */
    
    val newDepNodes: RDD[(Long, DependencyNode)] =
        windowPatternGraph.triplets
          .flatMap(triple => {
            val dependencyNode = Iterable(
              (getPatternId(triple.srcAttr.getPattern), new DependencyNode(triple.srcAttr.getPattern)),
              (getPatternId(triple.dstAttr.getPattern), new DependencyNode(triple.dstAttr.getPattern)),
              (getPatternId(triple.srcAttr.getPattern ++ triple.dstAttr.getPattern),
                new DependencyNode(triple.srcAttr.getPattern ++ triple.dstAttr.getPattern)))
            dependencyNode
          }).cache

      /*
       * Create edges of Dep graph
       */
      val newDepEdges: RDD[Edge[Long]] =
        windowPatternGraph.triplets
          .flatMap(triple => {
            val dependencyEdge = Iterable(new Edge(getPatternId(triple.srcAttr.getPattern),
              getPatternId(triple.srcAttr.getPattern ++ triple.dstAttr.getPattern), 1L),
              new Edge(getPatternId(triple.dstAttr.getPattern),
                getPatternId(triple.srcAttr.getPattern ++ triple.dstAttr.getPattern), 1L))
            dependencyEdge
          }).distinct.cache
      
      /* 
       * distinct above seems redundant as the line 
       * newDependencyGraph = Graph(dependencyGraph.vertices.union(newDepNodes).distinct, 
       * dependencyGraph.edges.union(newDepEdges).distinct) 
       * does performs distinct. But for some reason TBD, that does not force distinct edges.
       */
      var newDependencyGraph : DependencyGraph = null
      if(dependencyGraph == null)
      {
      	newDependencyGraph = Graph(newDepNodes,newDepEdges)
      }else
      {   newDependencyGraph = Graph(dependencyGraph.vertices.union(newDepNodes).distinct,
        dependencyGraph.edges.union(newDepEdges).distinct)
      }  

    return newDependencyGraph
    
  }
  
  def getUpdateWindowPatternGraph(windowPatternGraph: PatternGraph , 
      dependencyGraph:DependencyGraph, currentIteration: Int,
      frequentPatternBroacdCasted: Broadcast[Array[(PatternId, Int)]],
      sc:SparkContext, currentBatchId : Int)
  : PatternGraph =
  {
       /*
			 * Input is a pattern graph. each node of existing pattern graph have a
			 * n-size pattern, every edge mean two patterns can be joined
			 * 
			 * This function follows the similar design as used in creating 1 edge 
			 * PatternGraph from DataGraph.
			 * 
			 * Vertex generation code is much simpler here.
			 * Edge Generation is almost same as getGIPEdges, but getGIPEdges creates key 
			 * as data node i.e. "Long" where as this methods creates an edge as key
			 * i.e. (Long, Long, Long)
			 */
    var olderjoin = 0
    var newjoin = 0
      println("in join : current batch id " , currentBatchId)

      
      var  t0=System.nanoTime()
      val allGIPNodes: RDD[(Long, PatternInstanceNode)] =
        windowPatternGraph.triplets.filter(triple => (triple.srcAttr.timestamp == currentBatchId 
        || triple.dstAttr.timestamp == currentBatchId) ).filter(triple=>{
        	// only join different patterns. 1/24/2018. May be not the most efficient one.
        	List.fromArray(triple.srcAttr.patternInstMap) != List.fromArray(triple.dstAttr.patternInstMap)
        })
          .map(triple => {
            // Now remove any common edge between 2 2-edge patters instances.
          	// List(P1,P2), and List(P3, P4)
          	val patList1 = triple.srcAttr.patternInstMap
          	val patList2 = triple.dstAttr.patternInstMap
          	/*
          	 * So far this is only to cover a joining to two 2-edge and 2-edges patterns as we are targetting 
          	 * 3 edge patterns max
          	 */
          	
          	val timestamp = getMinTripleTime(triple)
          	if(patList1.size == 2)
          	{
							if (patList1(0) == patList2(0)) {
								val newPatternInstanceMap = patList1 ++ patList2.drop(0)
								val pattern = (getPatternInstanceNodeid(newPatternInstanceMap),
									new PatternInstanceNode(newPatternInstanceMap, timestamp))
								pattern

							}
							else if (patList1(0) == patList2(1)) {
								val newPatternInstanceMap = patList1 ++ patList2.drop(1)
								val pattern = (getPatternInstanceNodeid(newPatternInstanceMap),
									new PatternInstanceNode(newPatternInstanceMap, timestamp))
								pattern

							}
							else if (patList1(1) == patList2(0)) {
								val newPatternInstanceMap = patList1 ++ patList2.drop(0)
								val pattern = (getPatternInstanceNodeid(newPatternInstanceMap),
									new PatternInstanceNode(newPatternInstanceMap, timestamp))
								pattern
							}
							else if (patList1(1) == patList2(1)) {
								val newPatternInstanceMap = patList1 ++ patList2.drop(1)
								val pattern = (getPatternInstanceNodeid(newPatternInstanceMap),
									new PatternInstanceNode(newPatternInstanceMap, timestamp))
								pattern
							}
							else {
								val newPatternInstanceMap = triple.srcAttr.patternInstMap ++ triple.dstAttr.patternInstMap
								val pattern = (getPatternInstanceNodeid(newPatternInstanceMap),
									new PatternInstanceNode(newPatternInstanceMap, timestamp))
								pattern
							}

						}
          	else // looking at 1-edge join (not effective)
          	{
							if (patList1(0) == patList2(0)) {
								val newPatternInstanceMap = patList1 ++ patList2.drop(0)
								val pattern = (getPatternInstanceNodeid(newPatternInstanceMap),
									new PatternInstanceNode(newPatternInstanceMap, timestamp))
								pattern
							} else {
								val newPatternInstanceMap = triple.srcAttr.patternInstMap ++ triple.dstAttr.patternInstMap
							val pattern = (getPatternInstanceNodeid(newPatternInstanceMap),
								new PatternInstanceNode(newPatternInstanceMap, timestamp))
							pattern
							}
          		
						}
          		
          	
          }).cache
      var t1=System.nanoTime()
      //println("\nNOUS: GIP Join Node  Construction with count ", ((t1 - t0) * 1e-9 + "seconds"))

      
      /*
     * Create Edges of the GIP
     * 
     * We try to join the nodes based on each common edge in samller graph.
     * (P1 (sp,wrkAt,pnnl)) and (P2 (pnnl localtedin Richland)):
     * we create RDD where the key is an edge ex: (sp,wrkAt,pnnl) and value is
     * the pattern i.e. P1, P2, or (P1P2)
     * After the "groupBy" on that key, we create edges between every pair of the
     * pattern. such as ((sp,wrkAt,pnnl) , Iterable(P1, P2, P1P2))
     */
      val allPatternIdsPerInstanceEdge = allGIPNodes.flatMap(patterVertex => {
        patterVertex._2.getInstance.flatMap(patternInstanceEdge => {
          Iterable((patternInstanceEdge, patterVertex._1))
        })
      }).groupByKey()

      /*
       * create edges btweeen two nodes if there is a common instance edges
       * IF ((sp,wrkAt,pnnl) , Iterable(P1, P2)) then an edge between 
       * P1 and P2
       */
      val gipEdges = allPatternIdsPerInstanceEdge.flatMap(gipNode => {
        val edgeList = gipNode._2.toList
        val patternGraphVertexId = gipNode._1
        val edgeLimit = 4 // We only wants 2 edges from each possible pair at each node
        /*
         * If ((sp,wrkAt,pnnl) , Iterable(P1, P2, P3, P4))
         * then instead of creating 3 edges for P1 (i.e. P1P2, P1P2, P1P4)
         * we only create 2
         */
        var local_edges: scala.collection.mutable.ArrayBuffer[Edge[Long]] =
          scala.collection.mutable.ArrayBuffer()
        for (i <- 0 to (edgeList.size - 2)) {
          var edgeCnt = 0
          for (j <- i + 1 to (edgeList.size - 1)) {
            if(edgeCnt < edgeLimit)
            {
            	local_edges += Edge(edgeList(i), edgeList(j), 1L)
            	edgeCnt = edgeCnt + 1
            }
            
            //we put 1L as the edge type. This means, we dont have any data on the 
            // edge. Only information is that 
          }
        }
        local_edges.toList
      }).cache

      
      
      val existingGIPNodes = windowPatternGraph.vertices.cache
      val existingGIPEdges = windowPatternGraph.edges.cache
      
      /*
       * Newly created Nodes and Edges are already cached
       * NOTE: removing call to distinct
       */
      //print("existingGIPEdes count = " + existingGIPEdges.count)
      
      val allnewNodes = existingGIPNodes.union(allGIPNodes).cache
      val allnewEdges = existingGIPEdges.union(gipEdges).cache
      val newwindowPatternGraph = Graph(allnewNodes, allnewEdges)
      
      return newwindowPatternGraph  
  }
  
  /*
   * This method takes all GOPNodes as input.
   * It computes number of patterns associated with each vertex.
   * Then it get a normalized number for each vertex.
   * 
   * It joins (node, GIP) and (node, normalized_diversity) to get
   * (normalized_diversity,(GIP_ID, GIP))
   */
  
  def getStratifiedSample_Diversity(allGIPNodes: RDD[(Long, PatternInstanceNode)],
      frequentPatternBroacdCasted: Broadcast[RDD[(PatternId, Int)]],
      sc:SparkContext) : RDD[(Long, PatternInstanceNode)] =
      {
      //vertexPatternSet is like ((sp, Iterable(id(P1), id(P2)) , (sc, Iterable(id(P2), id(P3)) ))
      val vertexPatternSet = allGIPNodes.flatMap(patterVertex => {
        patterVertex._2.getAllNodesInPatternInstance.map(vertex => {
          (vertex, patterVertex._1)
        })
      }).groupByKey()

      val vertexNumberOfPatterns = vertexPatternSet.map(vertexPatternSet => (vertexPatternSet._1, vertexPatternSet._2.size))

      val maxPatternAtAnyVertex = vertexNumberOfPatterns.values.max

      //(id(sp), normalized_number_of_patterns)
      val vertexNumberOfPatternsNormalized = vertexNumberOfPatterns.map(vertex => (vertex._1, vertex._2.toDouble / maxPatternAtAnyVertex))

      //vertexGIPSet is like ( (sp, Iterable((id(gip1), gip1), (id(gip1), gip1))) 
      //    , (sc, Iterable((id(gip1), gip1), (id(gip1), gip1)) ))
      // vertexGIPSet is an RDD (vetex_id,(gipnode_id,gipnode_object))
      val vertexGIPSet = allGIPNodes.flatMap(patterVertex => {
        patterVertex._2.getAllNodesInPatternInstance.map(vertex => {
          (vertex, (patterVertex._1, patterVertex._2))
        })
      })

      // vertexGIPSet is an RDD (vetex_id,(gipnode_id,gipnode_object))
      // vertexNumberOfPatternsNormalized is an RDD (vertex_id, normalized_diversity)
      // gipNodesWithDiversityRaw is and RDD (vertex_id,( (gipnode_id,gipnode_object), normalized_diversity))
      val vertexNodesWithDiversityRaw = vertexGIPSet.leftOuterJoin(vertexNumberOfPatternsNormalized)
      
      // gipNodesWithDiversity is an RDD ( normalized_diversity_bin, (vertex_id,(gipnode_id,gipnode_object)))
      val vertexNodesWithDiversity = vertexNodesWithDiversityRaw.map(gipNode 
          => (scala.math.floor(gipNode._2._2.getOrElse(0.0) * 10).toInt, (gipNode._1, gipNode._2._1)))

      // gipNodesWithDiversity is an RDD ( GIPNODE_ID, (GIPNODE_ID, GIPNODE), Max(diversity)
      val gipNodesWithDiversity = vertexNodesWithDiversity.map(vertexDiversityRaw=>(vertexDiversityRaw._2._2._1,
          (vertexDiversityRaw._2._2,vertexDiversityRaw._1))).reduceByKey((a,b)=>(a._1,scala.math.max(a._2, b._2)))  
          
      val gipNodesWithDiversityAsKey = gipNodesWithDiversity.map(gipnode=>(gipnode._2._2,gipnode._2._1))    
      // specify the exact fraction desired from each key
      val fractions = Map(0 -> 0.0, 1 -> 0.05, 2 -> 0.05, 3 -> 0.1, 4 -> .2, 5 -> .2, 6 -> .2, 7 -> .075, 8 -> .075, 9 -> .05)

      // sampledAllVertexWithDiversity is an RDD (GIPNODE_ID, GIPNODE)
      val sampledAllVertexWithDiversity = gipNodesWithDiversityAsKey.sampleByKey(withReplacement = false, fractions = fractions).values
      
      return sampledAllVertexWithDiversity
    }
  
  def getStratifiedSample_Support(allGIPNodes: RDD[(Long, PatternInstanceNode)],
      frequentPatternBroacdCasted: Broadcast[RDD[(PatternId, Int)]],
      sc:SparkContext) : RDD[(Long, PatternInstanceNode)] =
  {
    /*
       * Perform Statified Sampling Based on Support value Only
       */
      val frequentPatterns = frequentPatternBroacdCasted.value
      val maxIntances = frequentPatterns.map(fp=>fp._2).max
      
      // (PatternId, normalized_support)
      // normalized_support for a pattern is support/Max_Support
      val strataFrequentPatterns : RDD[(PatternId,Double)]= frequentPatterns.map(freq_pattern 
          => (freq_pattern._1,((freq_pattern._2.toDouble/maxIntances))))
      
      // BoradCast it
      val strataFrequentPatternsBC = sc.broadcast(strataFrequentPatterns.collectAsMap)
      
      // Collect it at each node
      val localStrataFrequentPatterns = strataFrequentPatternsBC.value
      
      // Get all GIP node with a binned key between 0 to 10
      val strataAllGIPNodes = allGIPNodes.map(gip_node => {
        val fraction = scala.math.floor(localStrataFrequentPatterns.getOrElse(gip_node._2.getPattern.toList, 0.0) * 10 )
        (fraction.toInt,gip_node)
      })
      // specify the exact fraction desired from each key
      val fractions  = Map(0 -> 0.0, 1 -> 0.05, 2 -> 0.05, 3 -> 0.1, 4->.2, 5-> .2, 6-> .2, 7 -> .075, 8->.075, 9 -> .05 )
      
      val sampledAllGIPNodes = strataAllGIPNodes.sampleByKey(withReplacement = false, fractions = fractions).values
      
      return sampledAllGIPNodes
  }
  
  
  def getMinTripleTime(triple:EdgeTriplet[PatternInstanceNode, DataGraphNodeId]) : Long =
  {
    /*
     * Not using scala.math.min as it makes it a heavy call
     */
    val srcTime = triple.srcAttr.timestamp
    val dstTime = triple.dstAttr.timestamp
    if(srcTime < dstTime)
      return dstTime
    return srcTime  
  }
  
  def getBatchId(startTime : String, batchSizeInTime : String) : Int =
  {
    val startTimeMilliSeconds = getStartTimeInMillSeconds(startTime)
    val batchSizeInTimeIntMilliSeconds = getBatchSizerInMillSeconds(batchSizeInTime)
    return (startTimeMilliSeconds / batchSizeInTimeIntMilliSeconds).toInt
  }
  
  def getBatchSizerInMillSeconds(batchSize : String) : Long =
  {
    val MSecondsInYear = 31556952000L
    val MSecondsInDay = 86400000L
    if(batchSize.endsWith("y"))
    {
      val batchSizeInYear : Int = batchSize.replaceAll("y", "").toInt
      return batchSizeInYear * MSecondsInYear
    }else if(batchSize.endsWith("d"))
    {
      val batchSizeInDay : Int = batchSize.replaceAll("d", "").toInt
      return batchSizeInDay * MSecondsInDay
    }
    return MSecondsInYear
  }
  
  def getStartTimeInMillSeconds(startTime:String) : Long = 
  {
    //val startTimeString = startTime + "/01/01 00:00:00.000"
    val f = DateTimeFormat.forPattern("yyyy/MM/dd HH:mm:ss.SSS");
    val dateTime = f.parseDateTime(startTime);
    return dateTime.getMillis()
  }
  
  def getPatternGraph(dataGraph: DataGraph, typePred: Int): PatternGraph = {
    
    /*
     * Get initial typed graph
     */
    var t0=System.nanoTime()
    val SUPPORT = 3
    val typedGraph: Graph[LabelWithTypes, KGEdgeInt] = getTypedGraph(dataGraph, typePred)
    
   

    val numTypeGEdges = typedGraph.edges.count
    var t1=System.nanoTime()
    /*
     * Get nodes in the GIP
     * Using those nodes, create edges in GIP
     */
    val gipVertices : RDD[(Long,PatternInstanceNode)] = getGIPVerticesNoMap(typedGraph, typePred).cache
    val gipEdge : RDD[Edge[Long]] = getGIPEdges(gipVertices).cache
    //gipEdge.collect.foreach(e=>println(e.srcId, e.attr, e.dstId))
    val result = Graph(gipVertices, gipEdge).cache
    return result
    
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
     * (0) is used because the Instance has only 1 edge instance
     */
     var t0 = System.nanoTime()
      val gipPatternIdPerDataNode = gip_vertices.flatMap(patterVertex =>{
        Iterable((patterVertex._2.getAllSourceInstances(0), Iterable(patterVertex._1)),
          (patterVertex._2.getAllDestinationInstances(0), Iterable(patterVertex._1))
      )}
        ).reduceByKey((lop1,lop2)=>lop1++lop2)
     // lop is list of patternVertex
        // removed GroupBy as that is inefficient.
      //println("\nNOUS: size of gipPatternIdPerDataNode",gipPatternIdPerDataNode.count )
      var t1=System.nanoTime()
      //println("\nNOUS: time to get gipPatternIdPerDataNode with count", ((t1 - t0) * 1e-9 + "seconds"))
      
//      val numberGIPPatternPerDataNode = gipPatternIdPerDataNode.map(entry=>(entry._1, entry._2.size))
//      numberGIPPatternPerDataNode.saveAsTextFile("numberGIPPatternPerDataNode")
      
      
/*        val totalEdges = gipPatternIdPerDataNode.flatMap(gipNode => {
        val edgeList = gipNode._2.toList
        val dataGraphVertexId = gipNode._1
        //var local_edges: scala.collection.mutable.ArrayBuffer[Edge[Long]] = scala.collection.mutable.ArrayBuffer()
        var localedge_count = 0;
        val edgeLimit = 2 // We only wants 2 edges from each possible pair at each node
        for (i <- 0 to (edgeList.size - 2)) {
           var edgeCnt = 0
          for (j <- i + 1 to (edgeList.size - 1)) {
            if(edgeCnt < edgeLimit)
            {
              //local_edges += Edge(edgeList(i), edgeList(j), dataGraphVertexId)
              edgeCnt = edgeCnt + 1
              localedge_count = localedge_count + 1
            }
            	
          }
        }
        Iterable(localedge_count)
      }).sum
        
      println("total sum is ", totalEdges)*/
      t0=System.nanoTime()
      val gipEdges = gipPatternIdPerDataNode.flatMap(gipNode => {
        val edgeList = gipNode._2.toList
        val dataGraphVertexId = gipNode._1
        var local_edges: scala.collection.mutable.ArrayBuffer[Edge[Long]] = scala.collection.mutable.ArrayBuffer()
        val edgeLimit = 10 // We only wants 2 edges from each possible pair at each node
        for (i <- 0 to (edgeList.size - 2)) {
           var edgeCnt = 0
          for (j <- i + 1 to (edgeList.size - 1)) {
            if(edgeCnt < edgeLimit)
            {
              local_edges += Edge(edgeList(i), edgeList(j), dataGraphVertexId)
              edgeCnt = edgeCnt + 1
            }
            	
          }
        }
        local_edges
      })
      return gipEdges
    }
  
   def getGIPVerticesNoMap(typedAugmentedGraph: Graph[LabelWithTypes, KGEdgeInt], typePred:Int ) :
 RDD[(Long,PatternInstanceNode)] =
 {
      /*
     * Create GIP from this graph
     * 
     * Every node has following structure:
     * (Long,Array[(SinglePatternEdge, SingleInstanceEdge)]
     *
     */
     /*
      * validTriples are the triples without any 'type' edge, and also without
      * any edge where either the source or destination has pattern to 
      * contribute 
      */
      val validTriples = typedAugmentedGraph.triplets.filter(triple 
          => (triple.attr.getlabel != typePred) &&
          	(triple.srcAttr._2.size > 0) &&
          	(triple.dstAttr._2.size > 0)
          )

      val allGIPNodes: RDD[(Long, PatternInstanceNode)] =
        validTriples
          .map(triple => {

            //Local Execution on a triple edge; but needs source and destination vertices
            val source_node = triple.srcAttr
            val destination_node = triple.dstAttr
            val pred_type = triple.attr.getlabel
            val src_type = source_node._2.head //because, it is only 1 edge
            val dst_type = destination_node._2.head //because, it is only 1 edge

            /*
             * Construct a 1-size array of (pattern, instance)
             * 1-edge patter is always in DSF laxicographic ordering.
             */
            val singlePatternEdge: SinglePatternEdge = (src_type, pred_type, dst_type)
            val singleInstacneEdge: SingleInstanceEdge = (source_node._1, pred_type.toLong,
              destination_node._1)
            val patternInstanceMap: Array[(SinglePatternEdge, SingleInstanceEdge)] =
              Array((singlePatternEdge, singleInstacneEdge))
            val timestamp = triple.attr.getdatetime

            val pattern = (getPatternInstanceNodeid(patternInstanceMap),
              new PatternInstanceNode(patternInstanceMap, timestamp))
            pattern
          })
      return allGIPNodes
    }
  
  
   def getTypedGraph(g: DataGraph, typePred: Int): Graph[LabelWithTypes, KGEdgeInt] =
   {
      // Get Node Types
      val typedVertexRDD: VertexRDD[List[Int]] = g.aggregateMessages[List[Int]](edge => {
        if (edge.attr.getlabel == (typePred))
          edge.sendToSrc(List(edge.dstAttr))
      },
        (a, b) => a ++ b)
      // Join Node Original Data With NodeType Data
      g.outerJoinVertices(typedVertexRDD) {
        case (id, label, Some(nbr)) => (label, nbr)
        case (id, label, None) => (label, List.empty[Int])
      }
    }
   
   
def maintainWindow(input_gpi: PatternGraph, currentBatchId : Long, windowSizeInBatchs : Int) : PatternGraph =
	{
		
  
  val cutOffBatchId = currentBatchId - windowSizeInBatchs
  println("****In maintainWindow *****, cutOffBatchId is ", cutOffBatchId, " current batch id is ", currentBatchId, " and windowsize in batch is ", windowSizeInBatchs)
		  return input_gpi.subgraph(vpred = (vid,attr) => {
		  (attr.timestamp > cutOffBatchId) || (attr.timestamp == -1L)
		})
	}
 
def log2(x: Double) = scala.math.log(x)/scala.math.log(2)

/**
 * 
 */
def trimGraph(patternGraph: PatternGraph,sc:SparkContext, 
    frequentPatternBC: Broadcast[RDD[(Pattern,Int)]]) : PatternGraph = 
{
  /*
   * Now remove all the nodes with zero degree
   */
  val nonzeroVertices = patternGraph.degrees.filter(v=>v._2 > 0)
  // Join Node Original Data With NodeType Data
  val nonzeroVGraph  = patternGraph.outerJoinVertices(nonzeroVertices) {
    case (id, label, Some(nbr)) => (label)
    case (id, label, None) => (null)
  }
  return nonzeroVGraph.subgraph(vpred= (vid,attr) => attr!=null)
  
}

def getMISFrequentGraph(patternGraph: PatternGraph,sc:SparkContext,
    frequentPatternBC: Broadcast[Array[(PatternId,Int)]] ) : PatternGraph =
{
  //If next line is inside subgraph method, it hangs.
	val allfrequentPatterns = frequentPatternBC.value
	
	val allfrequentPatternsArray : Array[(PatternId)]= allfrequentPatterns.map(pattern 
	    => (pattern._1))
  val frequentGraph = patternGraph.subgraph(vpred = (vid,attr) => {
    /*
     * As Arrays is not a stable DS for key comparison so compare it element by element
     * NOTE : do i need to look all the pattern everytime ?
     */
		 allfrequentPatternsArray.map(pattern
		     =>{
		       /*
		        * Probabilistically pick the subgraph using two conditions
		        * 1. the vertex belongs to a frequent pattern
		        * 2. IF it belongs to a frequent pattern, it should be pick up
		        *    probabilistically based on its cutOff limit
		        */
		       
		       var keepEdge = pattern.sameElements(attr.getPattern) 
		       if(Random.nextDouble <= 1) //TMP: 1 value.
		         keepEdge = keepEdge && true
		         else keepEdge = keepEdge && false
		       keepEdge
		     }).reduce((ans1,ans2)=>ans1 || ans2)

		})
	return frequentGraph
}
  def computeMinImageSupport(input_gpi : PatternGraph)
	  :RDD[(PatternId, Int)] =
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
        vertex._2.patternInstMap.flatMap(pattern_instance_pair => {
          Iterable((vertex._2.getPattern.toList, pattern_instance_pair._1._1, pattern_instance_pair._2._1),
              (vertex._2.getPattern.toList, pattern_instance_pair._1._3, pattern_instance_pair._2._3))
        })
      }).distinct
      val mis_rdd = sub_pattern_key_rdd.map(key=>{
        ((key._1, key._2),1)
         /*
         * ((P1,person) , 1) from (P1,person,sp)
         * ((P1,person) , 1) from (P1,person,sc)
         * ((P1,org) , 1) from (P1,org,pnnl)
         * 
         */

      })
      .reduceByKey((unique_instance1_count,unique_instance2_count) 
          => unique_instance1_count + unique_instance2_count)
          /*
     * Input is 'mis_rdd' which is a Cumulative RDD like:
     * P1:person, 2
     * P1:org, 1
     * 
     * Output is patternSup which gets minimum of all P1:x 
     * so return (P1, 1)
     * 
     * Exception in thread "main" org.apache.spark.SparkException: 
     * Cannot use map-side combining with array keys.
     * Reason: Scala Array is just a wrapper around Java array and its hashCode doesn't depend on a content:
     */
      val patternSup  : RDD[(List[SinglePatternEdge], Int)] = mis_rdd.map(sup_pattern_key => {
        //Emit (P1, 2) and (P1 1)
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

      /*
       * TODO : Change it to List in the data structure because Array can not be used as a key.
       * We can save the array-list-array work 
       */
      return patternSup.map(entry=>(entry._1,entry._2))
    }
  
}