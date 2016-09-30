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
  class DependencyNode(val pattern: Pattern) extends Serializable
  {
    val id: Long  = getPatternId(pattern)
    var ptype: PType = 0
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
  
  def main(args: Array[String]): Unit = {
    if (args.length != 1) {
      println("Usage : <configuration file path>")
      exit
    } 
    val confFilePath = args(0)
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)

    val sc = SparkContextInitializer.sc

    val ini = new Wini(new File(confFilePath));
    val pathOfBatchGraph = ini.get("run", "batchInfoFilePath");
    val outDir = ini.get("run", "outDir")
    val typePred = ini.get("run", "typeEdge").toInt
    val isoSupport = ini.get("run", "isoSupport").toInt
    val misSupport = ini.get("run", "misSupport").toInt
    val startTime = ini.get("run", "startTime").toInt
    val batchSizeInTime = ini.get("run", "batchSizeInTime")
    val windowSizeInBatchs = ini.get("run", "windowSizeInBatch").toInt
    val maxIterations = log2(ini.get("run", "maxPatternSize").toInt)
    
    
    val batchSizeInMilliSeconds = getBatchSizerInMillSeconds(batchSizeInTime)
    var currentBatchId = getBatchId(startTime, batchSizeInTime)
    var windowPatternGraph: PatternGraph = null
    var dependencyGraph: DependencyGraph = null
    /*
     * Read the files/folder one-by-one and construct an input graph
     */
    for (graphFile <- Source.fromFile(pathOfBatchGraph).
        getLines().filter(str => !str.startsWith("#"))) {
    	
      currentBatchId = currentBatchId + 1
      
      val incomingDataGraph: DataGraph = ReadHugeGraph.getGraphFileTypeInt(graphFile, sc, batchSizeInMilliSeconds)
      val incomingPatternGraph: PatternGraph = getPatternGraph(incomingDataGraph,typePred)//getMISFrequentGraph(,sc,misSupport)
      
      var frequentPattern = getFrequentPatterns(computeMinImageSupport(incomingPatternGraph),misSupport)
      println("frequent  pattern count",frequentPattern.count)
		  /*
		   * assumption is that number of frequent pattern will not be HUGE
		   */
		  var frequentPatternBroacdCasted : Broadcast[RDD[(PatternId,Int)]] = sc.broadcast(frequentPattern)
		  var misFrequentIncomingPatternGraph = 
		    getMISFrequentGraph(incomingPatternGraph,sc,frequentPatternBroacdCasted,null)
      
      /*
       *  Support for Sliding Window
       *
       *	If the windowPatternGraph is null, i.e. it is the first batch.
       *	Return the newly created incomingPatternGraph as the 'windowPatternGraph'
       * 
     	 */
     
		   if(windowPatternGraph == null)
		   {
		      windowPatternGraph = misFrequentIncomingPatternGraph
		   }
		
		   /*
		    * Remove out-of-window edges and nodes
		    */
       windowPatternGraph = maintainWindow(windowPatternGraph, currentBatchId, windowSizeInBatchs)
		    
      	
      	/*
      	 * Make a union of the incomingPatternGraph and windowPatternGraph
      	 * windowPatternGraph is the one used in mining
      	 */
      	
		    windowPatternGraph =  Graph(windowPatternGraph.vertices.union(misFrequentIncomingPatternGraph.vertices).distinct,
		        windowPatternGraph.edges.union(misFrequentIncomingPatternGraph.edges).distinct)

		   /*
       * TODO: Mine the windowPatternGraph by pattern Join
       */
      var currentIteration = 1
      while(currentIteration <= maxIterations)
      {
        println("iteration " , currentIteration, s"finding 2^$currentIteration max-size pattern")
        currentIteration = currentIteration + 1
        //1 .join graph
        val joinResult : (PatternGraph,DependencyGraph) = joinGraph(windowPatternGraph,dependencyGraph)
        windowPatternGraph = joinResult._1
        dependencyGraph = joinResult._2
        
        
        //2. get new frequent Patterns, union them with existing patterns and broadcast
        frequentPattern = frequentPattern.union(getFrequentPatterns(computeMinImageSupport(windowPatternGraph),misSupport)).distinct.cache
        var frequentPatternBroacdCasted : Broadcast[RDD[(PatternId,Int)]] = sc.broadcast(frequentPattern)
        
        // update status of all patterns
        dependencyGraph = updateGDepStatus(dependencyGraph,sc,frequentPatternBroacdCasted)
        
        //Get redundant patterns
        val redundantPatterns = getRedundantPatterns(dependencyGraph)
        var redundantPatternsBroacdCasted : Broadcast[RDD[(PatternId,Int)]] = sc.broadcast(redundantPatterns)
        //Filter frequent pattern and get all non-redundant frequent patterns.
        val nonreduncantFrequentPattern = frequentPattern.subtract(redundantPatterns)
        var nonreduncantFrequentPatternBroacdCasted : Broadcast[RDD[(PatternId,Int)]] = sc.broadcast(nonreduncantFrequentPattern)
        
        
        //3. Get new graph
        windowPatternGraph = 
		    getMISFrequentGraph(windowPatternGraph,sc,nonreduncantFrequentPatternBroacdCasted,redundantPatterns)
        
		    //4. trim the graph to remove orphan nodes (degree = 0)
		    //windowPatternGraph = trimGraph(windowPatternGraph, sc, frequentPatternBroacdCasted)
      }
      
      

      
      /*
       * Save batch level patternGraph
       *  
       
      frequentPattern.map(entry
          =>{
        var pattern = ""
        entry._1.foreach(f => pattern = pattern + f._1 + "\t" + f._2 + "\t" + f._3 + "\t")
        pattern = pattern + entry._2
        pattern
      }).saveAsTextFile(outDir + "/" + "GIP/frequentPattern" 
                  + System.nanoTime())
    
    
        //dependencyGraph.vertices.filter(v=>v._2!=null).collect.foreach(f=>println(f._1, f._2.pattern.toList))
        //dependencyGraph.edges.collect.foreach(e=>println(e.srcId, e.attr, e.dstId))
        dependencyGraph.triplets.map(e=>
          e.srcAttr.pattern.toList +"#"+e.attr+"#"+e.dstAttr.pattern.toList+"#"+
          e.srcAttr.ptype+"#"+e.dstAttr.ptype+"#"+e.srcAttr.support+"#"+e.dstAttr.support)
          .saveAsTextFile("DepG"+System.nanoTime()+"/")
          * 
          */
    }
    
  }

  def getFrequentPatterns(patternsWithMIS :RDD[(PatternId, Int)],misSupport :Int) 
  	:RDD[(PatternId, Int)] =
  {
    patternsWithMIS.filter(pattern_entry 
      => pattern_entry._2 >= misSupport)
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
    frequentPatternBC: Broadcast[RDD[(PatternId,Int)]]) : DependencyGraph =
  {
    //dependencyGraph.vertices.collect.foreach(f=>println("dep graph v : " , f._1, f._2.pattern.toList, f._2.ptype, f._2.support))
    
    val allfrequentPatterns = frequentPatternBC.value.collect
    val allfrequentPatternsMap = allfrequentPatterns.toMap
    val new_graph = dependencyGraph.mapVertices((id, attr) => {
      attr.support = allfrequentPatternsMap.getOrElse(attr.pattern.toList, -1)
      attr
    })
    val frequentGDepGraph = new_graph.subgraph(vpred = (vid, attr)=>attr.support != -1)

    val newGraph 
        = frequentGDepGraph.pregel[List[Int]](List.empty,
        		3, EdgeDirection.In)(
          (id, dist, newDist) =>
            {
              if (!newDist.contains(2))
                dist.ptype = 1
              else
                dist.ptype = 2
              dist
            }, // Vertex Program
          triplet => { // Send Message
            if (triplet.srcAttr.support == triplet.dstAttr.support) {
              Iterator((triplet.srcId, List(2)))
            } else
              Iterator((triplet.srcId, List(1)))
            //val newprop = triplet.dstAttr.getnode_label

          },
          (a, b) => a ++ b // Merge Message
          )
     
//       newGraph.vertices.collect.foreach(f
//        =>println("newGraph with ptype dep graph v : " , 
//            f._1, f._2.pattern.toList, f._2.ptype, f._2.support))

    newGraph
  }
  def joinGraph(windowPatternGraph: PatternGraph , dependencyGraph:DependencyGraph) :
  (PatternGraph,DependencyGraph) = 
  {
      val newwindowPatternGraph = getUpdateWindowPatternGraph(windowPatternGraph, dependencyGraph)
      val newDependencyGraph = getUpdatedDepGraph(windowPatternGraph, dependencyGraph)

      return (newwindowPatternGraph, newDependencyGraph)
  }
  
  def getUpdatedDepGraph(windowPatternGraph: PatternGraph , dependencyGraph:DependencyGraph)
  : DependencyGraph =
  {
    /*
     * For every edges between two patternNodes leads to 3 nodes and 2 edges
     * 3 nodes have 2 parent nodes and one child node
     * 2 edges are an edge between each parent and the child
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
  
  def getUpdateWindowPatternGraph(windowPatternGraph: PatternGraph , dependencyGraph:DependencyGraph)
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
 * as data node i.e. "Long" where as this methods creats an edge as key
 * i.e. (Long, Long, Long)
 */
      println("in join")
      val allGIPNodes: RDD[(Long, PatternInstanceNode)] =
        windowPatternGraph.triplets
          .map(triple => {
            val newPatternInstanceMap = triple.srcAttr.patternInstMap ++ triple.dstAttr.patternInstMap
            val timestamp = getMinTripleTime(triple)
            val pattern = (getPatternInstanceNodeid(newPatternInstanceMap),
              new PatternInstanceNode(newPatternInstanceMap, timestamp))
            val dependencyEdge: Edge[Long] = new Edge(getPatternId(triple.srcAttr.getPattern),
              getPatternId(triple.dstAttr.getPattern), getPatternInstanceNodeid(newPatternInstanceMap))
            pattern
          }).cache

      
      /*
     * Create Edges of the GIP
     * 
     * We try to join the nodes based on each common edge in samller graph.
     * (P1 (sp,wrkAt,pnnl)) and (P2 (pnnl localtedin Richland)):
     * we create RDD where the key is and edge ex: (sp,wrkAt,pnnl) and value is
     * the pattern i.e. P1, P2, or (P1P2)
     * After the "groupBy" on that key, we create edges between every pair of the
     * patter.
     */
      val allPatternIdsPerInstanceEdge = allGIPNodes.flatMap(patterVertex => {
        patterVertex._2.getInstance.flatMap(patternInstanceEdge => {
          Iterable((patternInstanceEdge, patterVertex._1))
        })
      }).groupByKey()

      val gipEdges = allPatternIdsPerInstanceEdge.flatMap(gipNode => {
        val edgeList = gipNode._2.toList
        val patternGraphVertexId = gipNode._1
        var local_edges: scala.collection.mutable.ArrayBuffer[Edge[Long]] =
          scala.collection.mutable.ArrayBuffer()
        for (i <- 0 to (edgeList.size - 2)) {
          for (j <- i + 1 to (edgeList.size - 1)) {
            local_edges += Edge(edgeList(i), edgeList(j), 1L)
            //we put 1L as the edge type. This means, we dont have any data on the 
            // edge. Only information is that 
          }
        }
        local_edges
      }).cache

      val existingGIPNodes = windowPatternGraph.vertices.cache
      val existingGIPEdges = windowPatternGraph.edges.cache

      /*
       * Newly created Nodes and Edges are already cached
       * NOTE: removing call to distinct
       */
      val newwindowPatternGraph = Graph(existingGIPNodes.union(allGIPNodes),
        existingGIPEdges.union(gipEdges))
        
      return newwindowPatternGraph  
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
  
  def getBatchId(startTime : Int, batchSizeInTime : String) : Long =
  {
    val startTimeMilliSeconds = getStartTimeInMillSeconds(startTime)
    val batchSizeInTimeIntMilliSeconds = getBatchSizerInMillSeconds(batchSizeInTime)
    return (startTimeMilliSeconds / batchSizeInTimeIntMilliSeconds)
  }
  
  def getBatchSizerInMillSeconds(batchSize : String) : Long =
  {
    val MSecondsInYear = 31556952000L
    if(batchSize.endsWith("y"))
    {
      val batchSizeInYear : Int = batchSize.replaceAll("y", "").toInt
      return batchSizeInYear * MSecondsInYear
    }
    return MSecondsInYear
  }
  
  def getStartTimeInMillSeconds(startTime:Int) : Long = 
  {
    val startTimeString = startTime + "/01/01 00:00:00.000"
    val f = DateTimeFormat.forPattern("yyyy/MM/dd HH:mm:ss.SSS");
    val dateTime = f.parseDateTime(startTimeString);
    return dateTime.getMillis()
  }
  
  def getPatternGraph(dataGraph: DataGraph, typePred: Int): PatternGraph = {
    
    /*
     * Get initial typed graph
     */
    val typedGraph: Graph[LabelWithTypes, KGEdgeInt] = getTypedGraph(dataGraph, typePred)

    /*
     * Get nodes in the GIP
     * Using those nodes, create edges in GIP
     */
    val gipVertices : RDD[(Long,PatternInstanceNode)] = getGIPVerticesNoMap(typedGraph, typePred).cache
    val gipEdge : RDD[Edge[Long]] = getGIPEdges(gipVertices)
    
    return Graph(gipVertices, gipEdge)
    
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
      val gipPatternIdPerDataNode = gip_vertices.flatMap(patterVertex =>{
        Iterable((patterVertex._2.getAllSourceInstances(0), patterVertex._1),
          (patterVertex._2.getAllDestinationInstances(0), patterVertex._1)
      )}
      
        ).groupByKey()
      
      val gipEdges = gipPatternIdPerDataNode.flatMap(gipNode => {
        val edgeList = gipNode._2.toList
        val dataGraphVertexId = gipNode._1
        var local_edges: scala.collection.mutable.ArrayBuffer[Edge[Long]] = scala.collection.mutable.ArrayBuffer()
        for (i <- 0 to (edgeList.size - 2)) {
          for (j <- i + 1 to (edgeList.size - 1)) {
            local_edges += Edge(edgeList(i), edgeList(j), dataGraphVertexId)
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
    frequentPatternBC: Broadcast[RDD[(PatternId,Int)]],
    redundantPatterns:RDD[(PatternId,Int)] = null) : PatternGraph =
{
  //If next line is inside subgraph method, it hangs.
	val allfrequentPatterns = frequentPatternBC.value.collect
	println("*****size of frquent patterns " , allfrequentPatterns.size)
	allfrequentPatterns.foreach(f=>println("pattern ins " , f._1, f._2))
	val allfrequentPatternsArray : Array[PatternId]= allfrequentPatterns.map(_._1)
  val frequentGraph = patternGraph.subgraph(vpred = (vid,attr) => {
    /*
     * As Arrays is not a stable DS for key comparison so compare it element by element
     */
		 allfrequentPatternsArray.map(pattern=>pattern.sameElements(attr.getPattern)).reduce((ans1,ans2)=>ans1 || ans2)
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