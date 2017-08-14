/**
 *
 * @author puro755
 * @dApr 24, 2017
 * @Mining
 */
package gov.pnnl.aristotle.algorithms

import org.ini4j.Wini
import java.io.File
import org.joda.time.format.DateTimeFormat
import scala.io.Source
import org.apache.spark.graphx.Graph
import gov.pnnl.aristotle.algorithms.mining.datamodel.KGEdgeInt
import org.apache.spark.graphx.EdgeDirection
import org.apache.spark.graphx.VertexRDD
import scala.collection.mutable.ListBuffer

/**
 * @author puro755
 *
 */
object getGraphDistributionsWithLabels {

  def main(args: Array[String]): Unit = {
    
    val sc = SparkContextInitializer.sc
    type DataGraph = Graph[Int, KGEdgeInt]

    /*
     * Read configuration parameters.
     * Please change the parameter in the conf file 'args(0)'. sample file is conf/knowledge_graph.conf
     */
    val confFilePath = args(0)
    val ini = new Wini(new File(confFilePath));
    val pathOfBatchGraph = ini.get("run", "batchInfoFilePath");
    val startTime = ini.get("run", "startTime").toInt
    val batchSizeInTime = ini.get("run", "batchSizeInTime")
    val typePred = ini.get("run", "typeEdge").toInt
    val dateTimeFormatPattern = ini.get("run","dateTimeFormatPattern")
    val EdgeLabelDistributionDir = ini.get("output","EdgeLabelDistributionDir")
    
     /*
     * Initialize various global parameters.
     */
    val batchSizeInMilliSeconds = getBatchSizerInMillSeconds(batchSizeInTime)
    var currentBatchId = getBatchId(startTime, batchSizeInTime) - 1 

    /*
     * for MAG, the predicates are :
     * 
     * object predicates {
			  val hasPublishDate = 1
			  val hasPublishYear = 2
			  val hasConfId = 3
			  val hasAuthor = 4
			  //val paperHasAff = 5
			  val authorHasAff = 6
			  //val hasKeyword = 7
			  val hasFieldOfStudy = 8
			  val cites = 9
			  val hasType = 10
			  }
     * 
     */
    
    for (graphFile <- Source.fromFile(pathOfBatchGraph).
        getLines().filter(str => !str.startsWith("#"))) {
      
      var t0_batch = System.nanoTime()
    	
      currentBatchId = currentBatchId + 1
      var t0 = System.nanoTime()
      val incomingDataGraph: DataGraph = ReadHugeGraph.getTemporalGraphInt(graphFile, 
          sc, batchSizeInMilliSeconds,dateTimeFormatPattern).cache
      
      val typeGraph = DataToPatternGraph.getTypedGraph(incomingDataGraph, typePred)
      println("v size is", typeGraph.vertices.count)
      println("e size is", typeGraph.edges.count)
      
      
      val bctype = sc.broadcast(typePred)
      val loclatype = bctype.value
      //val validEdgeGraph = typeGraph.subgraph(epred  => (epred.attr.getlabel != loclatype))
      // the code in next line takes care of basetpe edges also
      //typeGraph.vertices.mapValues(vval=>(vval._1,vval._2.size)).saveAsTextFile("typenode")
      val validGraph = typeGraph.subgraph(vpred = (id,atr) => atr._2.size > 0)
      
      println("valid v size is", validGraph.vertices.count)
      println("valid e size is", validGraph.edges.count)
      

      /*
       * need to create a property graph like structure where each node contains the edge labels.
       * ie SP will have <coworker sc> <hasCountry india>
       */
      //Lets use pregel
      // Before we could use pregal we need to initialize the nodes with placeholder list
      val newValidPropGraph = validGraph.mapVertices((id, attr) => {
        val lop : List[List[Int]] = List.empty
        // list of pattern on the node
        (id,(attr._1,attr._2,lop))
      })
      val newPropGraph = newValidPropGraph.pregel[List[List[Int]]](List.empty,
      1, EdgeDirection.In)(
        (id, dist, newDist) =>
          {
            (dist._1,(dist._2._1,dist._2._2, newDist))
          }, // Vertex Program
          triplet => { // Send Message
            //val existingPatternsAtDst: List[List[Int]] = triplet.dstAttr._2._3
            //NOTE: we store <edge type> <dst label> on the srouce node
              val newpatterns = List(List(triplet.attr.getlabel, triplet.dstAttr._2._1))
              Iterator((triplet.srcId, newpatterns))
          },
        (a, b) => a ++ b // Merge Message
        )
      
      
      //newPropGraph.triplets.collect.foreach(t=>println(t.toString))
      
      val oneEdgeRDD = newPropGraph.triplets.flatMap(triple=>{
        val src = triple.srcAttr
        val dst = triple.dstAttr
        val edgeLabel = triple.attr.getlabel
        val allSrcProps = src._2._3
        val allDstProps = dst._2._3
        
       var newSignatures : ListBuffer[(List[Int],Int)] = ListBuffer.empty   
        allSrcProps.map(sprop=>{
          allDstProps.map(dprop =>{
            //attribute label distribution at both source and dst
            if((sprop(0) != edgeLabel) && (dprop(0) != edgeLabel))
            {
              
              /*
               * Some Examples:
               * 
               * person hasName Sumit worskWith person hasCountry India
               * 
               * author affiliatedTo PNNL publishIn Conf hasFoS AI
               */ 
              
              val tmpSign = List(src._2._2(0)) ++ sprop ++ List(edgeLabel, dst._2._2(0)) ++ dprop
              newSignatures += ((tmpSign, 1))
            }
            
          })
        })
        
        allSrcProps.map(sprop=>{
            // attribute label distribution only at source and take dst node label only 
          if((sprop(0) != edgeLabel) )
            {
            val tmpSign = List(src._2._2(0)) ++ sprop ++ List(edgeLabel, dst._2._2(0)) 
            //newSignatures += ((tmpSign, 1))

          }
        })

       /* allDstProps.map(dprop=>{
            // attribute label distribution only at source and take dst node label only 
            val tmpSign = List(src._2._2(0)) ++  List(src._2._1)  ++ List(edgeLabel, dst._2._2(0)) ++ dprop 
            newSignatures += ((tmpSign, 1))
        })*/
       
        newSignatures
      }).reduceByKey((cnt1,cnt2)=>cnt1+cnt2)
      
      oneEdgeRDD.saveAsTextFile("EdgeLabelDistribution4NoClust")
      
       
      
    }
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
  
    def getBatchId(startTime : Int, batchSizeInTime : String) : Int =
  {
    val startTimeMilliSeconds = getStartTimeInMillSeconds(startTime)
    val batchSizeInTimeIntMilliSeconds = getBatchSizerInMillSeconds(batchSizeInTime)
    return (startTimeMilliSeconds / batchSizeInTimeIntMilliSeconds).toInt
  }
  def getStartTimeInMillSeconds(startTime:Int) : Long = 
  {
    val startTimeString = startTime + "/01/01 00:00:00.000"
    val f = DateTimeFormat.forPattern("yyyy/MM/dd HH:mm:ss.SSS");
    val dateTime = f.parseDateTime(startTimeString);
    return dateTime.getMillis()
  }

}