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
object getGraphDistributions {

  def main(args: Array[String]): Unit = {
    
    val sc = SparkContextInitializer.sc
    type DataGraph = Graph[Int, KGEdgeInt]

    /*
     * Read configuration parameters.
     */
    val confFilePath = args(0)
    val ini = new Wini(new File(confFilePath));
    val pathOfBatchGraph = ini.get("run", "batchInfoFilePath");
    val startTime = ini.get("run", "startTime").toInt
    val batchSizeInTime = ini.get("run", "batchSizeInTime")
    val typePred = ini.get("run", "typeEdge").toInt
    val dateTimeFormatPattern = ini.get("run","dateTimeFormatPattern")
    val totalSignaturesDistributionDir = ini.get("output","totalSignaturesDistributionDir")
    val totalSignaturesDistributionLabelDir = ini.get("output", "totalSignaturesDistributionLabelDir")
    val HistogramDir = ini.get("output","HistogramDir")
    val HistogramLabelDir = ini.get("output","HistogramLabelDir")
     /*
     * Initialize various global parameters.
     */
    val batchSizeInMilliSeconds = getBatchSizerInMillSeconds(batchSizeInTime)
    var currentBatchId = getBatchId(startTime, batchSizeInTime) - 1 

    for (graphFile <- Source.fromFile(pathOfBatchGraph).
        getLines().filter(str => !str.startsWith("#"))) {
      
      var t0_batch = System.nanoTime()
    	
      currentBatchId = currentBatchId + 1
      var t0 = System.nanoTime()
      val incomingDataGraph: DataGraph = ReadHugeGraph.getTemporalGraphInt(graphFile, 
          sc, batchSizeInMilliSeconds,dateTimeFormatPattern).cache
      println("v size is", incomingDataGraph.vertices.count)
      println("e size is", incomingDataGraph.edges.count)
      
      val typeGraph = DataToPatternGraph.getTypedGraph(incomingDataGraph, typePred)
      println("v size is", typeGraph.vertices.count)
      
      val bctype = sc.broadcast(typePred)
      val loclatype = bctype.value
      //val validEdgeGraph = typeGraph.subgraph(epred  => (epred.attr.getlabel != loclatype))
      // the code in next line takes care of basetpe edges also
      val validGraph = typeGraph.subgraph(vpred = (id,atr) => atr._2.size > 0)
      

      val oneEdgeRDD = validGraph.triplets.map(triple=>{
        val src = triple.srcAttr
        val dst = triple.dstAttr
        val edgeSignature = List(src._2(0),triple.attr.getlabel,dst._2(0))
        (edgeSignature, 1)
      }).reduceByKey((count1,count2)=>count1+count2)
      
      /*
       * Get Label based distribution of one edge patterns Represented as list of 5 int
       * ie. person sumit cowork person sutanay
       * person sumit cowork person sutanay; person sutanay hascountry country india
       */
      val oneEdgeRDD_Label = validGraph.triplets.map(triple=>{
        val src = triple.srcAttr
        val dst = triple.dstAttr
        val edgeSignature = List(src._2(0),src._1,triple.attr.getlabel,dst._2(0),dst._1)
        (edgeSignature, 1)
      }).reduceByKey((count1,count2)=>count1+count2)
      
      //oneEdgeRDD.collect.foreach(f=>println(f._1.toString(),f._2))
      
      //Aggregate code is unnecessary complex and requires more steps.
      /*
       * We get 3 types of 2 edge patterns
       * A->B->C
       * A<-B, A-> C
       * A<-B, A<-C
       * 
       */
      
     val ab_ac_OnA: VertexRDD[List[List[Int]]] = 
       validGraph.aggregateMessages[List[List[Int]]](edge => {
          val src = edge.srcAttr
          val dst = edge.dstAttr
          val edgeSignature = List(src._2(0), edge.attr.getlabel, dst._2(0))
          edge.sendToSrc(List(edgeSignature))
        },
        (a, b) => a ++ b)

      //Now get global ab_ac
      val res_ab_ac = ab_ac_OnA.flatMap(vertexInfo => {
        val allLocalPatternInstances = vertexInfo._2
        val innerList: List[Int] = List.empty;
        var allLocal_AB_AC_OnA: ListBuffer[(List[Int], Int)] = ListBuffer.empty

        for (i <- 0 until allLocalPatternInstances.length) {
          for (j <- i + 1 until allLocalPatternInstances.length) {
            val two_edge_Pattern = ((allLocalPatternInstances(i) ++ allLocalPatternInstances(j), 1))
            allLocal_AB_AC_OnA += (two_edge_Pattern)
          }
        }
        allLocal_AB_AC_OnA
      }).reduceByKey((count1,count2) => count1+count2)
      
      //Lable version
           val ab_ac_OnA_Label: VertexRDD[List[List[Int]]] = 
       validGraph.aggregateMessages[List[List[Int]]](edge => {
          val src = edge.srcAttr
          val dst = edge.dstAttr
          val edgeSignature = List(src._2(0),src._1, edge.attr.getlabel, dst._2(0),dst._1)
          edge.sendToSrc(List(edgeSignature))
        },
        (a, b) => a ++ b)

      //Now get global ab_ac
      val res_ab_ac_Label = ab_ac_OnA_Label.flatMap(vertexInfo => {
        val allLocalPatternInstances = vertexInfo._2
        val innerList: List[Int] = List.empty;
        var allLocal_AB_AC_OnA: ListBuffer[(List[Int], Int)] = ListBuffer.empty

        for (i <- 0 until allLocalPatternInstances.length) {
          for (j <- i + 1 until allLocalPatternInstances.length) {
            val two_edge_Pattern = ((allLocalPatternInstances(i) ++ allLocalPatternInstances(j), 1))
            allLocal_AB_AC_OnA += (two_edge_Pattern)
          }
        }
        allLocal_AB_AC_OnA
      }).reduceByKey((count1,count2) => count1+count2)
      
      
      
           val ba_ca_OnA: VertexRDD[List[List[Int]]] = 
       validGraph.aggregateMessages[List[List[Int]]](edge => {
          val src = edge.srcAttr
          val dst = edge.dstAttr
          val edgeSignature = List(src._2(0), edge.attr.getlabel, dst._2(0))
          edge.sendToDst(List(edgeSignature))
        },
        (a, b) => a ++ b)

      //Now get global ab_ac
      val res_ba_ca = ba_ca_OnA.flatMap(vertexInfo => {
        val allLocalPatternInstances = vertexInfo._2
        val innerList: List[Int] = List.empty;
        var allLocal_BA_CA_OnA: ListBuffer[(List[Int], Int)] = ListBuffer.empty

        for (i <- 0 until allLocalPatternInstances.length) {
          for (j <- i + 1 until allLocalPatternInstances.length) {
            val two_edge_Pattern = ((allLocalPatternInstances(i) ++ allLocalPatternInstances(j), 1))
            allLocal_BA_CA_OnA += (two_edge_Pattern)
          }
        }
        allLocal_BA_CA_OnA
      }).reduceByKey((count1,count2) => count1+count2)

      //Again, labled version
           val ba_ca_OnA_Label: VertexRDD[List[List[Int]]] = 
       validGraph.aggregateMessages[List[List[Int]]](edge => {
          val src = edge.srcAttr
          val dst = edge.dstAttr
          val edgeSignature = List(src._2(0),src._1, edge.attr.getlabel, dst._2(0),dst._1)
          edge.sendToDst(List(edgeSignature))
        },
        (a, b) => a ++ b)

      //Now get global ab_ac
      val res_ba_ca_Label = ba_ca_OnA_Label.flatMap(vertexInfo => {
        val allLocalPatternInstances = vertexInfo._2
        val innerList: List[Int] = List.empty;
        var allLocal_BA_CA_OnA: ListBuffer[(List[Int], Int)] = ListBuffer.empty

        for (i <- 0 until allLocalPatternInstances.length) {
          for (j <- i + 1 until allLocalPatternInstances.length) {
            val two_edge_Pattern = ((allLocalPatternInstances(i) ++ allLocalPatternInstances(j), 1))
            allLocal_BA_CA_OnA += (two_edge_Pattern)
          }
        }
        allLocal_BA_CA_OnA
      }).reduceByKey((count1,count2) => count1+count2)

      
      val result = res_ba_ca.union(res_ab_ac).reduceByKey((count1,count2) => count1+count2)
      val result_Label = res_ba_ca_Label.union(res_ab_ac_Label).reduceByKey((count1,count2) => count1+count2)
      
      
      /* For A->B->C
       * From Pregal Doc in http://spark.apache.org/docs/latest/graphx-programming-guide.html
       * Notice that Pregel takes two argument lists (i.e., graph.pregel(list1)(list2)). The first argument list 
       * contains configuration parameters including the initial message, the maximum number of iterations, and the edge
       * direction in which to send messages (by default along out edges). The second argument list contains the user 
       * defined functions for receiving messages (the vertex program vprog), computing messages (sendMsg), and 
       * combining messages mergeMsg.
       * 
       */
      // Before we could use pregal we need to initialize the nodes with placeholder list
      val newValidGraph = validGraph.mapVertices((id, attr) => {
        val lop : List[List[Int]] = List.empty
        // list of pattern on the node
        (id,(attr._1,attr._2,lop))
      })
      
      //Collect all super graph on the source (up to size 3 iterations) 
      // TODO : find why is the vertex structure includes extra vertex id
      val newGraph = newValidGraph.pregel[List[List[Int]]](List.empty,
      2, EdgeDirection.In)(
        (id, dist, newDist) =>
          {
            (dist._1,(dist._2._1,dist._2._2, newDist))
          }, // Vertex Program
          triplet => { // Send Message
            val existingPatternsAtDst: List[List[Int]] = triplet.dstAttr._2._3
            if(existingPatternsAtDst.size == 0)
            {
              val newpatterns = List(List(triplet.srcAttr._2._2(0), triplet.attr.getlabel, triplet.dstAttr._2._2(0)))
              Iterator((triplet.srcId, newpatterns))
            }else
            {
              val newpatterns = existingPatternsAtDst.map(aPattern => {
                List(triplet.srcAttr._2._2(0), triplet.attr.getlabel, triplet.dstAttr._2._2(0)) ++ aPattern
            })
            Iterator((triplet.srcId, newpatterns))

            }
          },
        (a, b) => a ++ b // Merge Message
        )
      
        val abc_vertices = newGraph.vertices
        //get all 2 edge patterns 
        val global_abc_patterns = newGraph.vertices.flatMap(v=>{
          val local_abc_patterns = v._2._2._3.filter(pattern=>pattern.size == 6)
          local_abc_patterns
        })
        val global_abc_patterns_count = global_abc_patterns.map(pattern=>(pattern,1)).reduceByKey((count1, count2) => count1+count2)
        
        
        //Labeled version
        val newGraph_Label = newValidGraph.pregel[List[List[Int]]](List.empty,
      2, EdgeDirection.In)(
        (id, dist, newDist) =>
          {
            (dist._1,(dist._2._1,dist._2._2, newDist))
          }, // Vertex Program
          triplet => { // Send Message
            val existingPatternsAtDst: List[List[Int]] = triplet.dstAttr._2._3
            if(existingPatternsAtDst.size == 0)
            {
              val newpatterns = List(List(triplet.srcAttr._2._2(0),triplet.srcAttr._2._1, triplet.attr.getlabel, triplet.dstAttr._2._2(0),triplet.dstAttr._2._1))
              Iterator((triplet.srcId, newpatterns))
            }else
            {
              val newpatterns = existingPatternsAtDst.map(aPattern => {
                List(triplet.srcAttr._2._2(0),triplet.srcAttr._2._1, triplet.attr.getlabel, triplet.dstAttr._2._2(0),triplet.dstAttr._2._1) ++ aPattern
            })
            Iterator((triplet.srcId, newpatterns))

            }
          },
        (a, b) => a ++ b // Merge Message
        )
      
        //val abc_vertices_Label = newGraph_Label.vertices
        //get all 2 edge patterns 
        val global_abc_patterns_label = newGraph_Label.vertices.flatMap(v=>{
          val local_abc_patterns = v._2._2._3.filter(pattern=>pattern.size == 10)
          local_abc_patterns
        })
        val global_abc_patterns_count_label = global_abc_patterns_label.map(pattern=>(pattern,1)).reduceByKey((count1, count2) => count1+count2)
        
        
        
        val totalSignatures = global_abc_patterns_count.union(result)
        .reduceByKey((count1,count2) => count1+count2).union(oneEdgeRDD).reduceByKey((count1,count2) => count1+count2)
        
        //totalSignatures.collect.foreach(f=>println(f))
        val totalSum = totalSignatures.map(_._2).sum
        println("sum is ", totalSum)
        val totalSignaturesDistribution = totalSignatures.map(signature => (signature._1,signature._2, signature._2/totalSum))
        totalSignaturesDistribution.map(p=>p._1.toString.replaceAll("List", "")+"\t"+p._2+"\t"+p._3).saveAsTextFile(totalSignaturesDistributionDir)
      
        //Labeled version
        
        val totalSignatures_label = global_abc_patterns_count_label.union(result_Label)
        .reduceByKey((count1,count2) => count1+count2).union(oneEdgeRDD_Label).reduceByKey((count1,count2) => count1+count2)
        
        val totalSum_label = totalSignatures_label.map(_._2).sum
        println("sum is with Labels ", totalSum_label)
        val totalSignaturesDistribution_label = totalSignatures_label.map(signature 
            => (signature._1,signature._2, signature._2/totalSum_label))
        //totalSignaturesDistribution_label.collect.foreach(f=>println(f))
        totalSignaturesDistribution_label.map(p=>p._1.toString.replaceAll("List", "")+"\t"+p._2+"\t"+p._3).saveAsTextFile(totalSignaturesDistributionLabelDir)
      
        
        /*
         * to create histograms.
         * 
         */
        val hist1 = totalSignaturesDistribution.map(sign=>(sign._2,1)).reduceByKey((c1,c2)=>c1+c2)
        hist1.map(f=>f._1+"\t"+f._2).saveAsTextFile(HistogramDir)
        
        //Labeled version
        val hist1_label = totalSignaturesDistribution_label.map(sign=>(sign._2,1)).reduceByKey((c1,c2)=>c1+c2)
        hist1_label.map(f=>f._1+"\t"+f._2).saveAsTextFile(HistogramLabelDir)
       /* 
       * 
       * 
       * 
       * val newGraph = validGraph.pregel[List[(List[Int],Int)]](List.empty,
        2, EdgeDirection.In)(
          (id, dist, newDist) =>
            {
              
					     * ptype:
					     * -1 : Infrequent
					     *  0 : Promising
					     *  1 : Closed
					     *  2 : Redundant
					     
              
                
              dist
            }, // Vertex Program
          triplet => { // Send Message
            
            val src = triplet.srcAttr
        val dst = triplet.dstAttr
        val edgeSignature = List(src._2(0),triplet.attr.getlabel,dst._2(0))
        Iterator((triplet.srcId, List((edgeSignature, 1))))
            
          },
          (a, b) => a ++ b // Merge Message
          )*/
      
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