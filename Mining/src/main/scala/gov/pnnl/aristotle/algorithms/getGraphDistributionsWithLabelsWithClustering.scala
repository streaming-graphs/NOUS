/**
 *
 * @author puro755
 * @dJun 13, 2017
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
import org.apache.spark.mllib.clustering.PowerIterationClustering
import org.apache.spark.rdd.RDD
import java.io.PrintWriter
import org.apache.spark.mllib.clustering.{ KMeans, KMeansModel }
import org.apache.spark.mllib.linalg.Vectors
import scala.collection.JavaConversions._

/**
 * @author puro755
 *
 */
object getGraphDistributionsWithLabelsWithClustering {

  def main(args: Array[String]): Unit = {

    val sc = SparkContextInitializer.sc
    type DataGraph = Graph[Int, KGEdgeInt]
    type NbrTypes = Set[Int]
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
    val dateTimeFormatPattern = ini.get("run", "dateTimeFormatPattern")
    val EdgeLabelDistributionDir = ini.get("output", "EdgeLabelDistributionDir")
    val allAttributeEdgesLine = ini.get("run", "attributeEdge");
    val allAttributeEdges: Array[Int] = allAttributeEdgesLine.split(",").map(_.toInt)
    //int[] allAttributeEdges2 = ini.get("run").getAll("fortuneNumber", int[].class);

    // TODO: how to read it as int array
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

    for (
      graphFile <- Source.fromFile(pathOfBatchGraph).
        getLines().filter(str => !str.startsWith("#"))
    ) {

      var t0_batch = System.nanoTime()

      currentBatchId = currentBatchId + 1
      var t0 = System.nanoTime()
      val incomingDataGraph: DataGraph = ReadHugeGraph.getTemporalGraphInt(graphFile,
        sc, batchSizeInMilliSeconds, dateTimeFormatPattern).cache

      val typeGraph = DataToPatternGraph.getTypedGraph(incomingDataGraph, typePred)
      println("v size is", typeGraph.vertices.count)
      println("e size is", typeGraph.edges.count)

      val bctype = sc.broadcast(typePred)
      val loclatype = bctype.value
      //val validEdgeGraph = typeGraph.subgraph(epred  => (epred.attr.getlabel != loclatype))
      // the code in next line takes care of basetpe edges also
      //typeGraph.vertices.mapValues(vval=>(vval._1,vval._2.size)).saveAsTextFile("typenode")
      // But it does not remove self-loop edges
      // 177049  10      177049  1111/11/11 05:01:00.000
      val validGraph = typeGraph.subgraph(
          vpred = (id, atr) => atr._2.size > 0).subgraph(epred => epred.attr.getlabel != bctype.value)

      println("valid v size is", validGraph.vertices.count)
      println("valid e size is", validGraph.edges.count)

      //validGraph.triplets.collect.foreach(f=>println(f.srcId, f.srcAttr, f.attr, f.dstId, f.dstAttr))
      //System.exit(1)
      /*
       * Get k-mean clustering of lables
       * 
       * We create the graph with predicate 9 i.e. cites and rest of the predicates
       * are converted to attributes
       * 
       *  
       *         val allAttributeEdgeType = validGraph.triplets.filter(e=>e.attr != 9).map(e=>{
        (Set(e.attr))
      }).reduce((a,b) => a ++ b) 
      
      3 is has confId
       */

      type nodeAttribute = (Long, Long)
      type nodeAttributes = List[nodeAttribute]

      var newAttributedGraph = validGraph.mapVertices((id, attr) => {
        val ndAttr: nodeAttributes = List.empty
        // list of pattern on the node
        (id, (attr._1, attr._2, ndAttr))
      })

      println("get size of all attr edges" + allAttributeEdges.length)
      //var newAttributedGraph : Graph[(Long,(DataToPatternGraph.Label,List[Int],nodeAttributes)), KGEdgeInt] = null
      allAttributeEdges.foreach(attributeEdge => {
        val edgeType = attributeEdge.toInt

        val validConfLablesRDD = validGraph.triplets.filter(e => e.attr.getlabel == edgeType)
        val validConfLabels = validGraph.triplets.filter(e => e.attr.getlabel == edgeType).map(e => Vectors.dense(Array(e.dstAttr._1.toDouble)))

        println("***size of lable ", validConfLabels.count)
        if (validConfLabels.count > 0) {
          // Cluster the data into two classes using KMeans
          val numClusters = 5
          val numIterations = 20
          val clusters = KMeans.train(validConfLabels, numClusters, numIterations)
          //clusters.clusterCenters.foreach(c=>println(" size is " , c.toString()))

          val predictions = validConfLablesRDD.map(entry =>
            (entry.dstAttr._1, clusters.predict(Vectors.dense(entry.dstAttr._1))))

          //Get RDD for this edge labels
          val newAttributeRDD = newAttributedGraph.subgraph(epred => (epred.attr.getlabel == edgeType)).triplets.map(t => {
            (t.srcId, clusters.predict(Vectors.dense(t.dstAttr._2._1.toDouble)))
          })

          //create new graph
          newAttributedGraph = newAttributedGraph.outerJoinVertices(newAttributeRDD) {

            case (uid, deg, Some(attribute)) => {
              val newNbrAttr: nodeAttribute = (edgeType, attribute)
              (uid, (deg._2._1, deg._2._2, deg._2._3 :+ newNbrAttr))
            }
            // Some users may not have attributes so we set them as empty
            case (uid, deg, None) => (uid, (deg._2._1, deg._2._2, deg._2._3))
          }

        }
      })

      println("graph isze is ", newAttributedGraph.vertices.count)
      newAttributedGraph.triplets.filter(t => t.srcAttr._2._3.size > 0).collect.foreach(v => println(v.srcId, v.srcAttr.toString))

      // val graph = getPowerIterationCluteringGraph(validGraph)

      val oneEdgeRDD = newAttributedGraph.triplets.flatMap(triple => {
        val src = triple.srcAttr
        val dst = triple.dstAttr
        val edgeLabel = triple.attr.getlabel
        val allSrcProps = src._2._3
        val allDstProps = dst._2._3

        var newSignatures: ListBuffer[(List[Int], Int)] = ListBuffer.empty
        allSrcProps.map(sprop => {
          allDstProps.map(dprop => {
            //attribute label distribution at both source and dst
            if ((sprop._1.toInt != edgeLabel) && (dprop._1.toInt != edgeLabel)) {
              /*
               * Some Examples:
               * 
               * person withClusterID 3 worskWith person withClusterID 4
               * 
               * 
               */
              //val tmpSign = List(sprop) ++ List(edgeLabel) ++ List(dprop)
              val tmpSign = List(src._2._2(0)) ++ List(sprop._1.toInt, sprop._2.toInt) ++ List(edgeLabel, dst._2._2(0)) ++ List(dprop._1.toInt, dprop._2.toInt)
              newSignatures += ((tmpSign, 1))
            }

          })
        })

        allSrcProps.map(sprop => {
          // attribute label distribution only at source and take dst node label only 
          if ((sprop != edgeLabel)) {

            /*
               * Some Examples:
               * 
               * person withClusterID 3 worskWith person 
               * 
               *  
               */

            val tmpSign = List(src._2._2(0)) ++ List(sprop) ++ List(edgeLabel, dst._2._2(0))
            //newSignatures += ((tmpSign, 1))

          }
        })

        /* allDstProps.map(dprop=>{
            // attribute label distribution only at source and take dst node label only 
            val tmpSign = List(src._2._2(0)) ++  List(src._2._1)  ++ List(edgeLabel, dst._2._2(0)) ++ dprop 
            newSignatures += ((tmpSign, 1))
        })*/

        newSignatures
      }).reduceByKey((cnt1, cnt2) => cnt1 + cnt2)

      oneEdgeRDD.saveAsTextFile("TypeClusetIDinSignature_withKmeanClusering5")
      //      val noderdd = newPropGraph.vertices

    }

  }

  def getPowerIterationCluteringGraph(validGraph: Graph[(DataToPatternGraph.Label, List[Int]), KGEdgeInt]): Graph[(Long, (DataToPatternGraph.Label, List[Int], List[Int])), KGEdgeInt] =
    {
      /*
       * need to create a property graph like structure where each node contains the edge labels.
       * ie SP will have <coworker sc> <hasCountry india>
       */
      //Lets use pregel
      // Before we could use pregal we need to initialize the nodes with placeholder list
      type NbrhoodSignature = Set[Int] // one entry is 2 Int
      type nodeTypes = List[Int]

      val basePropGraph = validGraph.mapVertices((id, attr) => {
        val nbrSign: NbrhoodSignature = Set.empty
        // list of pattern on the node
        (id, (attr._1, attr._2, nbrSign))
      })

      val newPropGraph = basePropGraph.pregel[NbrhoodSignature](Set.empty,
        2, EdgeDirection.In)(
          (id, dist, newDist) =>
            {
              (dist._1, (dist._2._1, dist._2._2, dist._2._3 ++ newDist)) //step 0 and 3, one each node
            }, // Vertex Program
          triplet => { // Send Message step 1
            //val existingPatternsAtDst: List[List[Int]] = triplet.dstAttr._2._3
            //NOTE: we store <edge type> <dst label> on the srouce node
            val newNbrSign = Set(triplet.attr.getlabel, triplet.dstAttr._2._1)
            Iterator((triplet.srcId, newNbrSign))
          },
          (a, b) => a ++ b // Merge Message step 2
          )

      //newPropGraph.vertices.map(v=> v._2._2._1 +"\t" + v._2._2._2 +"\t"+ v._2._2._3).saveAsTextFile("progrpah5")
      val trainingRDD: RDD[(Long, Long, Double)] = newPropGraph.triplets.map(triple => {
        (triple.srcId.toLong, triple.dstId.toLong, getJaccardSimilarity(triple.srcAttr._2._3, triple.dstAttr._2._3))
      }).cache

      val model = new PowerIterationClustering()
        .setK(5)
        .setMaxIterations(20)
        .setInitializationMode("degree")
        .run(trainingRDD)

      val assignmentRDD = model.assignments
      val eachassRDDEntryMap = assignmentRDD.map(f => (f.id, f.cluster)) //.zipWithUniqueId.collectAsMap

      val graph = newPropGraph.outerJoinVertices(eachassRDDEntryMap) {
        case (uid, deg, Some(attrList)) => (uid, (deg._2._1, deg._2._2, List(attrList)))
        // Some users may not have attributes so we set them as empty
        case (uid, deg, None) => (uid, (deg._2._1, deg._2._2, List.empty))
      }
      return graph
    }

  def getJaccardSimilarity(set1: Set[Int], set2: Set[Int]): Double = {

    val common = set1.intersect(set2).size.toDouble
    val total = set2.union(set1).size.toDouble

    return (common / total)
  }

  def getBatchSizerInMillSeconds(batchSize: String): Long =
    {
      val MSecondsInYear = 31556952000L
      if (batchSize.endsWith("y")) {
        val batchSizeInYear: Int = batchSize.replaceAll("y", "").toInt
        return batchSizeInYear * MSecondsInYear
      }
      return MSecondsInYear
    }

  def getBatchId(startTime: Int, batchSizeInTime: String): Int =
    {
      val startTimeMilliSeconds = getStartTimeInMillSeconds(startTime)
      val batchSizeInTimeIntMilliSeconds = getBatchSizerInMillSeconds(batchSizeInTime)
      return (startTimeMilliSeconds / batchSizeInTimeIntMilliSeconds).toInt
    }
  def getStartTimeInMillSeconds(startTime: Int): Long =
    {
      val startTimeString = startTime + "/01/01 00:00:00.000"
      val f = DateTimeFormat.forPattern("yyyy/MM/dd HH:mm:ss.SSS");
      val dateTime = f.parseDateTime(startTimeString);
      return dateTime.getMillis()
    }
}