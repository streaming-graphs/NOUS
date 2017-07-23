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
import java.util.regex.Pattern
import org.apache.spark.SparkContext

/**
 * @author puro755
 *
 */
object getGraphDistributionsWithLabelsWithClustering {

  val sc = SparkContextInitializer.sc
    
  
  def main(args: Array[String]): Unit = {

    type DataGraph = Graph[Int, KGEdgeInt]
    type NbrTypes = Set[Int]
    
    /*
     * Read configuration parameters.
     * Please change the parameter in the conf file 'args(0)'. sample file is conf/knowledge_graph.conf
     */
    val confFilePath = args(0)
    val ini = new Wini(new File(confFilePath));
    val allAttributeEdgesLine = ini.get("run", "attributeEdge");
    val allAttributeEdges: Array[Int] = allAttributeEdgesLine.split(",").map(_.toInt)
    val fosTreePath : String  = ini.get("run", "FoSTreePath");
    val pathOfDescriptionFile = ini.get("run", "pathOfDescriptionFile")
    //int[] allAttributeEdges2 = ini.get("run").getAll("fortuneNumber", int[].class);

    val pathOfBatchGraph = ini.get("run", "batchInfoFilePath");
    val startTime = ini.get("run", "startTime").toInt
    val batchSizeInTime = ini.get("run", "batchSizeInTime")
    val typePred = ini.get("run", "typeEdge").toInt
    val dateTimeFormatPattern = ini.get("run", "dateTimeFormatPattern")
    val EdgeLabelDistributionDir = ini.get("output", "EdgeLabelDistributionDir")
    

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
      val allL2WithL2MappingDominantL1 = getCurrentYearL2IntWithL1Int(sc, fosTreePath, pathOfDescriptionFile)
      getClusteredAttributedGraph(graphFile, allL2WithL2MappingDominantL1, ini)
    }

  }

  def getClusteredAttributedGraph(graphFile : String,allL2WithL2MappingDominantL1:RDD[(Int,Int)],ini : Wini)
  {

    val numClusters = ini.get("Learning", "numClusters").toInt
    val numIterations = ini.get("Learning", "numIterations").toInt

    /*
     * Output parameters
     */
    val citationGraphDir = ini.get("output", "citationGraphDir")
    val authorGraphDir = ini.get("output", "authorGraphDir")
    val paperAttributeDir = ini.get("output", "paperAttributeDir")
    val authorAttributeDir = ini.get("output", "authorAttributeDir")
    val fosClusterDir = ini.get("output", "fosClusterDir")

    /*
       * Read graph files as RDD of 
       * (<subject>			<(edgeType,object)> i.e.
       * 
       * (<paper1>			<(cites,paper10>) 
       */

    val baseRDD = getQuadruplesRDD(graphFile, sc)

    /*
       * Get Citation Graph
       * 
       * <paper1> <9> <paper2>
       */
    val citationEdge = 9
    val citationGraph = baseRDD.filter(entry => {
      entry._2._1 == citationEdge
    }).map(citationEdge => (citationEdge._1, citationEdge._2._1, citationEdge._2._2))

    /*
       * Get Reputation of a Paper
       * (paper0 12345)
       */
    val paperReputation = citationGraph.map(citationEdge => (citationEdge._3, 1)).reduceByKey((citeCnt1, citeCnt2) => citeCnt1 + citeCnt2)

    /*
       *    Get Log scale bins
       */
    val minLog = scala.math.log(paperReputation.values.min) / scala.math.log(2)
    val maxLog = scala.math.log(paperReputation.values.max) / scala.math.log(2)
    val (zero, one3, two3, full) = (0, maxLog / 3, 2 * maxLog / 3, maxLog)

    /*
       * Get paper Reputation as {1,2,3}
       */
    val binnedPaperReputation = paperReputation.map(paper => {
      if (paper._2 < one3) (paper._1, 1)
      else if (paper._2 < two3) (paper._1, 2)
      else (paper._1, 3)
    })

    /*
        * get Author Reputation.
        * We use already computed paperReputation for this task.
        * paperReputation:
        * 		(paper10 25) i.e. paper10 has 25 citations
        *   
        * We also compute
        * 		(paper10 sp) , (paper10 sc) 
        * ie. paper10 is authored by sp and sc
        * 
        *     
        */
    val hasAuthorEdge = 4
    val authorGraph = baseRDD.filter(entry => {
      entry._2._1 == hasAuthorEdge
    }).map(paperAuthorEdge //(paper1, (4, paper10, time1)
    => (paperAuthorEdge._1, paperAuthorEdge._2._1, paperAuthorEdge._2._2)) // (paper1 4 paper10)

    /*
       * get (paper10, List(sp, sc))
       */
    val authorRDD = authorGraph.map(AuthorFact => (AuthorFact._1, List(AuthorFact._3))).reduceByKey((authList1, authList2) => authList1 ++ authList2)

    /*
       *    Left Outer Join authorRDD and paperReputation
       *    
       *    It is left join at authorRDD so that in case we dont have paper reputation for 
       *    a paper, we still keep the author information form the authorRDD 
       *    
       *    authorRDD is : (paper10, List(sp, sc))
       *    paperReputation is : (paper0 12345)
       */
    val resultingAuthorRDD = authorRDD.leftOuterJoin(paperReputation)

    val authorReputation = resultingAuthorRDD.flatMap(authorEntry => {
      val authorCitationFrom1Paper = authorEntry._2._1.map(author => (author, authorEntry._2._2.getOrElse(0)))
      authorCitationFrom1Paper //List((sp,12345), (sc,12345))
    }).reduceByKey((cite1, cite2) => cite1 + cite2) //reduce (sp 12345) and (sp 6789)

    /*
       *    Get Log scale bins for Author Reputation
       */
    val minLogAuthCite = scala.math.log(authorReputation.values.min) / scala.math.log(2)
    val maxLogAuthCite = scala.math.log(authorReputation.values.max) / scala.math.log(2)
    val (zeroAR, one3AR, two3AR, fullAR) = (0, maxLogAuthCite / 3, 2 * maxLogAuthCite / 3, maxLog)

    /*
       * Get Author Reputation as {1,2,3}
       */
    val binnedAuthorReputation = authorReputation.map(author => {
      if (author._2 < one3AR) (author._1, 1)
      else if (author._2 < two3AR) (author._1, 2)
      else (author._1, 3)
    })

    /*
       * Perform KMean Clustering of FoS based on feature vactor of all
       * unique Conference Ids
       */
    /*
        * Get RDD of unique FoS
        */
    val hasFieldOfStudyEdge = 8
    val FoSUnique = baseRDD.filter(entry => entry._2._1 == hasFieldOfStudyEdge).map(paperFoSEdge => {
      paperFoSEdge._2._2
    }).distinct.sortBy(f => f) //.countByValue()
    val totalFoSCount = FoSUnique.count

    /*
       * construct Conference feature vector for Every FoS
       */
    val hasConfIdEdge = 3
    val paperConf = baseRDD.filter(entry => entry._2._1 == hasConfIdEdge)
    val paperFoS = baseRDD.filter(entry => entry._2._1 == hasFieldOfStudyEdge)

    /*
       * In the given dataset, FoS are L1L2 levels of MAG,
       * For better clustering convert all L2s to L1
       * 
       * (fos1,paper11)
       * (fos1,paper12)
       * (fos1,paper13)
       * 
       * (fos2,paper11)
       * (fos3,paper11)
       * (fos2,paper12)
       */
    val fosPap = paperFoS.map(paperEntry => (paperEntry._2._2, paperEntry._1))

    //Read FoSHeirarchy to for L2 only
    // joining (fosid, paperid) with (DM-Mapping, CS-Mapping) where we have L2 mappings only 
    // get (fosidOFDM, (paperid, CS-Mapping)
    /*
       * (fos1,(paper11, fos1L1))
       * (fos1,(paper12, fos1L1))
       * (fos1,(paper13, fos1L1))
       * 
       * (fos2,paper11, fos2L1))
       * (fos3,paper11, fos3L1))
       * (fos2,paper12, fos2L1))
       */
    val l1l2FosPaper = fosPap.join(allL2WithL2MappingDominantL1)

    //get (paperid, l1Mapping)
    /*
       * (paper11, fos1L1)
       * (paper12, fos1L1)
       * (paper13, fos1L1)
       * 
       * paper11, fos2L1)
       * paper11, fos3L1)
       * paper12, fos2L1)
       */
    val fixedPaperFoS = l1l2FosPaper.map(entry => (entry._2._1, entry._2._2))

    /*
       * (paper11, ((hasConf Confid1 timeid), fos1L1))
       * (paper12, ((hasConf Confid2 timeid), fos1L1))
       * (paper13, ((hasConf Confid3 timeid), fos1L1))
       * 
       * paper11, ((hasConf Confid1 timeid), fos2L1))
       * paper11, ((hasConf Confid1 timeid), fos3L1))
       * paper12, ((hasConf Confid2 timeid), fos2L1))
       */

    val joinPaperConfFoS = paperConf.join(fixedPaperFoS)
    // Now we have (paper, ((hasConf Confid timeid) L1FoSId)))

    // Get set of Conferences for each FoS
    val confsEachFoS = joinPaperConfFoS.map(entry => (entry._2._2, Set(entry._2._1._2))).reduceByKey((confset1, confset2) => confset1 ++ confset2)
    //Now we have (fos1, Set(conf1,conf2....))(fos2,Set(conf2,conf3...))
    /*
       * (fos1L1, set(Confid1,Confid2,ConfId3))
       * (fos2L1, set(Confid1,Confid2))
       * (fos2L1, set(Confid1))
       */

    //Get all unique Conferences
    val uniqueConfs = baseRDD.filter(entry => entry._2._1 == hasConfIdEdge).map(paperConfEdge => {
      paperConfEdge._2._2
    }).distinct
    val localUniqueConfs = uniqueConfs.collect
    val totalConfCount = localUniqueConfs.length

    var confMap: scala.collection.mutable.Map[Int, Int] = scala.collection.mutable.Map.empty
    for (conf <- localUniqueConfs) confMap.put(conf, 0)

    val confMapCast = sc.broadcast(confMap)
    //Now we have broadcasted a map of all the conferences to each executor

    /*
       * For Year 2010
       * 
       * total size of conf 17782
       * total size of fos 36214
       * 
       * paper conf size 3880494
       * paper fos size 28878760
       * size of pap conf joinPaperConfFoS  28878760
       */

    var localConfMap = confMapCast.value
    println("local map size is " + localConfMap.size)
    // (fos, set(conf1,conf2,conf3....))
    val fosFeatureVectorLabel = confsEachFoS.map(fosEntry => {
      fosEntry._2.map(confFosEntry => localConfMap.put(confFosEntry, 1)) //instruction ends here
      val localConfArray = localConfMap.map(fosExistsNot => fosExistsNot._2.toDouble).toArray
      var fosFeatureArrayWithConfs = new Array[Double](totalConfCount.toInt)
      // Array needs to be created with new to get the desired behavior 
      /// https://stackoverflow.com/questions/2700175/scala-array-constructor

      for (i <- 0 until totalConfCount.toInt)
        fosFeatureArrayWithConfs(i) = localConfArray(i)
      (fosEntry._1, Vectors.dense(fosFeatureArrayWithConfs))
    })
    val fosFeatureVector = fosFeatureVectorLabel.map(f => f._2)

    val clusters = KMeans.train(fosFeatureVector, numClusters, numIterations)
    //clusters.clusterCenters.foreach(c=>println(" size is " , c.toString()))

    val predictions = fosFeatureVectorLabel.map(entry =>
      (entry._1, clusters.predict(entry._2)))

    /*
       * (fosid, fosClusterId)
       */
    predictions.saveAsTextFile(fosClusterDir)

    /*
       * Now Augment Paper Attributes
       * 
       * 1. create (fos0 paper1) from (paper1 fos0) : We already have it
       * 2. join (fos0 paper1) with predictions which is (fosid, fosClusterId) to create (fosid, paperid, clusterid)
       * 3. create (paperid, clusterid)
       * 4. joint it with paperreputationRDD
       * 
       */
    val fosWithClusterId = fosPap.leftOuterJoin(predictions) //(fos1, (paper2, OPTION[clustid3]))
    val paperWithFoSCluster = fosWithClusterId.map(fosEntry => (fosEntry._2._1, fosEntry._2._2.getOrElse(-1)))
    // TODO: check if there are multiple FOS for a paper that lead to different clusterID 
    val paperAttibutes = binnedPaperReputation.join(paperWithFoSCluster)

    /*
       * Now Augment Author Attributes
       * 
       * 1. create (fos0 paper1) from (paper1 fos0)
       * 2. join (fos0 paper1) with predictions which is (fosid, fosClusterId) to create (fosid, paperid, clusterid)
       * 3. create (paperid, clusterid)
       * 4. joint it with paperreputationRDD
       * 
       */
    val paperAuthor = baseRDD.filter(entry => entry._2._1 == hasAuthorEdge)

    // We already have paperWithFoSCluster from above
    val paperWithFoSAuthor = paperAuthor.join(paperWithFoSCluster)
    // We have (paper0 ((hasAuth sp time0),clustId1))
    val AuthorFoSClusterID = paperWithFoSAuthor.map(entry => (entry._2._1._2, entry._2._2))
    val AuthorAttributes = binnedAuthorReputation.join(AuthorFoSClusterID)

    /*
       * Serialize Citation Graph
       */
    citationGraph.map(entry => entry._1 + "\t" + entry._2 + "\t" + entry._3).saveAsTextFile(citationGraphDir)

    /*
       * Serialize Authorship Graph
       */
    authorGraph.map(entry => entry._1 + "\t" + entry._2 + "\t" + entry._3).saveAsTextFile(authorGraphDir)

    /*
       *  Serialize Paper Attribute List
       */
    paperAttibutes.map(entry => entry._1 + "\t" + entry._2._1 + "\t" + entry._2._2).saveAsTextFile(paperAttributeDir)

    /*
       * Serialize Author Attribute List
       */
    AuthorAttributes.map(entry => entry._1 + "\t" + entry._2._1 + "\t" + entry._2._2).saveAsTextFile(authorAttributeDir)

    /*
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
*/

  }
  
  
  def getCurrentYearL2IntWithL1Int(sc: SparkContext,fosTreePath:String,pathOfDescriptionFile:String) : RDD[(Int,Int)] = 
  {
      // ABCDEF  GHIJK
      type FOS = String
      type Level = String
      type confidence = Double
      type FosTreeEntry = (FOS, Level, FOS, Level, confidence)
      val allFoS: RDD[FosTreeEntry] = sc.textFile(fosTreePath).filter(ln => ReadHugeGraph.isValidLineFromGraphFile(ln)).map(line =>
        {
          val cleanedLineArray = line.trim().split("\t")
          (cleanedLineArray(0), cleanedLineArray(1), cleanedLineArray(2),
            cleanedLineArray(3), cleanedLineArray(4).toDouble)
        })

      // (L1fos, L2Fos)
      val allL2L1FoS = allFoS.filter(entry => (entry._2.equalsIgnoreCase("L2")
        && entry._4.equalsIgnoreCase("L1"))).map(entry => (entry._3, entry._1)).distinct

      // ABCDEF  12345 
      // It is a year specific file
      type EntityDictionary = (String, Int)
      val allEntityDictionary: RDD[EntityDictionary] =
        sc.textFile(pathOfDescriptionFile).filter(ln => ReadHugeGraph.isValidLineFromGraphFile(ln)).map(line =>
          {
            //177942 09B4F1FA
            val cleanedLineArray = line.trim().split(" ")
            (cleanedLineArray(1), cleanedLineArray(0).toInt)
          }).distinct

      // Get All L1's Int mapping and its L2
      // CS(L1) (CS-mapping, DM(L2))
      val allL1ForExistingL2 = allEntityDictionary.join(allL2L1FoS)

      val allL2WithL1IntVal = allL1ForExistingL2.map(entry => {
        (entry._2._2, entry._2._1) // (DM, CS-Mapping) i.e. (DM, 24680)
      }).groupByKey

      //(DM, CS-IntMapping)
      val allL2WithDominantL1 = allL2WithL1IntVal.map(entry => {
        val allL1s = entry._2
        if (allL1s.size == 1)
          (entry._1, allL1s.toList(0))
        else {
          (entry._1, entry._2.groupBy(identity).maxBy(_._2.size)._1)
        }
      })

      // Join this with dictionary to get L2's Int Mapping
      // (DM, (CS-Mapping, DM-Mapping)) --> (DM-Mapping, CS-Mapping)
      val allL2WithL2MappingDominantL1 = allL2WithDominantL1.join(allEntityDictionary).map(entry //entry is (fos1L2String,(fosL1Id,fosL2Id))
      => (entry._2._2, entry._2._1)) //entry._1))

      return allL2WithL2MappingDominantL1
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

  def getQuadruplesRDD(graphFile: String, sc: SparkContext): RDD[(Int, (Int, Int, Int))] =
    {
      val quadruples: RDD[(Int, (Int, Int, Int))] =
        sc.textFile(graphFile).filter(ln => ReadHugeGraph.isValidLineFromGraphFile(ln)).map { line =>
          var longtime = -1
          val fields = ReadHugeGraph.getFieldsFromLine(line);
          try {
            var parsedDate = fields(3).replaceAll("t", " ").trim()
            longtime = parsedDate.hashCode()
            if (fields.length == 4) {
              (fields(0).toInt, (fields(1).toInt, fields(2).toInt, longtime))
            } else if (fields.length == 3)
              (fields(0).toInt, (fields(1).toInt, fields(2).toInt, 0))
            else {
              //println("Exception reading graph file line", line)
              (-1, (-1, -1, -1))
            }
          } catch {
            case ex: org.joda.time.IllegalFieldValueException => {
              println("IllegalFieldValueException exception")
              (-1, (-1, -1, -1))
            }
            case ex: java.lang.ArrayIndexOutOfBoundsException =>
              {
                println("AIOB:", line)
                (-1, (-1, -1, -1))
              }
            case ex: java.lang.NumberFormatException =>
              println("AIOB2:", line)
              (-1, (-1, -1, -1))
          }

        }

      quadruples.cache
      return quadruples
    }
}