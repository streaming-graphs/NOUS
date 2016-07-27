package gov.pnnl.aristotle.algorithms.PathSearch

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.log4j.Logger
import org.apache.log4j.Level
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import scala.collection.{SortedSet, Set}
import org.apache.spark.mllib.clustering._
import gov.pnnl.aristotle.utils.KGraphProp
import java.io._
import gov.pnnl.aristotle.algorithms.ReadHugeGraph
import org.apache.spark.rdd.RDD.numericRDDToDoubleRDDFunctions
import org.apache.spark.rdd.RDD.rddToPairRDDFunctions
import scala.Array.canBuildFrom
import gov.pnnl.aristotle.algorithms.PathSearch.PathSearchUtils._

object VertexOntologyGenerator {
  
  type EdgeWeight = Long
  type OntologyGraph = Graph[String, EdgeWeight]
  type NbrNode = (VertexId)
  def main(args: Array[String]): Unit = {
    
    val sparkConf = new SparkConf().setAppName("LASDemo").setMaster("local")
    val sc = new SparkContext(sparkConf)
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)
    println("In ontology clustering")
    if(args.length != 4){
      println("check command line arguments")
      exit
    }
    val inputFile = args(0)
    val outputDir = args(1)
    val numClusters = args(2).toInt
    val numIter = args(3).toInt
    val g = ReadHugeGraph.getGraph(inputFile, sc)
    val vertexlabelFile =  outputDir + "/graphVertLabels"
    val ontologyGraphFile = outputDir + "/ontologyGraph"
    val clustersByIdFile = outputDir + "/ontologyClustersById"
    val clusterWithLabelsFile = outputDir + "/ontologyClustersByLabel"
    val finalOut = outputDir + "/finalClusters"
    //CreateAndSaveOntologyGraph(g, ontologyGraphFile)
    //ReadOntologyGraph(ontologyGraphFile, sc)
    //CreateAndSaveGraphClusters(ontologyGraphFile, clustersByIdFile, numClusters, numIter,  sc)
    //mapClustersToLabels(clustersByIdFile, vertexlabelFile, clusterWithLabelsFile, sc)
    //saveClusters(clusterWithLabelsFile, finalOut, sc )
    //groupEdgesByVertex(inputFile, sc)
    PrintWordnetOntologyStats(g, outputDir)
    PrintNodeToWordNetCategories(g, outputDir)
    //PrintWikiOntologyStats(g, outputDir)
  }
  
  /* Get number of yago entities connected to each wordnet category 
   * NOte1: each yago entity may connect to multiple wordnet categories
   * Note2: We are not counting the wikicategory_ nodes connected to wordnet categories
   */
  def PrintWordnetOntologyStats(g: Graph[String, String], outputDir: String): Unit = {
    val wordnetEntities: VertexRDD[Int] = g.aggregateMessages[Int](triplet => {
      if( (!triplet.srcAttr.startsWith("wikicategory")) && 
          (triplet.dstAttr.startsWith("wordnet")) )
        triplet.sendToDst(1)
    }, (a,b) => a+b)
    
    val wordnetWithLabels = wordnetEntities.innerZipJoin(g.vertices)((id, numNbrs, label) => 
      (label, numNbrs))
    println("number of total wordnet categories=", wordnetWithLabels.count)
    wordnetWithLabels.saveAsTextFile(outputDir + "/wordnetClusterStats")
  }
  
  /* For each yago entity, including yago "wikicategory_" nodes, create a Node2vec function, 
   *containing node's wordnet category information
   */
  def PrintNodeToWordNetCategories(g: Graph[String, String], outputDir: String): Unit = {
    val entitiesWithWordNetCatg : VertexRDD[Set[String]] = g.aggregateMessages[Set[String]](triplet => {
      if(triplet.dstAttr.startsWith("wordnet"))
        triplet.sendToSrc(Set(triplet.dstAttr))
    }, (a,b) => a ++ b)
    
    val entityLabelsWithWordNetCatg = entitiesWithWordNetCatg.innerZipJoin(g.vertices)(
        (id, wordnetCatg, label) => (label, wordnetCatg.fold[String]("")((a,b) => a + "," + b))) 
    println("number of total entities(regular nodes + wikicategory nodes) with some wordnet categories=", entityLabelsWithWordNetCatg.count)
    entityLabelsWithWordNetCatg.saveAsTextFile(outputDir + "/yagoEntitiesWordNetCatg")
  }
    
  def PrintWikiOntologyStats(g: Graph[String, String], outputDir: String): Unit = {
  
    val wikiEntities: VertexRDD[Array[String]] = g.aggregateMessages[Array[String]](triplet => {
      if(triplet.attr == KGraphProp.edgeLabelNodeType && triplet.dstAttr.startsWith("wikicategory"))
        triplet.sendToDst(Array(triplet.srcAttr))
    }, (a,b) => a++b)
    
    val wikiWithLabels = wikiEntities.innerZipJoin(g.vertices)((id, nbrs, label) => (label, nbrs.size, nbrs))
    println("number of total wiki categories=", wikiWithLabels.count)
    val totalmemberSize = wikiWithLabels.map(_._2._2).sum
    println("number of total vertices with wikicategory available=", totalmemberSize)
    //wikiWithLabels.foreach(v => println(v._2._1, v._2._2, v._2._3.foreach(nbr => 
    //  print(nbr + " , ")) ))
    //wikiWithLabels.saveAsTextFile(outputDir + "/wikicategoryClusters")
  }
  
  def ReadOntologyGraph(ontologyGraphFile: String, sc: SparkContext): Unit = {
    val ontologyEdges = sc.textFile(ontologyGraphFile).map(ln => ln.split(" , ")).filter(_.length == 3)
    .map(v => (v(0).trim.toLong, v(1).trim.toLong, v(2).trim.toDouble))
    println("Number of Edges in Ontology Graph", ontologyEdges.count )
  
    val ontologyEdgesSimGT1 = ontologyEdges.filter(v => v._3 > 1.0)
    val ontologyEdgesSimGT1K = ontologyEdges.filter(v => v._3 > 1000.0)
    println("#Edges in ontology with more than 1 common entity =" , ontologyEdgesSimGT1.count)
    println("#Edges in ontology with more than 1000 common entity =", ontologyEdgesSimGT1K.count)
  
  
    val uniqueVert: RDD[Long] =  ontologyEdges.flatMap(edge => Iterator(edge._1, edge._2)).distinct
    println("Number of Vertices in Ontology Graph", uniqueVert.count )
    val uniqueVertGT1 = ontologyEdgesSimGT1.flatMap(edge => Iterator(edge._1, edge._2)).distinct
    println("Number of Vertices in Ontology Graph (Edgewt> 1)", uniqueVertGT1.count )
     val uniqueVertGT1K = ontologyEdgesSimGT1K.flatMap(edge => Iterator(edge._1, edge._2)).distinct
    println("Number of Vertices in Ontology Graph (Edgewt> 1)", uniqueVertGT1K.count )
  }
  
  def getFieldsFromLine(line :String) : Array[String] = {
    return line.toLowerCase().replaceAllLiterally("<", "").replaceAllLiterally(">", "").replace(" .", "").split("\\t").map(str => str.stripPrefix(" ").stripSuffix(" "));
  }
  
  
  /* Finds entity pairs with more than 1 edge between them. */
  def groupEdgesByVertexPair(inputFile: String, sc: SparkContext): Unit = {
    val triples = sc.textFile(inputFile).filter(ln => PathSearchUtils.isValidLine(ln)).map { line =>
        val fields = getFieldsFromLine(line);
        if (fields.length == 4)
          (fields(1), fields(2), fields(3))
        else if(fields.length == 3)
          (fields(0), fields(1), fields(2))
        else {
          println("Exception reading graph file line", line)
          ("None", "None", "None")
        }
      }

    triples.cache
    println("Number of triples =", triples.count)
    val edges: RDD[(String, String, String)] = triples.map(triple => {
      val src = triple._1
      val dst = triple._3
      if(src < dst){
        (src, triple._2, dst)
      }else 
        (dst, triple._2, src)
    }).distinct
    println("Number of unique triples =", edges.count)
    val edgeCountsBetweenEntities = edges.map(v => (v._1, v._3)).countByValue
    val multiEdgeEntities = edgeCountsBetweenEntities.filter(v => v._2 > 1)
    val multiEdges2 = multiEdgeEntities.filter(v => v._2 > 2)
    println("Number of vertex pairs with multiple edges > 1 ", multiEdgeEntities.size)
    println("Number of vertex pairs with multiple edges > 2 ", multiEdges2.size)
    multiEdges2.foreach(entityPairCount => 
        println(entityPairCount._1.toString, entityPairCount._2))
  }
  
  def saveClusters(clusterWithLabelsFile: String, outputFile: String, sc: SparkContext): Unit = {
    val clusterAssignments: RDD[(Long, Int, String)] = sc.textFile(clusterWithLabelsFile)
      .map{ln => 
      val len = ln.length()
      ln.substring(1, len-1).split(",")
      }.filter(arr => (arr.length == 3)).map(v => (
          v(0).trim.toLong, 
          v(1).substring(1).trim.toInt, 
          v(2).substring(0, v(2).length()-1).trim
          ))
          
     val clusters = clusterAssignments.groupBy(v => v._2)
     .mapValues(clusterMembers => clusterMembers.map(v => (v._1, v._3)))
     
     clusters.saveAsTextFile(outputFile)
  }
  /*Given a KB, find clusters in "ontology nodes" using an LDA model 
   *  An "Ontology node" is defined as any node which is the destination of an
   *  "rdf:type"(kGraphProp.edgeLabelNodeType) relationship in KB.
   *  
   *  Each node in KB ontology is described as a collection of its neighbors
   *  For e.g : 
   *  wikicategory_cricket_captains = Set(Virat Kohli, Sachin Tendulkar, Wasim Akram)
   *  wikicategory_IPL_players = Set(Virat Kohli, Raina, Andrew Symons, Sachin tendulkar)
   *  and so on
   *  
   *  An LDA is used to find clusters of nodes with similar word(nbr) distributions
   */ 
  def mapClustersToLabels(clusterByIdFile: String, vertexLabelFile:String, 
      outputFile: String, sc : SparkContext): Unit = {
   
    val clusterAssignments: RDD[(Long, Int)] = sc.textFile(clusterByIdFile)
    .map(ln => ln.split(",")).filter(arr => (arr.length == 2)).map(v => (v(0).trim.toLong, 
        v(1).trim.toInt))
    
    val vertLabels: RDD[(Long, String)] = sc.textFile(vertexLabelFile)
    .map{ln => 
      val len = ln.length()
      ln.substring(1, len-1).split(",")
      }.filter(v => v.length == 2).map(v => (v(0).trim.toLong, v(1).trim))
    
    val nodesWithClusterIdAndLabels = clusterAssignments.join(vertLabels)
    nodesWithClusterIdAndLabels.saveAsTextFile(outputFile)
  }
  /*
  def CreateAndSaveGraphClusters(graphInputFile: String, outputFile : String, 
      numClusters : Int, numIter:Int, sc: SparkContext): 
  PowerIterationClusteringModel = {
    val ontologySim = sc.textFile(graphInputFile).map(ln => ln.split(" , ")).filter(_.length == 3)
    .map(v => (v(0).trim.toLong, v(1).trim.toLong, v(2).trim.toDouble))
    println("Number of ontology vertex pairs", ontologySim.count )
    val maxScore = ontologySim.map(v=> v._3).max
    val normalizedScore = ontologySim.filter(v => v._3 >= 2).map(keysWithValues => 
      (keysWithValues._1, keysWithValues._2, keysWithValues._3.toDouble))
    println("max score", maxScore)
    println("running with num edges ", normalizedScore.count)
    val model = runPIC(normalizedScore, numClusters, numIter)
    model.assignments.map(v => 
      v.id.toString() + " , " + v.cluster.toString).saveAsTextFile(outputFile)
    model
  }
  
  def runPIC(ontologySim : RDD[(VertexId, VertexId, Double)], numClusters: Int, maxIter: Int): PowerIterationClusteringModel = {
    println("calculating clusters, given number of vertex pairs ", ontologySim.count)
    val pic = new PowerIterationClustering().setK(numClusters).setMaxIterations(maxIter)
    val model = pic.run(ontologySim)
    
    println("done clustering, number of assignments in PIC", model.assignments.count)
  /*  model.assignments.foreach { a =>
     println(s"${a.id} -> ${a.cluster}")
    }
    */
    model.assignments.groupBy(assignment => assignment.cluster).foreach { clusterGroup =>
      println(clusterGroup._1, clusterGroup._2.size, clusterGroup._2)
    }
    model
  }
  * 
  */

  def CreateAndSaveOntologyGraph(g: Graph[String, String], outFile: String): Unit = {
    val nodesWithTypes: VertexRDD[SortedSet[VertexId]] = getNodeTypesSortedById(g)
    val nodesWithTypesPaired: VertexRDD[Iterator[(VertexId, VertexId)]] = 
      nodesWithTypes.mapValues(typeData => 
        typeData.subsets(2).filter(_.size == 2).map(_.toArray).map(v => (v(0), v(1))))
    println("Number of  nodes with type data ", nodesWithTypesPaired.count)
    
    val typePairCounts = nodesWithTypesPaired.flatMap(_._2).countByValue
    //typePairCounts.foreach(keyValue => println(keyValue._1._1,  keyValue._1._2, keyValue._2))
     val outputFile = new File(outFile)
     val outputFileWriter = new BufferedWriter(new FileWriter(outputFile))
     for(keyValue <- typePairCounts) {
       outputFileWriter.write(keyValue._1._1.toString + " , " + 
           keyValue._1._2.toString + " , " +  keyValue._2.toString + "\n")
     }
     outputFileWriter.close()
  }
  
   /* Given a KB, return a vertexRDD[Node neighbors], such that 
   * nodes receive their ontology set */
  def getNodeTypesSortedById(g: Graph[String, String]): VertexRDD[SortedSet[NbrNode]] = {
    val vertWithType =  g.aggregateMessages[SortedSet[NbrNode]](triplet => {
      if(triplet.attr == KGraphProp.edgeLabelNodeType)
        triplet.sendToSrc(SortedSet((triplet.dstId)))
      },
      (a,b)=> a++b)  
      println("Number of vertices receiving type data = ", vertWithType.count)
      vertWithType
  }
  
  /* given a vertexRDD[Neighbour Nodes], 
   * compute all pair similarity using "number of common neighbours"
   */
  def createWeighedOntologyGraph(ontologyVertWithNbrs: VertexRDD[Set[NbrNode]], 
      simThreshold: Double = 0.1): 
  RDD[(VertexId, VertexId, Double)] = {
    
    val commonNbrs: RDD[(VertexId, VertexId, Double)] = 
      ontologyVertWithNbrs.cartesian(ontologyVertWithNbrs).map{ v => 
        val node1Id = v._1._1
        val node1Nbrs = v._1._2
        val node2Id = v._2._1
        val node2Nbrs = v._2._2
        val numCommonNbrs = node1Nbrs.intersect(node2Nbrs).size
        (node1Id, node2Id, (numCommonNbrs.toDouble*2.0)/(node1Nbrs.size + node2Nbrs.size))
      }     
    commonNbrs.filter(v => ((v._3 > simThreshold) && (v._1 != v._2))).distinct()
  }
  
}