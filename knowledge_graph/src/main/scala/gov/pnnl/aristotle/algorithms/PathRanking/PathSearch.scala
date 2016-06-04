package gov.pnnl.aristotle.algorithms

import org.apache.spark._
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import org.apache.spark.Aggregator
import org.apache.spark.rdd.RDD
import scala.math.Ordering
import scala.util.Sorting
import scala.collection.Set
import scala.collection.mutable.ArrayBuffer
import scala.collection.mutable.HashMap
import org.apache.log4j.Logger
import org.apache.log4j.Level
import scala.io.Source
import java.nio.file.{Paths, Files}
import java.nio.charset.StandardCharsets
import gov.pnnl.aristotle.utils.{Gen_Utils, FileLoader}
import gov.pnnl.aristotle.utils.NodeProp
import gov.pnnl.aristotle.algorithms.entity.EntityDisambiguation

object PathSearch {


  def main(args: Array[String]): Unit = {     
    val sparkConf = new SparkConf().setAppName("PathSearch")
    val sc = new SparkContext(sparkConf)
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)
    println("starting from main")
    if(args.length < 3 || args.length > 4) {      println("Usage <pathToGraph> <Entity1> <Entity2> < outputDir> or")
      println("Usage <pathToGraphFile> <entity_pair_file> < outputFile> ")
      exit
    }    
    
    val g: Graph[String, String] = Gen_Utils.time(ReadHugeGraph.getGraph(args(0), sc), "in Readgraph")
    val allGraphNbrs: VertexRDD[Set[(Long, String, String)]] = Gen_Utils.time(NodeProp.getOneHopNbrIdsLabels(g), "collecting one hop nbrs ")
    
    
    if(args.length == 4){
      var moreEntities = true
      val outDir = args(3)
      val initialLabels: Array[String] = Array(args(1), args(2)) 
      var entityLabels = initialLabels
      
      while(moreEntities == true){
    	val allPaths = Gen_Utils.time(FindPaths(entityLabels, g, allGraphNbrs, sc), "finding path")
        val result = entityLabels(0) +"," + entityLabels(1) + "\n" + createString(allPaths)
        val outFile = outDir + "/" + entityLabels(0) + "_" + entityLabels(1)+ ".txt"
        println(result)
        Gen_Utils.writeToFile(outFile, result)
        
        val userInput: String = readLine("Done finding path b/w previous entity pair, do you wish to continue(Yes/no):")
        if(userInput.toLowerCase() == "no") exit
        entityLabels = readLine("Enter entity label, separated by comma : ").split(",")
      }
    } else if (args.length == 3) {
      println("Reading entity Pairs from", args(1) )
      val lines = Source.fromFile(args(1)).getLines()
      val outDir = args(2)
      for(line <- lines) {
         val fields = line.split(",")
         if(fields.length == 3) {
            println("Finding path b/w", fields(0), " and ", fields(1))
            val entityLabels = Array(fields(0), fields(1)) 
            val allPaths = Gen_Utils.time(FindPaths(entityLabels, g, allGraphNbrs, sc), "finding path")
            val outputFile = outDir +  "/" + fields(0) + "_" +fields(1) + ".out"
            var result =  line + "\n" + createString(allPaths)
            println(result)
            Gen_Utils.writeToFile(outputFile, result)
         }
      }
    }
    
    sc.stop() 
  }
  
  def FindPaths(entityLabels: Array[String], g:Graph[String, String],allGraphNbrs: VertexRDD[Set[(Long, String, String)]], sc:SparkContext):  List[List[(Long, String, String)]] ={

    if(entityLabels.length != 2) { 
      println("Provide only 2 labels for path finding ")
      return List.empty 
    }
    
    // Find ids's of src and dest
    println("Finding graph ids for ")
    entityLabels.foreach(println(_))
    val idLabel : Array[(VertexId, String)]  = EntityDisambiguation.getBestStringMatchLabel(entityLabels, g)
    if((idLabel(0)._1 == -1) || (idLabel(1)._1 == -1)) { 
      print("Cannot find matching entities for given labels, (src, dest) match =", idLabel(0)._2, idLabel(1)._2)
      return List.empty 
    }
    println("Choosing first one, among available matches for " + entityLabels(0) + " = " +  idLabel(0)._2)
    println("Choosing first one, among available matches for " + entityLabels(1) + " = " +  idLabel(1)._2)
    
    //Now find Paths between these known vertices
    return FindPathKnownEntity(idLabel(0), idLabel(1), allGraphNbrs, sc)
  }
  
  def FindPaths(entityLabels: Array[String], g:Graph[String, String], sc:SparkContext, ignoreRelations: Set[String]=Set.empty):  List[List[(Long, String, String)]] ={
    val allGraphNbrs: VertexRDD[Set[(Long, String, String)]] = Gen_Utils.time(NodeProp.getOneHopNbrIdsLabels(g, ignoreRelations), "collecting one hop nbrs ")
    allGraphNbrs.persist()
    Gen_Utils.time(FindPaths(entityLabels, g, allGraphNbrs, sc), "finding path")
  }

   /* given 2 node ids, finds all 0-4 hop path b/w them */
  def FindPathKnownEntity(src: (VertexId, String), dest: (VertexId, String), allGraphNbrs:VertexRDD[Set[(Long, String, String)]], sc:SparkContext ): List[List[(Long, String, String)]] ={

   val srcId: VertexId = src._1; val srcLabel: String = src._2
   val dstId: VertexId = dest._1; val dstLabel: String = dest._2  
   val srcDestNbrs: VertexRDD[Set[(Long, String, String)]] = allGraphNbrs.filter(v => (v._1 == src._1) || v._1 == dest._1)
   val snbrs_temp: Set[(Long, String, String)] = srcDestNbrs.filter(v => v._1 == srcId).map(v=>v._2).collect.apply(0)
   val dnbrs_temp: Set[(Long, String, String)] = srcDestNbrs.filter(v => v._1 == dstId).map(v=> v._2).collect.apply(0)  
   val snbrs = sc.broadcast(snbrs_temp)
   val dnbrs = sc.broadcast(dnbrs_temp)
   println("Number of source nbrs", snbrs_temp.size, "destination nbrs ", dnbrs_temp.size)
       
   /* initialize answers*/
   var flagDone = false
   var paths = List[List[(Long, String, String)]]() // Nil
     
   /* If connected with single edge path, add that to list */
    val directPath = snbrs.value.filter(_._1 == dstId)
    for(p <- directPath)
      paths = paths ++ List(List(p))
    
    println(srcLabel + " and " + dstLabel + " are connected through " + directPath.size + " direct edges" )
   
 
    /* Collect 1 Hop Paths (Note that the common node may be connected with src and dest  using different labels) */
    println("Finding 2 edge(1 hop) paths")
    var commonNbrs: Set[(Long, String, String)] = CommonNbrs(snbrs.value, dnbrs.value)
    println(srcLabel + " and " + dstLabel + " are connected through " + commonNbrs.size + " 1 hop edges" )     
    for(nbr <- commonNbrs) {
      val snbrEdge = snbrs.value.find(_._1 == nbr._1 ).get
      val dnbrEdge = dnbrs.value.find(_._1 == nbr._1).get
      val edgeToAddToPath: (Long, String, String) = (dstId, dstLabel, dnbrEdge._3) 
      paths = paths ++ List(List(snbrEdge, edgeToAddToPath)) 
    }
  
   /* Collect 2 and 3 hop paths */
   /* Get neighbour RDD's for all vertices that are either source or destination neighbours*/
   /* Note that since RDD's cannot be double looped we do a collect() on one of RDD and keep another RDD distributed */
    val snbrIds: Set[Long] = snbrs.value.map(_._1)
    val dnbrIds: Set[Long] = dnbrs.value.map(_._1)
    val srcFrontier = allGraphNbrs.filter(v =>snbrIds.contains(v._1))
   // val dstFrontier: VertexRDD[Set[(Long, String)]] = allGraphNbrs.filter(v=> dnbrIds.contains(v._1))
    val dstFrontierTemp = allGraphNbrs.filter(v=> dnbrIds.contains(v._1)).collect
    val dstFrontier = sc.broadcast(dstFrontierTemp)
    srcFrontier.persist
    
   
    println("source 1 hop neighbours = ", srcFrontier.count)
    println("destination 1 hop neighbours = ", dstFrontier.value.length)
   
    /* No nodes that are part of current path should appear as part of subpath i..e no looping around same node to generate answers*/
    var avoidNodes = sc.broadcast(Set(srcId, dstId))
    //println("Avoiding nodes ", avoidNodes.toString)
    
    //val allids = srcFrontier.map(id => (id.toString)).reduce((a,b) =>  a + "  = "+ b)
    
    val temppath: List[List[( Long, String, String)]] = srcFrontier.map(srcNbr =>  {
      val src1HopNbrLabel: (Long, String, String) = snbrs.value.find(_._1 == srcNbr._1).get
      /* get two hop neighbours of source , excluding source itself (to avoid cycles ) */
      val nbrOfSrc1HopNbr: Set[(Long, String, String)] = srcNbr._2.filterNot(v => v._1 == srcId)
      //println("lOOking for neighbours of ", src1HopNbr.toString, ", neighbours=", nbrOfSrc1HopNbr.toString)
      var lpaths :List[List[(Long,String, String)]] = List.empty
      for(destNbr <- dstFrontier.value) {
        val dest1HopNbrLabel: (Long, String, String) = dnbrs.value.find(_._1 == destNbr._1).get
        val nbrOfDest1HopNbr: Set[(Long, String, String)] = destNbr._2.filterNot(v => v._1 == dstId)
        val new2to3hopPaths:List[List[(Long, String, String)]] = Get2to3HopPaths(src1HopNbrLabel, nbrOfSrc1HopNbr,dest1HopNbrLabel, nbrOfDest1HopNbr, avoidNodes.value, dest)
         if(new2to3hopPaths.isEmpty == false) {  
           println("Adding ", new2to3hopPaths.length, " new paths to list")
           lpaths = lpaths ++ new2to3hopPaths
         } 
      }
      lpaths
    }).reduce((a,b) => a ++ b )
        
    println("Found num paths" , paths.length + temppath.length)
    return paths  ++ temppath
  }
  
    
  def Get2to3HopPaths(node1: (Long, String, String), node1nbrs: Set[(Long, String, String)], node2: (Long, String, String) , node2nbrs: Set[(Long, String, String)], avoidNodes: Set[Long], dest: (Long, String)): List[List[(Long, String, String)]] = {
      
  
    /* if you have direct edges or nodes are alreay path of the path , return null */
    if(avoidNodes.exists(_ == node1._1) || avoidNodes.exists(_ == node2._1) || node1._1 == node2._1) { return List.empty}
    
    var paths: List[List[(Long, String, String)]] = List.empty
    //println("Finding path b/w  node neighbours", node1, node1nbrs.toString)
    //println("Finding path b/w  node neighbours", node2, node2nbrs.toString)
    /* for 3 hop paths */
    val commonNbrs = CommonNbrs(node1nbrs, node2nbrs).filterNot(v => avoidNodes.exists(_ == v._1))
    if(commonNbrs.size == 0) { 
      //println("No common neighbours, no 4 hop path found ")
      }
    else {
      for(node <- commonNbrs){
       val node1Edge = node1nbrs.find(_._1 == node._1 ).get
      val node2Edge = node2nbrs.find(_._1 == node._1).get
      val edge1 = (node2._1, node2._2, node2Edge._3)
       val edge2 = (dest._1, dest._2, node2._3)
        paths = paths ++ List(List(node1, node1Edge, edge1, edge2))
      }
    }
    
    /* for 2 hop paths */
    /* src2hop to dest1hop */
    //println("Looking for 2 hop path ")
    if(node1nbrs.exists(_._1 == node2._1)){
      val edge1 = node1nbrs.find(_._1 == node2._1).get
      val edge2 = (dest._1, dest._2, node2._3)
      paths = paths ++ List(List(node1,edge1,edge2))
    }
     //println("No of 2 and 3  hop paths between" , node1, node2, " = " +  paths.length)
 
    return paths 
  
  }
  
  


  def ConvertPathToText(entityLabel: String, path : List[(Long, String, String)],predicateToTextMap: HashMap[String, (String, Boolean)] , sc:SparkContext): String ={
    var result = ""
    var srcLabel = entityLabel
    for(edge <- path) {
      var predText= "";
      
      if(predicateToTextMap.contains(edge._3)) {
        val mapping = predicateToTextMap.get(edge._3).get
        predText = mapping._1
      	val flipSrcDest = mapping._2
      	var temp:String = ""
      	if(flipSrcDest) {
      		 temp = edge._2 + " " + predText + " " + srcLabel +". "
      	} else  { 
        	 temp = srcLabel +" " + predText + " " + edge._2 + ". "
      	}
        //result = temp +: result
        result = result + temp
        
      } else {
        println(" Skipping realtion", edge._3)
      }
      srcLabel = edge._2
    } 
    return result
  }
  
  /* each edge is of type (destid, destlabel, predicate)*/
  def createString(allPaths: List[List[(Long, String, String)]]): String ={
   var result = ""
   for(path <- allPaths){ for(edge <- path){ result += "<->{" + edge._3 + "}<->" + edge._2 }
    result += "\n" 
   }  
   return result
  }
  
  /* GIven 2  sets of form ( id, label), finds common members based on id */
  def CommonNbrs(snbrs :Set[(Long,String, String)], dnbrs : Set[(Long, String, String)]): Set[(Long, String, String)] = {
    var commonNbrs = Set.empty[(Long, String, String)]
    //println("common nbrs")
    for (s <- snbrs) {
      for (d <- dnbrs){
       //if(s.stripPrefix(" ").stripSuffix(" ").equalsIgnoreCase(d.stripPrefix(" ").stripSuffix(" "))) {
          if(s._1 == d._1) { 
            commonNbrs += s  
          }
      }
    }   
    return commonNbrs
  }

}