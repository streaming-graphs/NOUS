package gov.pnnl.aristotle.utils

import org.apache.spark._
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import org.apache.spark.rdd.RDD
import scala.math.Ordering
import scala.util.Sorting
import scala.collection.Set
import org.apache.log4j.Logger
import org.apache.log4j.Level
import gov.pnnl.aristotle.utils.NodeProp
import gov.pnnl.aristotle.algorithms.ReadHugeGraph
import gov.pnnl.aristotle.utils.NodeProp

object GraphWalk {
  
  def time[R](block: => R, function_desc : String ): R = {
    val t0 = System.nanoTime()
    val result = block    // call-by-name
    val t1 = System.nanoTime()
    println("Elapsed time: " + function_desc + " =" + (t1 - t0)*1e-9 + "s")
    result
  }

    
  /* each edge is of type (destid, destlabel, predicate)*/
  def createString(allPaths: List[List[(Long, String, String)]]): String ={
   var result = ""
   for(path <- allPaths){ for(edge <- path){ result += "<->{" + edge._3 + "}<->" + edge._2 }
    result += "\n" 
   }  
   return result
  }
   def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("PathSearch").setMaster("local")
    val sc = new SparkContext(sparkConf)
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)
    println("starting from main")
    if(args.length < 5 ) {
      println("Usage <pathToGraph> <EntityLabel> <TerminatinLabel> <CountTerminationLabel>")
      exit
    }    
    
    val g: Graph[String, String] = time(ReadHugeGraph.getGraph(args(0), sc), "in Readgraph")
    IterateGraphWalk(args, g, sc)
    sc.stop() 
   }
   
   def IterateGraphWalk(args: Array[String], g:Graph[String, String], sc: SparkContext) ={
     var moreEntities = true
      
      var entityLabel = args(1)
      var terminationLabel = args(2)
      var maxOccur = args(3).toInt
      val outDir = args(4)
   
      while(moreEntities == true){
        val outFile = outDir + "/" + entityLabel + "_profile.txt"
        val profile = GraphWalk(entityLabel, terminationLabel, g, maxOccur, outFile)      
        val userInput: String = readLine("Done profiling entity" + entityLabel + "do you wish to continue(Y/N):")
        if(userInput.toLowerCase() == "n" || userInput.toLowerCase() == "no") exit
        val inputValues = readLine("Enter <EntityLabel>,<TerminatinLabel>,<CountTerminationLabel>, comma separated:\n").split(",")
        entityLabel = inputValues(0)
        terminationLabel = inputValues(1)
        maxOccur  = inputValues(2).toInt
      }
   }
  
   def GraphWalk(entityLabel:String, terminatingLabel:String,  graph: Graph[String, String], 
      maxOccur: Int, outFile: String, maxHop:Int = 2): Unit = {
    
    val node: (VertexId, String) = MapLabelToId(entityLabel, graph)
    val nodeid = node._1
    if(nodeid == -1) return 
    //val oneHopNbrs: VertexRDD[Set[(Long, String, String)]] = NodeProp.getOneHopNbrIdsLabels(graph, Array(nodeid))
    val oneHopNbrs: VertexRDD[Set[(Long, String, String)]] = null
    
    
    // Get one hop nbrs ids
    val profileData: Set[(Long, String, String)] = oneHopNbrs.filter(v => v._1 == nodeid).map(v => v._2).collect.apply(0)
    var result = ""
    for(v <- profileData) {
      result = result + " { " + v._3 + " } " + v._2 + "\n"
    }
    println("ONE HOP PROFILE : " +  node._2 + " =  " + profileData.size + "\n" + result)
    //PathSearch.writeToFile(outFile, result) 
    if(checkMatches(profileData, terminatingLabel, maxOccur)) { 
      return
    }
    
    // get 2 hop paths
    val oneHopNbrIds : Array[Long] = oneHopNbrs.filter(v => v._1 != nodeid).map(v => v._1).collect()
    //val twoHopNbrs:VertexRDD[Set[(Long, String, String)]]  = NodeProp.getOneHopNbrIdsLabels(graph, oneHopNbrIds).filter(v => oneHopNbrIds.contains(v._1))
    val twoHopNbrs:VertexRDD[Set[(Long, String, String)]]  = null
    println("TWO HOP PROFILE:" + twoHopNbrs.count)
    //twoHopNbrs.foreach(println(_))
    
    val twoHopPaths: RDD[String] = twoHopNbrs.map(twoHopPath => {
      var result = ""
      val commonNbr: (Long, String, String) = profileData.find(_._1 == twoHopPath._1).get
      for(path <- twoHopPath._2) {
        result = result + "{ " + commonNbr._3 + " } " + commonNbr._2 + " { " + path._3 + " } " + path._2 + "\n"  
      }
      result
    })

   println("TWO HOP PROFILE " + node._2 )
   twoHopPaths.foreach(println(_))
   //PathSearch.writeToFile(outFile, twoHopPaths.toString)
  }
   
  
  // test this
  def checkMatches(profileData: Set[(Long, String, String)], terminatingLabel: String, maxOccur:Int): Boolean  = {
    return (profileData.map(v => v._2).filter(v => v.containsSlice(terminatingLabel)).size > maxOccur)
  }
  
  def MapLabelToId(label: String, g: Graph[String, String]): (VertexId, String) = {
    val candidates : Array[(VertexId, String)] = g.vertices.filter(v => v._2.toLowerCase().startsWith(label.toLowerCase())).collect
    if(candidates.length < 1 ){
      println("No Match found for the provided label" + label +  " Try another" )
      return ((-1, "None"))
    }
    Sorting.quickSort(candidates)(Ordering.by[(VertexId, String), String](_._2))
    println("Choosing first one, among available matches for " + label + " = ")
    candidates.foreach(v => println(v._2)) //(v = > println(v._2))
    return candidates(0)
    
    
  }
}