package gov.pnnl.aristotle.algorithms.test
import org.apache.spark._
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import org.apache.spark.rdd.RDD
import scala.collection.Set
import org.apache.log4j.Logger
import org.apache.log4j.Level
import gov.pnnl.aristotle.algorithms._
import java.io.PrintWriter
import java.io.File
import java.io.InputStream
import org.apache.commons.io.IOUtils
import gov.pnnl.aristotle.utils.NodeProp
import gov.pnnl.aristotle.utils.MatchStringCandidates



object Driver {
        
  def main(args: Array[String]): Unit = {
    
    
    val sparkConf = new SparkConf().setAppName("Driver").setMaster("local")
    val sc = new SparkContext(sparkConf)
    Logger.getLogger("org").setLevel(Level.OFF)
     Logger.getLogger("akka").setLevel(Level.OFF)
     println("starting from main")
  
    if(args.length < 1) println("Not enough arguments, Usage:<pathToGraphFile(s)>")
    
    val graphFilename :String = args(0)
    println("Reading graph")
    val g : Graph[String, String] =  ReadHugeGraph.getGraph(graphFilename, sc)
    
    var mode = "listen"
    while(mode.toLowerCase() != "exit"){
       
      val userInput: Array[String] = readLine("Enter mode -profile [-type <type>]|[-entity <entity>]  OR -search  <entity> OR -path <entity1> <entity2> <numPaths> \n").
      stripPrefix(" ").stripSuffix(" ").split(" ")
      
       mode = userInput(0).toLowerCase()
       if(mode =="-profile" && userInput.length >= 2) {
         var filename: String =  "sampleProfileOutput.txt"
         val writerSG = new PrintWriter(new File(filename))
    	 if(userInput(1) == "-type") {
    	   val profileArgs = userInput(2)
    	   val typemap = GraphProfiling.getAugmentedTypeMap(g, writerSG);
	       //GraphProfiling.showNodeTypeProfile(typemap, profileArgs)
	     } else if(userInput(1) == "-entity") {
    	   val profileArgs = userInput(2)
    	   val augGraph = GraphProfiling.getAugmentedGraph(g, writerSG)
	       GraphProfiling.showNodeProfile(augGraph, profileArgs)
    	 } else {
    	   // check for both
    	   val profileArgs = userInput(1)
	       val typemap = GraphProfiling.getAugmentedTypeMap(g, writerSG);
	       //GraphProfiling.showNodeTypeProfile(typemap, profileArgs)
	       val augGraph = GraphProfiling.getAugmentedGraph(g, writerSG)
	       GraphProfiling.showNodeProfile(augGraph, profileArgs)
    	 }
      } 
      
      else if(mode == "-search"  && userInput.length == 2) {
         val queryLabel =  userInput(1)
         val nodesWithStringMatchesRDD : RDD[(VertexId, String)] = MatchStringCandidates.getMatchesRDD(queryLabel, g)
         println("NUm Nodes that match candidate string:" + queryLabel + "=" + nodesWithStringMatchesRDD.count)
         val candidates = nodesWithStringMatchesRDD.map(v => v._1).collect()

         val nbrLabels : VertexRDD[Set[String]] =  NodeProp.getOneHopNbrLabels(g).filter(v=> candidates.contains(v._1))
         
         val candidateLabels:VertexRDD[String] = g.vertices.filter(v =>  candidates.contains(v._1))
         val verticesLabelsNeighbours: VertexRDD[(String, Set[String])] = candidateLabels.innerZipJoin(nbrLabels)((id,label,nbrlist) => (label, nbrlist))
         
         verticesLabelsNeighbours.foreach(v=>  println("ENTITY: " + v._2._1 + ", Neighbour Labels = "  + v._2._2.toString))
         
    } else if( mode == "-path" && userInput.length >= 3) {
         val entityLabel1 = userInput(1)
         val entityLabel2 = userInput(2)
         val entityLabels = Array(entityLabel1, entityLabel2)
         var maxLength = 4
         println("Trying to find " + maxLength + "  length path between "+ entityLabel1 , entityLabel2 )
         val all2EdgePaths: List[List[(Long, String, String)]] = PathSearch.FindPaths(entityLabels, g, sc)

         for(path <- all2EdgePaths){
           for(edge <- path){ print(edge._2 + "->") }
           println()
         }
         
    } else {
      println("Invalid arguments", args)
    }
  }
  println("Exiting..")
}
}
