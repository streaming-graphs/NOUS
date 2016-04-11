package gov.pnnl.aristotle.algorithms.test;

import org.scalatest._
import org.apache.spark._
import org.apache.spark.graphx._
import org.apache.log4j.Logger
import org.apache.log4j.Level
import scala.collection.Map
import breeze.linalg._

import gov.pnnl.aristotle.algorithms.ColEntityDisamb
import gov.pnnl.aristotle.algorithms.{ReadHugeGraph}
import gov.pnnl.aristotle.utils.{NLPTripleParser, NLPTriple, MentionData, MatchStringCandidates}


object TestColDisamb{
  
  def main(args: Array[String]): Unit = {
    
    val sparkConf = new SparkConf().setAppName("EntityDisamb").setMaster("local")
    val sc = new SparkContext(sparkConf)
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)
    println("starting from main")
    
    if(args.length != 4){
      println("provide <path to graph> <path to new_triples file> <StringPhraseMatchThreshold> <MentionToEntityMatchThreshold>")
      exit
    }
    
   
    
    val phraseMatchThreshold = args(2).toDouble
    val mentionToEntityMatchThreshold = args(3).toDouble
    println("Reading triple file")
    val allTriples: List[List[NLPTriple]] = NLPTripleParser.readTriples(args(1), sc)
    println("No of blocks=" + allTriples.size + "\n")
    
    println("Reading graph")
    val g: Graph[String, String] = ReadHugeGraph.getGraph(args(0), sc)
    
    println("geting aliases")
    val verticesWithAlias = MatchStringCandidates.constructVertexRDDWithAlias(g)
    println("Done getting aliases" + verticesWithAlias.count + ", starting disambiguation\n")
   
    
    val colEntityDisObj = new ColEntityDisamb[String, String]
    for(triplesInBlock <- allTriples){
      val allMentionsWithData: Map[String, MentionData] = 
        NLPTripleParser.getEntitiesWithTypeMapFromTriples(triplesInBlock)
      println("Disambiguating follwing entities together as a block", allMentionsWithData.size)
      allMentionsWithData.foreach(v => println(v._1 + "--(type, evidence weight)-->"+ v._2.toString))  
      
      val mentionMatches: Map[String, ((VertexId, String), Double)] = colEntityDisObj.disambiguate(
          allMentionsWithData, g, verticesWithAlias, phraseMatchThreshold, mentionToEntityMatchThreshold, 0.00001)
      
      println
      println("Disambiguation completed:")
      mentionMatches.foreach((MentionWithMatch) => println(
          MentionWithMatch._1 , "=>" , MentionWithMatch._2._1._1,  MentionWithMatch._2._1._2, 
          MentionWithMatch._2._2))              
     
    }
   
  }
}
