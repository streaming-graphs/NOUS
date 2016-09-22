package gov.pnnl.aristotle.algorithms.test;

import org.scalatest._
import org.apache.spark._
import org.apache.spark.rdd.RDD
import org.apache.spark.graphx._
import org.apache.log4j.Logger
import org.apache.log4j.Level
import scala.collection.Map
import breeze.linalg._

import gov.pnnl.aristotle.algorithms.entity.ColEntityDisamb
import gov.pnnl.aristotle.algorithms.{ReadHugeGraph}
import gov.pnnl.aristotle.utils.{NLPTripleParser, NLPTriple, 
  NLPTripleWithTimeWithUrl, MentionData, MatchStringCandidates}

import org.json4s._
//import org.json4s.JsonDSL._
import org.json4s.jackson.JsonMethods._
//import org.json4s.native.JsonMethods._
import org.json4s.native.Serialization._
import org.json4s.native.Serialization
import collection.immutable.ListMap

object RunColDisambRefer{
  
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
    
   
    val graphFile = args(0)
    var NLPTripleFile = args(1)
    val phraseMatchThreshold = args(2).toDouble
    val mentionToEntityMatchThreshold = args(3).toDouble
    //println("Reading triple file")
    //val allTriples: List[List[NLPTriple]] = NLPTripleParser.readTriples(NLPTripleFile, sc)
    // println("No of blocks=" + allTriples.size + "\n")
    //var allTriplesByUrl: List[List[NLPTripleWithTimeWithUrl]] = 
    //  NLPTripleParser.readTriplesWithTimestampWithUrl(NLPTripleFile, sc).values.toList
    //println("No of unique triple blocks =" + allTriplesByUrl.size + "\n")
    
    println("Reading graph")
    val g: Graph[String, String] = ReadHugeGraph.getGraph(args(0), sc)
    
    println("geting aliases")
    val verticesWithAlias = MatchStringCandidates.constructVertexRDDWithAlias(g)
    println("Done getting aliases" + verticesWithAlias.count + ", starting disambiguation\n")
   
    
    val colEntityDisObj = new ColEntityDisamb[String, String]
    var userInput: String = NLPTripleFile

    case class Contents(sentence_id:Int, entities: List[String], triples:List[String])
    case class NLPoutput(doc_name:String, nlp_output:List[Contents])
    def jsonStrToMap(jsonStr: String): NLPoutput = {
      implicit val formats = DefaultFormats
      parse(jsonStr).extract[NLPoutput]
    
    }

    val lines = scala.io.Source.fromFile(userInput).getLines.mkString 
    val originalOutput = jsonStrToMap(lines)

      
    def reverse(a: String) : Map[String, MentionData] = Map(a.split(":")(1) -> a.split(":")(0).asInstanceOf[MentionData])

    val preThree = originalOutput.nlp_output.take(3)
    for(entry <- preThree){
      val allMentionsWithData: Map[String, MentionData] = entry.entities.map(reverse).reduce(_++_) 
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
