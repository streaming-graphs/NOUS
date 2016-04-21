package gov.pnnl.aristotle.algorithms

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import org.apache.spark.Aggregator
import org.apache.spark.rdd.RDD
import scala.collection.Set
import scala.util.Sorting
import scala.math.Ordering
import scala.collection.mutable.HashMap
import org.apache.log4j.Logger
import org.apache.log4j.Level
import gov.pnnl.aristotle.utils.{NLPTripleParser}
import java.io.{BufferedWriter, OutputStreamWriter, FileOutputStream}
import gov.pnnl.aristotle.utils._



object EntityDisambiguation {

  def main(args: Array[String]): Unit = {   
    val sparkConf = new SparkConf().setAppName("EntityDisamb")
    val sc = new SparkContext(sparkConf)
     Logger.getLogger("org").setLevel(Level.OFF)
     Logger.getLogger("akka").setLevel(Level.OFF)
    println("starting from main")
    if(args.length != 2){
      println("provide <path to graph> <path to new_triples file>")
      exit
    }
    
    val allTriples: List[List[NLPTriple]] = NLPTripleParser.readTriples(args(1), sc)
    println("No of blocks=" + allTriples.size)
    println("size of each block=")
    allTriples.foreach(v => println(v.size))
    
    val g: Graph[String, String] = ReadHugeGraph.getGraph(args(0), sc)
    println("Done reading graph" + g.vertices.count)
    
    println("geting aliases")
    val verticesWithAlias = MatchStringCandidates.constructVertexRDDWithAlias(g)
    println("Done getting aliases" + verticesWithAlias.count + ", starting disambiguation\n")
    
    val graphNbrs: VertexRDD[Set[String]] = NodeProp.getOneHopNbrLabels(g)
    for(triplesInBlock <- allTriples){
      val entityMap = NLPTripleParser.getEntitiesWithTypeMapFromTriples(triplesInBlock)
      println("No. of unique entities in block", entityMap.size)
      entityMap.foreach{v => 
        println(" Calculating entity Disambguation on " + v._1 +"----entityData (type)--->"+ v._2)  
        val allAnswers = disambiguate(g, verticesWithAlias, v._1 , entityMap.keys.toSet, 0.8, graphNbrs, v._2.entityType)
        val topAnswers = allAnswers.top(10)(Ordering.by[(VertexId, (String, Double)), Double](_._2._2))
        topAnswers.foreach(v => println(v._1, v._2._1, v._2._2))
      }
      println("end of block")
    }
    
  }
  
   

 
  

  
  /*Given a graph, find a matching entity to "serachForMe" where  context is given as a set of other entities */
  def disambiguate(g: Graph[String, String], verticesWithAlias: VertexRDD[String], 
      searchForMe : String, queryContextEntities: Set[String], 
      scoreThreshold: Double, graphNbrs:VertexRDD[Set[String]], suggestedEntityType: String = "", 
      phraseMatchThreshold: Double = 0.75): VertexRDD[(String, Double)]= {
       
    println("Searching for=" + searchForMe + " in:" + queryContextEntities)
   
    // Get all candidates matches
    println("Finding candidates for ", searchForMe)
    //val nodesWithStringMatches : Array[Long] = MatchStringCandidates.getMatches(searchForMe, g)
    val nodesWithStringMatchesRDD : RDD[( String, Iterable[(VertexId, String)] )] = 
      MatchStringCandidates.getMatchesRDDWithAlias(List(searchForMe), verticesWithAlias, phraseMatchThreshold)
    val nodesWithStringMatchesArray: Array[(VertexId, String)] = nodesWithStringMatchesRDD.collectAsMap.get(searchForMe).getOrElse(Iterable.empty).toArray
    val nodesWithStringMatches: Array[VertexId] = nodesWithStringMatchesArray.map(v => v._1)
    println("NUm Nodes that match candidate string:" + searchForMe + "=" + nodesWithStringMatchesRDD.count)

   
    // 1. Calculate Popularity SCore of each candidate as #incomingLinks/TotalLinks 
    println("Finding popularity of matching candidates")
    val validRelations : String = KGraphProp.edgeLabelNeighbourEntity
    val popularScore: VertexRDD[Double] = popularityScore(g, nodesWithStringMatches, validRelations)
    println("Popular nodes (> 1% connections) and their scores(#neighbours/totalLinks):")
    val score_with_labels = g.vertices.innerJoin(popularScore)((id, u, v)=> (u,v))
    score_with_labels.foreach(v => if(v._2._2 > .01) println(v._2._1 + " Popular score =" + v._2._2))
    score_with_labels.unpersist(false)
    //exit()
    
       
    // 2. Compute Context score similarity :  
    //val simScore: VertexRDD[Double] = similarityScore(g, nodesWithStringMatches, queryContextText, sennaConfig)
    val simScore: VertexRDD[Double] = similarityScore(nodesWithStringMatches, queryContextEntities, graphNbrs)  
    
    val typeSimScore = getTypeSimScore(g,  nodesWithStringMatches, suggestedEntityType)
     println("Size of type similiarity RDD", typeSimScore.count)
    // Combine Scores
    val wtPopularity = 0.25
    val wtSimilarity = 0.7
    val wtTypeSimilarity = 0.05
     
    println("\nCalculating total scores")
    val tempScore: VertexRDD[Double] = simScore.innerZipJoin[Double, Double](popularScore) {(id, u,v) => u*wtSimilarity + v*wtPopularity } 
    println("Size of score RDD tenp", tempScore.count)
    val totalScore: VertexRDD[Double] = tempScore.leftJoin(typeSimScore)((id, u,v) => u*(wtSimilarity+wtPopularity) + v.get*wtTypeSimilarity)
    
    println("completed score join, final score set size =", totalScore.count) 
    
    println("Top scoring vetrtices:")
   //total_score.foreach(v => if(v._2 > score_threshold) println(v._1, "total score=", v._2)) 
   
   val resultVerticesId : Array[Long] = totalScore.filter(v=> v._2 > scoreThreshold).map(v=> v._1).collect
   val vertex_with_labels:VertexRDD[String] = g.vertices.filter(v =>  resultVerticesId.contains(v._1))
   val final_matches_with_labels_and_score: VertexRDD[(String, Double)] = vertex_with_labels.innerZipJoin(totalScore)((id,label,score) => (label, score))
   
   final_matches_with_labels_and_score//.top(10)(Ordering.by[(VertexId, (String, Double)), Double](_._2._2))   
  }

  //Do better here
  //Map PER -> person
  // map LOC -> location
  //consider yago type heirarchy
  def getTypeSimScore(g:Graph[String, String], candidates: Array[Long], suggestedType: String) : VertexRDD[Double] ={
    val candidateTypes: VertexRDD[String] = NodeProp.getNodeType(g, candidates)
    println("size of RDD containing node Type", candidateTypes.count)
    val typeMatch : VertexRDD[Double] =  candidateTypes.mapValues(v => KGraphProp.compareTypesUsingHeirarchy(suggestedType, v))
    println("size of RDD after scoring similarity in types",typeMatch.count )
    return typeMatch
  }
  
  def popularityScore(g: Graph[String, String], candidates : Array[Long], validRelations: String) : VertexRDD[Double] = {
        //Can we do better here: use PageRank or  refine the links considered (.e.g. only a particular type of relation)
    val countWikiLinksOfAllCandidates : VertexRDD[Int] = NodeProp.getWikiLinks(candidates, validRelations, false, g).mapValues(v=>v.length)
    println("Num matching nodes with linksTo relation" , countWikiLinksOfAllCandidates.count)
    
    val totalLinks: Double =  countWikiLinksOfAllCandidates.aggregate(0)( 
        (count1, v) => count1 + v._2, 
        (u, v) => u+v
        )
    
    println("Total Links =", totalLinks)
    val popularScore : VertexRDD[Double] =  countWikiLinksOfAllCandidates.mapValues(v => v/totalLinks).persist
    popularScore
  }
  
  def similarityScore(candidates : Array[Long], mention_keywords:Set[String], graphNbrs : VertexRDD[Set[String]]) : VertexRDD[Double]= {
    println("calculating similarity (context) score")
    // Get candidates relevant neighbourhood
    // Options : get two hop neighbrs Assign weights to 1 hop neighbours  vs second hop neighbours
    // Also normalize weight by the degree of the neighbor
    val candidate_keywords: VertexRDD[Set[String]] = graphNbrs.filter(v => candidates.contains(v._1))
   
    //TODO : how do we compare the feature vectors correctly, Use ClueWeb
    val keywords_with_similarity_score: VertexRDD[(Set[String], Double)] = candidate_keywords.mapValues(v=> (v, Context.simScore(mention_keywords, v)))
   
    println("Most similar nodes with at least 1 context match")
    keywords_with_similarity_score.foreach(v=> if(v._2._2 > 0) println(v._1, "similarity score" , v._2._2, v._2._1))

    val similarityScore : VertexRDD[Double] = keywords_with_similarity_score.mapValues(v => v._2)
    //similarityScore.persist()
    return similarityScore
  }
  
  def similarityScore(candidates : Array[Long], textContext : String, sennaConfig: SennaConfig,  graphNbrs : VertexRDD[Set[String]]) : VertexRDD[Double]= {
    //Get context keywords and call SimilarityScore between contexts
    val mention_keywords : Set[String]= Context.getObjectList(textContext, sennaConfig)
    println("Mention Keywords :" + mention_keywords ) 
    similarityScore(candidates, mention_keywords, graphNbrs)
  }
  
  def getBestStringMatchLabel(labels: Array[String], g: Graph[String, String]): Array[(VertexId, String)] = {
    val candidateRDD = g.vertices.mapValues(_.toLowerCase()).filter(v => {
      var foundCand = false
      for(label <- labels){
        if(label.startsWith(v._2)) foundCand = true
      }
      foundCand
    })
   
    if(candidateRDD.count < 1){
      println("No Match found for the provided labels ", labels.toString)
      return Array.empty[(VertexId, String)]
    }
    
    val candidates = candidateRDD.collect
    val results = Array.ofDim[(VertexId, String)](labels.length)
    var i = 0
    for(label <- labels) {
      val candidatesForLabel = candidates.filter(_._2.startsWith(label))
      if(candidatesForLabel.length == 0) {
        results(i) = (-1, "")
      } else {
        val sortedcandidatesForLabel = Sorting.quickSort(candidatesForLabel)(Ordering.by[(VertexId, String), String](_._2))
        //val finalCandidateForLabel = sortedcandidatesForLabel(0)
      }
    }
    return results
  }
 
}