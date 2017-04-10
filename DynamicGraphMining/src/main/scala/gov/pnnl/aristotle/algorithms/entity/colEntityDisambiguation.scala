package gov.pnnl.aristotle.algorithms.entity

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.log4j.Logger
import org.apache.log4j.Level

import org.apache.spark.graphx._
import scala.collection.Set
import scala.collection.Map
import gov.pnnl.aristotle.utils.{NLPTripleParser}
import gov.pnnl.aristotle.utils._
import breeze.linalg.Transpose.LiftApply
import gov.pnnl.aristotle.algorithms.ReadHugeGraph
import ColEntityTypes._

/* This class implements the  collective entity linking algorithm as described in the paper 
 * "Collective entity linking in web text: a graph-based method". 
 * Please cite the following when using this class
 
 @inproceedings{Han:2011:CEL:2009916.2010019,
 author = {Han, Xianpei and Sun, Le and Zhao, Jun},
 title = {Collective Entity Linking in Web Text: A Graph-based Method},
 series = {SIGIR '11},
 year = {2011},
 url = {http://doi.acm.org/10.1145/2009916.2010019},
 * 
 */
class ColEntityDisamb[VD, ED] {
     
  /* creates a referent graph from the list of mentions and given a base knowledge graph */
  /* Need to handle case where a mention is not matched to any entity*/
  def disambiguate(allMentionsWithData: Map[String, MentionData], g: Graph[String, String], 
      vertexRDDWithAlias: VertexRDD[String],
      phraseMatchThreshold: Double = 0.7, mentionToEntityMatchThreshold: Double = 0.0, 
      lambda: Double= 0.1 ): 
      Map[Mention, (Entity, SimScore)] = {
    
    
    val numVerticesInGraph = g.vertices.count
    
    //1. Get candidates entities for each mention
    val mentionLabels: List[Mention] = allMentionsWithData.keys.toList
    val mentionToEntityMap: Map[Mention, Iterable[Entity]] = 
      getMatchCandidates(mentionLabels, vertexRDDWithAlias, phraseMatchThreshold)
    println("\nCandidate matches for each mention (in main function):")
    mentionToEntityMap.foreach(matches => println(matches._1 + "=>"  + matches._2.toString))
    // if no or only single candidates are found for each mention, then return
    var isDisambNeeded = false;
    for(cand <- mentionToEntityMap.values){
      if(cand.size > 1){
        isDisambNeeded = true;
      }
    }
    if(!isDisambNeeded) {
      println("No disambigutaion required as no mentions have  multiple cadidates")
      exit
    }
      
     //1.1 If unable to find any potential candidate for a given mention,
     // will create new entity in knowledge graph as nous:mention
    val mentionsWithoutEntityMatch: Set[Mention] = mentionLabels.toSet--mentionToEntityMap.keys.toSet
    val mentionsWithData = allMentionsWithData.--(mentionsWithoutEntityMatch)
    println("Mentions without any entity match")
    mentionsWithoutEntityMatch.foreach(println(_))
    val finalMatches = initMapWithNewEntities(mentionsWithoutEntityMatch)
    if(mentionsWithoutEntityMatch.size == mentionLabels.size)
      return finalMatches.toMap
    
    
    // 2. Score each candidate entity using graph neighborhood data
    val candidateIds: Array[NodeId] = mentionToEntityMap.values.flatMap(listEntities => 
      listEntities.map(entity => entity._1)).toArray
    println("Total number of potential candidates, to get neighbourhood data =", candidateIds.size)
    val  nbrsOfCandidateEntities: Map[NodeId, Set[Entity]] = NodeProp.getOneHopNbrIdsLabels(g, candidateIds).toArray.toMap
    // Put cheks that if no neighbourhood data is found about a node ,
    // and a mention has only that entity , should we return best string match
   
    nbrsOfCandidateEntities.foreach(idWithNbrs => println(idWithNbrs._1, idWithNbrs._2))
    val mentionToEntityScore : Map[Mention, Set[(Entity, SimScore)]] = 
      ColEntityDisScores.getEntityMentionCompScore(mentionsWithData, 
          mentionToEntityMap, nbrsOfCandidateEntities, 
          mentionToEntityMatchThreshold, phraseMatchThreshold)
  
          
    //3. Score semantic relatedness between entity candidates of each mention
    val entityToEntitySemanticRelScore: Map[(Entity, Entity), SimScore] = 
      ColEntityDisScores.getSemanticRelatedEntitiesScore(mentionToEntityScore.values, nbrsOfCandidateEntities, numVerticesInGraph)
    
    //4. Prepare ReferentGraph(vertices, edges) from mentions and entities
    println("Creating normalized referent graph")  
    val refGraph = new ReferentGraph(mentionsWithData, mentionToEntityScore, 
          entityToEntitySemanticRelScore)
   
    println(" Performing collective inference")
    getMatchUsingMaxReferentGraph(refGraph, mentionsWithoutEntityMatch, 
        mentionsWithData.size, lambda) 
  }
  
  
  
  private def initMapWithNewEntities(mentionsWithoutCand : Set[Mention]):
  scala.collection.mutable.Map[Mention, (Entity, SimScore)] = {
    var finalMatches = scala.collection.mutable.Map.empty[Mention, (Entity, SimScore)]
    for(mentionWithoutCand <- mentionsWithoutCand) {
      val label = "nous: " + mentionWithoutCand 
      finalMatches.+=((mentionWithoutCand, ((label.hashCode, label), 0)))
    }
    finalMatches
  } 
  
  
  /* finds matching candidate entities, for each mention
   * If no matching entity is found, add new entity as
   * (-randomNodeId, nous:mention)
   */
  
  private def getMatchCandidates(mentionLabels: List[Mention] , vertexRDDWithAlias: VertexRDD[String], 
      phraseMatchThreshold: Double): Map[Mention, Iterable[Entity]] = {
    
    val mentionToEntityCandidates : Map[Mention, Iterable[Entity]] = 
      MatchStringCandidates.getMatchesRDDWithAlias(mentionLabels, vertexRDDWithAlias, phraseMatchThreshold).collectAsMap 
    println("\nCandidate matches for each mention:")
    mentionToEntityCandidates.foreach(matches => println(matches._1 + "=>"  + matches._2.toString))
    mentionToEntityCandidates   
  }
  
  
      
   /* Given graph with vertices containing "mentions and entities"
   * Assumption: Mention vertex label starts with "mention:"
   * Creates a map, mapping
   * mention vertices to 0 -> NumMentions-1
   * entity vertices to NumMentions -> (NumMentions+ NumENtities)-1
   */
  private def mapVerticesToIndex(refGraph: ReferentGraph):  Map[Long, Int] = {  
    var vIdToIndexMapTemp= scala.collection.mutable.Map.empty[Long, Int]
    var i = 0
    for(vertex <- refGraph.vertices){
      if(vertex._2.label.startsWith("mention:")){
         vIdToIndexMapTemp.+= ((vertex._1, i))
         i= i+1
      }
    }    
    for(vertex <- refGraph.vertices){
      if(!vertex._2.label.startsWith("mention:")){
         vIdToIndexMapTemp.+=((vertex._1, i))
         i= i+1
      }
    } 
    assert(i==refGraph.vertices.size)
    vIdToIndexMapTemp.toMap
  }
 
  
  private def getMatchUsingMaxReferentGraph(refGraph: ReferentGraph,
      mentionsWithoutCand: Set[Mention], numMentions: Int, 
      lambda: Double): scala.collection.immutable.Map[Mention, (Entity, SimScore)] = {
    
    //For nodes in referent graph , map them as follows:
    // mentions : index 0 to #Mentions-1
    // candidate entities #Mentions to #Mentions+#Entities-1 
    val verticesToIndexMap: Map[Long, Int] = mapVerticesToIndex(refGraph)
    
    val verticesToEntityMatches : Map[Int, (Int, SimScore)] = 
      EvidenceProp.runColInference(refGraph, verticesToIndexMap, numMentions, lambda)
    
    assert(verticesToEntityMatches.size == numMentions)
 
    //Convert mention -> entity mappings to the KG vertex id form
    var finalMatches = scala.collection.mutable.Map.empty[Mention, (Entity, SimScore)]
    val indexToVertexIdMap = verticesToIndexMap.map((keyValue) => (keyValue._2, keyValue._1)).toMap
    for(matchIter <- verticesToEntityMatches){
      val mentionIndex:Int = matchIter._1
      val matchedEntityIndex: Int = matchIter._2._1
      val matchScore : Double = matchIter._2._2
      
      val mentionVertexId: Long = indexToVertexIdMap.get(mentionIndex).get
      val matchedEntityVertexId: Long = indexToVertexIdMap.get(matchedEntityIndex).get
      val mentionNodeData = refGraph.vertices.get(mentionVertexId).get
      val matchedEntityNodeData  = refGraph.vertices.get(matchedEntityVertexId).get
      
      finalMatches.+=((mentionNodeData.label, (
          (matchedEntityVertexId, matchedEntityNodeData.label), matchScore)))
    }
    
    // For mentions without any candidate matches, create new vertices
    for(mentionWithoutCand <- mentionsWithoutCand) {
      val label = "nous: " + mentionWithoutCand 
      finalMatches.+=((mentionWithoutCand, ((label.hashCode, label), 0)))
    }
    
    finalMatches.toMap
  
  }

}
