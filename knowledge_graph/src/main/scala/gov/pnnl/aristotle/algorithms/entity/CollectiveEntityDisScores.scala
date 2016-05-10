package gov.pnnl.aristotle.algorithms.entity

import scala.collection.Set
import scala.collection.Map
import org.apache.spark.graphx._
import gov.pnnl.aristotle.algorithms.entity.ColEntityTypes._
import gov.pnnl.aristotle.utils.{MentionData, Gen_Utils, KGraphProp}

object ColEntityDisScores {
  
   /*Given 
   * a list of mentions (co-occuring in a paragraph) and their associated data types(from NLP)
   * the list of candidate entities for each mention, is one exists
   * The information about candidate entities (Neighbourhood data)
   * 
   * Output:
   * For each mention, provide matching entities along with Similarity score 
   * */
  def getEntityMentionCompScore(mentionsWithData : Map[Mention, MentionData] , 
      mentionsToEntityMap: Map[Mention, Iterable[Entity]], 
      candidateNbrs: Map[VertexId, Set[Entity]],  mentionToEntityMatchThreshold: Double, 
      phraseMatchThreshold: Double) :  Map[Mention, Set[(Entity, SimScore)]]= {
    
    val allMentionsInGivenContext = mentionsWithData.keys.toSet
    
    val mentionToEntityCompScore : Map[Mention, Set[(Entity, SimScore)]] = 
      mentionsToEntityMap.map(mentionToEntityList => {
      val mention: Mention = mentionToEntityList._1
      val mentionData: MentionData = mentionsWithData.get(mention).get
      
      val candEntityList: Set[Entity] = mentionToEntityList._2.toSet
      
      val totalSizeCandEntityNbrs: Long = candEntityList.map(entity => {
        val entityId = entity._1
        val numEntityNbrs = candidateNbrs.get(entityId).get.size
        numEntityNbrs
      }).fold(0)((len1,len2) => len1+len2)
      
      val entityWithScore : Set[(Entity, SimScore)] = candEntityList.map( entity => {
        val entityId = entity._1
        val entityLabel = entity._2.split(KGraphProp.aliasSep)(0)
        val entityNbrs : Set[Entity] = candidateNbrs.get(entityId).get
        val simScore = getMentionEntityScore(mentionData, entityNbrs , allMentionsInGivenContext-mention, phraseMatchThreshold, totalSizeCandEntityNbrs)
        //println("Scoring mention to entity similatity", mention, entityLabel, simScore)
        (entity, simScore)
      })
      (mention, entityWithScore.filter(entityScores => entityScores._2 >= mentionToEntityMatchThreshold))
    })
    println("\nMention -> Entity With Scores : ")
    mentionToEntityCompScore.foreach(mentionEntityScore => {
      print(mentionEntityScore._1 + "=>") 
      mentionEntityScore._2.foreach(entityScore => 
        print(entityScore._1._2, entityScore._2 ))
        println
    })
    mentionToEntityCompScore
  }
 

    /* Given candidate entities for each mention
   * Score (entity,entity) similarity
   * where entity pair belong to two different mentions
   * If two entities have no semnatic relatedness (<0), ignore that pair
   * 
   */
  def getSemanticRelatedEntitiesScore(entityListPerMention : Iterable[Set[(Entity, SimScore)]], 
       nbrsOfCandidate : Map[NodeId, Set[Entity]], graphNumVertices: Long): Map[(Entity, Entity), SimScore] = {
    
    val indexedEntityList = entityListPerMention.toIndexedSeq
    val entityToEntitySemanticSim = collection.mutable.Map.empty[(Entity, Entity), SimScore]
    
    for( i <- 0 to indexedEntityList.length-1) {
      val entityList1 = indexedEntityList(i) 
      for(j <- i+1 to indexedEntityList.length-1) {
        if(i != j) {
          val entityList2 = indexedEntityList(j)
          entityList1.foreach(entity1 => {
            entityList2.foreach(entity2 => {
              val semRel: Double = semanticRelatedness(entity1._1, entity2._1, 
                  nbrsOfCandidate, graphNumVertices)
              if(semRel > 0)
                entityToEntitySemanticSim.update((entity1._1, entity2._1), semRel)
            })
          })         
        }
      }      
    }
    println("\nEntity -entity scores:")
    entityToEntitySemanticSim.foreach(entityPairScore => {
      val entity1 = entityPairScore._1._1
      val entity2 = entityPairScore._1._2
      val score = entityPairScore._2
      println(entity1._2, entity2._2, score)
      
    })
    entityToEntitySemanticSim.toMap
  }
  
  
  /* Use popularity and semantic similarity to find mention to entity score
  */
  private def getMentionEntityScore(mentionData: MentionData, entityNbrs : Set[Entity], allMentionInContext : Set[Mention], 
      phraseMatchThreshold:Double, totalSizeCandEntityNbrs: Long ): Double = {
    
    val numMentionsInContext = allMentionInContext.size
    val numOfEntityNbrs = entityNbrs.size
    
    var commonEntities  = 0
    for (mention <- allMentionInContext) {
       val found = entityNbrs.exists(entity => Gen_Utils.stringSim(entity._2, mention) > phraseMatchThreshold)
       if(found) commonEntities+=1
    }
    //println("common nbrs, numMentionsInContext, numOfEntityNbrs", commonEntities, numMentionsInContext, numOfEntityNbrs)
    val simScore: Double =(commonEntities*2.0)/(numMentionsInContext*numOfEntityNbrs)
    val entityPopularity: Double = (entityNbrs.size*1.0)/totalSizeCandEntityNbrs
    
    //val finalScore = simScore
    val finalScore = entityPopularity*0.5 + simScore*0.5
    //println("entityNbrs, mentionNbrs, simcore, popularityScore", entityNbrs.toString, allMentionInContext.toString, simScore, entityPopularity, finalScore)
    finalScore
  }
  
  
  
  /* Given two entities, calculate smantic relatedness using their neighbourhood information, as:
   * SR(A, B)  =  1 -  ( (log(max(|A|, |B|)) - log(A intersection B) )   /  (log |W| - log(min(|A|, |B|)) ) )
   * */
  private  def semanticRelatedness(entity1: Entity, entity2: Entity, NbrMap: Map[NodeId, Set[Entity]], 
      graphNumVertices: Long): Double = {
    
    val nodeId1 = entity1._1
    val nodeId2 = entity2._1
    val nbrsEntity1 = NbrMap.getOrElse(nodeId1, Set.empty[Entity])
    val nbrsEntity2 = NbrMap.getOrElse(nodeId2, Set.empty[Entity])
    if(nbrsEntity1.size == 0 && nbrsEntity2.size == 0){ 
      println(" both entities j=have no nbrs in graph, returnning semanticSim =0" )
      return 0.0
    }
    val maxSize = Math.max(nbrsEntity1.size, nbrsEntity2.size)
    val commonNbrs = nbrsEntity1.intersect(nbrsEntity2)
    val minSize = Math.min(nbrsEntity1.size, nbrsEntity2.size)
 
    val numerator = Math.log(maxSize) - Math.log(commonNbrs.size)
    val denominator = Math.log(graphNumVertices) - Math.log(minSize)
    (1 - (numerator/denominator))    
  }
  
}
