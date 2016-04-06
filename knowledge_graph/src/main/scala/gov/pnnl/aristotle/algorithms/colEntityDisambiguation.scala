package gov.pnnl.aristotle.algorithms

import org.apache.spark._
import org.apache.spark.SparkContext._

import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import org.apache.spark.Aggregator
import org.apache.spark.rdd.RDD
import scala.collection.Set
import scala.collection.Map
import scala.util.Sorting
import scala.math.Ordering
import scala.collection.mutable.HashMap
import org.apache.log4j.Logger
import org.apache.log4j.Level
import gov.pnnl.aristotle.utils.{NLPTripleParser}
import java.io.{BufferedWriter, OutputStreamWriter, FileOutputStream}
import gov.pnnl.aristotle.utils._

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
    println("Done reading graph" + g.vertices.count + ", starting disambiguation\n")
   
    
    val colEntityDis = new ColEntityDisamb[String, String]
    for(triplesInBlock <- allTriples){
      val mentionMap: Map[String, MentionData] = NLPTripleParser.getEntitiesWithTypeMapFromTriples(triplesInBlock)
      println("Disambiguating follwing entities together as a block", mentionMap.size)
      mentionMap.foreach(v => println(v._1 + "--(type, evidence weight)-->"+ v._2.toString))  
      
      val refGraph = colEntityDis.getReferentGraph(mentionMap, g, phraseMatchThreshold, mentionToEntityMatchThreshold)
       
      println("end of block")
    }
   
  }
}


 
class ColEntityDisamb[VD, ED] {
  
  class LabelWithWt(val label: String, val wt : Double)
  class LabelWithType(val label: String, val vtype: String)
  type ReferentGraph = LocalGraph[LabelWithWt, Double]

  type Mention = String
  type SimScore = Double
  type NodeId = Long 
  type Entity = (NodeId, String)
  
  
 
  /* creates a referent graph from the list of mentions and given a base knowledge graph */
  def getReferentGraph(mentionsWithData: Map[String, MentionData], g: Graph[String, String], 
      phraseMatchThreshold: Double = 0.7, mentionToEntityMatchThreshold: Double = 0.0 ): ReferentGraph = {
    
    //Get candidates entities for each mention
    val mentionLabels: List[Mention] = mentionsWithData.keys.toList
    println("Constructing refrent graph for mentions", mentionLabels.size)
    val mentionToEntityMap : Map[Mention, Iterable[Entity]] = 
      MatchStringCandidates.getMatchesRDDWithAlias(mentionLabels, g, phraseMatchThreshold).collectAsMap
    
     
    // Calculate graph neighbourhood data for each candidate vertex
    val candidateIds: Array[NodeId] = mentionToEntityMap.values.flatMap(listEntities => listEntities.map(entity => entity._1)).toArray
    println("Number of potential candidates=", candidateIds.size)
    val  nbrsOfCandidateEntities: Map[NodeId, Set[Entity]] = NodeProp.getOneHopNbrIdsLabels(g, candidateIds).toArray.toMap
    
    //Find list of candidate entities for each mention
    val mentionToEntityScore : Map[Mention, Set[(Entity, SimScore)]] = 
      getEntityMentionCompScore(mentionsWithData, mentionToEntityMap, nbrsOfCandidateEntities, 
          mentionToEntityMatchThreshold, phraseMatchThreshold)
      
    // Score semantic relatedness between entity candidates of each mention
    val entityToEntitySemanticRelScore: Map[(Entity, Entity), SimScore] = 
      getSemanticRelatedEntitiesScore(mentionToEntityScore.values, nbrsOfCandidateEntities)
    
    // Prepare vertices from mentions and entities
    val allVertices: Map[NodeId, LabelWithWt] = getVertices(mentionsWithData, mentionToEntityScore)
    
    //prepare edge, grouped by source id
    val allEdges = getEdges(mentionToEntityScore, entityToEntitySemanticRelScore)
    val allEdgesBySrcId: Map[NodeId, List[LocalDirectedEdge[Double]]] = allEdges.groupBy(edge => edge._1).
    mapValues(iterOfEdges => iterOfEdges.toList.map(dirEdge => new LocalDirectedEdge[Double](dirEdge._2._1, dirEdge._2._2))) 
    
    new ReferentGraph(allVertices, allEdgesBySrcId)
  }
  
  

  
  
  /*Given 
   * a list of mentions (co-occuring in a paragraph) and their associated data types(from NLP)
   * the list of candidate entities for each mention
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
    
    val mentionToEntityCompScore : Map[Mention, Set[(Entity, SimScore)]] = mentionsToEntityMap.map(mentionToEntityList => {
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
        val entityNbrs : Set[Entity] = candidateNbrs.get(entityId).get
        (entity, getMentionEntityScore(mentionData, entityNbrs , allMentionsInGivenContext-mention, phraseMatchThreshold, totalSizeCandEntityNbrs))
      })
      (mention, entityWithScore.filter(entityScores => entityScores._2 > mentionToEntityMatchThreshold))
    })
    mentionToEntityCompScore
  }
 
  
  /*TODO */
  /* Note we can use some kind of weight on mentionType to Entity Type similarity 
  */
  def getMentionEntityScore(mentionData: MentionData, entityNbrs : Set[Entity], allMentionInContext : Set[Mention], 
      phraseMatchThreshold:Double, totalSizeCandEntityNbrs: Long ): Double = {
    
    val numMentionsInContext = allMentionInContext.size
    val numOfEntityNbrs = entityNbrs.size
    
    var commonEntities  = 0
    for (mention <- allMentionInContext) {
       val found = entityNbrs.exists(entity => Gen_Utils.stringSim(entity._2, mention) > phraseMatchThreshold)
       if(found) commonEntities+=1
    }
    
    val simScore: Double =(commonEntities*2.0)/(numMentionsInContext*numOfEntityNbrs)
    val entityPopularity: Double = (entityNbrs.size*1.0)/totalSizeCandEntityNbrs
    
    entityPopularity*0.5 + simScore*0.5
  }
  
  
  /* TODO */
  def getSemanticRelatedEntitiesScore(entityListPerMention : Iterable[Set[(Entity, SimScore)]], 
       nbrsOfCandidate : Map[NodeId, Set[Entity]]): Map[(Entity, Entity), SimScore] = {
    
    val indexedEntityList = entityListPerMention.toIndexedSeq
    val entityToEntitySemnaticSim = collection.mutable.Map.empty[(Entity, Entity), SimScore]
    
    for( i <- 0 to indexedEntityList.length) {
      val entityList1 = indexedEntityList(i) 
      for(j <- 0 to indexedEntityList.length) {
        if(i != j) {
          val entityList2 = indexedEntityList(j)
          entityList1.foreach(entity1 => {
            entityList2.foreach(entity2 => {
              val semRel: Double = semanticRelatedness(entity1._1, entity2._1, nbrsOfCandidate)
              entityToEntitySemnaticSim.update((entity1._1, entity2._1), semRel)
            })
          })         
        }
      }      
    }
    entityToEntitySemnaticSim.toMap
  }
  
  /* Given two entities, calculate smantic relatedness using their neighbourhood information, as:
   * SR(A, B)  =  1 -  ( (log(max(|A|, |B|)) - log(A intersection B) )   /  (log |W| - log(min(|A|, |B|)) ) )
   * */
  def semanticRelatedness(entity1: Entity, entity2: Entity, NbrMap: Map[NodeId, Set[Entity]]): Double = {
    
    val nodeId1 = entity1._1
    val nodeId2 = entity2._1
    val nbrsEntity1 = NbrMap.getOrElse(nodeId1, Set.empty[Entity])
    val nbrsEntity2 = NbrMap.getOrElse(nodeId2, Set.empty[Entity])
    
    val maxSize = Math.max(nbrsEntity1.size, nbrsEntity2.size)
    val commonNbrs = nbrsEntity1.intersect(nbrsEntity2)
    val totalNumberOfEntities = NbrMap.size
    
   val minSize = Math.min(nbrsEntity1.size, nbrsEntity2.size)
   
   val numerator = Math.log(maxSize) - Math.log(commonNbrs.size)
   val denominator = Math.log(totalNumberOfEntities) - Math.log(minSize)
   
   (1 - (numerator/denominator))    
  }
  /* Given a map containing mentions and a list of candidates for each mention,
   * returns the vertices in the form
   * Vertex -> VertexData(VertexLabel, VertexWtScore )
   */
  def getVertices(mentionsWithData: Map[Mention, MentionData], 
      mentionToEntityScore : Map[Mention, Set[(Entity, SimScore)]]): Map[NodeId, LabelWithWt] = {
    
    val mentionVert: Iterable[(NodeId, LabelWithWt)] =  mentionToEntityScore.keys.map(mention => 
      (mention.hashCode().toLong, new LabelWithWt(mention, mentionsWithData(mention).initialEvidenceWeight)))
    
    val entityVert : Iterable[(NodeId, LabelWithWt)] = mentionToEntityScore.values.flatMap(entityListWithScore => 
      entityListWithScore.map(entityWithScore => (entityWithScore._1._1, new LabelWithWt(entityWithScore._1._2, 0.0))))   
    
    val allVertices: Map[NodeId, LabelWithWt] = (mentionVert ++ entityVert).toMap
    allVertices
  }
  
  /* Given following 
   * @Input
   * Map[Mention -> list of candidate entities with Similarity Score]
   * Map[Pair of entities -> semantic similarity between them]
   * 
   * Generates a list of edges in form of 
   * @ Output
   * (SourceId, (edge Weight, destination id))
   * 
   */
  def getEdges(mentionToEntityScore: Map[Mention, Set[(Entity, SimScore)]], 
      entityToEntitySemanticRelScore: Map[(Entity, Entity), Double]) : 
      Iterable[(NodeId, (SimScore, NodeId))] = {
    
    // Get edges between mention and entity candidates
    val mentionToEntityEdges: Iterable[(NodeId, (SimScore, NodeId))] = mentionToEntityScore.flatMap(mentionWithListOfCandidates => {
      val mention: Mention = mentionWithListOfCandidates._1
      val entityListWithScore: Set[(Entity, Double)] = mentionWithListOfCandidates._2
      val mentionToEntityEdge: Set[(Long, (Double, Long))] = entityListWithScore.map(entityWithScore => 
        (mention.hashCode().toLong,  (entityWithScore._2, entityWithScore._1._1)))
     mentionToEntityEdge 
    })
    
    //Get Edges between different candidate entities
    val entityToEntityEdges : Iterable[(NodeId, (SimScore, NodeId))] = entityToEntitySemanticRelScore.
    filter(entityPairWithSimScore => (entityPairWithSimScore._2 > 0)).
    flatMap(entityPairWithSimScore => {
      val entityPair: (Entity, Entity) = entityPairWithSimScore._1
      val score: Double = entityPairWithSimScore._2
      Iterable((entityPair._1._1, (score, entityPair._2._1)), (entityPair._2._1, (score, entityPair._1._1)))
      })
    
    // Combine the (mention->entity) and (entity-entity) edge list 
    val allEdges :Iterable[(NodeId, (SimScore, NodeId))] = mentionToEntityEdges ++ entityToEntityEdges
    allEdges
  }
  
 
  def findMaxWeighedReferentGraph(refGraph: LocalGraph[VD, ED]) : Map[Mention, (Entity, SimScore)] = {
    
    
    return Map.empty
    
  }
  
 
    /** Given a graph and a list of entities per sentence of a paragraph, collectively disambiguate the entities
   * @Input 
   * 1) Map of entity (extracted from NLP) -> "NLPEntityData" structure, 
   *    NLP entity label -> information available about this entity using NLP
   *    Entity information can be anything
   *    Type, Topic, (EdgeType, EdgeWt) hints b/w two entities
   *   
   * 2) Existing graph in the form of adjacency data
   *    (vertexId, label) -> neighborhood data 
   * 
   * @ Output : Mapping of the form
   *  (NLP entity label -> graph vertex id, graph entity label)
   */
  /*
  def collectiveDisambiguation(mentionsWithData: Map[String, NLPEntityData], 
      g: Graph[String, String], matchThreshold: Double = 0.1): Map[String, List[VertexMatch]] = {
    
    //Get Candidates, considering aliases
    val mentionLabels = mentionsWithData.keys.toList
    val candidatesRDD : RDD[(VertexId, String)] = MatchStringCandidates.getMatchesRDDWithAlias(mentionLabels, g)
    val candidates: Array[VertexId] = candidatesRDD.map(v => v._1).collect
    println("NUm Nodes that match candidate string: = ", candidates.size)
    
    // Collect profile data for all candidates
    val candidatesNbrData: VertexRDD[List[GraphNodeNbrData[String, String]]] = g.aggregateMessages[List[GraphNodeNbrData[String, String]]](
        triplet => {
          if(candidates.contains(triplet.srcId))
            triplet.sendToSrc( List(new GraphNodeNbrData[String, String](triplet.attr, triplet.dstId, triplet.dstAttr, true)))
          else if(candidates.contains(triplet.dstId)) 
            triplet.sendToDst( List(new GraphNodeNbrData[String, String](triplet.attr, triplet.srcId, triplet.srcAttr, false)))
      
    }, (list1, list2) => list1 ++ list2)
    
    // Add Labels to Vertices along with Neighbour Profiles
    val candidatesNbrDataWithLabels = candidatesNbrData.innerJoin(candidatesRDD)((id, nbrData, label) => (label, nbrData))
    
    // Rank candidates for each mention, creating sorted list of matches 
    // mention => sortedList(vertexId, vertexLabel, matchScore)
    val allRankedMatches : Map[String, List[VertexMatch]] = alignAndRank(mentionsWithData, candidatesNbrDataWithLabels)
    allRankedMatches.foreach(mention => { 
      println("Mention=" + mention._1 + " , ") 
      val vMatches =  mention._2
      vMatches.foreach(vMatch => println(vMatch.toString))
    })  
    val topKMatches = allRankedMatches.mapValues(listMatches => listMatches.filter(_.matchScore >= matchThreshold))    
    return topKMatches
  }
  
  */
}
