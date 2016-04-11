package gov.pnnl.aristotle.algorithms

import org.apache.spark._
import org.apache.spark.SparkContext._
import breeze.linalg._

import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import org.apache.spark.Aggregator
import org.apache.spark.rdd.RDD
import scala.collection.Set
import scala.collection.Map
import scala.util.Sorting
import scala.math.Ordering
import scala.collection.mutable.HashMap
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
 
class ColEntityDisamb[VD, ED] {
  
  class LabelWithWt(val label: String, val wt : Double){
    override def toString(): String ={
      label + ";" + wt.toString
    }
  }
  
  type ReferentGraph = LocalGraph[LabelWithWt, Double]

  type Mention = String
  type SimScore = Double
  type NodeId = Long 
  type Entity = (NodeId, String)
   
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
    val mentionsWithoutEntityMatch: Set[Mention] = mentionLabels.toSet--mentionToEntityMap.keys.toSet
    val mentionsWithData = allMentionsWithData.--(mentionsWithoutEntityMatch)
    println("Mentions without any entity match")
    mentionsWithoutEntityMatch.foreach(println(_))    
    
    
    // 2. Score each candidate entity using graph neighbourhood data
    val candidateIds: Array[NodeId] = mentionToEntityMap.values.flatMap(listEntities => listEntities.map(entity => entity._1)).toArray
    println("Number of potential candidates=", candidateIds.size)
    val  nbrsOfCandidateEntities: Map[NodeId, Set[Entity]] = NodeProp.getOneHopNbrIdsLabels(g, candidateIds).toArray.toMap    
    val mentionToEntityScore : Map[Mention, Set[(Entity, SimScore)]] = 
      getEntityMentionCompScore(mentionsWithData, mentionToEntityMap, nbrsOfCandidateEntities, 
          mentionToEntityMatchThreshold, phraseMatchThreshold)
    
    //3. Score semantic relatedness between entity candidates of each mention
    val entityToEntitySemanticRelScore: Map[(Entity, Entity), SimScore] = 
      getSemanticRelatedEntitiesScore(mentionToEntityScore.values, nbrsOfCandidateEntities, numVerticesInGraph)
      
    
    //4. Prepare ReferentGraph(vertices, edges) from mentions and entities
    println("Creating normalized referent graph")
    val refGraph: ReferentGraph = 
      createReferentGraph(mentionsWithData, mentionToEntityScore, 
          entityToEntitySemanticRelScore)
   
    println(" Performing collective inference")
    getMatchUsingMaxReferentGraph(refGraph, mentionsWithoutEntityMatch, 
        mentionsWithData.size, lambda) 
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
  
  
  /*Given 
   * a list of mentions (co-occuring in a paragraph) and their associated data types(from NLP)
   * the list of candidate entities for each mention, is one exists
   * The information about candidate entities (Neighbourhood data)
   * 
   * Output:
   * For each mention, provide matching entities along with Similarity score 
   * */
  private def getEntityMentionCompScore(mentionsWithData : Map[Mention, MentionData] , 
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
      (mention, entityWithScore.filter(entityScores => entityScores._2 > mentionToEntityMatchThreshold))
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
    //println("common nbrs, numMentionsInContext, numOfEntityNbrs", commonEntities, numMentionsInContext, numOfEntityNbrs)
    val simScore: Double =(commonEntities*2.0)/(numMentionsInContext*numOfEntityNbrs)
    val entityPopularity: Double = (entityNbrs.size*1.0)/totalSizeCandEntityNbrs
    
    val finalScore = simScore
    //val finalScore = entityPopularity*0.5 + simScore*0.5
    //println("entityNbrs, mentionNbrs, simcore, popularityScore", entityNbrs.toString, allMentionInContext.toString, simScore, entityPopularity, finalScore)
    finalScore
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
  
  /* Given two entities, calculate smantic relatedness using their neighbourhood information, as:
   * SR(A, B)  =  1 -  ( (log(max(|A|, |B|)) - log(A intersection B) )   /  (log |W| - log(min(|A|, |B|)) ) )
   * */
  def semanticRelatedness(entity1: Entity, entity2: Entity, NbrMap: Map[NodeId, Set[Entity]], 
      graphNumVertices: Long): Double = {
    
    val nodeId1 = entity1._1
    val nodeId2 = entity2._1
    val nbrsEntity1 = NbrMap.getOrElse(nodeId1, Set.empty[Entity])
    val nbrsEntity2 = NbrMap.getOrElse(nodeId2, Set.empty[Entity])
    
    val maxSize = Math.max(nbrsEntity1.size, nbrsEntity2.size)
    val commonNbrs = nbrsEntity1.intersect(nbrsEntity2)
    val minSize = Math.min(nbrsEntity1.size, nbrsEntity2.size)
 
    val numerator = Math.log(maxSize) - Math.log(commonNbrs.size)
    val denominator = Math.log(graphNumVertices) - Math.log(minSize)
    (1 - (numerator/denominator))    
  }
  
  

  
  
  def createReferentGraph(mentionsWithData: Map[String, MentionData], 
      mentionToEntityScore: Map[Mention, Set[(Entity, SimScore)]], 
      entityToEntitySemanticRelScore: Map[(Entity, Entity), SimScore]): ReferentGraph = {
    
    val allVertices: Map[NodeId, LabelWithWt] = getVertices(mentionsWithData, mentionToEntityScore)
    val allEdges = getEdges(mentionToEntityScore, entityToEntitySemanticRelScore)
    val allEdgesBySrcId: Map[NodeId, List[LocalDirectedEdge[Double]]] = 
      allEdges.groupBy(edge => edge._1).mapValues(iterOfEdges => {
        val totalWtEdges: Double = iterOfEdges.map(edge => edge._2._1).fold(0.0)((s1, s2) => s1+s2)
        iterOfEdges.toList.map(dirEdge => new LocalDirectedEdge[Double](dirEdge._2._1/totalWtEdges, dirEdge._2._2))
    })
    
    val refGraph = new ReferentGraph(allVertices, allEdgesBySrcId)
    refGraph.printGraph
    refGraph
  }
 
  
  def getMatchUsingMaxReferentGraph(refGraph: ReferentGraph,
      mentionsWithoutCand: Set[Mention], numMentions: Int, 
      lambda: Double): scala.collection.immutable.Map[Mention, (Entity, SimScore)] = {
    
    //For nodes in referent graph , map them as follows:
    // mentions : index 0 to #Mentions-1
    // candidate entities #Mentions to #Mentions+#Entities-1 
    val verticesToIndexMap: Map[Long, Int] = mapVerticesToIndex(refGraph)
    
    val verticesToEntityMatches : Map[Int, (Int, SimScore)] = 
      runColInference(refGraph, verticesToIndexMap, numMentions, lambda)
    
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

  
  def runColInference(refGraph: ReferentGraph, verticesToIndexMap:  Map[Long, Int] , 
      numMentions: Int, lambda: Double) : 
  Map[Int, (Int, SimScore)] = {
    
    //  [N*1] matrix containing initial score for each vertex
    val initialEvidence = getInitialEvidence(refGraph, verticesToIndexMap)
    
    // [N*N] matrix containing edge weights between vertices (0 otherwise)
    val evidencePropMatrix = initEvidencePropMatrix(refGraph, verticesToIndexMap)
    
    // [N*1] inferred evidence matrix
    val inferredEvidence : DenseMatrix[Double] = runBeliefProp(initialEvidence, evidencePropMatrix, lambda)
    println("Inferred Evidence")
    println(inferredEvidence)
    
    //[M*N] mention to Entity weights
    // Note : Initial Evidence Propagation Matrix is constructed as 
    // T[i, j] = weight of edge going from j to i
    // We transpose back for calculating final scores
    val mentionToEntityMatrix: DenseMatrix[Double] = evidencePropMatrix.t(0 to numMentions-1, ::)
    val numTotalVertices: Int = refGraph.vertices.size
    
    var finalMatchesIndexed = scala.collection.mutable.Map.empty[Int, (Int, SimScore)]
    for(mentionIndex <- 0 to numMentions-1){
      val mentionEntityComp = mentionToEntityMatrix(mentionIndex, ::)  
      var maxScore: Double = Double.NegativeInfinity
      var matchingEntityId = -1
      for(j <- numMentions to numTotalVertices-1) {
        val newScore = mentionEntityComp(j)*inferredEvidence(j, 0)
        //println("mentionIndex, entityIndex, score", mentionIndex, j, mentionEntityComp(j), inferredEvidence(j, 0), newScore)
        if(newScore > maxScore){
          maxScore = newScore
          matchingEntityId = j
          //println("changinng score for mention to ", mentionIndex, j, newScore)
        }
      } 
      finalMatchesIndexed.+=((mentionIndex, (matchingEntityId, maxScore)))
    }
    finalMatchesIndexed
  }


    /* Given a map containing mentions and a list of candidates for each mention,
   * returns the vertices in the form
   * Vertex -> VertexData(VertexLabel, VertexWtScore )
   */
  def getVertices(mentionsWithData: Map[Mention, MentionData], 
      mentionToEntityScore : Map[Mention, Set[(Entity, SimScore)]]): Map[NodeId, LabelWithWt] = {
    
    val mentionVert: Iterable[(NodeId, LabelWithWt)] =  mentionToEntityScore.keys.map(mention => 
      (mention.hashCode().toLong, new LabelWithWt("mention:" + mention, mentionsWithData(mention).initialEvidenceWeight)))
    
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
   * @ Output s
   * (SourceId, (edge Weight, destination id))
   * 
   */
  def getEdges(mentionToEntityScore: Map[Mention, Set[(Entity, SimScore)]], 
      entityToEntitySemanticRelScore: Map[(Entity, Entity), Double]) : 
      Iterable[(NodeId, (SimScore, NodeId))] = {
    
    println
   
    // Get edges between mention and entity candidates
    val mentionToEntityEdges: Iterable[(NodeId, (SimScore, NodeId))] = mentionToEntityScore.
    flatten(mentionWithListOfCandidates => {
      val mention: Mention = mentionWithListOfCandidates._1
      val entityListWithScore: Set[(Entity, Double)] = mentionWithListOfCandidates._2
      
      val mentionToEntityEdge: Set[(Long, (Double, Long))] = entityListWithScore.map(entityWithScore => 
        (mention.hashCode().toLong,  (entityWithScore._2, entityWithScore._1._1)))
     mentionToEntityEdge 
    })
    
    
    //Get Edges between different candidate entities
    val entityToEntityEdges : Iterable[(NodeId, (SimScore, NodeId))] = entityToEntitySemanticRelScore.
    flatten(entityPairWithSimScore => {
      val entityPair: (Entity, Entity) = entityPairWithSimScore._1
      val score: Double = entityPairWithSimScore._2
      Iterable((entityPair._1._1, (score, entityPair._2._1)), (entityPair._2._1, (score, entityPair._1._1)))
      })
    
    // Combine the (mention->entity) and (entity-entity) edge list 
    val allEdges :Iterable[(NodeId, (SimScore, NodeId))] = mentionToEntityEdges ++ entityToEntityEdges
    
    allEdges
  }
  
  
  /* Given graph with vertices containing "mentions and entities"
   * Assumption: Mention vertex label starts with "mention:"
   * Creates a map, mapping
   * mention vertices to 0 -> NumMentions-1
   * entity vertices to NumMentions -> (NumMentions+ NumENtities)-1
   */
  def mapVerticesToIndex(refGraph: ReferentGraph):  Map[Long, Int] = {  
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
 
  
  /* create a vector containing vertices.evienceWt 
   * evidence[0.... NumMentions-1] => mention.wt
   * evidence[NumMentions ...... NumMentions+NumENtities-1] => entityWt=>0
   * 
   */ 
  def getInitialEvidence(refGraph: ReferentGraph, verticesToIndexMap: Map[Long, Int]): 
  DenseMatrix[Double] = {
    
    val numVertices = refGraph.vertices.size 
    val initialEvidence = DenseMatrix.zeros[Double](numVertices, 1)
    for(vertex <- refGraph.vertices){
      val vId = vertex._1
      //println("getting index for ",vId)
      val vIndex = verticesToIndexMap.get(vId).get 
      initialEvidence(vIndex, 0) = vertex._2.wt
    }
    initialEvidence
   }
  
  
  /* create a evidence propagation matrix such that 
   * evidenceProp[i, j] => edgeWt(nodeIndex_j, nodeIndex_i)
   * nodeIndex are mapped from the given map
   */
  def initEvidencePropMatrix(refGraph: ReferentGraph, verticesToIndexMap: Map[Long, Int]):
  DenseMatrix[Double]= {
    
    val numVertices = refGraph.vertices.size 
    val evidencePropMatrix = DenseMatrix.zeros[Double](numVertices, numVertices)
    for(edgesForSrc <- refGraph.edges){
      val srcId = edgesForSrc._1
      val srcIndex = verticesToIndexMap.get(srcId).get
      val allEdgesSrc = edgesForSrc._2
      for(srcEdge <- allEdgesSrc) {
        val dstId = srcEdge.nodeid
        val dstIndex = verticesToIndexMap.get(dstId).get
        evidencePropMatrix(dstIndex, srcIndex) = srcEdge.edgeAttr
      } 
    }
    evidencePropMatrix  
  }
  
  /* Implements 
   * Result = lambda * (Inverse(I - cT)) * initialEvidence
   * where:
   * lambda = fraction of back propagation 
   * I : Identity matrix 
   * T : evidence Propagation Matrix
   * c : 1- lambda
   */
  def runBeliefProp(initialEvidence: DenseMatrix[Double], 
      evidencePropMatrix: DenseMatrix[Double], lambda: Double): DenseMatrix[Double] = {  
    val nodeCount = initialEvidence.rows
    
   // println("Initial evidence")
   // println(initialEvidence)
    val eyeN = DenseMatrix.eye[Double](nodeCount)
    val tmp1 = (1-lambda)*evidencePropMatrix
    val tmp2 = lambda * inv(eyeN - tmp1)
    val result = tmp2*initialEvidence
    result 
  }

}
