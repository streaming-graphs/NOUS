package gov.pnnl.aristotle.algorithms.entity

import org.apache.spark._
import scala.collection.Map
import scala.collection.Set
import gov.pnnl.aristotle.utils._
import ColEntityTypes._

class ReferentGraph(mentionsWithData: Map[String, MentionData], 
      mentionToEntityScore: Map[Mention, Set[(Entity, SimScore)]], 
      entityToEntitySemanticRelScore: Map[(Entity, Entity), SimScore]) 
      extends LocalGraph[LabelWithWt, SimScore] {
   
    
    val vertices: Map[NodeId, LabelWithWt] = getVertices(mentionsWithData, mentionToEntityScore)
    private val allEdges = getEdges(mentionToEntityScore, entityToEntitySemanticRelScore)
    private val allEdgesBySrcId: Map[NodeId, List[LocalDirectedEdge[Double]]] = 
      allEdges.groupBy(edge => edge._1).mapValues(iterOfEdges => {
        val totalWtEdges: Double = iterOfEdges.map(edge => edge._2._1).fold(0.0)((s1, s2) => s1+s2)
        iterOfEdges.toList.map(dirEdge => new LocalDirectedEdge[Double](dirEdge._2._1/totalWtEdges, dirEdge._2._2))
    })
    
    val edges = allEdgesBySrcId
    printGraph()
  
      /* Given a map containing mentions and a list of candidates for each mention,
   * returns the vertices in the form
   * Vertex -> VertexData(VertexLabel, VertexWtScore )
   */
  private def getVertices(mentionsWithData: Map[Mention, MentionData], 
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
  private def getEdges(mentionToEntityScore: Map[Mention, Set[(Entity, SimScore)]], 
      entityToEntitySemanticRelScore: Map[(Entity, Entity), Double]) : 
      Iterable[(NodeId, (SimScore, NodeId))] = {
    
    println
   
    // Get edges between mention and entity candidates
    val mentionToEntityEdges: Iterable[(NodeId, (SimScore, NodeId))] = mentionToEntityScore.
    flatten(mentionWithListOfCandidates => {
      val mention: Mention = mentionWithListOfCandidates._1
      val entityListWithScore: Set[(Entity, Double)] = mentionWithListOfCandidates._2
      
      val mentionToEntityEdge: Set[(Long, (Double, Long))] = entityListWithScore.map(
          entityWithScore => 
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
}