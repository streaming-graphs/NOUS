package gov.pnnl.nous.utils

import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD

object NodeProp {
  def getOneHopNbrIdsLabels( g: Graph[String, String], id: Array[Long],
      filterRelations:Set[String] = Set.empty) : VertexRDD[Set[(Long, String)]] = {
    val nbrlist: VertexRDD[Set[(Long, String)]] =  g.aggregateMessages[Set[(Long, String)]](
        edge => {
          if(id.contains(edge.srcId) && (!filterRelations.contains(edge.attr) || filterRelations.isEmpty)) {
            edge.sendToSrc(Set((edge.dstId, edge.dstAttr)))
          }
          if(id.contains(edge.dstId) && (!filterRelations.contains(edge.attr) || filterRelations.isEmpty)) {
            edge.sendToDst(Set((edge.srcId, edge.srcAttr)))
          }
        },
        (a,b) => a ++ b
    )
    println(" collected one hop nbrs for nodes:", nbrlist.count)
    return nbrlist;
  }
  
  
  class MatchMentions(mentions: Set[String], simThreshold: Double) extends Serializable{

    // Returns all mentions that match this entity
    // Matches the given entity label+alias to
   def getMatch(vert: (VertexId, String), aliasSep: String): Set[String] = {

     val labelWithAlias : Array[String] = vert._2.split(aliasSep)

     if(labelWithAlias.length == 1){

        val matchingMentions = mentions.filter( StringSim.getsim(labelWithAlias(0), _) >= simThreshold)
        if(matchingMentions.size > 0) {
        println("Found no alias, matched node : ", vert._2, " with :")
        matchingMentions.foreach(println(_))
        }
        matchingMentions
     } else if (labelWithAlias.length == 2) {
       val matchingMentions = mentions.filter(mention => StringSim.getsim(labelWithAlias(0), mention) >= simThreshold ||
           StringSim.getsim(labelWithAlias(1), mention) > simThreshold)
       //val matchingMentions = mentions.filter(mention => labelWithAlias(0).contains(mention) || labelWithAlias(1).contains(mention) )
        //mentions.filter(labelWithAlias(0).contains(_))
        if(matchingMentions.size > 0) {
          println("Found alias, matched node : ", vert._2, " with :")
          matchingMentions.foreach(println(_))
        }
        matchingMentions
          } else {
       Set.empty[String]
     }
   }

 }

   /* Given a list of "mentions" and a graph , return
  *
  * VertexId ->
  */
  def getMatchesRDDWithAlias(mentions :List[String],  
      allVerticesWithAlias: VertexRDD[String], phraseSimThreshold : Double, aliasSep: String): RDD[(String, Iterable[(VertexId, String)] )]  = {
    // Get aliases for all nodes in the form "alias=$nodeAlias,

    println("Getting matches for each mention")
    val mentionFilter = new MatchMentions(mentions.toSet, phraseSimThreshold)

    val allMatches: RDD[( String, (VertexId, String))] = allVerticesWithAlias.flatMap(v => {
      val matches: Set[String] = mentionFilter.getMatch(v, aliasSep)
      val vertexlabelWithoutAlias = v._2.split(aliasSep)(0)
      val allMatchesVertex = matches.map(mention => (mention, (v._1 ,vertexlabelWithoutAlias)))
      allMatchesVertex
    })

   val groupedMatches = allMatches.groupByKey
   groupedMatches
 }
  
  def constructVertexRDDWithAlias(g: Graph[String, String], aliasSep: String, edgeLabelNodeAlias: Array[String]): VertexRDD[String] = {

    val verticesWithAlias: VertexRDD[String] = getNodeAlias(g, edgeLabelNodeAlias)

    // The vertices labels are joined with their aliases
    // Vertex labels are of the form
    // "$nodeLabel;alias=$nodeAlias"
    g.vertices.leftZipJoin(verticesWithAlias)((id, label, alias) => {
      alias match{
        case Some(aliasValues) => label + aliasSep + aliasValues
        case None => label
      }})
  }

  
  def getNodeAlias(g: Graph[String, String], edgeLabelNodeAlias: Array[String], id: Array[Long] = Array.empty) : VertexRDD[String] = {
      val verticesWithAlias =  g.aggregateMessages[String]( edge => {
        if(id.isEmpty || id.contains(edge.srcId)) {
          for( aliasPredicate <- edgeLabelNodeAlias) {
            if(aliasPredicate.toLowerCase() == edge.attr.toLowerCase())
              edge.sendToSrc(edge.dstAttr)
          }
        }
      },
      (a, b) => a +"__"+ b
      )
      //verticesWithAlias.mapValues(alias => "ALIAS="+alias)
      verticesWithAlias
  }

}