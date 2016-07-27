package gov.pnnl.aristotle.algorithms

import org.apache.spark._
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import org.apache.spark.Aggregator
import org.apache.spark.rdd.RDD
import scala.math.Ordering
import scala.util.Sorting
import scala.collection.Set
import scala.collection.mutable.ArrayBuffer
import scala.collection.mutable.HashMap
import org.apache.log4j.Logger
import org.apache.log4j.Level
import scala.io.Source
import java.nio.file.{Paths, Files}
import java.nio.charset.StandardCharsets
import gov.pnnl.aristotle.utils.{NbrEdge, Gen_Utils, FileLoader}
import gov.pnnl.aristotle.utils.{NodeProp, KGraphProp}
import gov.pnnl.aristotle.algorithms.PathSearch.PathSearchPregel
import gov.pnnl.aristotle.algorithms.PathSearch.MaxDegreeFilter
import gov.pnnl.aristotle.algorithms.PathSearch.ExtendedVD
//import gov.pnnl.aristotle.algorithms.entity.EntityDisambiguation



class Query(val qtype : String, val entity1 : String, val entity2: String, 
    val contextFilters: Array[String] = Array.empty) {
  def qprint(): Unit = {
    val filterString = contextFilters.fold("")((v1, v2)=> v1+","+v2)
    println("Query type = " + qtype + ", entity1 = " + entity1 +
        ", entity2 = " + entity2 + ", filters = " + filterString)
  }
}
    
object DemoDriver {
  
  val maxEntityLabelSize = 80
  def parseQuery(queryString : String): Query = {
    val tokens = queryString.toLowerCase().split("_")
    if(tokens.length < 2){
      println(" Invalid Query, skipping this\n")
      new Query("-1", "", "")
    }
    
    val qType = tokens(0)
    val entity1 = tokens(1)
    
    qType match {
      case "1" => new Query(qType, entity1, "NONE")
      case "2" => new Query(qType, entity1, "NONE", tokens.drop(2))
      case "3" => {
        if(tokens.length < 3){
          println(" Invalid Query, type 3 query requires at least 2 entities\n")
          new Query("-1", "", "")
        }
        new Query(qType, entity1, tokens(2), tokens.drop(3)) 
      }
      case "aggr" => new Query(qType, entity1, "NONE", tokens.drop(2))
      case default => new Query("-1", "", "")
    }
  }
  
  def getQueryFromFile(filename: String): Array[Query] = {
    Source.fromFile(filename).getLines().map(line => parseQuery(line)).toArray
  }

  def executeQuery(query: Query, g: Graph[String, String], 
      allGraphNbrs:VertexRDD[Set[NbrEdge]]): String = {
    query.qtype match {
      case "1" => executeEntityQuery(query, g, allGraphNbrs)
      case "2" => executeEntityQuery(query, g, allGraphNbrs)
      case "3" => executePathQuery(query, g, allGraphNbrs, 100, 3)
      case "aggr" => executeAggrQuery(query, g, allGraphNbrs)
      case "-1" => {
        println("Invalid query found")
        return ""
      }
    }
  }
  
  def executeEntityQuery(query: Query, g: Graph[String, String], 
      allGraphNbrs:VertexRDD[Set[NbrEdge]]): String = {
    
    println("\nIn Entity Query, getting candidate matches")
    val candWithLabelAndNbrsSortedByPopularity = getValidEntitiesSortedbyPopularity(query.entity1, g, allGraphNbrs)
    println("Number of valid Candidates=", candWithLabelAndNbrsSortedByPopularity.count)
    if(query.qtype == "1"){
      executeEntityQueryWithoutFilters(candWithLabelAndNbrsSortedByPopularity)
    } else {
      println("Keeping all candidates, Filtering on :") 
      query.contextFilters.foreach(println(_))
      executeEntityQueryWithFilters(candWithLabelAndNbrsSortedByPopularity, 
          query.contextFilters)
    }
  }
  
  def executeEntityQueryWithFilters(candWithLabelAndNbrsSortedByPopularity: 
      RDD[(VertexId, (String, Set[NbrEdge] ) )], 
      queryFilters : Array[String]): String = {
    
    var result = ""  
    candWithLabelAndNbrsSortedByPopularity.take(10).foreach(entityLabelAndNbrs => {
        val entityData = entityLabelAndNbrs._2
        val entityLabel = entityData._1
        val entityNbrs = entityData._2
        println("Candidate Entity with Nbrs, filtering", entityLabel, entityNbrs.size)
        assert(entityNbrs.size !=0)
        val entityFilteredEdges = entityNbrs.filter(entityEdge => 
          {
            val edgeLabel = entityEdge.edgeAttr
            var found = false;
            for(qfilter <- queryFilters) {
              if(edgeLabel.contains(qfilter))
                found  = true
            }
            found
          })
        println("After filtering, Entity with Nbrs", entityLabel, entityFilteredEdges.size)
        if(entityFilteredEdges.size > 0) {
          val entityFilteredEdgesString = entityFilteredEdges.map(edge => 
            edge.toString(entityLabel,maxEntityLabelSize)).foldLeft("")((edge1, edge2)=> edge1+"\n"+edge2)
          result = result + entityFilteredEdgesString.replaceFirst(",", "") + "\n\n"
        }
      })
      result
  }
  
  def executeEntityQueryWithoutFilters(candWithLabelAndNbrsSortedByPopularity: 
      RDD[(VertexId, (String, Set[NbrEdge] ) )]): String = {
    
    println("\nNo Filter on candidate entities, selecting TOP 3 popular candidates:")
    val topKEntities = candWithLabelAndNbrsSortedByPopularity.take(3)
    topKEntities.foreach(cand => println(cand._2._1, cand._2._2.size))
    println()
      
    var result = ""
    topKEntities.foreach(entityLabelAndNbrs => {
        val entityData = entityLabelAndNbrs._2
        val entityLabel = entityData._1
        val entityNbrs = 
          if(entityData._2.size > 10)
            entityData._2.take(10)
          else
            entityData._2
        val mappedEdgesEntity = entityNbrs.map(edge => edge.toString(entityLabel, maxEntityLabelSize)).
        foldLeft("")((edge1, edge2)=> edge1+"\n"+edge2)
        result = result + mappedEdgesEntity.replaceFirst(",", "") + "\n\n"
    })
    result
  }
  
   def executePathQuery(query: Query, g: Graph[String, String], 
       allGraphNbrs:VertexRDD[Set[NbrEdge]], maxDegree:Int, maxPathSize:Int): String = {
     
     println("\nIn Path Query")
     val candEntity1SortedByPopularity: RDD[(VertexId, (String, Set[NbrEdge]))] = 
       getValidEntitiesSortedbyPopularity(query.entity1, g, allGraphNbrs)
     val candEntity2SortedByPopularity: RDD[(VertexId, (String, Set[NbrEdge]))] = 
       getValidEntitiesSortedbyPopularity(query.entity2, g, allGraphNbrs)
     if(candEntity1SortedByPopularity.count < 1 || candEntity2SortedByPopularity.count < 1){
       return ""
     }
     
     val candEntity1 = candEntity1SortedByPopularity.first
     val candEntity2 = candEntity2SortedByPopularity.first
     val src = (candEntity1._1, new ExtendedVD(candEntity1._2._1, Option(candEntity1._2._2.size)))
     val dest = (candEntity2._1, new ExtendedVD(candEntity2._2._1, Option(candEntity2._2._2.size)))
     println("srcId, srcLabel =", src._1, src._2.label)
     println("destId, destLabel =", dest._1, dest._2.label)
     if(src._1 == dest._1)
       return ""
     println("Adding vertex degree to graph")
     
     val vertexDegreeRDD = g.degrees 
     val gExtended = g.outerJoinVertices(vertexDegreeRDD)((id, value, degree) => new ExtendedVD(value, degree))
     val filterObj = new MaxDegreeFilter(maxDegree)

     println("Finding paths")
     val allPaths = PathSearchPregel.runPregel(src, dest, gExtended, filterObj, maxPathSize, EdgeDirection.Either)
     var resultPaths = allPaths
     if(query.contextFilters.length != 0 && resultPaths.length > 0) {
       println("Filtering on query filters")
       resultPaths = allPaths.filter(path => path.exists(pathedge =>   
         {
            val edgeLabel = pathedge.edgeLabel
            var found = false;
            for(qfilter <- query.contextFilters) {
              if(edgeLabel.contains(qfilter))
                found  = true
            }
            found
          }))
     }
     if(resultPaths.length > 10)
       resultPaths = resultPaths.take(10)
     if(resultPaths.length > 0) {
       val pathString: String = resultPaths.map(path => 
            path.map(pathedge => {
              if(pathedge.isOutgoing){
                pathedge.srcLabel + "\t" + pathedge.edgeLabel + "\t" + pathedge.dstLabel
              } else {
                pathedge.dstLabel + "\t" + pathedge.edgeLabel + "\t" + pathedge.srcLabel
              }
            }).
            reduce((edge1String, edge2String) => edge1String + "\n" + edge2String)).
            reduce((path1String, path2String) => path1String + "\n\n" + path2String)
       pathString
     } else ""
   }
  
   def executeAggrQuery(query: Query, g: Graph[String, String], 
      allGraphNbrs:VertexRDD[Set[NbrEdge]]): String = {
      println("\nIn Aggregate QUery\n")
      
      // Note that each entity's type is already suffixed as part of its label
      val candEntity1SortedByPopularity: RDD[(VertexId, (String, Set[NbrEdge]))] = 
       getValidEntitiesSortedbyPopularity(query.entity1, g, allGraphNbrs, true)
    
     if(candEntity1SortedByPopularity.count < 1){
       println("Could not find anyentity of type", query.entity1)
       return ""
     }
     println("No of valid candidates for entity1 ", 
         candEntity1SortedByPopularity.count)
     
      val allValidCandEdges: RDD[(VertexId, String, NbrEdge)] = candEntity1SortedByPopularity.flatMap(entityLabelAndNbrs => {
        val id = entityLabelAndNbrs._1
        val entityData = entityLabelAndNbrs._2
        val entityLabel = entityData._1
        val entityNbrs = entityData._2
        println("Entity with Nbrs", entityLabel, entityNbrs.size)
        assert(entityNbrs.size !=0)
        val entityFilteredandMappedEdges: Set[(VertexId, String, NbrEdge)] = entityNbrs.filter(entityEdge => 
          entityEdge.edgeAttr.endsWith(query.entity2)).map(edge => (id, entityLabel, edge))
        //println("After filtering, Entity with Nbrs", entityLabel, entityFilteredEdges.size)
        entityFilteredandMappedEdges
      })
      
      val groupedEdgesByEntity2: RDD[( String, Iterable[( VertexId, String, NbrEdge)] )] = 
        allValidCandEdges.groupBy(edge => edge._3.dstAttr)
      
      groupedEdgesByEntity2.foreach(edgesPerEntity => 
        println(edgesPerEntity._1 + "=>" + edgesPerEntity._2.foreach(edge => 
          print(edge._3.edgeAttr +"\t" +edge._2))))
      var result = ""
      result
   }
  
   def getValidEntitiesSortedbyPopularity(entity: String, g:Graph[String, String], 
       allGraphNbrs:VertexRDD[Set[NbrEdge]], endWithFilter: Boolean = false): 
       RDD[(VertexId, (String, Set[NbrEdge]) )] = {
   
     val candidateRDD = 
     if(endWithFilter) 
       g.vertices.mapValues(_.toLowerCase()).filter(v => v._2.endsWith(entity))
     else 
       g.vertices.mapValues(_.toLowerCase()).filter(v => v._2.contains(entity))
     
     val candidateIds = candidateRDD.collect.map(_._1)

     val candidateNbrRDD = allGraphNbrs.filter(v=> candidateIds.contains(v._1))
     val candidatesWithLabelAndNbrs =  candidateRDD.innerJoin(candidateNbrRDD)((id, label, nbrs) => (label, nbrs))
     
     val sortedCandidates = candidatesWithLabelAndNbrs.sortBy(vertexWithLabelNbrs => 
       vertexWithLabelNbrs._2._2.size, false)
     sortedCandidates
   }
   

  def main(args: Array[String]): Unit = {     
    val sparkConf = new SparkConf().setAppName("LASDemo").setMaster("local")
    val sc = new SparkContext(sparkConf)
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)
    println("starting from main")
    if(args.length <1 || args.length > 2) {      
      println("Usage <pathToGraph> <Optional:queryFile>")
      return
    }

    val graphFile = args(0)
    var queryFile = "NONE"
    if(args.length == 1 ) {      
      println("Optional:queryFile not specified, will get queries from command line" )
    }else {
      queryFile = args(1)
    }
    
    val g: Graph[String, String] = Gen_Utils.time(
        ReadHugeGraph.getGraphTimeStampedLAS(graphFile, sc), "in Readgraph")
    //println("geting aliases")
    //val verticesWithAlias = MatchStringCandidates.constructVertexRDDWithAlias(g)
    val allGraphNbrs: VertexRDD[Set[NbrEdge]] = 
      Gen_Utils.time(NodeProp.getOneHopNbrEdges(g, Set(KGraphProp.edgeLabelNodeType)), "collecting one hop nbrs ")
    
    val graphWithTypeData = getTypedGraph(g) 
    println(" printing a sample of graph vertices after type and label are joined")
    graphWithTypeData.vertices.take(10).foreach(idLabel => println(idLabel._2))
    
    if(queryFile != "NONE"){
      val queriesFromFile: Array[Query] = getQueryFromFile(queryFile)
      queriesFromFile.foreach(q=> executeQuery(q, graphWithTypeData, allGraphNbrs))
    }
    
    var userInput = ""
    while(userInput != "exit"){

      val userInput: String = readLine("Enter Query as QueryType_EntityName_EntityName(Optional)_ContextFilters(Optional) or exit\n")
      if(userInput.toLowerCase() == "exit") 
        exit
      println("Parsing query" , userInput)
      if(userInput.split("_").length >=2){
        val query = parseQuery(userInput)
        query.qprint
        val result: String = executeQuery(query, graphWithTypeData, allGraphNbrs)
        println("Query Result : \n" + result)
        val filterString = query.contextFilters.fold("")((v1, v2) => v1+"_"+v2)
        //val filename = graphFile + "_" + query.qtype + "_" + query.entity1 + 
        //"_" + query.entity2 + "_" + filterString
        //Gen_Utils.writeToFile(filename, result)
      } else {
        println("Invalid format , enter again!")
      }
    }
    sc.stop() 
  }
  
  def getTypedGraph(g: Graph[String, String]): Graph[String, String] = {
    val verticesWithType = NodeProp.getNodeType(g)
    g.joinVertices(verticesWithType)((id, label, typeInfoString) => {
      //println(" LABEL =", label, " Type Data =", typeInfoString)
      label + "__" + typeInfoString
    })
  }
}
