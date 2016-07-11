package gov.pnnl.aristotle.algorithms.mining

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.solr.client.solrj.impl.HttpSolrServer
import org.apache.spark.graphx.Graph
import org.apache.spark.graphx.{VertexRDD,VertexId}
import scalaz.Scalaz._
import java.io.PrintWriter
import java.io.File
import gov.pnnl.aristotle.aiminterface.NousProfileAnswerStreamRecord
import collection.JavaConversions._
import gov.pnnl.aristotle.algorithms.mining.datamodel.KGEdge
import gov.pnnl.aristotle.algorithms.ReadHugeGraph
import org.apache.spark.graphx.Graph.graphToGraphOps
import org.apache.spark.rdd.RDD

object GraphProfiling {
    //val TYPE= "IS-A";
   // val TYPE= "rdf:type";
	//val TYPE = "http://www.w3.org/1999/02/22-rdf-syntax-ns#type"
  def main(args: Array[String]): Unit = {

    val t00 = System.currentTimeMillis();
    val writerSG = new PrintWriter(new File("typeMapSorted.txt"))
    val sparkConf = new SparkConf().setAppName("Load Huge Graph Main").
    	setMaster("local").set("spark.driver.maxResultSize", "10G")
    val sc = new SparkContext(sparkConf)
    val urlString = "http://localhost:8983/solr/aristotle0";
    val solr = new HttpSolrServer(urlString);

    val t0 = System.currentTimeMillis();
    println("Reading graph[Int, Int] Start")
    val graph: Graph[String, String] = ReadHugeGraph.getGraph(args(0), sc)
    println("done" + args(0));
//    val allTypesInGraphMap = getAugmentedTypeMap(graph, writerSG) 
//    val sortedtypemap = getSortedTypeMap(allTypesInGraphMap._1)
//    //sortedtypemap.foreach(a => println(a._1 + "   " +a._2))
//    var LOBProfile :NousProfileAnswerStreamRecord = 
//      GraphProfiling.getNodeTypeProfileObject(
//	    allTypesInGraphMap._1, "wikicategory_car_manufacturers",
//	    "OutboundPredicateObjType")
//	    var pp = prettyProfile(LOBProfile.getProfile())
//	 LOBProfile  = 
//      GraphProfiling.getPredicateTypeProfileObject(
//	    allTypesInGraphMap._2, "createdby","OutboundPredicate")
//	 pp = prettyProfile(LOBProfile.getProfile())
//	
//	    pp.foreach(p => println(p))  
//	  val augGraph = getAugmentedGraph(graph, writerSG)
//	  val a = GraphProfiling.getNodeProfile(augGraph, "toyota")
//	  println("NODE PROFILE IS : \n" +a)
//    val tp = getFeatureVectorOfTypeMap(typemap)
//    println("*****Printing feature vector")
//    tp.foreach(f => 
//      {
//        println(f._1 +" :")
//        f._2.foreach(t => println("\t\t\t\t:"+t._1 + " " +t._2))
//        
//      }
//    )
    println("done")
    //getNodeTypeProfileObject(typemap, "wikicategory_Bengali_people")
  }

  
def getFeatureVectorOfTypeMap(typemap: Map[String,
  List[(VertexId,(String, Map[String, Map[String, Int]]))]])
: Map[String, Map[String, Int]] =
  {
      val obot = getFeatureVectorOfTypeMapByKey("OutboundObjType", typemap);
      val ibot = getFeatureVectorOfTypeMapByKey("InboundObjType", typemap);
      val obp = getFeatureVectorOfTypeMapByKey("OutboundPredicate", typemap);
      val ibp = getFeatureVectorOfTypeMapByKey("InboundPredicate", typemap);
      return obot |+| ibot |+| obp |+| ibp
  }


def getFeatureVectorOfTypeMapByObjType(typemap: Map[String,
  List[(VertexId,(String, Map[String, Map[String, Int]]))]])
: Map[String, Map[String, Int]] =
  {
    
    return getFeatureVectorOfTypeMapByKey("OutboundObjType",typemap)  |+| 
    		getFeatureVectorOfTypeMapByKey("InboundObjType",typemap)
    
  }

  def getFeatureVectorOfTypeMapByPredicateLabel(typemap: 
      Map[String, List[(VertexId, (String, Map[String, Map[String, Int]]))]])
  		: Map[String, Map[String, Int]] =
    {
      return getFeatureVectorOfTypeMapByKey("OutboundPredicate", typemap) |+|
        getFeatureVectorOfTypeMapByKey("InboundPredicate", typemap)

    }

/**
 * output :
 * 
(wikicategory_Bengali_people,Map(wikicategory_Bengali_people -> 1, wikicategory_Sicilian_Wars -> 1, unknown -> 0))

Non Functional Version:
  val a = typemap.map(tm => {
  val featureVectorOneCat =
    tm._2.map(vertex => 
    {
      val featureVectorOneVertex = vertex._2._2.getOrElse("OutboundObjType", Map("unknown"->0)).map(predicatetype => 
      Map(predicatetype._1 -> predicatetype._2)   // want this (1, predicatetype._2))
      ).reduce((a,b) => { a |+| b})  //no need of reduce as every predicate type is already mapped. 
      Map(tm._1 -> featureVectorOneVertex)
    } 
    ).reduce((a,b) => {a |+| b} )
    
  featureVectorOneCat
  }
  ).reduce((a,b) => {a |+| b} )
  
  return a
 */
def getFeatureVectorOfTypeMapByKey(key : String,typemap: Map[String,
  List[(VertexId,(String, Map[String, Map[String, Int]]))]])
: Map[String, Map[String, Int]] =
{
  return typemap.map(tm => tm._2.map(vertex => 
    Map(tm._1 -> vertex._2._2.getOrElse(key, Map("unknown"->0)).
  		map(predicatetype => 
  			Map(predicatetype._1 -> predicatetype._2)
  			// want this (1, predicatetype._2))
  		).reduce((a,b) => { a |+| b}) 
  		//no need of reduce as every predicate type is already mapped. 
    )
    ).reduce((a,b) => {a |+| b} )
  ).reduce((a,b) => {a |+| b} )

}
  
def getSortedTypeMap(typemap: Map[String, 
  List[(VertexId,(String, Map[String, Map[String, Int]]))]])
: Seq[(String, List[(VertexId,(String, Map[String, Map[String, Int]]))])] =
{
return typemap.toSeq.sortWith((a,b)=> a._2.length > b._2.length); 
}
  
  def getNodeTypeProfile(typemap : Map[String, List[(VertexId,(String, Map[String, Map[String, Int]]))]], inputType : String) : List[(VertexId,(String, Map[String, Map[String, Int]]))] = {
    return typemap.getOrElse(inputType, List())
  }

  
def showNodeTypeProfile(typemap  : RDD[(String,List[String])], inputType : String) : Unit = {
    
  val entity_entry = typemap.filter(entry=> entry._1.equals(inputType)).collect
  
  entity_entry.foreach(entry=>{
      val listOfEntities = entry._2
      if (listOfEntities.length > 0) {
        println("\n\n**** Number of entities of type '" + inputType + "' :" + listOfEntities.size)
        listOfEntities.foreach(e =>
          {
            print(e + " | ")
          })
      }
    })
  		

  }
 
//TODO : better design this and getNodeTypeProfile.....method
def getPredicateTypeProfileObject(typemap : Map[String, List[(VertexId,(String,
     Map[String, Map[String, Int]]))]], queryString : String,
     queryCategory :String)  : 
     NousProfileAnswerStreamRecord = {
    var answer = new NousProfileAnswerStreamRecord();
    answer.setSource(queryString);
    var profile: java.util.List[java.lang.String] = List()
    	
    	val listOfEntities = typemap.getOrElse(queryString, List())
    if (listOfEntities.length > 0) {
      var profileEntry = "";
      var allNodeTypesMap : Map[String,Int]= Map();
      var alreadyListedOutboundObjType : Map[String,Int] = Map()
      listOfEntities.foreach(e =>
        {
          profileEntry = "";
          val vertexMap = e._2;
          val allTypes = vertexMap._2.getOrElse("nodeType", Map())
          var cval = 0;
          if (allTypes.size > 0) {
            allTypes.foreach(t =>
              allNodeTypesMap = allNodeTypesMap + 
              (t._1.toString() -> 
              	(allNodeTypesMap.getOrElse(t._1.toString(),0) + 1))
              )
          }
          //remove reference to itself
          allNodeTypesMap -= queryString

          val allOutboundNeighbourType = vertexMap._2.getOrElse(queryCategory, Map())
          if (allOutboundNeighbourType.size > 0) {
            //print("This entity has follwoing types of Outbound Neighbours type and their count: ")
            //t => profile = profile :+  inputType +"\t" + t._1
            allOutboundNeighbourType.foreach(t => 
              alreadyListedOutboundObjType = alreadyListedOutboundObjType +
                (t._1.toString() -> 
                (alreadyListedOutboundObjType.getOrElse(t._1.toString(), 0) +1)))
          }
          //profile = profile :+ profileEntry;
        })
        
        //Sort outBoundNbrType
        val ntLimit = 10
        var cnt = 0;
      
        val sortedOBNT = alreadyListedOutboundObjType.toSeq.sortWith((a,b) => 
          a._2 > b._2)
        var size = sortedOBNT.length
        if(size > ntLimit) size = ntLimit
        val obntItr = sortedOBNT.iterator
        while(obntItr.hasNext && (cnt < ntLimit))
        {
          val t = obntItr.next;
          profile = profile :+  queryString +"\t" +"hasNeighbourPredicate"+ "\t"+ t._1
          cnt = cnt +1
        }
        
        //Sort related nodeTypes
        val sortedANTM = allNodeTypesMap.toSeq.sortWith((a,b) => a._2 > b._2)
        size = sortedANTM.length
        
        if(size > ntLimit) size = ntLimit
        val si = sortedANTM.iterator
        cnt = 0;
        while(si.hasNext && (cnt < ntLimit))
        {
          val n = si.next;
      	  profile :+ queryString +"\t" +"relatedToEntityType" +"\t"+n._1 
      	  cnt = cnt +1		
        }
        sortedANTM.foreach(n => profile = profile :+ queryString +"\t" +"relatedTo" +"\t"+n._1)
    }
    answer.setProfile(profile)
    return answer;
  }


def getNodeTypeProfileObject(typemap : Map[String, List[(VertexId,(String,
     Map[String, Map[String, Int]]))]], queryString : String,
     queryCategory :String)  : 
     NousProfileAnswerStreamRecord = {
    var answer = new NousProfileAnswerStreamRecord();
    answer.setSource(queryString);
    var profile: java.util.List[java.lang.String] = List()
    	
    	val listOfEntities = typemap.getOrElse(queryString, List())
    if (listOfEntities.length > 0) {
      var profileEntry = "";
      var allNodeTypesMap : Map[String,Int]= Map();
      var alreadyListedOutboundObjType : Map[String,Int] = Map()
      listOfEntities.foreach(e =>
        {
          profileEntry = "";
          val vertexMap = e._2;
          val allTypes = vertexMap._2.getOrElse("nodeType", Map())
          var cval = 0;
          if (allTypes.size > 0) {
            allTypes.foreach(t =>
              allNodeTypesMap = allNodeTypesMap + 
              (t._1.toString() -> 
              	(allNodeTypesMap.getOrElse(t._1.toString(),0) + 1))
              )
          }
          //remove reference to itself
          allNodeTypesMap -= queryString

          val allOutboundNeighbourType = vertexMap._2.getOrElse(queryCategory, Map())
          if (allOutboundNeighbourType.size > 0) {
            //print("This entity has follwoing types of Outbound Neighbours type and their count: ")
            //t => profile = profile :+  inputType +"\t" + t._1
            allOutboundNeighbourType.foreach(t => 
              alreadyListedOutboundObjType = alreadyListedOutboundObjType +
                (t._1.toString() -> 
                (alreadyListedOutboundObjType.getOrElse(t._1.toString(), 0) +1)))
          }
          //profile = profile :+ profileEntry;
        })
        
        //Sort outBoundNbrType
        val ntLimit = 10
        var cnt = 0;
      
        val sortedOBNT = alreadyListedOutboundObjType.toSeq.sortWith((a,b) => 
          a._2 > b._2)
        var size = sortedOBNT.length
        if(size > ntLimit) size = ntLimit
        val obntItr = sortedOBNT.iterator
        while(obntItr.hasNext && (cnt < ntLimit))
        {
          val t = obntItr.next;
          profile = profile :+  queryString +"\t" + t._1
          cnt = cnt +1
        }
        
        //Sort related nodeTypes
        val sortedANTM = allNodeTypesMap.toSeq.sortWith((a,b) => a._2 > b._2)
        size = sortedANTM.length
        
        if(size > ntLimit) size = ntLimit
        val si = sortedANTM.iterator
        cnt = 0;
        while(si.hasNext && (cnt < ntLimit))
        {
          val n = si.next;
      	  profile :+ queryString +"\t" +"relatedTo" +"\t"+n._1 
      	  cnt = cnt +1		
        }
        sortedANTM.foreach(n => profile = profile :+ queryString +"\t" +"relatedTo" +"\t"+n._1)
    }
    answer.setProfile(profile)
    return answer;
  }
 def prettyProfile(profileTriples : java.util.List[java.lang.String]) : 
  java.util.List[java.lang.String]=
{
  var tmp :  java.util.List[java.lang.String]= List()
  profileTriples.foreach(pt =>
  tmp = tmp :+ pt.replaceAll("wikicategory_", "").replaceAll("wordnet_", "")  
  )
  
  return tmp
}
def showNodeProfile(graph :Graph[(String, Map[String, Map[String, Int]]), 
   String],inputType : String) : Unit = {
   
   val node = graph.vertices.filter(v => (v._2._1 == inputType))
   node.foreach(n=> println(n))
      
 }
 
def getNodeProfile(graph :Graph[(String, Map[String, Map[String, Int]]),
    String],inputType : String) : NousProfileAnswerStreamRecord = {
   
    val answer = new NousProfileAnswerStreamRecord();
    answer.setSource(inputType)
    //java.util.List<java.lang.String>
    var result : java.util.List[java.lang.String] = List()
    //var result1 = 
    /*
     *  var size = sortedANTM.length
        if(size > 5) size = 5
        val si = sortedANTM.iterator
        var cnt = 0;
        while(si.hasNext)
        {
          
          if(cnt<size)
       
     */
    
    val node = graph.vertices.filter(v => (v._2._1 == inputType))
    println("returning a nodeprofile ");
    node.collect.foreach(n=> 
      {
    	    val allOBObjType = n._2._2.getOrElse("OutboundPredicateObjType", Map())
    	    val allOBObjTypeSorted = allOBObjType.toSeq.sortWith((a,b)=> a._2 >= b._2)
        //TODO : need a summary of all objtype for each predicate.
    	    
//        val contextEntities = List("steel","shoe","cloth")
//        val allOBInstance = n._2._2.getOrElse("OutboundPredicateInstance", Map())
//    	    val allkeys = allOBObjTypeSorted.keys
    	    var size = allOBObjTypeSorted.length
        val nodelimit = 15
    	    if(size > nodelimit) size = nodelimit
        val si = allOBObjTypeSorted.iterator
        var cnt = 0;
    	    while(si.hasNext && (cnt < nodelimit))
        {
            	val k = si.next;
        	  	result = result :+ inputType.toString() + "\t"+k._1.toString()
            cnt = cnt +1
            
        }   
//    	    allOBObjTypeSorted.foreach(k =>
//    	    //if key contain any context entities
//    	      result = result :+ inputType.toString() + "\t"+k._1.toString()  
//    	    
//    	    )
      }
      )
    //val newArray = new java.util.ArrayList[String]
    //newArray.add(0,"test string")
    
      answer.setProfile(result)
    return answer
      
 }
  
// def getTypedVertexRDD(graph : Graph[String, String], writerSG : PrintWriter)
// :VertexRDD[Map[String, Map[String, Int]]] =
// {
//    return graph.aggregateMessages[Map[String, Map[String, Int]]](
//          edge => 
//          {
//            if (edge.attr.equalsIgnoreCase(TYPE))
//            {
//              edge.sendToSrc(Map("nodeType" -> Map(edge.dstAttr -> 1)))
//            }
//          },
//          (a, b) => { a |+| b })
//   
// }

 def getTypedAugmentedGraph_Temporal(graph: Graph[String, KGEdge], writerSG: PrintWriter,
     typedVertexRDD : VertexRDD[Map[String, Map[String, Int]]])
 :Graph[(String, Map[String, Map[String, Int]]), KGEdge] =
 {
	  return graph.outerJoinVertices(typedVertexRDD) {
      case (id, label, Some(nbr)) => (label, nbr)
      case (id, label, None) => (label, Map())
      //TODO: WHY 	WILL NONE COMES INTO PICTURE ? EVERY NBR IS FROM ORIGINAL LIST 
    }

 }

 def getTypedAugmentedGraph(graph: Graph[String, String], writerSG: PrintWriter,
     typedVertexRDD : VertexRDD[Map[String, Map[String, Int]]])
 :Graph[(String, Map[String, Map[String, Int]]), String] =
 {
	  return graph.outerJoinVertices(typedVertexRDD) {
      case (id, label, Some(nbr)) => (label, nbr)
      case (id, label, None) => (label, Map())
      //TODO: WHY 	WILL NONE COMES INTO PICTURE ? EVERY NBR IS FROM ORIGINAL LIST 
    }
}
 
 
 /**
 *  Returns a RDD where every vertex stores Map[String, Map[String, Int]].
 *  In this case, it is actually a single key value pair map.  The key is "nodeType".
 *  The value is a pair representing (type of the node, and the count of how many times the type/is-a relation occurred.
 */ 
def getTypedVertexRDD_Temporal(graph : Graph[String, KGEdge], writerSG : PrintWriter,degreeLimit:Int,
    type_predicate:String)
 :VertexRDD[Map[String, Map[String, Int]]] =
 {

      var degrees: VertexRDD[Int] = graph.degrees
      //degrees.collect.foreach(f=>writerSG.println("degree" + f.toString))
      println("finding tpye")
      println("type support is" + degreeLimit)
      var degreeGraph: Graph[(String, Map[String, Int]), KGEdge] = graph.outerJoinVertices(degrees) { (id, oldAttr, outDegOpt) =>
        outDegOpt match {
          case Some(deg) => {
            if(oldAttr.equals("pentagon"))
              println("pentagon value is" + id + " and " + outDegOpt.toString())
            if (deg >= degreeLimit)
              (oldAttr, Map("degree" -> deg))
            else
              (oldAttr, Map())
          }
          case None => (oldAttr, Map()) // No outDegree means zero outDegree
        }
      }

      return degreeGraph.aggregateMessages[Map[String, Map[String, Int]]](
        edge =>
          {
            if (edge.srcAttr._2.contains("degree"))
              edge.sendToSrc(Map("nodeType" -> Map(edge.srcAttr._1 -> 1)))
            if (edge.dstAttr._2.contains("degree"))
              edge.sendToDst(Map("nodeType" -> Map(edge.dstAttr._1 -> 1)))
            if (edge.attr.getlabel.equalsIgnoreCase(type_predicate)) {
              edge.sendToSrc(Map("nodeType" -> Map("type:"+edge.dstAttr._1 -> 1)))
            }
          },
        (a, b) => { a |+| b })

    }
 
/**
 *  Returns a RDD where every vertex stores Map[String, Map[String, Int]].
 *  In this case, it is actually a single key value pair map.  The key is "nodeType".
 *  The value is a pair representing (type of the node, and the count of how many times the type/is-a relation occurred.
 */ 
def getTypedVertexRDD(graph : Graph[String, String], writerSG : PrintWriter,degreeLimit:Int,
    type_predicate:String)
 :VertexRDD[Map[String, Map[String, Int]]] =
 {

      printf("type predicate is" + type_predicate)

      var degrees: VertexRDD[Int] = graph.degrees
      var degreeGraph: Graph[(String, Map[String, Int]), String] = graph.outerJoinVertices(degrees) { (id, oldAttr, outDegOpt) =>
        outDegOpt match {
          case Some(deg) => {
            if (deg > degreeLimit)
              (oldAttr, Map("degree" -> deg))
            else
              (oldAttr, Map())
          }
          case None => (oldAttr, Map()) // No outDegree means zero outDegree
        }
      }
      return degreeGraph.aggregateMessages[Map[String, Map[String, Int]]](
        edge =>
          {
            if (edge.srcAttr._2.contains("degree"))
              edge.sendToSrc(Map("nodeType" -> Map(edge.srcAttr._1 -> 1)))
            if (edge.attr.equalsIgnoreCase(type_predicate)) {
              edge.sendToSrc(Map("nodeType" -> Map(edge.dstAttr._1 -> 1)))
            }
          },
        (a, b) => { a |+| b })

    }
 
 
 def getAugmentedGraphFromAllRDD(typedAugmentedGraph : Graph[(String, Map[String, Map[String, Int]]), String]
, nonTypedVertexRDD:VertexRDD[Map[String, Map[String, Int]]] )
 : Graph[(String, Map[String, Map[String, Int]]), String] =
 {
       // Set up a new graph , with label, inbound/outbound predicate and outboundObject type  as node property 
    // TODO: try to merge the operations.
   return typedAugmentedGraph.outerJoinVertices(nonTypedVertexRDD) {
      case (id, (label, somethint), Some(nbr)) => (label, somethint |+| nbr)
      case (id, (label, somethint), None) => (label, somethint |+| Map())
    }
 }
 

}
