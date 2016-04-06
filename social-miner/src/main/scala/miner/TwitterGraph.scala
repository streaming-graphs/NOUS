package miner
import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD
import org.apache.spark.rdd._
import org.apache.spark.graphx._

object TwitterGraph {  
  
def isValidLine(ln : String) : Boolean ={
    ( (ln.startsWith("@") ==false) && (ln.startsWith("#")==false) && (ln.isEmpty()==false) && ln.startsWith(")") == false)
  }
  
  def isValidTweet(ln: String): Boolean = {
    var cleaned_line = ln
    if(ln.startsWith("CompactBuffer(")) {
          cleaned_line = cleaned_line.substring(14, cleaned_line.length())
    } else if(ln.startsWith(", ")){
          cleaned_line = cleaned_line.substring(2, cleaned_line.length())
    }
    val fields = cleaned_line.split(',')
    if(fields.length != 3) return false
    val hashtag = fields(2).dropWhile(_ == '#')
    val actualtag = hashtag.dropWhile(_ == '?')
    return(actualtag.length > 0)
  }

    def getLabelledEdge_FromTriple(fields : Array[String]) : Edge[String] = {
    try{return Edge(fields(0).hashCode().toLong, 
            fields(2).hashCode().toLong, fields(1))
    } catch {
      case ex: java.lang.ArrayIndexOutOfBoundsException => {
                println(fields.toSeq.toList.toString)

            println("IO Exception" )
            return Edge("None".hashCode().toLong,
                "None".hashCode().toLong,
                "None")
         }
}        
  }
  
  def getVertex_FromString(field : String) : (VertexId, String) = {
    try{ return (field.hashCode().toLong, field)
      
    }catch { 
      case ex: java.lang.ArrayIndexOutOfBoundsException => {
            println("Making Node :Array index Exception ")
            return ("None".hashCode().toLong,
                "None")
         }
    }  
  }
  
  def getGraph(filename : String, sc : SparkContext): Graph[String, String] = {
    println("starting map phase1");
    
    val allFields: RDD[Array[String]] = sc.textFile(filename).filter(line => isValidLine(line) && isValidTweet(line)).map { line => 
        var cleaned_line = line
        if(line.startsWith("CompactBuffer(")) {
          cleaned_line = cleaned_line.substring(14, cleaned_line.length())
        } else if(line.startsWith(", ")){
          cleaned_line = cleaned_line.substring(2, cleaned_line.length())
        }
        val fields = cleaned_line.split(',')
        (fields)
    }
      
    val edges: RDD[Edge[String]] = allFields.map(fields => getLabelledEdge_FromTriple(fields))
    val vertexRDD1 = allFields.map(fields => getVertex_FromString(fields(0)))
    val vertexRDD2 = allFields.map(fields => getVertex_FromString(fields(2)))   
    //vertexRDD2.cache
    println("starting map phase4 > doing union");
    val allvertex = vertexRDD1.union(vertexRDD2)

    println("starting map phase5 > Building graph");
    val graph = Graph(allvertex, edges);
    println("edge count " + graph.edges.count)
    println("vertices count" + graph.vertices.count)
    return graph
  }
 
  def dumpGraphObj(filename:String, graph: Graph[String,String]) :Unit = {
    val vertexFile: String = filename+".vertices" 
    val  edgeFile:String = filename+".edges"
    println("saving graph as text file")
    graph.edges.saveAsObjectFile(edgeFile)
    graph.vertices.saveAsObjectFile(vertexFile)
  }
 
  def loadGraphObj(vertexFile: String, edgeFile:String, sc : SparkContext): Graph[String,String] = {
    val vertices: RDD[(VertexId,String)] = sc.objectFile(vertexFile, 8)
    val edges: RDD[Edge[String]] = sc.objectFile(edgeFile, 8)
    println("starting phase > Building graph")
    val graph : Graph[String,String] = Graph(vertices, edges)
 
    println("edge count " + graph.edges.count)
    println("vertices count" + graph.vertices.count)
    return graph
  }
  
  def getGraph(filename : String, sc : SparkContext, objectFormat : Boolean): Graph[String, String] = {
    if(!objectFormat)
      return(getGraph(filename, sc))
    else
      return loadGraphObj(filename+".vertices" , filename+".edges", sc)       
  }
  
  def getNbrLabels(g: Graph[String, String], relationLabel: List[String], isOutgoing: Boolean): VertexRDD[String] = {
  /* collects labels of 1 hop neighbors, filtered based on edge types
   *  and incoming or outgoing edges as specified in "isOutgoing" flag 
   */ 
    var nbrlist : VertexRDD[String] = null
    if(isOutgoing) {
      nbrlist = g.aggregateMessages[String](
        edge => { if(relationLabel.contains(edge.attr)) {edge.sendToSrc(edge.dstAttr)}},
        (a, b) => a + ", " + b
        )
    } else {
      nbrlist = g.aggregateMessages[String](
        edge => { if(relationLabel.contains(edge.attr)) {edge.sendToDst(edge.srcAttr)}},
        (a, b) => a + ", " + b
        ) 
    } 
    
    return nbrlist;
  }
   
  def TriplesToUserMap(filename: String, outfile: String, objectFormat: Boolean, sc:SparkContext) ={
    
    val graph : Graph[String, String] = TwitterGraph.getGraph(filename, sc, objectFormat)
	  val interestingTweetProp :List[String] = List("hasKey", "hasTag")
	  //val interestingTweetProp :Array[String] = Array("hasKey", "hasTag", "hasSenti", "hasLang")
	  val tweetProperties: VertexRDD[String]=  TwitterGraph.getNbrLabels(graph, interestingTweetProp, true)
	  println("No of tweets="   + tweetProperties.count)
	  // The tweet nodes only keep the incoming nbr labels
	  // Rest of the nodes keep their original label
	  
	  val graphWithTweetProp = graph.outerJoinVertices(tweetProperties){
	    case(id, label, Some(keywords)) => keywords.toString
	    case(id, label, None) => label
	  }
	  
	  println("joining complete for all graph", graphWithTweetProp.vertices.count)
	  
	  // Get only the user nodes with their hashtag+keyword list
	  val userToTweet: List[String] = List("hasTweet")
	  val usersWords: VertexRDD[String]  = TwitterGraph.getNbrLabels(graphWithTweetProp, userToTweet, true)
	  println("Done collecting User RDD with word list " + usersWords.count)

	  // Merge user names + bag of words they ever tweeted
	  val userLabelsWithWords: VertexRDD[String] = graphWithTweetProp.vertices.innerZipJoin(usersWords)(
	      (id, label, keywords)=> "<" + label + ">," + keywords)
	  println("Saving, (User names-> words list) ", userLabelsWithWords.count)

	  if(objectFormat)
	    userLabelsWithWords.saveAsObjectFile(outfile)
	  else
	    userLabelsWithWords.saveAsTextFile(outfile)
  }
 
}